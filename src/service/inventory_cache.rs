use std::sync::LazyLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use redis::Script;
use redis::aio::ConnectionManager;
use serde::{Deserialize, Serialize};
use sqlx::MySqlPool;

use crate::dto::inventory::{InventoryListResponse, InventoryResponse};
use crate::error::{BizError, ERR_INTERNAL_SERVER, ERR_INVENTORY_NOT_FOUND};
use crate::models::inventory::Inventory;
use crate::repository;

const INVENTORY_INFO_PREFIX: &str = "inventory:info:";
const INVENTORY_PRODUCT_PREFIX: &str = "inventory:product:";
const INVENTORY_CACHE_ZSET: &str = "inventory:zset";
const LOCAL_CACHE_TTL: Duration = Duration::from_secs(60);
const LOCAL_LIST_CACHE_TTL: Duration = Duration::from_secs(30);

// ====== Lua Script ======

static ZRANGE_MGET_SCRIPT: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r#"
local ids
if ARGV[3] == "desc" then
    ids = redis.call("ZREVRANGE", KEYS[1], ARGV[1], ARGV[2])
else
    ids = redis.call("ZRANGE", KEYS[1], ARGV[1], ARGV[2])
end
local total = redis.call("ZCARD", KEYS[1])
if #ids == 0 then return {total, {}} end
local keys = {}
for i, id in ipairs(ids) do
    keys[i] = ARGV[4] .. id
end
local values = redis.call("MGET", unpack(keys))
return {total, values}
"#,
    )
});

// ====== Cached Item ======

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CachedInventoryItem {
    id: i64,
    product_id: i64,
    quantity: i64,
    status: String,
    reserved: i64,
    threshold: i64,
}

impl From<Inventory> for CachedInventoryItem {
    fn from(i: Inventory) -> Self {
        CachedInventoryItem {
            id: i.id,
            product_id: i.product_id,
            quantity: i.quantity,
            status: i.status,
            reserved: i.reserved,
            threshold: i.threshold,
        }
    }
}

impl From<&CachedInventoryItem> for InventoryResponse {
    fn from(c: &CachedInventoryItem) -> Self {
        InventoryResponse {
            id: c.id,
            product_id: c.product_id,
            quantity: c.quantity,
            status: c.status.clone(),
            reserved: c.reserved,
            threshold: c.threshold,
            created_at: None,
            updated_at: None,
        }
    }
}

// ====== Helper: wrap data bytes into full ApiResponse JSON ======

fn build_full_response(data_bytes: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(data_bytes.len() + 64);
    buf.extend_from_slice(b"{\"code\":0,\"message\":\"success\",\"data\":");
    buf.extend_from_slice(data_bytes);
    buf.push(b'}');
    buf
}

// ====== Cached Response Types ======

pub enum CachedItemResult {
    FullResponse(bytes::Bytes),
    Fresh(InventoryResponse),
}

pub enum CachedListResult {
    FullResponse(bytes::Bytes),
    Fresh(InventoryListResponse),
}

// ====== Local Cache ======

struct LocalCache {
    single_by_id: DashMap<i64, (Instant, bytes::Bytes)>,
    single_by_product: DashMap<i64, (Instant, bytes::Bytes)>,
    lists: DashMap<(u32, u32, String), (Instant, bytes::Bytes)>,
}

impl LocalCache {
    fn new() -> Self {
        LocalCache {
            single_by_id: DashMap::new(),
            single_by_product: DashMap::new(),
            lists: DashMap::new(),
        }
    }

    fn get_by_id(&self, id: i64) -> Option<bytes::Bytes> {
        if let Some(entry) = self.single_by_id.get(&id) {
            if entry.0.elapsed() < LOCAL_CACHE_TTL {
                return Some(entry.1.clone());
            }
        }
        None
    }

    fn set_by_id(&self, id: i64, bytes: bytes::Bytes) {
        self.single_by_id.insert(id, (Instant::now(), bytes));
    }

    fn get_by_product_id(&self, product_id: i64) -> Option<bytes::Bytes> {
        if let Some(entry) = self.single_by_product.get(&product_id) {
            if entry.0.elapsed() < LOCAL_CACHE_TTL {
                return Some(entry.1.clone());
            }
        }
        None
    }

    fn set_by_product_id(&self, product_id: i64, bytes: bytes::Bytes) {
        self.single_by_product
            .insert(product_id, (Instant::now(), bytes));
    }

    fn remove_any(&self, id: i64, product_id: i64) {
        self.single_by_id.remove(&id);
        self.single_by_product.remove(&product_id);
    }

    fn get_list(&self, page: u32, page_size: u32, status: &str) -> Option<bytes::Bytes> {
        let key = (page, page_size, status.to_string());
        if let Some(entry) = self.lists.get(&key) {
            if entry.0.elapsed() < LOCAL_LIST_CACHE_TTL {
                return Some(entry.1.clone());
            }
        }
        None
    }

    fn set_list(&self, page: u32, page_size: u32, status: String, bytes: bytes::Bytes) {
        self.lists
            .insert((page, page_size, status), (Instant::now(), bytes));
    }

    fn warmup_all(&self, items: Vec<CachedInventoryItem>) {
        for item in items {
            if let Ok(item_bytes) = serde_json::to_vec(&item) {
                let full = bytes::Bytes::from(build_full_response(&item_bytes));
                self.set_by_id(item.id, full.clone());
                self.set_by_product_id(item.product_id, full);
            }
        }
    }
}

// ====== Inner Cache ======

struct InnerCache {
    redis_conns: Vec<tokio::sync::Mutex<ConnectionManager>>,
    next_conn: AtomicUsize,
    local: LocalCache,
}

impl InnerCache {
    fn get_conn(&self) -> &tokio::sync::Mutex<ConnectionManager> {
        let idx = self
            .next_conn
            .fetch_add(1, Ordering::Relaxed)
            % self.redis_conns.len();
        &self.redis_conns[idx]
    }

    async fn get_by_id(&self, pool: &MySqlPool, id: i64) -> Result<CachedItemResult, BizError> {
        if let Some(full) = self.local.get_by_id(id) {
            return Ok(CachedItemResult::FullResponse(full));
        }

        let redis_key = format!("{}{}", INVENTORY_INFO_PREFIX, id);
        let mut conn = self.get_conn().lock().await;

        let result: redis::RedisResult<Option<String>> = redis::cmd("GET")
            .arg(&redis_key)
            .query_async(&mut *conn)
            .await;

        match result {
            Ok(Some(data)) => {
                if let Ok(item) = serde_json::from_str::<CachedInventoryItem>(&data) {
                    if let Ok(item_bytes) = serde_json::to_vec(&item) {
                        let full = bytes::Bytes::from(build_full_response(&item_bytes));
                        let product_full = full.clone();
                        self.local.set_by_id(id, full);
                        self.local.set_by_product_id(item.product_id, product_full);
                        return Ok(CachedItemResult::FullResponse(
                            bytes::Bytes::from(build_full_response(&item_bytes)),
                        ));
                    }
                }
            }
            _ => {}
        }

        let inventory = repository::inventory::find_by_id(pool, id)
            .await
            .map_err(|e| {
                log::error!("[inventory_cache] find_by_id error: {e}");
                ERR_INTERNAL_SERVER
            })?;

        match inventory {
            Some(i) => {
                let cached = CachedInventoryItem::from(i);
                let resp = InventoryResponse::from(&cached);

                if let Ok(item_bytes) = serde_json::to_vec(&cached) {
                    let full = bytes::Bytes::from(build_full_response(&item_bytes));
                    self.local.set_by_id(cached.id, full.clone());
                    self.local.set_by_product_id(cached.product_id, full);
                }

                if let Ok(json) = serde_json::to_string(&cached) {
                    let info_key = format!("{}{}", INVENTORY_INFO_PREFIX, cached.id);
                    let product_key = format!("{}{}", INVENTORY_PRODUCT_PREFIX, cached.product_id);
                    let _: redis::RedisResult<()> = redis::cmd("SET")
                        .arg(&info_key)
                        .arg(&json)
                        .query_async(&mut *conn)
                        .await;
                    let _: redis::RedisResult<()> = redis::cmd("SET")
                        .arg(&product_key)
                        .arg(&json)
                        .query_async(&mut *conn)
                        .await;
                }

                Ok(CachedItemResult::Fresh(resp))
            }
            None => Err(ERR_INVENTORY_NOT_FOUND),
        }
    }

    async fn get_by_product_id(
        &self,
        pool: &MySqlPool,
        product_id: i64,
    ) -> Result<CachedItemResult, BizError> {
        if let Some(full) = self.local.get_by_product_id(product_id) {
            return Ok(CachedItemResult::FullResponse(full));
        }

        let redis_key = format!("{}{}", INVENTORY_PRODUCT_PREFIX, product_id);
        let mut conn = self.get_conn().lock().await;

        let result: redis::RedisResult<Option<String>> = redis::cmd("GET")
            .arg(&redis_key)
            .query_async(&mut *conn)
            .await;

        match result {
            Ok(Some(data)) => {
                if let Ok(item) = serde_json::from_str::<CachedInventoryItem>(&data) {
                    if let Ok(item_bytes) = serde_json::to_vec(&item) {
                        let full = bytes::Bytes::from(build_full_response(&item_bytes));
                        let id_full = full.clone();
                        self.local.set_by_product_id(product_id, full);
                        self.local.set_by_id(item.id, id_full);
                        return Ok(CachedItemResult::FullResponse(
                            bytes::Bytes::from(build_full_response(&item_bytes)),
                        ));
                    }
                }
            }
            _ => {}
        }

        let inventory = repository::inventory::find_by_product_id(pool, product_id)
            .await
            .map_err(|e| {
                log::error!("[inventory_cache] find_by_product_id error: {e}");
                ERR_INTERNAL_SERVER
            })?;

        match inventory {
            Some(i) => {
                let cached = CachedInventoryItem::from(i);
                let resp = InventoryResponse::from(&cached);

                if let Ok(item_bytes) = serde_json::to_vec(&cached) {
                    let full = bytes::Bytes::from(build_full_response(&item_bytes));
                    self.local.set_by_id(cached.id, full.clone());
                    self.local.set_by_product_id(cached.product_id, full);
                }

                if let Ok(json) = serde_json::to_string(&cached) {
                    let info_key = format!("{}{}", INVENTORY_INFO_PREFIX, cached.id);
                    let product_key = format!("{}{}", INVENTORY_PRODUCT_PREFIX, cached.product_id);
                    let _: redis::RedisResult<()> = redis::cmd("SET")
                        .arg(&info_key)
                        .arg(&json)
                        .query_async(&mut *conn)
                        .await;
                    let _: redis::RedisResult<()> = redis::cmd("SET")
                        .arg(&product_key)
                        .arg(&json)
                        .query_async(&mut *conn)
                        .await;
                }

                Ok(CachedItemResult::Fresh(resp))
            }
            None => Err(ERR_INVENTORY_NOT_FOUND),
        }
    }

    async fn evict(&self, id: i64, product_id: i64) {
        self.local.remove_any(id, product_id);

        let mut conn = self.get_conn().lock().await;
        let info_key = format!("{}{}", INVENTORY_INFO_PREFIX, id);
        let product_key = format!("{}{}", INVENTORY_PRODUCT_PREFIX, product_id);

        let _: redis::RedisResult<()> = redis::cmd("DEL")
            .arg(&[info_key.as_str(), product_key.as_str()])
            .query_async(&mut *conn)
            .await;
        let _: redis::RedisResult<()> = redis::cmd("ZREM")
            .arg(INVENTORY_CACHE_ZSET)
            .arg(id)
            .query_async(&mut *conn)
            .await;
    }

    fn parse_zset_result(val: redis::Value) -> Option<(i64, Vec<String>)> {
        match val {
            redis::Value::Bulk(items) if items.len() == 2 => {
                let total = match &items[0] {
                    redis::Value::Int(n) => *n,
                    redis::Value::Data(bytes) => {
                        String::from_utf8_lossy(bytes).parse::<i64>().ok()?
                    }
                    _ => return None,
                };

                let values = match &items[1] {
                    redis::Value::Bulk(arr) => arr
                        .iter()
                        .filter_map(|v| match v {
                            redis::Value::Data(bytes) => {
                                Some(String::from_utf8_lossy(bytes).to_string())
                            }
                            redis::Value::Nil => None,
                            _ => None,
                        })
                        .collect(),
                    redis::Value::Nil => vec![],
                    _ => return None,
                };

                Some((total, values))
            }
            _ => None,
        }
    }

    async fn list(
        &self,
        pool: &MySqlPool,
        status: Option<&str>,
        page: u32,
        page_size: u32,
    ) -> Result<CachedListResult, BizError> {
        if status.is_some() {
            let offset = (page - 1) * page_size;
            let inventories = repository::inventory::find_list(pool, status, offset, page_size)
                .await
                .map_err(|e| {
                    log::error!("[inventory_cache] find_list error: {e}");
                    ERR_INTERNAL_SERVER
                })?;

            let total = repository::inventory::count_list(pool, status)
                .await
                .map_err(|e| {
                    log::error!("[inventory_cache] count_list error: {e}");
                    ERR_INTERNAL_SERVER
                })?;

            return Ok(CachedListResult::Fresh(InventoryListResponse {
                list: inventories.into_iter().map(InventoryResponse::from).collect(),
                total,
            }));
        }

        let status_key = String::new();
        if let Some(full) = self.local.get_list(page, page_size, &status_key) {
            return Ok(CachedListResult::FullResponse(full));
        }

        let offset = (page.saturating_sub(1)) as i64;
        let stop = offset + page_size as i64 - 1;

        let mut conn = self.get_conn().lock().await;

        let result: redis::RedisResult<redis::Value> = ZRANGE_MGET_SCRIPT
            .key(INVENTORY_CACHE_ZSET)
            .arg(offset)
            .arg(stop)
            .arg("desc")
            .arg(INVENTORY_INFO_PREFIX)
            .invoke_async(&mut *conn)
            .await;

        match result {
            Ok(val) => {
                if let Some((total, items)) = Self::parse_zset_result(val) {
                    if total > 0 {
                        let inventories: Vec<InventoryResponse> = items
                            .iter()
                            .filter_map(|json| serde_json::from_str::<CachedInventoryItem>(json).ok())
                            .map(|cached| InventoryResponse::from(&cached))
                            .collect();
                        if !inventories.is_empty() {
                            let resp = InventoryListResponse {
                                list: inventories,
                                total,
                            };
                            if let Ok(list_bytes) = serde_json::to_vec(&resp) {
                                let full = bytes::Bytes::from(build_full_response(&list_bytes));
                                self.local.set_list(page, page_size, status_key, full.clone());
                                return Ok(CachedListResult::FullResponse(full));
                            }
                        }
                    }
                }
            }
            Err(e) => {
                log::warn!("[inventory_cache] Redis ZSET list failed, falling back to DB: {e}");
            }
        }

        drop(conn);

        let db_offset = (page - 1) * page_size;
        let inventories = repository::inventory::find_list(pool, None, db_offset, page_size)
            .await
            .map_err(|e| {
                log::error!("[inventory_cache] find_list error: {e}");
                ERR_INTERNAL_SERVER
            })?;

        let total = repository::inventory::count_list(pool, None)
            .await
            .map_err(|e| {
                log::error!("[inventory_cache] count_list error: {e}");
                ERR_INTERNAL_SERVER
            })?;

        Ok(CachedListResult::Fresh(InventoryListResponse {
            list: inventories.into_iter().map(InventoryResponse::from).collect(),
            total,
        }))
    }

    async fn warmup(&self, pool: &MySqlPool) -> Result<i32, BizError> {
        let inventories = repository::inventory::find_list(pool, None, 0, u32::MAX)
            .await
            .map_err(|e| {
                log::error!("[inventory_cache] warmup find_list error: {e}");
                ERR_INTERNAL_SERVER
            })?;

        let count = inventories.len() as i32;
        let ids: Vec<i64> = inventories.iter().map(|i| i.id).collect();
        let items: Vec<CachedInventoryItem> =
            inventories.into_iter().map(CachedInventoryItem::from).collect();

        let mut conn = self.get_conn().lock().await;

        let _: redis::RedisResult<()> = redis::cmd("DEL")
            .arg(INVENTORY_CACHE_ZSET)
            .query_async(&mut *conn)
            .await;

        for item in &items {
            if let Ok(json) = serde_json::to_string(item) {
                let info_key = format!("{}{}", INVENTORY_INFO_PREFIX, item.id);
                let product_key = format!("{}{}", INVENTORY_PRODUCT_PREFIX, item.product_id);
                let _: redis::RedisResult<()> = redis::cmd("SET")
                    .arg(&info_key)
                    .arg(&json)
                    .query_async(&mut *conn)
                    .await;
                let _: redis::RedisResult<()> = redis::cmd("SET")
                    .arg(&product_key)
                    .arg(&json)
                    .query_async(&mut *conn)
                    .await;
                let _: redis::RedisResult<()> = redis::cmd("ZADD")
                    .arg(INVENTORY_CACHE_ZSET)
                    .arg(item.id)
                    .arg(item.id)
                    .query_async(&mut *conn)
                    .await;
            }
        }

        drop(conn);
        self.local.warmup_all(items);

        // pre-warm local list cache with first 3 pages
        let page_sizes = [10u32, 20u32, 50u32];
        for page_size in page_sizes {
            let total = ids.len() as i64;
            let mut page = 1u32;
            loop {
                let offset = ((page.saturating_sub(1)) as i64).min(ids.len() as i64 - 1);
                let stop = (offset + page_size as i64 - 1).min(ids.len() as i64 - 1);
                if offset > stop {
                    break;
                }
                let page_ids: Vec<i64> = ids.iter()
                    .rev()
                    .skip(offset as usize)
                    .take((stop - offset + 1) as usize)
                    .copied()
                    .collect();
                let list: Vec<InventoryResponse> = page_ids.iter()
                    .filter_map(|id| self.local.get_by_id(*id))
                    .filter_map(|full| {
                        let bytes = &full[..];
                        let data_prefix = b"\"data\":";
                        if let Some(pos) = bytes.windows(data_prefix.len()).position(|w| w == data_prefix) {
                            let data_start = pos + data_prefix.len();
                            serde_json::from_slice::<CachedInventoryItem>(&bytes[data_start..]).ok()
                        } else {
                            None
                        }
                    })
                    .map(|cached| InventoryResponse::from(&cached))
                    .collect();
                if list.is_empty() {
                    break;
                }
                let resp = InventoryListResponse { list, total };
                if let Ok(list_bytes) = serde_json::to_vec(&resp) {
                    let full = bytes::Bytes::from(build_full_response(&list_bytes));
                    self.local.set_list(page, page_size, String::new(), full);
                }
                page += 1;
                if offset as usize + page_size as usize >= ids.len() {
                    break;
                }
            }
        }

        Ok(count)
    }
}

// ====== Public API ======

pub struct InventoryCache {
    inner: Option<InnerCache>,
}

impl InventoryCache {
    pub fn new(redis_conns: Option<Vec<ConnectionManager>>) -> Self {
        let inner = redis_conns.map(|conns| InnerCache {
            redis_conns: conns.into_iter().map(tokio::sync::Mutex::new).collect(),
            next_conn: AtomicUsize::new(0),
            local: LocalCache::new(),
        });
        InventoryCache { inner }
    }

    pub async fn get_by_id(&self, pool: &MySqlPool, id: i64) -> Result<CachedItemResult, BizError> {
        match &self.inner {
            Some(cache) => cache.get_by_id(pool, id).await,
            None => {
                let inventory = repository::inventory::find_by_id(pool, id)
                    .await
                    .map_err(|e| {
                        log::error!("[inventory_cache] find_by_id error: {e}");
                        ERR_INTERNAL_SERVER
                    })?;
                inventory
                    .map(InventoryResponse::from)
                    .map(CachedItemResult::Fresh)
                    .ok_or(ERR_INVENTORY_NOT_FOUND)
            }
        }
    }

    pub async fn get_by_product_id(
        &self,
        pool: &MySqlPool,
        product_id: i64,
    ) -> Result<CachedItemResult, BizError> {
        match &self.inner {
            Some(cache) => cache.get_by_product_id(pool, product_id).await,
            None => {
                let inventory = repository::inventory::find_by_product_id(pool, product_id)
                    .await
                    .map_err(|e| {
                        log::error!("[inventory_cache] find_by_product_id error: {e}");
                        ERR_INTERNAL_SERVER
                    })?;
                inventory
                    .map(InventoryResponse::from)
                    .map(CachedItemResult::Fresh)
                    .ok_or(ERR_INVENTORY_NOT_FOUND)
            }
        }
    }

    pub async fn evict(&self, id: i64, product_id: i64) {
        if let Some(cache) = &self.inner {
            cache.evict(id, product_id).await;
        }
    }

    pub async fn list(
        &self,
        pool: &MySqlPool,
        status: Option<&str>,
        page: u32,
        page_size: u32,
    ) -> Result<CachedListResult, BizError> {
        match &self.inner {
            Some(cache) => cache.list(pool, status, page, page_size).await,
            None => {
                let offset = (page - 1) * page_size;
                let total = repository::inventory::count_list(pool, status)
                    .await
                    .map_err(|e| {
                        log::error!("[inventory_cache] count_list error: {e}");
                        ERR_INTERNAL_SERVER
                    })?;
                let inventories = repository::inventory::find_list(pool, status, offset, page_size)
                    .await
                    .map_err(|e| {
                        log::error!("[inventory_cache] find_list error: {e}");
                        ERR_INTERNAL_SERVER
                    })?;
                Ok(CachedListResult::Fresh(InventoryListResponse {
                    list: inventories.into_iter().map(InventoryResponse::from).collect(),
                    total,
                }))
            }
        }
    }

    pub async fn warmup(&self, pool: &MySqlPool) -> Result<i32, BizError> {
        match &self.inner {
            Some(cache) => cache.warmup(pool).await,
            None => Ok(0),
        }
    }
}