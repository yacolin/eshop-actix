use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use redis::aio::ConnectionManager;
use serde::{Deserialize, Serialize};
use sqlx::MySqlPool;

use crate::dto::inventory::{InventoryListResponse, InventoryResponse};
use crate::error::{BizError, ERR_INTERNAL_SERVER, ERR_INVENTORY_NOT_FOUND};
use crate::models::inventory::Inventory;
use crate::repository;

const INVENTORY_INFO_PREFIX: &str = "inventory:info:";
const INVENTORY_PRODUCT_PREFIX: &str = "inventory:product:";
const LOCAL_CACHE_TTL: Duration = Duration::from_secs(60);
const LOCAL_LIST_CACHE_TTL: Duration = Duration::from_secs(30);

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

    fn remove_by_id(&self, id: i64) {
        self.single_by_id.remove(&id);
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

    fn remove_by_product_id(&self, product_id: i64) {
        self.single_by_product.remove(&product_id);
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

    fn clear_lists(&self) {
        self.lists.clear();
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
    }

    async fn list(
        &self,
        pool: &MySqlPool,
        status: Option<&str>,
        page: u32,
        page_size: u32,
    ) -> Result<CachedListResult, BizError> {
        let status_key = status.unwrap_or("").to_string();
        if let Some(full) = self.local.get_list(page, page_size, &status_key) {
            return Ok(CachedListResult::FullResponse(full));
        }

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

        let resp = InventoryListResponse {
            list: inventories.into_iter().map(InventoryResponse::from).collect(),
            total,
        };

        if status.is_none() {
            if let Ok(list_bytes) = serde_json::to_vec(&resp) {
                let full = bytes::Bytes::from(build_full_response(&list_bytes));
                self.local.set_list(page, page_size, status_key, full.clone());
                return Ok(CachedListResult::FullResponse(full));
            }
        }

        Ok(CachedListResult::Fresh(resp))
    }

    async fn warmup(&self, pool: &MySqlPool) -> Result<i32, BizError> {
        let inventories = repository::inventory::find_list(pool, None, 0, u32::MAX)
            .await
            .map_err(|e| {
                log::error!("[inventory_cache] warmup find_list error: {e}");
                ERR_INTERNAL_SERVER
            })?;

        let count = inventories.len() as i32;
        let items: Vec<CachedInventoryItem> =
            inventories.into_iter().map(CachedInventoryItem::from).collect();

        let mut conn = self.get_conn().lock().await;

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
            }
        }

        drop(conn);
        self.local.warmup_all(items);

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