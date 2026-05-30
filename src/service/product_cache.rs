use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::LazyLock;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use redis::Script;
use redis::aio::ConnectionManager;
use sqlx::MySqlPool;

use crate::dto::product::{CachedProductItem, ProductListResponse, ProductResponse};
use crate::error::{BizError, ERR_INTERNAL_SERVER, ERR_PRODUCT_NOT_FOUND};
use crate::repository;

// ====== Cached Response Types (pre-serialized bytes) ======

pub enum CachedItemResult {
    Serialized(Vec<u8>),
    Fresh(ProductResponse),
}

pub enum CachedListResult {
    Serialized(Vec<u8>),
    Fresh(ProductListResponse),
}

// ====== Constants ======

const PRODUCT_CACHE_ZSET: &str = "product:zset";
const PRODUCT_INFO_PREFIX: &str = "product:info:";
const LOCAL_CACHE_TTL: Duration = Duration::from_secs(60);
const LOCAL_LIST_CACHE_TTL: Duration = Duration::from_secs(30);
const HOT_KEY_THRESHOLD: u64 = 1000;
const HOT_KEY_WINDOW: Duration = Duration::from_secs(10);
const EMPTY_PLACEHOLDER: &str = "__EMPTY__";
const EMPTY_CACHE_TTL: u64 = 30;
const BLOOM_SIZE: usize = 1_000_000;
const BLOOM_HASHES: u32 = 7;

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

// ====== Bloom Filter ======

struct BloomFilter {
    bits: Mutex<Vec<u64>>,
    size: usize,
    num_hashes: u32,
}

impl BloomFilter {
    fn new(size: usize, num_hashes: u32) -> Self {
        BloomFilter {
            bits: Mutex::new(vec![0u64; (size + 63) / 64]),
            size,
            num_hashes,
        }
    }

    fn hash(&self, id: i64, seed: u32) -> usize {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        id.hash(&mut hasher);
        seed.hash(&mut hasher);
        (hasher.finish() as usize) % self.size
    }

    fn add(&self, id: i64) {
        let mut bits = self.bits.lock().unwrap();
        for i in 0..self.num_hashes {
            let idx = self.hash(id, i);
            bits[idx / 64] |= 1u64 << (idx % 64);
        }
    }

    fn may_exist(&self, id: i64) -> bool {
        let bits = self.bits.lock().unwrap();
        for i in 0..self.num_hashes {
            let idx = self.hash(id, i);
            if bits[idx / 64] & (1u64 << (idx % 64)) == 0 {
                return false;
            }
        }
        true
    }

    fn clear(&self) {
        let mut bits = self.bits.lock().unwrap();
        bits.iter_mut().for_each(|b| *b = 0);
    }

    fn add_all(&self, ids: &[i64]) {
        for &id in ids {
            self.add(id);
        }
    }
}

// ====== Hot Key Counter ======

struct HotKeyCounter {
    counters: Mutex<HashMap<i64, (u64, Instant)>>,
}

impl HotKeyCounter {
    fn new() -> Self {
        HotKeyCounter {
            counters: Mutex::new(HashMap::new()),
        }
    }

    fn increment(&self, id: i64) -> bool {
        let mut counters = self.counters.lock().unwrap();
        let now = Instant::now();
        let entry = counters.entry(id).or_insert((0, now));
        if now.duration_since(entry.1) > HOT_KEY_WINDOW {
            *entry = (0, now);
        }
        entry.0 += 1;
        entry.0 >= HOT_KEY_THRESHOLD
    }

    #[allow(dead_code)]
    fn reset(&self, id: i64) {
        let mut counters = self.counters.lock().unwrap();
        counters.remove(&id);
    }
}

// ====== Local Cache ======

struct LocalCache {
    single_bytes: DashMap<i64, (Instant, Vec<u8>)>,
    list_bytes: DashMap<(u32, u32), (Instant, Vec<u8>)>,
}

impl LocalCache {
    fn new() -> Self {
        LocalCache {
            single_bytes: DashMap::new(),
            list_bytes: DashMap::new(),
        }
    }

    fn get_single_bytes(&self, id: i64) -> Option<Vec<u8>> {
        if let Some(entry) = self.single_bytes.get(&id) {
            if entry.0.elapsed() < LOCAL_CACHE_TTL {
                return Some(entry.1.clone());
            }
        }
        None
    }

    fn set_single_bytes(&self, id: i64, bytes: Vec<u8>) {
        self.single_bytes.insert(id, (Instant::now(), bytes));
    }

    fn remove_single(&self, id: i64) {
        self.single_bytes.remove(&id);
    }

    fn get_list_bytes(&self, page: u32, page_size: u32) -> Option<Vec<u8>> {
        if let Some(entry) = self.list_bytes.get(&(page, page_size)) {
            if entry.0.elapsed() < LOCAL_LIST_CACHE_TTL {
                return Some(entry.1.clone());
            }
        }
        None
    }

    fn set_list_bytes(&self, page: u32, page_size: u32, bytes: Vec<u8>) {
        self.list_bytes
            .insert((page, page_size), (Instant::now(), bytes));
    }

    fn clear_lists(&self) {
        self.list_bytes.clear();
    }

    fn warmup_singles(&self, items: Vec<CachedProductItem>) {
        for item in items {
            if let Ok(bytes) = serde_json::to_vec(&item) {
                self.set_single_bytes(item.id, bytes);
            }
        }
    }
}

// ====== Inner Cache (active when Redis is available) ======

struct InnerCache {
    redis_conns: Vec<tokio::sync::Mutex<ConnectionManager>>,
    next_conn: AtomicUsize,
    bloom: BloomFilter,
    local: LocalCache,
    hot_counter: HotKeyCounter,
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
        let redis_key = format!("{}{}", PRODUCT_INFO_PREFIX, id);

        if self.bloom.may_exist(id) {
            if let Some(bytes) = self.local.get_single_bytes(id) {
                self.hot_counter.increment(id);
                return Ok(CachedItemResult::Serialized(bytes));
            }
        }

        let mut conn = self.get_conn().lock().await;

        let result: redis::RedisResult<Option<String>> = redis::cmd("GET")
            .arg(&redis_key)
            .query_async(&mut *conn)
            .await;

        match result {
            Ok(Some(data)) if data != EMPTY_PLACEHOLDER => {
                if let Ok(item) = serde_json::from_str::<CachedProductItem>(&data) {
                    if let Ok(bytes) = serde_json::to_vec(&item) {
                        self.local.set_single_bytes(id, bytes.clone());
                        return Ok(CachedItemResult::Serialized(bytes));
                    }
                }
            }
            Ok(Some(_)) => {
                return Err(ERR_PRODUCT_NOT_FOUND);
            }
            _ => {}
        }

        let product = repository::product::find_by_id(pool, id)
            .await
            .map_err(|e| {
                log::error!("[product_cache] find_by_id error: {e}");
                ERR_INTERNAL_SERVER
            })?;

        match product {
            Some(p) => {
                let cached = CachedProductItem::from(&p);
                if let Ok(bytes) = serde_json::to_vec(&cached) {
                    self.local.set_single_bytes(id, bytes);
                }

                if let Ok(json) = serde_json::to_string(&cached) {
                    let _: redis::RedisResult<()> = redis::cmd("SET")
                        .arg(&redis_key)
                        .arg(&json)
                        .query_async(&mut *conn)
                        .await;
                }

                Ok(CachedItemResult::Fresh(ProductResponse::from(p)))
            }
            None => {
                let _: redis::RedisResult<()> = redis::cmd("SETEX")
                    .arg(&redis_key)
                    .arg(EMPTY_CACHE_TTL)
                    .arg(EMPTY_PLACEHOLDER)
                    .query_async(&mut *conn)
                    .await;
                Err(ERR_PRODUCT_NOT_FOUND)
            }
        }
    }

    async fn list(
        &self,
        pool: &MySqlPool,
        keyword: Option<&str>,
        page: u32,
        page_size: u32,
    ) -> Result<CachedListResult, BizError> {
        if keyword.is_some() {
            let resp = Self::list_from_db(pool, keyword, page, page_size).await?;
            return Ok(CachedListResult::Fresh(resp));
        }

        if let Some(bytes) = self.local.get_list_bytes(page, page_size) {
            return Ok(CachedListResult::Serialized(bytes));
        }

        let offset = (page.saturating_sub(1)) as i64;
        let stop = offset + page_size as i64 - 1;

        let mut conn = self.get_conn().lock().await;

        let result: redis::RedisResult<redis::Value> = ZRANGE_MGET_SCRIPT
            .key(PRODUCT_CACHE_ZSET)
            .arg(offset)
            .arg(stop)
            .arg("desc")
            .arg(PRODUCT_INFO_PREFIX)
            .invoke_async(&mut *conn)
            .await;

        match result {
            Ok(val) => {
                if let Some((total, items)) = Self::parse_zset_result(val) {
                    if total > 0 {
                        let products: Vec<ProductResponse> = items
                            .iter()
                            .filter_map(|json| serde_json::from_str::<CachedProductItem>(json).ok())
                            .map(|cached| ProductResponse::from(&cached))
                            .collect();
                        if !products.is_empty() {
                            let resp = ProductListResponse {
                                list: products,
                                total,
                            };
                            if let Ok(bytes) = serde_json::to_vec(&resp) {
                                self.local.set_list_bytes(page, page_size, bytes.clone());
                                return Ok(CachedListResult::Serialized(bytes));
                            }
                        }
                    }
                }
            }
            Err(e) => {
                log::warn!("[product_cache] Redis ZSET list failed, falling back to DB: {e}");
            }
        }

        let resp = Self::list_from_db(pool, None, page, page_size).await?;
        Ok(CachedListResult::Fresh(resp))
    }

    async fn list_from_db(
        pool: &MySqlPool,
        keyword: Option<&str>,
        page: u32,
        page_size: u32,
    ) -> Result<ProductListResponse, BizError> {
        let offset = (page.saturating_sub(1)) * page_size;

        let total = repository::product::count_list(pool, keyword)
            .await
            .map_err(|e| {
                log::error!("[product_cache] count_list error: {e}");
                ERR_INTERNAL_SERVER
            })?;

        let products = repository::product::find_list(pool, keyword, offset, page_size)
            .await
            .map_err(|e| {
                log::error!("[product_cache] find_list error: {e}");
                ERR_INTERNAL_SERVER
            })?;

        Ok(ProductListResponse {
            list: products.into_iter().map(ProductResponse::from).collect(),
            total,
        })
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

    async fn warmup(&self, pool: &MySqlPool) -> Result<i32, BizError> {
        let products = repository::product::find_list(pool, None, 0, u32::MAX)
            .await
            .map_err(|e| {
                log::error!("[product_cache] warmup find_list error: {e}");
                ERR_INTERNAL_SERVER
            })?;

        let mut conn = self.get_conn().lock().await;

        let _: redis::RedisResult<()> = redis::cmd("DEL")
            .arg(PRODUCT_CACHE_ZSET)
            .query_async(&mut *conn)
            .await;

        let ids: Vec<i64> = products.iter().map(|p| p.id).collect();
        let items: Vec<CachedProductItem> =
            products.into_iter().map(CachedProductItem::from).collect();

        for item in &items {
            if let Ok(json) = serde_json::to_string(item) {
                let key = format!("{}{}", PRODUCT_INFO_PREFIX, item.id);
                let _: redis::RedisResult<()> = redis::cmd("SET")
                    .arg(&key)
                    .arg(&json)
                    .query_async(&mut *conn)
                    .await;
                let _: redis::RedisResult<()> = redis::cmd("ZADD")
                    .arg(PRODUCT_CACHE_ZSET)
                    .arg(item.id)
                    .arg(item.id)
                    .query_async(&mut *conn)
                    .await;
            }
        }

        drop(conn);

        self.bloom.clear();
        self.bloom.add_all(&ids);
        self.local.warmup_singles(items);

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
                let list: Vec<ProductResponse> = page_ids.iter()
                    .filter_map(|id| self.local.get_single_bytes(*id))
                    .filter_map(|bytes| serde_json::from_slice::<CachedProductItem>(&bytes).ok())
                    .map(|cached| ProductResponse::from(&cached))
                    .collect();
                if list.is_empty() {
                    break;
                }
                let resp = ProductListResponse { list, total };
                if let Ok(bytes) = serde_json::to_vec(&resp) {
                    self.local.set_list_bytes(page, page_size, bytes);
                }
                page += 1;
                if offset as usize + page_size as usize >= ids.len() {
                    break;
                }
            }
        }

        Ok(ids.len() as i32)
    }

    async fn evict_product(&self, id: i64) {
        self.local.remove_single(id);
        self.local.clear_lists();

        let mut conn = self.get_conn().lock().await;
        let key = format!("{}{}", PRODUCT_INFO_PREFIX, id);
        let _: redis::RedisResult<()> =
            redis::cmd("DEL").arg(&key).query_async(&mut *conn).await;
        let _: redis::RedisResult<()> = redis::cmd("ZREM")
            .arg(PRODUCT_CACHE_ZSET)
            .arg(id)
            .query_async(&mut *conn)
            .await;
    }

    fn bloom_add(&self, id: i64) {
        self.bloom.add(id);
    }
}

// ====== Public ProductCache ======

pub struct ProductCache {
    inner: Option<InnerCache>,
}

impl ProductCache {
    pub fn new(conns: Option<Vec<ConnectionManager>>) -> Self {
        ProductCache {
            inner: conns.map(|managers| InnerCache {
                redis_conns: managers.into_iter().map(tokio::sync::Mutex::new).collect(),
                next_conn: AtomicUsize::new(0),
                bloom: BloomFilter::new(BLOOM_SIZE, BLOOM_HASHES),
                local: LocalCache::new(),
                hot_counter: HotKeyCounter::new(),
            }),
        }
    }

    pub async fn get_by_id(&self, pool: &MySqlPool, id: i64) -> Result<CachedItemResult, BizError> {
        match &self.inner {
            Some(cache) => cache.get_by_id(pool, id).await,
            None => {
                let product = repository::product::find_by_id(pool, id)
                    .await
                    .map_err(|e| {
                        log::error!("[product_cache] find_by_id error: {e}");
                        ERR_INTERNAL_SERVER
                    })?;
                product
                    .map(ProductResponse::from)
                    .map(CachedItemResult::Fresh)
                    .ok_or(ERR_PRODUCT_NOT_FOUND)
            }
        }
    }

    pub async fn list(
        &self,
        pool: &MySqlPool,
        keyword: Option<&str>,
        page: u32,
        page_size: u32,
    ) -> Result<CachedListResult, BizError> {
        match &self.inner {
            Some(cache) => cache.list(pool, keyword, page, page_size).await,
            None => {
                let offset = (page.saturating_sub(1)) * page_size;
                let total = repository::product::count_list(pool, keyword)
                    .await
                    .map_err(|e| {
                        log::error!("[product_cache] count_list error: {e}");
                        ERR_INTERNAL_SERVER
                    })?;
                let products = repository::product::find_list(pool, keyword, offset, page_size)
                    .await
                    .map_err(|e| {
                        log::error!("[product_cache] find_list error: {e}");
                        ERR_INTERNAL_SERVER
                    })?;
                Ok(CachedListResult::Fresh(ProductListResponse {
                    list: products.into_iter().map(ProductResponse::from).collect(),
                    total,
                }))
            }
        }
    }

    pub async fn warmup(&self, pool: &MySqlPool) -> Result<i32, BizError> {
        match &self.inner {
            Some(cache) => cache.warmup(pool).await,
            None => {
                log::warn!("[product_cache] Redis not configured, skipping warmup");
                Ok(0)
            }
        }
    }

    #[allow(dead_code)]
    pub async fn evict_product(&self, id: i64) {
        if let Some(cache) = &self.inner {
            cache.evict_product(id).await;
        }
    }

    pub async fn delayed_double_delete(&self, id: i64) {
        if let Some(cache) = &self.inner {
            cache.evict_product(id).await;

            let redis_url = std::env::var("REDIS_URL").unwrap_or_default();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(500)).await;
                if let Ok(client) = redis::Client::open(redis_url.as_str()) {
                    if let Ok(mut conn) = ConnectionManager::new(client).await {
                        let key = format!("{}{}", PRODUCT_INFO_PREFIX, id);
                        let _: redis::RedisResult<()> =
                            redis::cmd("DEL").arg(&key).query_async(&mut conn).await;
                        let _: redis::RedisResult<()> = redis::cmd("ZREM")
                            .arg(PRODUCT_CACHE_ZSET)
                            .arg(id)
                            .query_async(&mut conn)
                            .await;
                    }
                }
            });
        }
    }

    pub fn bloom_add(&self, id: i64) {
        if let Some(cache) = &self.inner {
            cache.bloom_add(id);
        }
    }
}