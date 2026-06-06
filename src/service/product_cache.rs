use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::LazyLock;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use redis::Script;
use redis::aio::ConnectionManager;
use sqlx::MySqlPool;

use crate::dto::product::{CachedProductItem, ProductListResponse, ProductResponse};
use crate::error::{BizError, ERR_INTERNAL_SERVER, ERR_PRODUCT_NOT_FOUND};
use crate::repository;

// ====== 辅助函数：将数据字节包装成完整的 ApiResponse JSON ======
// 将传入的 data_bytes 放入 {"code":0,"message":"success","data":<data_bytes>} 中
fn build_full_response(data_bytes: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(data_bytes.len() + 64);
    buf.extend_from_slice(b"{\"code\":0,\"message\":\"success\",\"data\":");
    buf.extend_from_slice(data_bytes);
    buf.push(b'}');
    buf
}

// ====== 缓存响应类型（预序列化字节 vs 新鲜数据） ======
// FullResponse: 直接从缓存返回的完整 JSON 字节，无需序列化
// Fresh: 从数据库查询到的新数据，由调用方处理

pub enum CachedItemResult {
    FullResponse(bytes::Bytes),
    Fresh(ProductResponse),
}

pub enum CachedListResult {
    FullResponse(bytes::Bytes),
    Fresh(ProductListResponse),
}

// ====== 常量配置 ======
// ZSET: Redis 有序集合，用于存储产品 ID 列表，支持分页排序
// INFO_PREFIX: 产品详情缓存键前缀
// LOCAL_CACHE_TTL: 本地单条缓存有效期 60 秒
// LOCAL_LIST_CACHE_TTL: 本地列表缓存有效期 30 秒
// HOT_KEY_THRESHOLD: 热点 key 判定阈值（10 秒内访问超过 1000 次）
// EMPTY_PLACEHOLDER: 空值占位符，防止缓存穿透
// BLOOM_SIZE/HASHES: 布隆过滤器参数（100 万位，7 个哈希函数）

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

// ====== Redis Lua 脚本：原子化分页查询 ======
// 功能：一次往返完成 ZSET 分页 + MGET 批量获取详情
// 参数：KEYS[1]=zset键, ARGV[1]=起始偏移, ARGV[2]=结束偏移, ARGV[3]=排序方向(desc/asc), ARGV[4]=详情键前缀
// 返回：{total_count, [value1, value2, ...]}

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

// ====== 布隆过滤器（Bloom Filter） ======
// 用于快速判断一个产品 ID 是否可能存在，减少对本地缓存的无效查询
// 使用无锁 AtomicU64 位数组，支持高并发写入
// 特点：不存在 → 一定不存在；存在 → 可能存在（有误判率）

struct BloomFilter {
    bits: Vec<AtomicU64>,
    size: usize,
    num_hashes: u32,
}

impl BloomFilter {
    fn new(size: usize, num_hashes: u32) -> Self {
        let word_count = (size + 63) / 64;
        let mut bits = Vec::with_capacity(word_count);
        for _ in 0..word_count {
            bits.push(AtomicU64::new(0));
        }
        BloomFilter {
            bits,
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

    // 将 id 映射到位数组中的 k 个位置，将对应位设为 1
    fn add(&self, id: i64) {
        for i in 0..self.num_hashes {
            let idx = self.hash(id, i);
            let word_idx = idx / 64;
            let bit_idx = idx % 64;
            self.bits[word_idx].fetch_or(1u64 << bit_idx, Ordering::Relaxed);
        }
    }

    // 检查 id 是否可能存在（返回 false 则一定不存在）
    fn may_exist(&self, id: i64) -> bool {
        for i in 0..self.num_hashes {
            let idx = self.hash(id, i);
            let word_idx = idx / 64;
            let bit_idx = idx % 64;
            if self.bits[word_idx].load(Ordering::Relaxed) & (1u64 << bit_idx) == 0 {
                return false;
            }
        }
        true
    }

    // 清空所有位
    fn clear(&self) {
        for word in &self.bits {
            word.store(0, Ordering::Relaxed);
        }
    }

    fn add_all(&self, ids: &[i64]) {
        for &id in ids {
            self.add(id);
        }
    }
}

// ====== 热点 Key 计数器 ======
// 用于识别热点数据，为后续可能的本地缓存提升做决策依据
// DashMap 实现无锁并发，每个 key 记录窗口期内的访问次数

struct HotKeyCounter {
    counters: DashMap<i64, (u64, Instant)>,
}

impl HotKeyCounter {
    fn new() -> Self {
        HotKeyCounter {
            counters: DashMap::new(),
        }
    }

    // 增加计数，返回是否已达到热点阈值
    fn increment(&self, id: i64) -> bool {
        let now = Instant::now();
        let mut entry = self.counters.entry(id).or_insert((0, now));
        if now.duration_since(entry.1) > HOT_KEY_WINDOW {
            *entry = (0, now);
        }
        entry.0 += 1;
        entry.0 >= HOT_KEY_THRESHOLD
    }

    #[allow(dead_code)]
    fn reset(&self, id: i64) {
        self.counters.remove(&id);
    }
}

// ====== 本地缓存（一级缓存） ======
// 使用 DashMap 实现无锁并发，存储预序列化后的完整响应字节
// 单条缓存：key=产品ID, value=(存入时间, 完整JSON字节)
// 列表缓存：key=(页码, 每页数量), value=(存入时间, 完整JSON字节)

struct LocalCache {
    single_full: DashMap<i64, (Instant, bytes::Bytes)>,
    list_full: DashMap<(u32, u32), (Instant, bytes::Bytes)>,
}

impl LocalCache {
    fn new() -> Self {
        LocalCache {
            single_full: DashMap::new(),
            list_full: DashMap::new(),
        }
    }

    fn get_single_full(&self, id: i64) -> Option<bytes::Bytes> {
        if let Some(entry) = self.single_full.get(&id) {
            if entry.0.elapsed() < LOCAL_CACHE_TTL {
                return Some(entry.1.clone());
            }
        }
        None
    }

    fn set_single_full(&self, id: i64, bytes: bytes::Bytes) {
        self.single_full.insert(id, (Instant::now(), bytes));
    }

    fn remove_single(&self, id: i64) {
        self.single_full.remove(&id);
    }

    fn get_list_full(&self, page: u32, page_size: u32) -> Option<bytes::Bytes> {
        if let Some(entry) = self.list_full.get(&(page, page_size)) {
            if entry.0.elapsed() < LOCAL_LIST_CACHE_TTL {
                return Some(entry.1.clone());
            }
        }
        None
    }

    fn set_list_full(&self, page: u32, page_size: u32, bytes: bytes::Bytes) {
        self.list_full
            .insert((page, page_size), (Instant::now(), bytes));
    }

    fn clear_lists(&self) {
        self.list_full.clear();
    }

    fn warmup_singles(&self, items: Vec<CachedProductItem>) {
        for item in items {
            if let Ok(item_bytes) = serde_json::to_vec(&item) {
                let full = bytes::Bytes::from(build_full_response(&item_bytes));
                self.set_single_full(item.id, full);
            }
        }
    }
}

// ====== 内部缓存引擎（Redis 可用时激活） ======
// 实现三级缓存策略：本地缓存(L1) → Redis(L2) → 数据库(L3)
// 包含连接池轮询、布隆过滤器、热点计数等优化机制

struct InnerCache {
    redis_conns: Vec<tokio::sync::Mutex<ConnectionManager>>,
    next_conn: AtomicUsize,
    bloom: BloomFilter,
    local: LocalCache,
    hot_counter: HotKeyCounter,
}

impl InnerCache {
    // 轮询获取 Redis 连接，实现连接的负载均衡
    fn get_conn(&self) -> &tokio::sync::Mutex<ConnectionManager> {
        let idx = self
            .next_conn
            .fetch_add(1, Ordering::Relaxed)
            % self.redis_conns.len();
        &self.redis_conns[idx]
    }

    // 三级缓存查询：本地缓存(L1) → Redis(L2) → 数据库(L3)
    // 1. 先查布隆过滤器 + 本地缓存（最快，毫秒级）
    // 2. 本地未命中则查 Redis（网络往返，微秒级）
    // 3. Redis 未命中则查数据库，并回写 Redis + 本地缓存
    async fn get_by_id(&self, pool: &MySqlPool, id: i64) -> Result<CachedItemResult, BizError> {
        let redis_key = format!("{}{}", PRODUCT_INFO_PREFIX, id);

        if self.bloom.may_exist(id) {
            if let Some(full) = self.local.get_single_full(id) {
                self.hot_counter.increment(id);
                return Ok(CachedItemResult::FullResponse(full));
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
                    if let Ok(item_bytes) = serde_json::to_vec(&item) {
                        let full = bytes::Bytes::from(build_full_response(&item_bytes));
                        self.local.set_single_full(id, full.clone());
                        return Ok(CachedItemResult::FullResponse(full));
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
                if let Ok(item_bytes) = serde_json::to_vec(&cached) {
                    let full = bytes::Bytes::from(build_full_response(&item_bytes));
                    self.local.set_single_full(id, full);
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

    // 分页列表查询（三级缓存）
    // 有关键词时直接查数据库（不支持关键词的缓存）
    // 无关键词时：本地缓存列表 → Redis ZSET+MGET → 数据库
    async fn list(
        &self,
        pool: &MySqlPool,
        keyword: Option<&str>,
        page: u32,
        page_size: u32,
    ) -> Result<CachedListResult, BizError> {
        // 有关键词搜索时跳过缓存，直接查数据库
        if keyword.is_some() {
            let resp = Self::list_from_db(pool, keyword, page, page_size).await?;
            return Ok(CachedListResult::Fresh(resp));
        }

        if let Some(full) = self.local.get_list_full(page, page_size) {
            return Ok(CachedListResult::FullResponse(full));
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
                            if let Ok(list_bytes) = serde_json::to_vec(&resp) {
                                let full = bytes::Bytes::from(build_full_response(&list_bytes));
                                self.local.set_list_full(page, page_size, full.clone());
                                return Ok(CachedListResult::FullResponse(full));
                            }
                        }
                    }
                }
            }
            Err(e) => {
                log::warn!("[product_cache] Redis ZSET list failed, falling back to DB: {e}");
            }
        }

        // Redis 缓存未命中或失败时，回退到数据库查询
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

    // 缓存预热：从数据库加载所有产品，填充 Redis 和本地缓存
    // 步骤：清空旧缓存 → 批量写入 Redis SET+ZADD → 重建布隆过滤器 → 预热本地列表缓存
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
                    .filter_map(|id| self.local.get_single_full(*id))
                    .filter_map(|full| {
                        // extract data portion from full response: {"code":0,"message":"success","data":{...}}
                        // find the position of "data": and parse from there
                        let bytes = &full[..];
                        let data_prefix = b"\"data\":";
                        if let Some(pos) = bytes.windows(data_prefix.len()).position(|w| w == data_prefix) {
                            let data_start = pos + data_prefix.len();
                            serde_json::from_slice::<CachedProductItem>(&bytes[data_start..]).ok()
                        } else {
                            None
                        }
                    })
                    .map(|cached| ProductResponse::from(&cached))
                    .collect();
                if list.is_empty() {
                    break;
                }
                let resp = ProductListResponse { list, total };
                if let Ok(list_bytes) = serde_json::to_vec(&resp) {
                    let full = bytes::Bytes::from(build_full_response(&list_bytes));
                    self.local.set_list_full(page, page_size, full);
                }
                page += 1;
                if offset as usize + page_size as usize >= ids.len() {
                    break;
                }
            }
        }

        Ok(ids.len() as i32)
    }

    // 删除缓存：同时清理本地缓存和 Redis（用于数据更新后失效缓存）
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

// ====== 公开的 ProductCache 结构体 ======
// 对外暴露缓存接口，内部封装 InnerCache 的可选逻辑
// 当 Redis 不可用时（inner=None），自动降级为直接查询数据库

pub struct ProductCache {
    inner: Option<InnerCache>,
}

impl ProductCache {
    // 创建缓存实例，传入可选的 Redis 连接列表
    // Some(conns)：启用三级缓存（本地+Redis+DB）
    // None：降级为直连数据库
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

    // 对外查询接口：有缓存走三级缓存，无缓存直查数据库
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

    // 对外列表查询接口
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

    // 缓存预热接口：将数据库数据加载到 Redis 和本地缓存
    pub async fn warmup(&self, pool: &MySqlPool) -> Result<i32, BizError> {
        match &self.inner {
            Some(cache) => cache.warmup(pool).await,
            None => {
                log::warn!("[product_cache] Redis not configured, skipping warmup");
                Ok(0)
            }
        }
    }

    // 删除单个产品缓存（用于数据更新后失效缓存）
    #[allow(dead_code)]
    pub async fn evict_product(&self, id: i64) {
        if let Some(cache) = &self.inner {
            cache.evict_product(id).await;
        }
    }

    // 延迟双删策略：解决缓存与数据库的一致性问题
    // 先立即删除缓存，500ms 后再次删除，确保并发读写时不会读到旧数据
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

    // 向布隆过滤器添加 ID（新产品创建时调用）
    pub fn bloom_add(&self, id: i64) {
        if let Some(cache) = &self.inner {
            cache.bloom_add(id);
        }
    }
}