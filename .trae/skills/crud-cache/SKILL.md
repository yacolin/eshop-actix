---
name: "crud-cache"
description: "eshop-actix 项目 CRUD 与三级缓存编码规范。定义标准 CURD 流程、缓存集成模式。Invoke when adding new business modules or implementing cache for existing modules."
---

# CRUD 与三级缓存编码规范

## 1. 项目新增模块标准流程

新增一个业务模块（如 product/inventory）需要创建以下文件：

```
src/
├── api/xxx.rs           ← Handler 层
├── dto/xxx.rs           ← 数据传输对象
├── models/xxx.rs        ← 数据库模型
├── repository/xxx.rs    ← 数据访问层
├── service/xxx.rs       ← 业务逻辑层
├── service/xxx_cache.rs ← 三级缓存层（可选）
```

### 依赖层级

```
Handler(api/xxx.rs)
    ↓ 调用
Service(service/xxx.rs)
    ↓ 调用
Repository(repository/xxx.rs)
    ↓
(sqlx) → MySQL
```

Cache 层被 Service 嵌入调用：

```
Handler(api/xxx.rs)
    ↓ 调用
Service(service/xxx.rs)
    ├── 非缓存路径 → Repository → DB
    └── 缓存路径   → Cache 层 → (Local → Redis → DB)
```

---

## 2. DTO 编写规范

### 请求体

```rust
// src/dto/product.rs
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct CreateProductRequest {
    pub name: String,
    pub description: Option<String>,
    pub price: i64,
    pub sku: String,
}

#[derive(Debug, Deserialize)]
pub struct UpdateProductRequest {
    pub name: Option<String>,
    pub description: Option<String>,
    pub price: Option<i64>,
    pub sku: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ProductListQuery {
    pub keyword: Option<String>,
    pub page: Option<u32>,
    pub page_size: Option<u32>,
}

pub struct CachedProductItem {
    pub id: i64,
    pub name: String,
    pub description: Option<String>,
    pub price: i64,
    pub sku: String,
}
```

### 响应体

```rust
// src/dto/product.rs
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct ProductResponse {
    pub id: i64,
    pub name: String,
    pub description: Option<String>,
    pub price: i64,
    pub sku: String,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize)]
pub struct ProductListResponse {
    pub list: Vec<ProductResponse>,
    pub total: i64,
}
```

### 模型 → DTO 转换

```rust
impl From<Model> for Response {
    fn from(m: Model) -> Self {
        Response { ... }
    }
}

impl From<&CachedItem> for Response {
    fn from(c: &CachedItem) -> Self {
        Response { ... created_at: None, updated_at: None }
    }
}
```

---

## 3. Model 编写规范

```rust
// src/models/product.rs
use chrono::{DateTime, NaiveDateTime, Utc};
use sqlx::FromRow;
use serde::Serialize;

#[derive(Debug, FromRow, Serialize)]
pub struct Product {
    pub id: i64,
    pub name: String,
    pub description: Option<String>,
    pub price: i64,
    pub sku: String,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
    pub deleted_at: Option<NaiveDateTime>,
}
```

---

## 4. Repository 编写规范

### 单条查询

```rust
pub async fn find_by_id(pool: &MySqlPool, id: i64) -> Result<Option<Model>, sqlx::Error> {
    sqlx::query_as::<_, Model>(
        "SELECT * FROM table WHERE id = ? AND deleted_at IS NULL",
    )
    .bind(id)
    .fetch_optional(pool)
    .await
}
```

### 创建

```rust
pub async fn create(pool: &MySqlPool, field: &str, ...) -> Result<Model, sqlx::Error> {
    sqlx::query("INSERT INTO table (field, ...) VALUES (?, ...)")
        .bind(field)
        .bind(...)
        .execute(pool)
        .await?;

    // 根据唯一字段回查
    find_by_unique_field(pool, field).await.map(|p| p.unwrap())
}
```

### 更新（动态字段）

```rust
pub async fn update(
    pool: &MySqlPool,
    id: i64,
    field1: Option<&str>,
    field2: Option<i64>,
) -> Result<Option<Model>, sqlx::Error> {
    let mut sets: Vec<String> = Vec::new();
    if field1.is_some() { sets.push("field1 = ?".to_string()); }
    if field2.is_some() { sets.push("field2 = ?".to_string()); }

    if sets.is_empty() {
        return find_by_id(pool, id).await;
    }

    let sql = format!(
        "UPDATE table SET {} WHERE id = ? AND deleted_at IS NULL",
        sets.join(", ")
    );

    let mut q = sqlx::query(&sql);
    if let Some(v) = field1 { q = q.bind(v); }
    if let Some(v) = field2 { q = q.bind(v); }
    q = q.bind(id);

    let result = q.execute(pool).await?;
    if result.rows_affected() > 0 {
        find_by_id(pool, id).await
    } else {
        Ok(None)
    }
}
```

### 软删除

```rust
pub async fn soft_delete(pool: &MySqlPool, id: i64) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(
        "UPDATE table SET deleted_at = NOW(3) WHERE id = ? AND deleted_at IS NULL",
    )
    .bind(id)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() > 0)
}
```

### 列表分页

```rust
pub async fn find_list(
    pool: &MySqlPool,
    filter: Option<&str>,
    offset: u32,
    limit: u32,
) -> Result<Vec<Model>, sqlx::Error> {
    let mut where_clause = String::from("WHERE deleted_at IS NULL");
    if filter.is_some() {
        where_clause.push_str(" AND field = ?");
    }

    let sql = format!(
        "SELECT * FROM table {} ORDER BY id DESC LIMIT ? OFFSET ?",
        where_clause
    );

    let mut q = sqlx::query_as::<_, Model>(&sql);
    if let Some(v) = filter { q = q.bind(v); }
    q = q.bind(limit).bind(offset);
    q.fetch_all(pool).await
}

pub async fn count_list(pool: &MySqlPool, filter: Option<&str>) -> Result<i64, sqlx::Error> {
    let mut where_clause = String::from("WHERE deleted_at IS NULL");
    if filter.is_some() {
        where_clause.push_str(" AND field = ?");
    }

    let sql = format!("SELECT COUNT(*) FROM table {}", where_clause);
    let mut q = sqlx::query_scalar::<_, i64>(&sql);
    if let Some(v) = filter { q = q.bind(v); }
    q.fetch_one(pool).await
}
```

---

## 5. Service 层规范

### 非缓存 CRUD

```rust
use crate::error::{BizError, ERR_INTERNAL_SERVER, ERR_NOT_FOUND};
use crate::repository;

pub async fn get_by_id(pool: &MySqlPool, id: i64) -> Result<Response, BizError> {
    let item = repository::module::find_by_id(pool, id)
        .await
        .map_err(|e| {
            log::error!("[module_service] find_by_id error: {e}");
            ERR_INTERNAL_SERVER
        })?;
    item.map(Response::from).ok_or(ERR_NOT_FOUND)
}

pub async fn list(
    pool: &MySqlPool,
    filter: Option<&str>,
    page: u32,
    page_size: u32,
) -> Result<ListResponse, BizError> {
    let offset = (page - 1) * page_size;
    let items = repository::module::find_list(pool, filter, offset, page_size)
        .await.map_err(|e| { ... })?;
    let total = repository::module::count_list(pool, filter)
        .await.map_err(|e| { ... })?;
    Ok(ListResponse { list: items.into_iter().map(Response::from).collect(), total })
}
```

### 带缓存的 CRUD

Service 层提供两组函数：非缓存版和缓存版。非缓存版给非缓存路由使用，缓存版给缓存路由使用。CRUD 操作（create/update/delete）统一接收 `cache: Option<&Cache>`，操作后执行 evict。

```rust
use crate::service::module_cache::{CachedItemResult, CachedListResult, Cache};

// ====== 非缓存版本 ======

pub async fn get_by_id(pool: &MySqlPool, id: i64) -> Result<Response, BizError> {
    repository::module::find_by_id(pool, id)
        .await.map_err(|e| { ... })?
        .map(Response::from)
        .ok_or(ERR_NOT_FOUND)
}

pub async fn list(pool: &MySqlPool, filter: Option<&str>, page: u32, page_size: u32) -> Result<ListResponse, BizError> {
    let offset = (page - 1) * page_size;
    let total = repository::module::count_list(pool, filter).await.map_err(|e| { ... })?;
    let items = repository::module::find_list(pool, filter, offset, page_size).await.map_err(|e| { ... })?;
    Ok(ListResponse { list: items.into_iter().map(Response::from).collect(), total })
}

// ====== 缓存版本 ======

pub async fn get_by_id_cached(pool: &MySqlPool, cache: &Cache, id: i64) -> Result<CachedItemResult, BizError> {
    cache.get_by_id(pool, id).await
}

pub async fn list_cached(pool: &MySqlPool, cache: &Cache, filter: Option<&str>, page: u32, page_size: u32) -> Result<CachedListResult, BizError> {
    cache.list(pool, filter, page, page_size).await
}

// ====== 写操作（带缓存失效） ======

pub async fn create(pool: &MySqlPool, cache: Option<&Cache>, req: CreateRequest) -> Result<Response, BizError> {
    // 1. 业务校验
    // 2. repository::module::create
    // 3. 缓存失效: cache.evict(resp.id, ...)
    // 4. Ok(resp)
}

pub async fn update(pool: &MySqlPool, cache: Option<&Cache>, id: i64, req: UpdateRequest) -> Result<Response, BizError> {
    // 1. 业务校验
    // 2. repository::module::update
    // 3. 缓存失效: cache.evict(resp.id, ...)
    // 4. Ok(resp)
}

pub async fn delete(pool: &MySqlPool, cache: Option<&Cache>, id: i64) -> Result<(), BizError> {
    // 1. repository::module::soft_delete
    // 2. 缓存失效: cache.evict(id, ...)
    // 3. Ok(())
}
```

---

## 6. Cache 层规范（三级缓存）

### 6.1 总体架构

```
请求 → Handler → Service → Cache Layer
                                ├── L1: Local (DashMap, 60s TTL) ← 最快，无网络开销
                                ├── L2: Redis (内存型 KV)        ← 分布式共享
                                └── L3: DB (MySQL)               ← 最终一致性回退
```

### 6.2 核心结构

```rust
// ====== Cached Response Types ======

pub enum CachedItemResult {
    FullResponse(bytes::Bytes),  // 预序列化 JSON，免序列化直接返回
    Fresh(Response),             // 新鲜数据，需序列化后返回
}

pub enum CachedListResult {
    FullResponse(bytes::Bytes),
    Fresh(ListResponse),
}

// ====== Cached Item (精简版，不含时间戳) ======

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CachedItem {
    pub id: i64,
    pub field1: String,
    pub field2: i64,
    // ... 只包含必要业务字段，不包含 created_at/updated_at
}

impl From<Model> for CachedItem { ... }
impl From<&CachedItem> for Response { ... }

// ====== 预序列化 JSON 构建 ======

fn build_full_response(data_bytes: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(data_bytes.len() + 64);
    buf.extend_from_slice(b"{\"code\":0,\"message\":\"success\",\"data\":");
    buf.extend_from_slice(data_bytes);
    buf.push(b'}');
    buf
}
```

### 6.3 本地缓存

```rust
use dashmap::DashMap;
use std::time::{Duration, Instant};

const LOCAL_CACHE_TTL: Duration = Duration::from_secs(60);
const LOCAL_LIST_CACHE_TTL: Duration = Duration::from_secs(30);

struct LocalCache {
    single_full: DashMap<i64, (Instant, bytes::Bytes)>,       // 单条缓存
    list_full: DashMap<(u32, u32), (Instant, bytes::Bytes)>,  // 列表缓存 (page, page_size)
    // 按业务维度分: single_by_id, single_by_product 等
}

impl LocalCache {
    fn get_single(&self, id: i64) -> Option<bytes::Bytes> {
        if let Some(entry) = self.single_full.get(&id) {
            if entry.0.elapsed() < LOCAL_CACHE_TTL {
                return Some(entry.1.clone());
            }
        }
        None
    }

    fn set_single(&self, id: i64, bytes: bytes::Bytes) {
        self.single_full.insert(id, (Instant::now(), bytes));
    }

    fn remove_single(&self, id: i64) {
        self.single_full.remove(&id);
    }

    fn get_list(&self, page: u32, page_size: u32) -> Option<bytes::Bytes> { ... }
    fn set_list(&self, page: u32, page_size: u32, bytes: bytes::Bytes) { ... }
    fn clear_lists(&self) { self.list_full.clear(); }
}
```

### 6.4 Redis 连接池（轮询）

```rust
struct InnerCache {
    redis_conns: Vec<tokio::sync::Mutex<ConnectionManager>>,
    next_conn: AtomicUsize,
    local: LocalCache,
}

impl InnerCache {
    fn get_conn(&self) -> &tokio::sync::Mutex<ConnectionManager> {
        let idx = self.next_conn.fetch_add(1, Ordering::Relaxed) % self.redis_conns.len();
        &self.redis_conns[idx]
    }
}
```

### 6.5 单条查询三级缓存

```rust
async fn get_by_id(&self, pool: &MySqlPool, id: i64) -> Result<CachedItemResult, BizError> {
    // L1: 本地 DashMap
    if let Some(full) = self.local.get_single(id) {
        return Ok(CachedItemResult::FullResponse(full));
    }

    // L2: Redis
    let redis_key = format!("{}:info:{}", PREFIX, id);
    let mut conn = self.get_conn().lock().await;
    let result: redis::RedisResult<Option<String>> = redis::cmd("GET")
        .arg(&redis_key)
        .query_async(&mut *conn)
        .await;

    match result {
        Ok(Some(json)) => {
            if let Ok(item) = serde_json::from_str::<CachedItem>(&json) {
                if let Ok(item_bytes) = serde_json::to_vec(&item) {
                    let full = bytes::Bytes::from(build_full_response(&item_bytes));
                    self.local.set_single(id, full.clone());
                    return Ok(CachedItemResult::FullResponse(full));
                }
            }
        }
        _ => {}
    }

    // L3: DB 回退
    let item = repository::module::find_by_id(pool, id)
        .await.map_err(|e| { ... })?;

    match item {
        Some(i) => {
            let cached = CachedItem::from(i);
            let resp = Response::from(&cached);

            // 写入本地缓存
            if let Ok(item_bytes) = serde_json::to_vec(&cached) {
                let full = bytes::Bytes::from(build_full_response(&item_bytes));
                self.local.set_single(id, full);
            }

            // 写入 Redis
            if let Ok(json) = serde_json::to_string(&cached) {
                let _: redis::RedisResult<()> = redis::cmd("SET")
                    .arg(&redis_key)
                    .arg(&json)
                    .query_async(&mut *conn)
                    .await;
            }

            Ok(CachedItemResult::Fresh(resp))
        }
        None => Err(ERR_NOT_FOUND),
    }
}
```

### 6.6 列表查询三级缓存

使用 zset（有序集合）+ MGET 实现 Redis 分页：

```rust
// Lua 脚本：原子执行 ZREVRANGE + MGET
static ZRANGE_MGET_SCRIPT: LazyLock<Script> = LazyLock::new(|| {
    Script::new(r#"
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
"#)
});

async fn list(&self, pool: &MySqlPool, filter: Option<&str>, page: u32, page_size: u32) -> Result<CachedListResult, BizError> {
    // 有筛选条件时不走 zset（查询条件退化）
    if filter.is_some() {
        return Self::list_from_db(pool, filter, page, page_size).await.map(CachedListResult::Fresh);
    }

    // L1: 本地 DashMap
    if let Some(full) = self.local.get_list(page, page_size) {
        return Ok(CachedListResult::FullResponse(full));
    }

    // L2: Redis zset
    let offset = (page.saturating_sub(1)) as i64;
    let stop = offset + page_size as i64 - 1;
    let mut conn = self.get_conn().lock().await;

    let result: redis::RedisResult<redis::Value> = ZRANGE_MGET_SCRIPT
        .key(CACHE_ZSET)
        .arg(offset).arg(stop).arg("desc").arg(PREFIX)
        .invoke_async(&mut *conn).await;

    match result {
        Ok(val) => {
            if let Some((total, items)) = Self::parse_zset_result(val) {
                if total > 0 {
                    let responses: Vec<Response> = items.iter()
                        .filter_map(|json| serde_json::from_str::<CachedItem>(json).ok())
                        .map(|c| Response::from(&c))
                        .collect();
                    if !responses.is_empty() {
                        let resp = ListResponse { list: responses, total };
                        if let Ok(list_bytes) = serde_json::to_vec(&resp) {
                            let full = bytes::Bytes::from(build_full_response(&list_bytes));
                            self.local.set_list(page, page_size, full.clone());
                            return Ok(CachedListResult::FullResponse(full));
                        }
                    }
                }
            }
        }
        Err(e) => { log::warn!("[cache] Redis ZSET failed, fallback to DB: {e}"); }
    }

    // L3: DB 回退
    let resp = Self::list_from_db(pool, None, page, page_size).await?;
    Ok(CachedListResult::Fresh(resp))
}

fn parse_zset_result(val: redis::Value) -> Option<(i64, Vec<String>)> {
    match val {
        redis::Value::Bulk(items) if items.len() == 2 => {
            let total = match &items[0] {
                redis::Value::Int(n) => *n,
                redis::Value::Data(bytes) => String::from_utf8_lossy(bytes).parse::<i64>().ok()?,
                _ => return None,
            };
            let values = match &items[1] {
                redis::Value::Bulk(arr) => arr.iter()
                    .filter_map(|v| match v {
                        redis::Value::Data(bytes) => Some(String::from_utf8_lossy(bytes).to_string()),
                        redis::Value::Nil => None,
                        _ => None,
                    }).collect(),
                redis::Value::Nil => vec![],
                _ => return None,
            };
            Some((total, values))
        }
        _ => None,
    }
}

async fn list_from_db(pool, filter, page, page_size) -> Result<ListResponse, BizError> {
    let offset = (page.saturating_sub(1)) * page_size;
    let total = repository::module::count_list(pool, filter).await.map_err(|e| { ... })?;
    let items = repository::module::find_list(pool, filter, offset, page_size).await.map_err(|e| { ... })?;
    Ok(ListResponse { list: items.into_iter().map(Response::from).collect(), total })
}
```

### 6.7 缓存失效 (Evict)

```rust
async fn evict(&self, id: i64, ...extra_keys: i64) {
    // 1. 清除本地缓存
    self.local.remove_single(id);
    self.local.clear_lists();

    // 2. 清除 Redis 缓存
    let mut conn = self.get_conn().lock().await;
    let key = format!("{}:info:{}", PREFIX, id);
    let _: redis::RedisResult<()> = redis::cmd("DEL").arg(&key).query_async(&mut *conn).await;
    let _: redis::RedisResult<()> = redis::cmd("ZREM")
        .arg(CACHE_ZSET).arg(id).query_async(&mut *conn).await;
}
```

### 6.8 缓存预热 (Warmup)

```rust
async fn warmup(&self, pool: &MySqlPool) -> Result<i32, BizError> {
    // 1. 全量查 DB
    let items = repository::module::find_list(pool, None, 0, u32::MAX)
        .await.map_err(|e| { ... })?;
    let ids: Vec<i64> = items.iter().map(|i| i.id).collect();
    let cached_items: Vec<CachedItem> = items.into_iter().map(CachedItem::from).collect();

    let mut conn = self.get_conn().lock().await;

    // 2. 清除旧 zset
    let _: redis::RedisResult<()> = redis::cmd("DEL").arg(CACHE_ZSET).query_async(&mut *conn).await;

    // 3. 逐条写入 Redis（单条 key + zset 成员）
    for item in &cached_items {
        if let Ok(json) = serde_json::to_string(item) {
            let key = format!("{}:info:{}", PREFIX, item.id);
            let _: redis::RedisResult<()> = redis::cmd("SET").arg(&key).arg(&json).query_async(&mut *conn).await;
            let _: redis::RedisResult<()> = redis::cmd("ZADD").arg(CACHE_ZSET).arg(item.id).arg(item.id).query_async(&mut *conn).await;
        }
    }

    drop(conn);

    // 4. 写入本地缓存
    self.local.warmup_singles(cached_items);

    // 5. 预热本地列表缓存（前 3 页）
    let page_sizes = [10u32, 20u32, 50u32];
    for page_size in page_sizes {
        let count = ids.len() as i64;
        let mut page = 1u32;
        loop {
            let offset = ((page.saturating_sub(1)) as i64).min(ids.len() as i64 - 1);
            let stop = (offset + page_size as i64 - 1).min(ids.len() as i64 - 1);
            if offset > stop { break; }
            let page_ids: Vec<i64> = ids.iter().rev()
                .skip(offset as usize).take((stop - offset + 1) as usize).copied().collect();
            // 从本地缓存提取数据拼装列表
            // ...
            page += 1;
            if offset as usize + page_size as usize >= ids.len() { break; }
        }
    }

    Ok(ids.len() as i32)
}
```

### 6.9 公开 API

```rust
pub struct Cache {
    inner: Option<InnerCache>,
}

impl Cache {
    pub fn new(redis_conns: Option<Vec<ConnectionManager>>) -> Self {
        let inner = redis_conns.map(|conns| InnerCache {
            redis_conns: conns.into_iter().map(tokio::sync::Mutex::new).collect(),
            next_conn: AtomicUsize::new(0),
            local: LocalCache::new(),
        });
        Cache { inner }
    }

    // 所有公开方法都 match inner，Redis 不可用时退化到 DB
    pub async fn get_by_id(&self, pool, id) -> Result<CachedItemResult, BizError> {
        match &self.inner {
            Some(cache) => cache.get_by_id(pool, id).await,
            None => { /* DB 直查 + Fresh 响应 */ }
        }
    }

    pub async fn list(&self, pool, filter, page, page_size) -> Result<CachedListResult, BizError> {
        match &self.inner { ... }
    }

    pub async fn evict(&self, id: i64, ...) {
        if let Some(cache) = &self.inner { cache.evict(id, ...).await; }
    }

    pub async fn warmup(&self, pool) -> Result<i32, BizError> {
        match &self.inner { ... }
    }
}
```

---

## 7. Handler 层 - 缓存路由规范

### 单条缓存查询

```rust
pub async fn get_by_id_cached(
    _req: HttpRequest,
    pool: web::Data<MySqlPool>,
    cache: web::Data<Cache>,
    path: web::Path<i64>,
) -> HttpResponse {
    let id = path.into_inner();
    match service::module::get_by_id_cached(pool.get_ref(), cache.get_ref(), id).await {
        Ok(CachedItemResult::FullResponse(body)) => {
            HttpResponse::Ok().content_type("application/json").body(body)
        }
        Ok(CachedItemResult::Fresh(data)) => response::success(data),
        Err(err) if err == ERR_INTERNAL_SERVER => response::sys_error(err.message),
        Err(err) => response::biz_error(&err),
    }
}
```

### 列表缓存查询

```rust
pub async fn list_cached(
    _req: HttpRequest,
    pool: web::Data<MySqlPool>,
    cache: web::Data<Cache>,
    query: web::Query<ListQuery>,
) -> HttpResponse {
    let page = query.page.unwrap_or(1).clamp(1, 1000);
    let page_size = query.page_size.unwrap_or(10).clamp(1, 100);
    match service::module::list_cached(pool.get_ref(), cache.get_ref(), query.keyword.as_deref(), page, page_size).await {
        Ok(CachedListResult::FullResponse(body)) => {
            HttpResponse::Ok().content_type("application/json").body(body)
        }
        Ok(CachedListResult::Fresh(data)) => response::success(data),
        Err(err) if err == ERR_INTERNAL_SERVER => response::sys_error(err.message),
        Err(err) => response::biz_error(&err),
    }
}
```

### 预热端点

```rust
pub async fn warmup(
    _req: HttpRequest,
    pool: web::Data<MySqlPool>,
    cache: web::Data<Cache>,
) -> HttpResponse {
    match service::module::warmup_cache(pool.get_ref(), cache.get_ref()).await {
        Ok(count) => response::success(serde_json::json!({"warmed": count})),
        Err(_) => response::sys_error("warmup failed"),
    }
}
```

---

## 8. main.rs 注册规范

### Redis 连接池初始化

```rust
use service::module_cache::Cache;

let cache = match cache::init_redis().await {
    Ok(conns) => {
        log::info!("Redis pool established ({} connections)", conns.len());
        Cache::new(Some(conns))
    }
    Err(e) => {
        log::warn!("Redis not available, caching disabled: {e}");
        Cache::new(None)
    }
};
let cache_data = web::Data::new(cache);
```

如有多个模块需要缓存：

```rust
let product_conns = cache::init_redis().await;
let inventory_conns = cache::init_redis().await;
let product_cache = web::Data::new(ProductCache::new(product_conns.ok()));
let inventory_cache = web::Data::new(InventoryCache::new(inventory_conns.ok()));
```

### 路由注册

```rust
.app_data(product_cache.clone())
.app_data(inventory_cache.clone())
.service(
    web::scope("/api/v1/module")
        .route("", web::post().to(api::module::create))
        .route("", web::get().to(api::module::list))
        .route("/cache", web::get().to(api::module::list_cached))
        .route("/cache/{id}", web::get().to(api::module::get_by_id_cached))
        .route("/warmup", web::post().to(api::module::warmup))
        .route("/{id}", web::get().to(api::module::get_by_id))
        .route("/{id}", web::put().to(api::module::update))
        .route("/{id}", web::delete().to(api::module::delete)),
)
```

**路由顺序原则**：精确路径优先（`/cache/{id}`）放在通配路径（`/{id}`）之前，防止被通配路由抢断。

---

## 9. 完整新增模块 Checklist

- [ ] `models/mod.rs` 添加 `pub mod module;`
- [ ] `repository/mod.rs` 添加 `pub mod module;`
- [ ] `dto/mod.rs` 添加 `pub mod module;`
- [ ] `service/mod.rs` 添加 `pub mod module;` （如有缓存再加 `pub mod module_cache;`）
- [ ] `api/mod.rs` 添加 `pub mod module;`
- [ ] `error.rs` 添加业务错误码
- [ ] `main.rs` 添加 `mod module;`、初始化 Cache、注册路由