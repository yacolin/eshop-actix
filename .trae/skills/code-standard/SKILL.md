---
name: "code-standard"
description: "eshop-actix 项目的 Rust/Actix-Web 编码规范。定义模块分层、命名规则、错误处理、API 响应格式、数据库访问、中间件等标准。当开发者添加功能、修复 bug 或重构代码时应参考此规范。"
---

# eshop-actix 编码规范

## 1. 项目结构

```
src/
├── api/          ← HTTP 请求处理器（handler），负责参数提取、输入校验、调用 service、返回响应
│   ├── mod.rs
│   ├── response.rs     ← 统一响应封装
│   └── product.rs      ← 按业务模块划分
├── dto/          ← 数据传输对象（请求/响应结构体）
│   ├── mod.rs
│   └── product.rs
├── models/       ← 数据库模型（sqlx::FromRow）
│   ├── mod.rs
│   └── product.rs
├── repository/   ← 数据访问层（纯 SQL 查询，返回 Result<_, sqlx::Error>）
│   ├── mod.rs
│   └── product.rs
├── service/      ← 业务逻辑层（调用 repository，返回 Result<_, BizError>）
│   ├── mod.rs
│   └── product.rs
├── middleware/    ← 中间件（如 TraceMiddleware, ErrorHandler）
│   ├── mod.rs
│   ├── trace.rs
│   └── error_handler.rs
├── db.rs         ← 数据库连接池初始化
├── error.rs      ← 业务错误码定义
└── main.rs       ← 入口，注册模块 + 路由 + 中间件
```

### 分层调用关系

```
Client → Middleware(Logger → Trace → ErrorHandler) → Handler(api)
                                                          ↓
                                                      Service
                                                          ↓
                                                     Repository
                                                          ↓
                                                       (sqlx)
```

- **Handler** 只负责：提取参数、输入校验、调用 service、将结果转为 `HttpResponse`
- **Service** 只负责：业务逻辑、调用 repository、返回 `Result<T, BizError>`
- **Repository** 只负责：执行 SQL、返回 `Result<T, sqlx::Error>`

## 2. 命名规范

| 类别                      | 规范                 | 示例                                           |
| ------------------------- | -------------------- | ---------------------------------------------- |
| 类型（struct/enum/trait） | PascalCase           | `Product`, `BizError`, `TraceMiddleware`       |
| 函数/方法                 | snake_case           | `create_product`, `get_trace_id`               |
| 变量/参数                 | snake_case           | `pool`, `trace_id`, `page_size`                |
| 常量                      | SCREAMING_SNAKE_CASE | `ERR_PRODUCT_NOT_FOUND`, `ERR_INTERNAL_SERVER` |
| 模块文件                  | snake_case           | `product.rs`, `error_handler.rs`               |
| 路由路径                  | kebab-case           | `/api/v1/products`, `/db_status`               |
| JSON 字段                 | snake_case           | `trace_id`, `page_size`, `created_at`          |
| 错误码                    | 数字，按域分段       | 500(系统), 1001-1999(通用), 2001-2999(权限)    |

## 3. 错误处理

### 错误码定义（error.rs）

```rust
pub struct BizError {
    pub code: i64,
    pub message: &'static str,
}

impl BizError {
    pub const fn new(code: i64, message: &'static str) -> Self {
        BizError { code, message }
    }
}
```

**错误码分段规则：**

- `500` — 系统内部错误（DB 异常等）
- `1001-1999` — 通用业务错误
- `2001-2999` — 权限/认证错误

### 错误码 → HTTP 状态映射（middleware/error_handler.rs）

```rust
pub fn map_biz_error_to_status(err: &BizError) -> StatusCode {
    match err.code {
        500 => StatusCode::INTERNAL_SERVER_ERROR,
        1004 => StatusCode::UNAUTHORIZED,
        2002 => StatusCode::FORBIDDEN,
        1008 => StatusCode::BAD_GATEWAY,
        1001 | 1005 | 1006 | 1010 | 2001 => StatusCode::NOT_FOUND,
        1007 | 1023 => StatusCode::CONFLICT,
        _ => StatusCode::BAD_REQUEST,
    }
}
```

**新增错误码的步骤：**

1. 在 `error.rs` 中添加 `pub const ERR_XXX: BizError = BizError::new(code, "message");`
2. 如果需要特殊 HTTP 状态，在 `error_handler.rs` 的 `map_biz_error_to_status` 中增加映射

### Service 层错误处理模式

```rust
// 系统错误 → 使用 ERR_INTERNAL_SERVER
let product = repository::product::find_by_id(pool, id)
    .await
    .map_err(|e| {
        log::error!("[product_service] find_by_id error: {e}");
        ERR_INTERNAL_SERVER
    })?;

// 业务错误 → 使用具体错误码
product.ok_or(ERR_PRODUCT_NOT_FOUND)
```

### Handler 层错误处理模式

```rust
match service::product::create_product(pool.get_ref(), inner).await {
    Ok(product) => match trace_id {
        Some(tid) => response::success_with_trace(product, tid),
        None => response::success(product),
    },
    Err(err) if err == ERR_INTERNAL_SERVER => match trace_id {
        Some(tid) => response::sys_error_with_trace(err.message, tid),
        None => response::sys_error(err.message),
    },
    Err(err) => match trace_id {
        Some(tid) => response::biz_error_with_trace(&err, tid),
        None => response::biz_error(&err),
    },
}
```

**原则：** 系统错误（DB 异常等）走 `sys_error` 变体系列 → HTTP 500，业务错误走 `biz_error` 变体系列 → 按 code 映射 4xx。

## 4. API 响应格式

### 统一响应结构

```rust
pub struct ApiResponse<T: Serialize> {
    pub code: i32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
}
```

### 响应函数族（api/response.rs）

| 函数                                  | HTTP 状态    | code     | 适用场景                         |
| ------------------------------------- | ------------ | -------- | -------------------------------- |
| `success(data)`                       | 200          | 0        | 成功响应                         |
| `success_with_trace(data, tid)`       | 200          | 0        | 成功响应（有 trace_id）          |
| `biz_error(&err)`                     | 按 code 映射 | err.code | 业务错误                         |
| `biz_error_with_status(&err, status)` | 自定义       | err.code | 需要覆盖默认 HTTP 状态的业务错误 |
| `biz_error_with_trace(&err, tid)`     | 按 code 映射 | err.code | 业务错误（有 trace_id）          |
| `invalid_args(message)`               | 422          | 1002     | 输入参数校验失败                 |
| `sys_error(err)`                      | 500          | 500      | 系统内部错误                     |
| `sys_error_with_trace(err, tid)`      | 500          | 500      | 系统内部错误（有 trace_id）      |

### 响应示例

**成功：**

```json
{"code":0,"message":"success","data":{...}}
```

**成功（带追踪）：**

```json
{"code":0,"message":"success","data":{...},"trace_id":"550e8400-e29b-41d4-a716-446655440000"}
```

**业务错误：**

```json
{ "code": 1001, "message": "product not found" }
```

**系统错误：**

```json
{ "code": 500, "message": "internal server error" }
```

## 5. Handler 编写规范

### 标准模板

```rust
pub async fn action_name(
    req: HttpRequest,                          // 可选，需要 trace_id 时添加
    pool: web::Data<MySqlPool>,                // 数据库连接池
    body: web::Json<CreateRequest>,            // 请求体
    path: web::Path<i64>,                      // 路径参数
    query: web::Query<ListQuery>,              // 查询参数
) -> HttpResponse {
    let trace_id = get_trace_id(&req);
    let inner = body.into_inner();          // body 提前解包以便校验

    // 1. 输入校验 → invalid_args
    if inner.name.trim().is_empty() {
        return response::invalid_args("name is required");
    }

    // 2. 调用 service
    match service::domain::action(pool.get_ref(), inner).await {
        // 3. 转换结果 → 匹配 trace_id 调用对应的响应函数
        Ok(data) => match trace_id { ... },
        Err(err) if err == ERR_INTERNAL_SERVER => match trace_id { ... },
        Err(err) => match trace_id { ... },
    }
}
```

### 路由注册（main.rs）

```rust
.service(
    web::scope("/api/v1/products")
        .route("", web::post().to(api::product::create))
        .route("", web::get().to(api::product::list))
        .route("/{id}", web::get().to(api::product::get_by_id))
        .route("/{id}", web::put().to(api::product::update))
        .route("/{id}", web::delete().to(api::product::delete)),
)
```

- 路由路径使用 kebab-case
- 版本前缀 `/api/v{n}/`
- Scope 分组相关路由

## 6. 数据库访问

### 模型定义

```rust
#[derive(Debug, FromRow, Serialize)]
pub struct Product {
    pub id: i64,
    pub name: String,
    pub description: Option<String>,
    pub price: i64,
    pub sku: String,
    pub created_at: Option<DateTime<Utc>>,     // TIMESTAMP → DateTime<Utc>
    pub updated_at: Option<DateTime<Utc>>,     // TIMESTAMP → DateTime<Utc>
    pub deleted_at: Option<NaiveDateTime>,     // DATETIME → NaiveDateTime
}
```

### 类型映射规则

| MySQL 类型     | Rust 类型                   | Feature       |
| -------------- | --------------------------- | ------------- |
| `BIGINT`       | `i64`                       | -             |
| `VARCHAR/TEXT` | `String` / `Option<String>` | -             |
| `TIMESTAMP`    | `Option<DateTime<Utc>>`     | sqlx + chrono |
| `DATETIME`     | `Option<NaiveDateTime>`     | sqlx + chrono |

### Repository 编写规范

- 使用 `sqlx::query_as::<_, Model>(sql)` 查询
- 使用 `?` 占位符绑定参数
- 软删除使用 `UPDATE SET deleted_at = NOW(3)` + `WHERE deleted_at IS NULL` 过滤
- 分页查询：`LIMIT ? OFFSET ?`
- 动态条件：使用 `format!` 构建 SQL，注意生命周期管理

```rust
// 生命周期管理：pattern 必须在 q 持有引用期间存活
let mut q = sqlx::query_as::<_, Product>(&sql);
let pattern = keyword.map(|kw| format!("%{}%", kw));
if let Some(ref pat) = pattern {
    q = q.bind(pat).bind(pat);
}
q = q.bind(limit).bind(offset);
q.fetch_all(pool).await
```

## 7. 中间件规范

```rust
// Transform + Service 两层结构
impl<S, B> Transform<S, ServiceRequest> for MyMiddleware
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{ ... }

impl<S, B> Service<ServiceRequest> for MyMiddlewareService<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    forward_ready!(service);

    fn call(&self, req: ServiceRequest) -> Self::Future {
        // 前置处理
        // self.service.call(req)
        // 后置处理
    }
}
```

### 中间件注册顺序（main.rs）

```rust
.wrap(TraceMiddleware)    // 最先执行，生成 trace_id
.wrap(ErrorHandler)       // 捕获 panic 和服务错误
.wrap(Logger::new(...))   // 请求日志
```

**中间件执行顺序：** 从外到内 wrap → 从内到外执行。`TraceMiddleware` 在 wrap 列表最外层但最先处理请求。

## 8. 输入校验规则

| 字段类型                                | 校验规则             | response 函数                                         |
| --------------------------------------- | -------------------- | ----------------------------------------------------- |
| `String`                                | `.trim().is_empty()` | `invalid_args("xxx is required")`                     |
| `i64` (price)                           | `<= 0`               | `invalid_args("price must be positive")`              |
| `Option<Vec>`/`Option<String>` (update) | 至少一个有值         | `invalid_args("at least one field must be provided")` |
| 分页                                    | `clamp(1, 100)`      | 不用报错，自动修正                                    |

## 9. 日志规范

- 使用 `log` crate（通过 `env_logger` 输出）
- Service 层日志格式：`[domain_service] action error: {error_detail}`
- 系统错误使用 `log::error!` 输出详细错误到日志，响应中只返回"internal server error"
- 业务错误不需要额外日志（由 handler 的响应状态体现）
- 请求日志由 `Logger` 中间件统一处理

## 10. Cargo.toml 依赖管理

**核心依赖：**

- `actix-web` — Web 框架
- `actix-rt` — 运行时
- `sqlx` (features: `runtime-async-std-native-tls`, `mysql`, `chrono`) — 数据库
- `serde` + `serde_json` — 序列化
- `chrono` (feature: `serde`) — 时间处理
- `env_logger` + `log` — 日志
- `uuid` (feature: `v4`) — 生成 trace_id

## 11. 模块注册

在 `main.rs` 中添加新模块的固定步骤：

```rust
mod api;
mod db;
mod dto;          // 新增
mod error;
mod middleware;
mod models;       // 新增
mod repository;   // 新增
mod service;      // 新增
```

1. 创建模块目录和 `mod.rs` 文件
2. 在父模块的 `mod.rs` 中添加 `pub mod xxx;`
3. 在 `main.rs` 中添加 `mod xxx;` 声明

## 12. 禁止事项

- 禁止在 Handler/Service/Repository 中直接 panic（使用 `Result` 传播错误）
- 禁止将敏感信息（DB 连接串、密钥）硬编码在代码中
- 禁止暴露系统错误详情给客户端（DB error 只记录日志，不返回给客户端）
- 禁止在代码中添加无用注释
- 禁止在文件名、类型名、字段名中使用非英文命名
- 禁止使用 `unwrap()`（使用 `?` 或 明确的错误处理）
