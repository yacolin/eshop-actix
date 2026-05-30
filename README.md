# eshop-actix

基于 Rust + Actix-Web + sqlx 构建的 RESTful 电商后端服务。

## 技术栈

| 技术 | 用途 |
|------|------|
| [actix-web](https://actix.rs/) | Web 框架及 HTTP Server |
| [sqlx](https://github.com/launchbadge/sqlx) | 异步 MySQL 驱动（编译期 SQL 检查） |
| [serde](https://serde.rs/) | 序列化/反序列化 |
| [chrono](https://github.com/chronotope/chrono) | 时间日期处理 |
| [uuid](https://github.com/uuid-rs/uuid) | 请求追踪 ID 生成 |
| [env_logger](https://docs.rs/env_logger/) | 日志输出 |

## 快速开始

### 环境要求

- Rust 1.85+ (Edition 2024)
- MySQL 8.0+

### 配置

创建 `.env` 文件或设置环境变量：

```bash
DATABASE_URL=mysql://user:password@localhost:3306/eshop
```

### 启动

```bash
# 开发模式（含详细日志）
RUST_LOG=debug cargo run

# 生产模式
RUST_LOG=info cargo run

# 或使用项目脚本
./run.sh
```

服务默认监听 `http://127.0.0.1:8080`。

## 项目结构

```
src/
├── api/              # HTTP 请求处理器（Handler）
│   ├── mod.rs
│   ├── response.rs   # 统一响应封装
│   └── product.rs    # 商品相关接口
├── dto/              # 数据传输对象（请求/响应结构体）
│   ├── mod.rs
│   └── product.rs
├── models/           # 数据库模型（FromRow）
│   ├── mod.rs
│   └── product.rs
├── repository/       # 数据访问层（SQL 查询）
│   ├── mod.rs
│   └── product.rs
├── service/          # 业务逻辑层
│   ├── mod.rs
│   └── product.rs
├── middleware/        # 中间件
│   ├── mod.rs
│   ├── trace.rs      # 请求追踪 ID
│   └── error_handler.rs  # 全局错误处理 & panic 恢复
├── db.rs             # 数据库连接池
├── error.rs          # 业务错误码
└── main.rs           # 入口 & 路由注册
```

### 分层架构

```
Client → Middleware(Logger → Trace → ErrorHandler) → Handler(api)
                                                          ↓
                                                      Service
                                                          ↓
                                                     Repository
                                                          ↓
                                                       MySQL
```

## API 接口

### 商品管理

| 方法 | 路径 | 说明 |
|------|------|------|
| `POST`   | `/api/v1/products`          | 创建商品 |
| `GET`    | `/api/v1/products`          | 商品列表（分页+关键词） |
| `GET`    | `/api/v1/products/{id}`     | 查询单个商品 |
| `PUT`    | `/api/v1/products/{id}`     | 更新商品 |
| `DELETE` | `/api/v1/products/{id}`     | 软删除商品 |

### 其他接口

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET`    | `/`              | 健康检查 |
| `GET`    | `/db_status`     | 数据库连接状态 |
| `GET`    | `/unauthorized`  | 未授权测试 |
| `POST`   | `/echo`          | 回显测试 |
| `GET`    | `/trace`         | 追踪 ID 测试 |

### 请求示例

```bash
# 创建商品
curl -X POST http://localhost:8080/api/v1/products \
  -H "Content-Type: application/json" \
  -d '{"name":"iPhone 15","price":699900,"sku":"IP15-BLK"}'

# 商品列表（分页）
curl "http://localhost:8080/api/v1/products?page=1&page_size=20"

# 商品列表（关键词搜索）
curl "http://localhost:8080/api/v1/products?keyword=iphone"

# 查询单个商品
curl http://localhost:8080/api/v1/products/1

# 更新商品
curl -X PUT http://localhost:8080/api/v1/products/1 \
  -H "Content-Type: application/json" \
  -d '{"name":"iPhone 15 Pro","price":899900}'

# 删除商品
curl -X DELETE http://localhost:8080/api/v1/products/1
```

## 统一响应格式

所有接口返回统一的 JSON 结构：

```json
{
  "code": 0,
  "message": "success",
  "data": {},
  "trace_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

### 状态码说明

| 响应 `code` | HTTP 状态 | 说明 |
|-------------|-----------|------|
| `0`     | 200 | 成功 |
| `500`   | 500 | 系统内部错误 |
| `1001`  | 404 | 资源不存在 |
| `1002`  | 422 | 参数校验失败 |
| `1004`  | 401 | 未认证 |
| `1007`  | 409 | 资源冲突 |
| `1010`  | 404 | 路由未匹配 |
| `1023`  | 409 | 唯一约束冲突（如 SKU 重复） |
| `2001`  | 404 | 用户不存在 |
| `2002`  | 403 | 无权限 |

## 中间件（按注册顺序）

1. **TraceMiddleware** — 为每个请求生成 `trace_id`（UUID v4），注入请求扩展中，贯穿整条请求链路
2. **ErrorHandler** — 捕获 panic 和 Actix 服务层错误，返回统一错误响应，避免进程崩溃
3. **Logger** — 请求日志，格式：`[时间] | 方法 路径 | 状态 | 字节数 | 耗时`

## 数据库

### 商品表（products）

```sql
CREATE TABLE `products` (
  `id`          bigint       NOT NULL AUTO_INCREMENT,
  `name`        varchar(255) NOT NULL,
  `description` text,
  `price`       bigint       NOT NULL COMMENT '单位：分',
  `sku`         varchar(100) NOT NULL,
  `created_at`  timestamp    NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at`  timestamp    NULL DEFAULT CURRENT_TIMESTAMP,
  `deleted_at`  datetime(3)  NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_products_sku` (`sku`),
  KEY `idx_products_deleted_at` (`deleted_at`)
);
```

> `price` 以"分"为单位存储，避免浮点数精度问题。

## 开发

### 运行

```bash
RUST_LOG=debug cargo run
```

### 构建

```bash
cargo build --release
```

## 编码规范

本项目遵循 [code-standard](.trae/skills/code-standard/SKILL.md) 中定义的 Rust/Actix-Web 编码规范，包括分层架构、命名规则、错误处理、API 响应格式等约定。