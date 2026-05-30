---
alwaysApply: true
scene: git_message
---

# Commit Message 规范

## 格式

```
<type>(<scope>): <description>

[optional body]

[optional footer]
```

## 类型 (Type)

| 类型     | 描述           |
| -------- | -------------- |
| feat     | 新功能         |
| fix      | 修复bug        |
| refactor | 代码重构       |
| docs     | 文档更新       |
| style    | 代码风格调整   |
| test     | 测试相关       |
| chore    | 构建或依赖更新 |
| Merge    | 合并分支       |

## 范围 (Scope)

- `api` - API接口
- `routes` - 路由定义
- `rbac` - 权限管理
- `middleware` - 中间件
- `service` - 服务层
- `handler` - 请求处理器
- `model` - 数据模型
- `dto` - 数据传输对象
- `config` - 配置
- `db` - 数据库
- `mq` - 消息队列
- `docker` - Docker相关
- `nginx` - 反向代理

## 描述规则

- 使用中文描述
- 简洁明了，不超过50个字符
- 以动词开头，如"添加"、"修复"、"重构"、"删除"、"更新"

## 示例

```
feat(api): 添加用户登录API接口

添加了基于JWT的用户登录接口，支持密码登录和手机验证码登录
```

```
fix(routes): 解决用户路由冲突问题

修复了/users/:id和/users/:user_id/roles之间的路由冲突
```

```
refactor(service): 重构订单服务逻辑

将订单服务的业务逻辑拆分为多个子函数，提高代码可读性和可维护性
```

```
fix(middleware): 改进JWT认证中间件错误区分

区分token过期(invalid token)和无效token(token expired)的错误类型，返回精确的错误码
```

```
chore(docker): 添加Docker配置文件并统一JWT密钥

创建各服务的docker配置文件，统一JWT Secret确保跨服务认证一致
```
