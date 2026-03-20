# 电商平台数据库设计说明书
*Database Design Specification — E-commerce Platform*

> 用法 (Usage):
> ```
> four-dim-matrix scan --db sqlite --sqlite-path mydb.db --spec tasks/example_spec.md
> ```
> 或在任务文件中引用 (or reference from a task YAML):
> ```yaml
> spec:
>   file: tasks/example_spec.md
> ```

description: 电商平台核心业务数据库，涵盖用户、商品、订单、营销和运营五大业务域。

## tables

### customers
- description: 注册用户和会员账号信息，是整个系统的核心实体
- domain: user
- lifecycle: mature
- columns: id, name, email, phone, signup_date, loyalty_tier
- tags: core, pii

### users
- description: 系统登录账号（与 customers 一对一）
- domain: user
- lifecycle: mature
- columns: id, customer_id, username, password_hash, last_login
- tags: auth, pii

### products
- description: 商品目录，包含 SKU、价格和库存
- domain: product
- lifecycle: mature
- columns: id, sku, name, price, stock, category_id, active
- tags: catalog

### categories
- description: 商品分类层级（支持多级嵌套）
- domain: product
- lifecycle: mature
- columns: id, name, parent_id, description
- tags: catalog

### orders
- description: 购买订单主表
- domain: revenue
- lifecycle: mature
- columns: id, customer_id, status, total, created_at, shipped_at
- tags: transaction

### order_items
- description: 订单明细行，与订单和商品多对多关联
- domain: revenue
- lifecycle: mature
- columns: id, order_id, product_id, quantity, unit_price
- tags: transaction

### payments
- description: 支付记录，一个订单可有多次支付（分期等）
- domain: revenue
- lifecycle: growth
- columns: id, order_id, amount, method, status, paid_at
- tags: transaction, finance

### campaigns
- description: 营销活动/促销活动
- domain: marketing
- lifecycle: growth
- columns: id, name, type, starts_at, ends_at, budget, status
- tags: promotion

### promotions
- description: 优惠券/折扣码
- domain: marketing
- lifecycle: mature
- columns: id, code, discount_pct, starts_at, ends_at, usage_count
- tags: promotion

### inventory_log
- description: 库存变动日志，用于审计和溯源
- domain: operations
- lifecycle: mature
- columns: id, product_id, change_qty, reason, logged_at
- tags: audit, log

### audit_log
- description: 系统操作审计日志
- domain: operations
- lifecycle: legacy
- columns: id, user_id, action, target_type, target_id, created_at
- tags: audit, log

### config
- description: 系统配置项（键值对）
- domain: operations
- lifecycle: legacy
- columns: id, key, value, updated_at
- tags: config
