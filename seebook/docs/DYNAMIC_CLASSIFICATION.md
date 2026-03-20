# 动态主题域发现

## 问题

传统的数据治理系统通常预设固定的业务域（如user、revenue、product等），但：
1. **每个公司的业务不同** - 没有统一的域划分标准
2. **遗留系统命名混乱** - 无法通过表名推断业务含义
3. **新领域无参考** - AI、IoT等新兴领域没有成熟模型

## 解决方案：完全动态发现

系统不预设任何业务域，完全基于数据本身的特征进行动态聚类。

## 发现算法

### 1. 特征提取

从每个表提取多维特征：

```python
TableSignature {
    # 命名特征
    name_tokens: ["tbl", "usr"]  # 从tbl_usr提取
    
    # 结构特征
    column_names: ["uid", "uname", "email_addr"]
    column_types: ["bigint", "varchar", "varchar"]
    has_timestamp: true
    has_soft_delete: false
    
    # 关系特征
    foreign_keys: [{  # 关键！最强的聚类信号
        column: "usr_uid",
        ref_table: "tbl_usr",
        ref_column: "uid"
    }]
    
    # 规模特征
    row_count: 4800000
    column_count: 4
}
```

### 2. 多维度聚类

#### 2.1 关系图聚类（最强信号）

基于外键关系构建图，关联的表自然聚为一类：

```
tbl_usr ← usr_profile_data (外键关联)
   ↑
   └─ loginHistory (外键关联)
   └─ txn_main (外键关联)
          ↓
          txn_items, pay_record

→ 聚类结果: 用户+交易相关表在同一个域
```

#### 2.2 命名相似度聚类

计算表名的Jaccard相似度：

```python
# usr_profile_data vs user_logs
tokens1 = {"usr", "profile", "data"}
tokens2 = {"user", "logs"}
similarity = |交集| / |并集| = 0.3

阈值: 0.3 以上认为相关
```

#### 2.3 结构相似度聚类

相似结构的表可能属于同一域：

```
都有: [id, created_at, updated_at]
→ 可能是核心业务表

都有: [log_id, level, message, timestamp]
→ 可能是日志表
```

### 3. 动态命名

不预设名称，基于聚类结果自动生成：

```python
# 提取公共token
all_tokens = [
    {"txn", "main"},
    {"txn", "items"},
    {"pay", "record"}
]
common = {"txn"}  # 交集

→ 域名: "txn"

# 如果没有公共token，使用最长前缀
tables = ["AccountMaster", "AccountBalance"]
prefix = "Account"

→ 域名: "Account"

# 回退命名
tables = ["T001", "T002"]
→ 域名: "cluster_2_tables"
```

## 自适应生命周期分类

不预设规则，基于数据统计分布：

```python
# 计算行数分布
row_counts = [100, 5000, 1000000, 5000000, ...]
p20 = percentile(row_counts, 20)  # 小表阈值
p80 = percentile(row_counts, 80)  # 大表阈值

# 分类逻辑
if row_count <= p20:
    → "new" (可能是新建表)
elif row_count >= p80:
    → "mature" (大表通常是核心业务)
elif 有外键且不被引用:
    → "growth" (正在扩展)
elif 孤立且数据量小:
    → "legacy" (可能废弃)
```

## 使用示例

### 完全未知的数据库

```python
from hypercube.core.dynamic_classifier import UnknownDatabaseProcessor

# 原始元数据（从任意数据库扫描得到）
raw_metadata = [
    {"table_name": "tbl_usr", "columns": [...], "foreign_keys": [], ...},
    {"table_name": "usr_profile_data", "columns": [...], 
     "foreign_keys": [{"column": "usr_uid", "ref_table": "tbl_usr"}]},
    {"table_name": "txn_main", "columns": [...], ...},
    ...
]

# 动态分析
processor = UnknownDatabaseProcessor()
result = processor.process(raw_metadata)

# 结果
print(result['domains'])
# {
#   0: {
#       "name": "cluster_6_tables",  # 自动命名
#       "tables": ["tbl_usr", "usr_profile_data", ...],
#       "description": "包含6张表，共104M行，存在表间关联"
#   },
#   1: {"name": "sku", "tables": [...]},
#   ...
# }
```

### 不同命名规范的适应性

```python
# snake_case
"customers", "customer_addresses", "orders", "order_items"
→ 域名: "customer" (公共前缀)

# camelCase
"AccountMaster", "AccountBalance", "TransactionLog"
→ 域名: "Account" (公共前缀)

# 无意义命名
"T001", "T002", "CONFIG"
→ 域名: "T00" (最长公共前缀) / "config" (独立token)
```

## 优势

| 传统方法 | 动态发现 |
|---------|---------|
| 需要预设业务域模型 | 完全自适应 |
| 无法处理命名混乱 | 基于关系聚类 |
| 新领域需要人工标注 | 自动发现新模式 |
| 表名必须规范 | 支持任意命名规范 |

## 局限性

1. **需要外键信息** - 如果数据库没有外键约束，聚类效果会下降
2. **首字母缩写难以理解** - "T001"无法自动推断含义
3. **跨域关联表** - 如"用户-订单关联表"可能被分到任一域

## 建议

1. **启用外键约束** - 帮助系统更准确理解业务关系
2. **人工审核首版** - 确认自动发现的域划分是否合理
3. **迭代优化** - 根据反馈调整聚类阈值
