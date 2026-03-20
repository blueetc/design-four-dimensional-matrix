# 更新日志

## 2024-03-20 - 重要修复：去除预设业务域

### 问题识别
用户反馈：系统预设了 `user/revenue/product` 等业务域和 `users/orders` 等表名，这与"数据库是未知的"前提矛盾。

### 解决方案
新增 **动态分类器 (`dynamic_classifier.py`)**，实现完全自适应的数据库分析：

#### 1. 动态主题域发现 (`DynamicDomainDiscoverer`)
- **不预设任何业务域**
- 基于外键关系的图聚类（最强信号）
- 基于命名相似度的Jaccard聚类
- 基于结构特征的指纹聚类
- 动态域名生成（提取公共token/前缀）

#### 2. 自适应生命周期分类 (`AdaptiveLifecycleClassifier`)
- **不预设生命周期规则**
- 基于数据量分布的百分位分类
- 结合引用关系判断（孤立表识别）

#### 3. 未知数据库处理器 (`UnknownDatabaseProcessor`)
- 整合上述能力，提供统一接口
- 支持任意命名规范（snake_case, camelCase, 无意义命名）
- 自适应Y轴归一化（不依赖固定阈值）

### 使用方式变更

**旧方式（有预设）：**
```python
domain_to_z = {"user": 0, "revenue": 1, ...}  # 预设
domain = connector.infer_business_domain(name)  # 启发式推断
z = domain_to_z.get(domain, 6)
```

**新方式（动态发现）：**
```python
from hypercube.core.dynamic_classifier import UnknownDatabaseProcessor

processor = UnknownDatabaseProcessor()
result = processor.process(raw_metadata)  # 完全动态
z = result['domain_mapping'][table_name]  # 动态分配的Z轴
```

### 新增文件
- `src/hypercube/core/dynamic_classifier.py` - 动态分类器核心
- `examples/unknown_database_demo.py` - 未知数据库演示
- `docs/DYNAMIC_CLASSIFICATION.md` - 动态分类文档

### 更新文件
- `src/hypercube/cli.py` - 使用动态分类器替换预设逻辑
- `README.md` - 强调动态发现能力
- `PROJECT_SUMMARY.md` - 更新功能清单
- `examples/README.md` - 添加新演示说明

### 演示效果

**输入（混乱命名）：**
```
tbl_usr, usr_profile_data, loginHistory
txn_main, txn_items, pay_record
sku_master, inv_stock
sys_config, old_data_2020
```

**输出（动态发现）：**
```
发现 4 个主题域:
  Z=0: cluster_6_tables (6表, 104M行, 有关联)
  Z=1: sku (2表, 250K行)
  Z=2: config (1表, 100行)
  Z=3: data (1表, 5K行)
```

### 兼容性
- 旧示例仍可用，但推荐使用新演示
- CLI工具已更新为动态分类模式
- API向后兼容，新增模块为可选
