# four-dim-matrix

A dual-matrix knowledge system that converts relational databases into an
intuitive four-dimensional colour space.

## Concept

Two mirrored four-dimensional matrices let you *see* an entire database at a
glance:

| Coordinate | Meaning (schema mode) | Visual encoding |
|---|---|---|
| `t` | Snapshot timestamp | Colour temperature shift over time |
| `x` | Column count (schema width) | Saturation |
| `y` | Row count (data volume) | Lightness |
| `z` | Table index (each table = one topic) | Hue |

**Matrix 1 – Data Matrix**: stores full column/table metadata as a JSON
payload at every `(t, x, y, z)` address.

**Matrix 2 – Colour Matrix**: stores a `#rrggbb` colour at the same address.
Hovering over any colour block in Matrix 2 reveals the corresponding data
record in Matrix 1.

Loading a database schema into the two matrices *is* the act of rapid
database cognition: large, wide tables appear as bright, vivid blocks; small
lookup tables appear as muted specks; schema changes between snapshots are
detected automatically via `diff()`.

## Quick start

```python
import sqlite3
from four_dim_matrix import DatabaseAdapter

# Point at any SQLite file (or use from_connection for PostgreSQL / MySQL)
adapter = DatabaseAdapter.from_sqlite("my_database.db")
kb = adapter.to_knowledge_base()

# Inspect the colour snapshot – one topic per table
snap = kb.snapshot(t=adapter.snapshot_time)
for topic in snap["topics"]:
    print(topic["hex_color"], topic["total_y"], "rows –", topic)

# Reverse-lookup: colour block → full table metadata
results = kb.lookup_by_color("#3d6e9e")
print(results[0].payload)

# Detect schema drift between two snapshots
adapter2 = DatabaseAdapter.from_sqlite("my_database.db")  # re-introspect later
print(adapter.diff(adapter2))
```

## Package layout

```
four_dim_matrix/
├── data_matrix.py        # DataPoint + DataCell + DataMatrix (sparse 4D data store)
├── color_matrix.py       # ColorPoint + ColorMatrix (4D colour store)
├── color_mapping.py      # ColorConfig + ColorMapper (HSL colour mapping)
├── knowledge_base.py     # KnowledgeBase (high-level dual-matrix API)
├── db_adapter.py         # DatabaseAdapter (DB → both matrices in one call)
│
│  ── seebook 集成新增 ──────────────────────────────────────────────────────
├── hypercube.py          # HyperCube + RichDataMatrix + RichColorMatrix
├── lineage.py            # LineageTracker / Provenance（数据血缘追踪）
├── dynamic_classifier.py # DynamicDomainDiscoverer / UnknownDatabaseProcessor（无预设域动态聚类）
├── quality.py            # QualityEngine（颜色异常检测 + 结构合规评分）
├── relationship_extractor.py  # RelationshipExtractor（外键提取 + 命名推断）
├── changelog.py          # ChangeTracker（版本快照 + 增量变更追踪）
├── optimizer.py          # MatrixOptimizer（第一阶段 → 第二阶段矩阵优化）
├── dashboard.py          # create_hypercube_dashboard（Dash 交互式可视化）
├── cli.py                # CLI 命令行工具
└── connectors/
    ├── base.py           # BaseConnector（连接器基类）
    ├── postgres.py       # PostgresConnector
    └── mysql.py          # MySQLConnector
```

---

## seebook 集成说明

本项目已吸收 **seebook** 的核心优势，提升了以下能力：

### 1. 动态主题域发现（`dynamic_classifier.py`）

**旧方式**：需要预先定义 `user / revenue / product` 等固定业务域。  
**新能力**：`UnknownDatabaseProcessor` 通过 **外键关系图 + 表名语义相似度 +
列结构特征** 三路聚类，完全自适应地发现主题域，无需任何预设配置。适用于
遗留系统、未知数据库、混乱命名场景。

```python
from four_dim_matrix import UnknownDatabaseProcessor

processor = UnknownDatabaseProcessor()
result = processor.process(raw_metadata_list)
# result["domain_mapping"]   → {table_name: z_index}
# result["lifecycle_mapping"] → {table_name: "new"/"growth"/"mature"/"legacy"}
# result["domains"]           → {z_index: {"name": ..., "tables": [...]}}
```

### 2. 数据血缘追踪（`lineage.py`）

**新能力**：`LineageTracker` 将每个 `DataCell` 与其物理数据源绑定，记录从
原始数据库表到四维矩阵单元格的完整转换链（支持两阶段矩阵溯源）。

```python
from four_dim_matrix import LineageTracker, PhysicalLocation, TransformationType

tracker = LineageTracker()
loc = PhysicalLocation(db_type="postgres", host="db", port=5432,
                       database="prod", schema="public", table="orders")
tracker.register_first_stage("cell_orders_2024", loc)

# 查找某个物理位置影响的所有矩阵单元格
report = tracker.generate_impact_report(loc)
```

### 3. 数据质量引擎（`quality.py`）

**新能力**：`QualityEngine` 在四维颜色空间中发现质量问题：
- **颜色孤立点**：同域内与其他表颜色差异过大 → 可能分类错误
- **颜色漂移**：同一物理表在不同时间颜色发生突变 → 可能经历重大变更
- **结构合规**：无主键、无索引、宽表反模式等问题检测

```python
from four_dim_matrix import HyperCube, QualityEngine

cube = HyperCube()
# ... 填充数据 ...
engine = QualityEngine()
scores = engine.evaluate(cube)
report = engine.generate_report(scores)
```

### 4. 富数据单元格 `DataCell`（`data_matrix.py`）

**新能力**：在原有轻量 `DataPoint` 基础上，新增携带完整数据库表元数据的
`DataCell`，包含：
- `table_name / schema_name / column_count / row_count / size_bytes`
- `business_domain / lifecycle_stage / tags`
- `provenance`：血缘溯源信息

### 5. 双矩阵管理器 `HyperCube`（`hypercube.py`）

**新能力**：`HyperCube` 是 seebook 风格的双矩阵管理器，使用字典型稀疏存储
（`RichDataMatrix` + `RichColorMatrix`），支持：
- `query_by_color(hex, threshold)` — 颜色 → 数据反查
- `query_by_visual_region(z, x_range, y_range)` — 框选区域查询
- `get_business_trend(z)` — 按主题域获取时间趋势
- `get_color_flow(z)` — 颜色流动动画序列
- `export_for_visualization()` — 导出 Plotly/Dash 可视化格式

### 6. 关系提取器（`relationship_extractor.py`）

**新能力**：`RelationshipExtractor` 从 MySQL/PostgreSQL 中提取真实外键，并
对无外键定义的表基于列命名约定（`user_id → users.id`）推断隐式关联，发现
孤立表和循环依赖。

### 7. 变更追踪（`changelog.py`）

**新能力**：`ChangeTracker` 支持版本快照和增量变更检测，追踪表的新增、删除、
结构修改、主题域调整、生命周期变更等，并计算每次变更的影响评分。

### 8. 矩阵优化器（`optimizer.py`）

**新能力**：`MatrixOptimizer` 分析第一阶段矩阵，识别潜在的优化机会：
- **合并**：空间位置相近的表 → 宽表合并建议
- **重分类**：跨域但颜色相似的表 → 主题域重新分配
- **归档**：处于 legacy 阶段的低活跃表 → 归档建议

生成第二阶段矩阵并自动建立完整血缘链。

### 9. 数据库连接器（`connectors/`）

**新能力**：原生 PostgreSQL / MySQL 连接器（SQLAlchemy 驱动），带表元数据
自动推断（`business_domain` / `lifecycle_stage`）。

### 10. CLI 命令行工具（`cli.py`）

**新能力**：一条命令扫描任意 PostgreSQL/MySQL 数据库，自动构建四维矩阵并
启动可视化仪表盘。

```bash
python -m four_dim_matrix.cli scan \
    --db postgres \
    --host localhost \
    --user postgres \
    --password secret \
    --database mydb \
    --visualize
```

---

## 依赖安装

```bash
# 核心功能（无额外依赖）
pip install four-dim-matrix

# seebook 新增功能（numpy + SQLAlchemy + colorspacious）
pip install "four-dim-matrix[seebook]"

# 含 PostgreSQL/MySQL 连接器
pip install "four-dim-matrix[seebook-db]"

# 含 Dash 可视化仪表盘
pip install "four-dim-matrix[seebook-viz]"

# 全部功能
pip install "four-dim-matrix[all]"
```
