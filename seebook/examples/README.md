# 四维矩阵数据库可视化 - 示例

## 快速开始

### 1. 使用模拟数据体验

```bash
cd /Users/blue/seebook
python examples/demo_with_mock.py
```

这将启动一个交互式可视化界面，展示：
- 动态发现的业务域（不预设）
- 25个模拟表
- 四维矩阵的颜色编码

### 1.5 未知数据库动态分类演示（重点推荐）

```bash
python examples/unknown_database_demo.py
```

**本演示展示核心能力：**
- 处理完全未知结构的数据库
- 表名混乱：`tbl_usr`, `loginHistory`, `txn_main`
- 动态发现业务域（不预设user/revenue等）
- 适应不同命名规范：snake_case, camelCase, 无意义命名

**输出示例：**
```
发现 4 个主题域:
  Z=0: cluster_6_tables (用户+交易相关)
  Z=1: sku (商品库存)
  Z=2: config (配置表)
  Z=3: data (遗留数据)
```

### 2. 连接真实数据库

#### PostgreSQL

```bash
cd /Users/blue/seebook
python -m hypercube.cli scan \
    --db postgres \
    --host localhost \
    --user postgres \
    --password your_password \
    --database mydb \
    --output mydb_hypercube.json \
    --visualize
```

#### MySQL

```bash
python -m hypercube.cli scan \
    --db mysql \
    --host localhost \
    --user root \
    --password your_password \
    --database mydb \
    --visualize
```

### 3. 通过颜色查询数据

```bash
# 查找与蓝色相似的表
python -m hypercube.cli query \
    --input mydb_hypercube.json \
    --color "#3498db" \
    --threshold 50
```

## 核心概念

### 四维矩阵架构

```
矩阵一 (DataMatrix): 存储完整元数据
  ├── t: 时间维度（数据更新时间）
  ├── x: 业务阶段（new/growth/mature/legacy）
  ├── y: 量级（行数的对数压缩）
  ├── z: 主题分类（业务域）
  └── payload: 完整JSON记录

矩阵二 (ColorMatrix): 存储视觉编码
  └── 每个坐标对应一个RGB颜色
      ├── z → 色相（业务域识别）
      ├── x → 饱和度（阶段特征）
      ├── y → 亮度（量级大小）
      └── t → 色温偏移（时间流动）
```

### 颜色映射规则

| 维度 | 视觉通道 | 示例 |
|------|---------|------|
| Z (主题) | 色相 | 用户域=蓝(200°), 营收域=绿(120°) |
| X (阶段) | 饱和度 | 新建=30%, 成熟=90% |
| Y (量级) | 亮度 | 小表=暗, 大表=亮 |
| T (时间) | 色温偏移 | 随时间从冷色变暖色 |

## 可视化界面

启动后会看到：

1. **主视图**: XY平面散点图（可切换XZ/YZ/3D）
   - X轴: 业务阶段
   - Y轴: 量级（对数压缩）
   - 颜色: 由Z/X/Y/T共同决定

2. **颜色趋势**: 时间维度上的颜色变化

3. **详情面板**: 点击数据点查看完整JSON元数据

## 编程接口

```python
from hypercube.core.hypercube import HyperCube
from hypercube.core.data_matrix import DataCell
from hypercube.connectors.postgres import PostgresConnector

# 连接数据库
connector = PostgresConnector({
    "host": "localhost",
    "user": "postgres",
    "password": "xxx",
    "database": "mydb"
})
connector.connect()

# 获取元数据
metadata_list = connector.get_all_tables_metadata()

# 构建超立方体
hypercube = HyperCube()

for meta in metadata_list:
    cell = DataCell(
        t=datetime.now(),
        x=infer_stage(meta),      # 业务阶段
        y=normalize_rows(meta),   # 量级
        z=infer_domain(meta),     # 主题分类
        table_name=meta.table_name,
        # ... 其他属性
    )
    hypercube.add_cell(cell)

# 同步颜色
hypercube.sync_color_matrix()

# 颜色查询
results = hypercube.query_by_color("#3498db")

# 区域查询
region = hypercube.query_by_visual_region(z=0, x_range=(30, 60))

# 导出可视化
viz_data = hypercube.export_for_visualization()
```
