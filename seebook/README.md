# 四维矩阵数据库可视化系统

> 通过双矩阵架构实现数据库的快速认知和查询

## 核心思想

将数据库主题库映射到两个**一比一的四维矩阵**：

### 矩阵一：DataMatrix（数据字典）
- **t**: 时间维度 - 数据更新时间
- **x**: 业务时间 - 表的生命周期阶段（自适应分类）
- **y**: 总量维度 - 表大小/行数（自适应归一化）
- **z**: 类别维度 - **动态发现的业务主题域**（不预设任何域）

存储内容：完整的JSON元数据（表结构、列信息、索引等）

**关键创新：不预设业务域**
- 传统系统预设 user/revenue/product 等固定域
- 本系统通过**外键关系+命名相似度+结构特征**完全动态聚类
- 适应任意未知数据库（遗留系统、新领域、混乱命名）

### 矩阵二：ColorMatrix（视觉编码）
存储每个坐标位置的颜色定义，颜色编码策略：
- **z → 色相(Hue)**: 不同业务域用不同色系区分
- **x → 饱和度(Saturation)**: 生命周期阶段越成熟越饱和
- **y → 亮度(Lightness)**: 量级越大越亮
- **t → 色温偏移**: 时间越近越暖，体现数据新鲜度

### 双矩阵关系
```
用户视角 ──→ ColorMatrix ──→ 颜色认知 ──→ DataMatrix ──→ 具体含义
            (视觉层)                      (数据层)
```

## 快速开始

### 安装依赖

```bash
cd /Users/blue/seebook
pip install -e .
# 或安装开发依赖
pip install -e ".[dev]"
```

### 演示（无需数据库）

```bash
python examples/demo_with_mock.py
```

访问 http://127.0.0.1:8050 查看交互式可视化

### 连接PostgreSQL

```bash
python -m hypercube.cli scan \
    --db postgres \
    --host localhost \
    --user postgres \
    --password your_password \
    --database mydb \
    --visualize
```

### 连接MySQL

```bash
python -m hypercube.cli scan \
    --db mysql \
    --host localhost \
    --user root \
    --password your_password \
    --database mydb \
    --visualize
```

## 项目结构

```
/Users/blue/seebook/
├── src/hypercube/
│   ├── core/
│   │   ├── data_matrix.py      # 矩阵一：数据字典
│   │   ├── color_matrix.py     # 矩阵二：视觉编码
│   │   └── hypercube.py        # 双矩阵管理器
│   ├── connectors/
│   │   ├── base.py             # 连接器基类
│   │   ├── postgres.py         # PostgreSQL连接器
│   │   └── mysql.py            # MySQL连接器
│   ├── visualization/
│   │   └── dashboard.py        # Dash可视化仪表盘
│   └── cli.py                  # 命令行工具
├── examples/
│   ├── demo_with_mock.py       # 模拟数据演示
│   └── README.md               # 使用示例
├── tests/                      # 测试文件
├── pyproject.toml              # 项目配置
└── README.md                   # 本文档
```

## 核心功能

### 1. 数据库扫描与矩阵构建

自动扫描数据库所有表，推断业务域和生命周期阶段，构建四维矩阵。

### 2. 颜色查询

通过颜色快速定位相关数据表：

```python
# 查询与蓝色相似的表
results = hypercube.query_by_color("#3498db", threshold=50)
```

### 3. 区域查询

模拟可视化界面上的框选操作：

```python
# 查询Z=0（用户域）且X在30-60之间的表
region = hypercube.query_by_visual_region(z=0, x_range=(30, 60))
```

### 4. 趋势分析

观察颜色随时间的流动，发现业务趋势：

```python
# 获取用户域的颜色流动序列
flow = hypercube.get_color_flow(z=0)
```

### 5. 交互式可视化

- XY/XZ/YZ平面视图
- 3D散点图
- 颜色趋势图
- 数据点详情查看

## 颜色映射规则详解

| 维度 | 含义 | 视觉通道 | 映射示例 |
|------|------|---------|---------|
| Z | 业务域 | 色相 | 用户=蓝(200°), 营收=绿(120°), 产品=紫(280°) |
| X | 生命周期 | 饱和度 | 新建=30%, 增长=60%, 成熟=90%, 遗留=50% |
| Y | 量级 | 亮度 | 小表=暗(15%), 大表=亮(85%) |
| T | 时间 | 色温偏移 | 旧数据=冷色, 新数据=暖色(偏移30°) |

最终颜色 = HSL → RGB 转换

## 应用场景

### 1. 数据库快速认知
新接手项目时，通过颜色分布快速了解：
- 哪些业务域表最多
- 哪些表数据量最大
- 哪些表是新建的/遗留的

### 2. 数据治理
- 发现异常的表（颜色孤立的点）
- 识别需要归档的大表（亮黄色点）
- 监控表的增长趋势（颜色流动）

### 3. 业务趋势分析
- 通过颜色流动观察业务增长
- 关联色块的连线表示业务依赖

## API参考

### HyperCube 类

```python
from hypercube.core.hypercube import HyperCube

hypercube = HyperCube()

# 添加数据单元格
hypercube.add_cell(data_cell, compute_color=True)

# 全量同步颜色矩阵
hypercube.sync_color_matrix()

# 通过颜色查询
results = hypercube.query_by_color(hex_color, threshold=50)

# 区域查询
region = hypercube.query_by_visual_region(z, x_range, y_range)

# 获取趋势
trend_df = hypercube.get_business_trend(z)

# 导出可视化数据
viz_data = hypercube.export_for_visualization()

# 获取摘要
summary = hypercube.get_summary()
```

## 扩展开发

### 添加新的数据库连接器

```python
from hypercube.connectors.base import BaseConnector, TableMetadata

class NewConnector(BaseConnector):
    def connect(self):
        # 实现连接逻辑
        pass
    
    def get_all_tables_metadata(self):
        # 实现元数据获取
        pass
```

### 自定义颜色方案

```python
from hypercube.core.color_matrix import ColorScheme

scheme = ColorScheme()
scheme.domain_hues["custom"] = 45  # 自定义业务域颜色
scheme.stage_saturation["frozen"] = 0.1  # 自定义阶段饱和度

hypercube = HyperCube(color_scheme=scheme)
```

## 技术栈

- **Python 3.9+**
- **SQLAlchemy**: 数据库连接
- **NumPy/Pandas**: 数据处理
- **colorspacious**: 色彩空间转换
- **Plotly/Dash**: 可视化

## 许可证

MIT
