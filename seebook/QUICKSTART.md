# 快速上手指南

## 5分钟运行第一个案例

```bash
cd /Users/blue/seebook

# 1. 分析本地OA数据库
python examples/analyze_oa_db.py \
    --password "Tdsipass@@1234" \
    --database oa

# 2. 查看生成的报告
open oa_business_report.html
```

## 30分钟理解核心代码

### 文件阅读顺序

1. **src/hypercube/core/hypercube.py** (50行)
   - 理解双矩阵架构
   - 掌握基本API

2. **src/hypercube/core/dynamic_classifier.py** (100行)
   - 理解动态分类算法
   - 这是核心创新点

3. **examples/unknown_database_demo.py**
   - 看完整的使用示例
   - 理解输出结果

### 关键API

```python
from hypercube.core.hypercube import HyperCube
from hypercube.core.data_matrix import DataCell

# 创建超立方体
cube = HyperCube()

# 添加数据单元格
cell = DataCell(
    t=datetime.now(),
    x=80,  # 生命周期坐标
    y=254, # 数据量级坐标
    z=0,   # 主题域
    table_name="users",
    business_domain="user",  # 动态发现
    lifecycle_stage="mature",
)
cube.add_cell(cell, compute_color=True)

# 同步颜色矩阵
cube.sync_color_matrix()

# 导出可视化数据
viz_data = cube.export_for_visualization()
```

## 1小时完成第一个改进

### 任务：添加一个新数据库连接器（SQLite）

```python
# 1. 创建文件 src/hypercube/connectors/sqlite.py
from hypercube.connectors.base import BaseConnector, TableMetadata
import sqlite3

class SQLiteConnector(BaseConnector):
    def connect(self):
        db_path = self.params.get("database")
        self.connection = sqlite3.connect(db_path)
        return self
    
    def get_all_tables_metadata(self):
        cursor = self.connection.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        metadata_list = []
        for (table_name,) in tables:
            # 获取列信息
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            
            # 获取行数
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = cursor.fetchone()[0]
            
            metadata_list.append(TableMetadata(
                table_name=table_name,
                schema_name="main",
                column_count=len(columns),
                row_count=row_count,
                size_bytes=0,
                columns=[{"name": col[1], "type": col[2]} for col in columns],
            ))
        
        return metadata_list
```

```python
# 2. 测试新连接器
from hypercube.connectors.sqlite import SQLiteConnector

conn = SQLiteConnector({"database": "/path/to/your.db"})
conn.connect()
metadata = conn.get_all_tables_metadata()
print(f"发现 {len(metadata)} 个表")
```

## 常见问题排查

### 问题1: 导入错误
```bash
# 确保在正确目录
export PYTHONPATH=/Users/blue/seebook/src:$PYTHONPATH
```

### 问题2: 依赖缺失
```bash
pip install -e /Users/blue/seebook
```

### 问题3: MySQL连接失败
```python
# 检查密码中的特殊字符是否已编码
# 已在 mysql.py 中修复，使用 urllib.parse.quote_plus
```

## 下一步学习

1. 阅读 `HANDOVER_REPORT.md` 了解完整项目
2. 查看 `docs/ARCHITECTURE.md` 理解架构
3. 尝试修复一个已知问题（见HANDOVER_REPORT的"已知问题"章节）

## 获取帮助

- 查看代码注释（每个模块都有详细docstring）
- 运行示例脚本学习用法
- 查看测试文件了解预期行为
