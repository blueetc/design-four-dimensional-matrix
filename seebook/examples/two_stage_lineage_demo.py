"""
两阶段矩阵溯源演示

展示：
1. 第一阶段矩阵：直接从业务库扫描
2. 分析优化建议
3. 第二阶段矩阵：规范化结构
4. 完整的血缘追溯
"""

import sys
sys.path.insert(0, "/Users/blue/seebook/src")

from datetime import datetime, timedelta
import random

from hypercube.core.hypercube import HyperCube
from hypercube.core.data_matrix import DataCell
from hypercube.core.lineage import (
    LineageTracker, PhysicalLocation, Provenance
)
from hypercube.core.optimizer import MatrixOptimizer


def create_mock_database_scan():
    """
    模拟从真实业务库扫描得到的第一阶段矩阵
    
    包含一些"不完美"：
    - 表名混乱（user_logs 被分到技术域）
    - 有重复结构（users 和 user_profiles 应该合并）
    - 有遗留表（old_orders）
    """
    
    # 模拟物理库连接信息
    db_location = PhysicalLocation(
        db_type="postgres",
        host="prod-db.company.com",
        port=5432,
        database="business_db",
        schema="public",
        table="",  # 会在每个cell中填充
        snapshot_at=datetime.now(),
    )
    
    # 定义模拟表数据（包含不完美）
    mock_tables = [
        # 用户域表（Z=0）
        {"name": "users", "domain": "user", "rows": 5000000, "stage": "mature", "x": 80},
        {"name": "user_profiles", "domain": "user", "rows": 4800000, "stage": "mature", "x": 80},
        {"name": "user_sessions", "domain": "user", "rows": 10000000, "stage": "growth", "x": 50},
        # BUG: user_logs 被错分到技术域
        {"name": "user_logs", "domain": "tech", "rows": 50000000, "stage": "mature", "x": 80},
        
        # 营收域表（Z=1）
        {"name": "orders", "domain": "revenue", "rows": 20000000, "stage": "mature", "x": 85},
        {"name": "order_items", "domain": "revenue", "rows": 50000000, "stage": "mature", "x": 85},
        {"name": "payments", "domain": "revenue", "rows": 18000000, "stage": "mature", "x": 82},
        
        # 遗留表（应该归档）
        {"name": "old_orders", "domain": "revenue", "rows": 1000000, "stage": "legacy", "x": 20},
        {"name": "old_logs", "domain": "tech", "rows": 5000000, "stage": "legacy", "x": 15},
    ]
    
    return mock_tables, db_location


def build_first_stage_matrix(mock_tables, db_location):
    """构建第一阶段矩阵（直接从物理库映射）"""
    
    print("=" * 70)
    print("阶段一：构建发现层矩阵（直接来自物理库）")
    print("=" * 70)
    
    # 创建组件
    first_cube = HyperCube()
    lineage_tracker = LineageTracker()
    
    # 域到Z轴的映射
    domain_to_z = {"user": 0, "revenue": 1, "product": 2, "tech": 3}
    
    base_time = datetime.now() - timedelta(days=30)
    
    for table_data in mock_tables:
        table_name = table_data["name"]
        domain = table_data["domain"]
        
        # 创建物理位置（具体到表）
        location = PhysicalLocation(
            db_type=db_location.db_type,
            host=db_location.host,
            port=db_location.port,
            database=db_location.database,
            schema=db_location.schema,
            table=table_name,
            snapshot_at=db_location.snapshot_at,
            query_sql=f"SELECT * FROM {table_name} LIMIT 100",
        )
        
        # 创建单元格ID
        cell_id = f"stage1_{table_name}_{base_time.strftime('%Y%m%d')}"
        
        # 注册到血缘追踪器
        prov = lineage_tracker.register_first_stage(cell_id, location)
        
        # 创建DataCell
        cell = DataCell(
            t=base_time,
            x=table_data["x"],
            y=table_data["rows"] / 1000000,  # 归一化
            z=domain_to_z.get(domain, 4),
            table_name=table_name,
            schema_name="public",
            row_count=table_data["rows"],
            size_bytes=table_data["rows"] * 100,
            column_count=random.randint(5, 20),
            business_domain=domain,
            lifecycle_stage=table_data["stage"],
            tags=[domain, table_data["stage"]],
            payload={
                "columns": [{"name": f"col_{i}"} for i in range(5)],
                "indexes": [{"name": f"idx_{table_name}"}],
            },
            provenance=prov,  # 关键：附加溯源信息
        )
        
        first_cube.add_cell(cell, compute_color=True)
        print(f"  ✓ 注册: {table_name:20s} | 域: {domain:10s} | Z={cell.z} | 溯源ID: {cell_id}")
    
    # 同步颜色矩阵
    first_cube.sync_color_matrix()
    
    print(f"\n  第一阶段矩阵统计:")
    print(f"    - 数据单元格: {len(first_cube.data_matrix.cells)}")
    print(f"    - 颜色单元格: {len(first_cube.color_matrix.cells)}")
    print(f"    - 主题分类: {first_cube.data_matrix.z_categories}")
    
    return first_cube, lineage_tracker


def analyze_and_optimize(first_cube, lineage_tracker):
    """分析并生成第二阶段矩阵"""
    
    print("\n" + "=" * 70)
    print("阶段二：矩阵分析优化（发现结构问题）")
    print("=" * 70)
    
    # 创建优化器
    optimizer = MatrixOptimizer(first_cube, lineage_tracker)
    
    # 分析生成建议
    suggestions = optimizer.analyze()
    
    print(f"\n  发现 {len(suggestions)} 个优化建议:\n")
    
    # 分类展示
    by_type = {}
    for s in suggestions:
        by_type.setdefault(s.type, []).append(s)
    
    for type_name, type_suggestions in by_type.items():
        print(f"  【{type_name.upper()}】")
        for s in type_suggestions:
            target_names = [c.table_name for c in s.target_cells]
            print(f"    - {s.suggestion_id}")
            print(f"      目标: {', '.join(target_names)}")
            print(f"      原因: {s.reason}")
            print(f"      置信度: {s.confidence:.0%}")
            print(f"      可自动: {'是' if s.auto_applicable else '否（需人工确认）'}")
            if s.new_structure:
                print(f"      建议结构: {s.new_structure.get('new_table_name', 'N/A')}")
            print()
    
    # 应用所有可自动应用的建议
    print("  应用优化建议中...")
    optimized_cube = optimizer.apply_suggestions(auto_only=True)
    
    print(f"\n  第二阶段矩阵统计:")
    print(f"    - 数据单元格: {len(optimized_cube.data_matrix.cells)}")
    print(f"    - 颜色单元格: {len(optimized_cube.color_matrix.cells)}")
    
    return optimized_cube, optimizer


def demonstrate_lineage(first_cube, optimized_cube, lineage_tracker):
    """演示血缘追溯能力"""
    
    print("\n" + "=" * 70)
    print("阶段三：血缘追溯演示")
    print("=" * 70)
    
    # 1. 从第二阶段追溯第一阶段
    print("\n  1. 从优化表追溯到源表:")
    for key, cell in optimized_cube.data_matrix.cells.items():
        if cell.provenance and cell.provenance.is_second_stage():
            print(f"\n    优化表: {cell.table_name}")
            print(f"    ├── 业务域: {cell.business_domain}")
            print(f"    ├── 溯源链:")
            
            # 获取上游
            upstream = lineage_tracker.get_upstream(cell.provenance.cell_id, recursive=True)
            for prov in upstream:
                if prov.is_first_stage() and prov.physical_location:
                    loc = prov.physical_location
                    print(f"    │   └── {loc.db_type}://{loc.host}/{loc.database}/{loc.schema}/{loc.table}")
            
            # 显示字段映射
            if cell.payload.get("field_mappings"):
                print(f"    └── 字段映射:")
                for fm in cell.payload["field_mappings"][:3]:  # 只显示前3个
                    print(f"        {fm['source']} → {fm['target']}")
    
    # 2. 从物理位置查影响
    print("\n  2. 源表变更影响分析:")
    test_location = PhysicalLocation(
        db_type="postgres",
        host="prod-db.company.com",
        port=5432,
        database="business_db",
        schema="public",
        table="users",
    )
    
    impact = lineage_tracker.generate_impact_report(test_location)
    print(f"\n    如果 {test_location.table} 发生变更:")
    print(f"    ├── 直接影响的第一阶段单元格: {len(impact['affected_first_stage'])} 个")
    print(f"    ├── 影响的第二阶段单元格: {len(impact['affected_second_stage'])} 个")
    print(f"    └── 总影响范围: {impact['total_impact']} 个单元格")
    
    # 3. 导出完整血缘图
    print("\n  3. 血缘图谱导出:")
    graph = lineage_tracker.export_lineage_graph()
    print(f"    ├── 节点总数: {graph['stats']['total_nodes']}")
    print(f"    ├── 边总数: {graph['stats']['total_edges']}")
    print(f"    ├── 第一阶段节点: {graph['stats']['first_stage']}")
    print(f"    └── 第二阶段节点: {graph['stats']['second_stage']}")


def generate_ddl_output(optimizer):
    """生成DDL输出"""
    
    print("\n" + "=" * 70)
    print("阶段四：生成规范化DDL")
    print("=" * 70)
    
    ddl_statements = optimizer.generate_ddl()
    
    for table_name, ddl in ddl_statements.items():
        print(f"\n{ddl}")


def main():
    print("\n" + "=" * 70)
    print("四维矩阵两阶段转换与血缘溯源演示")
    print("=" * 70)
    print("\n本演示展示：")
    print("  1. 第一阶段：从混乱的业务库直接扫描（不完美但真实）")
    print("  2. 矩阵分析：通过颜色模式发现结构问题")
    print("  3. 第二阶段：生成规范化的主题库结构（目标）")
    print("  4. 完整溯源：保持两阶段之间的血缘关系")
    
    # 1. 创建模拟数据
    mock_tables, db_location = create_mock_database_scan()
    
    # 2. 构建第一阶段矩阵
    first_cube, lineage_tracker = build_first_stage_matrix(mock_tables, db_location)
    
    # 3. 分析优化
    optimized_cube, optimizer = analyze_and_optimize(first_cube, lineage_tracker)
    
    # 4. 演示血缘
    demonstrate_lineage(first_cube, optimized_cube, lineage_tracker)
    
    # 5. 生成DDL
    generate_ddl_output(optimizer)
    
    print("\n" + "=" * 70)
    print("演示完成")
    print("=" * 70)
    print("\n关键洞察：")
    print("  • 第一阶段矩阵保留了到物理库的直接溯源（URI可访问）")
    print("  • 通过颜色相似度分析发现：user_logs 分类错误、users/user_profiles 可合并")
    print("  • 第二阶段矩阵是目标：规范化结构 + 完整血缘链")
    print("  • 任何优化操作都可追溯到：谁在什么时候基于什么理由做了什么变更")


if __name__ == "__main__":
    main()
