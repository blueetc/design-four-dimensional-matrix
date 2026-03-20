#!/usr/bin/env python3
"""
OA数据库四维矩阵分析脚本

使用动态分类器分析OA数据库，不预设任何业务域。
"""

import sys
sys.path.insert(0, "/Users/blue/seebook/src")

from hypercube.connectors.mysql import MySQLConnector
from hypercube.core.dynamic_classifier import UnknownDatabaseProcessor
from hypercube.core.hypercube import HyperCube
from hypercube.core.data_matrix import DataCell
from hypercube.core.lineage import LineageTracker, PhysicalLocation
from hypercube.core.quality import QualityEngine
from datetime import datetime
import json
import argparse


def analyze_oa_db(host="localhost", port=3306, user="root", password="", 
                  database="oa", output_dir="/Users/blue/seebook"):
    """
    分析OA数据库并生成四维矩阵
    
    完全动态分析，不预设任何业务域
    """
    
    print("=" * 70)
    print(f"四维矩阵系统 - {database}数据库分析")
    print("=" * 70)
    
    # 连接数据库
    conn_params = {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "database": database,
    }
    
    print(f"\n正在连接MySQL {database}数据库 ({host}:{port})...")
    connector = MySQLConnector(conn_params)
    connector.connect()
    print("✓ 连接成功")
    
    # 获取元数据
    print("\n正在扫描表结构...")
    metadata_list = connector.get_all_tables_metadata()
    print(f"发现 {len(metadata_list)} 个表")
    
    # 转换为动态分类器需要的格式
    raw_metadata = []
    for meta in metadata_list:
        raw_metadata.append({
            "table_name": meta.table_name,
            "schema_name": meta.schema_name or database,
            "columns": meta.columns,
            "indexes": [{"name": i.get("name", "")} for i in meta.indexes],
            "primary_key": meta.primary_key,
            "foreign_keys": [],  # 动态分析不依赖外键
            "row_count": meta.row_count,
            "column_count": meta.column_count,
            "size_bytes": meta.size_bytes,
        })
    
    connector.disconnect()
    
    # 动态分类分析
    print("\n" + "-" * 70)
    print("正在进行动态主题域发现...")
    print("-" * 70)
    
    processor = UnknownDatabaseProcessor()
    result = processor.process(raw_metadata)
    
    print(f"\n✓ 分析完成")
    print(f"  发现 {result['stats']['domain_count']} 个主题域")
    print(f"  总表数: {result['stats']['total_tables']}")
    print(f"  总数据量: {result['stats']['total_rows']:,} 行")
    
    # 显示发现的域
    print(f"\n动态发现的业务域:")
    for z_id, domain_info in result['domains'].items():
        print(f"\n  【主题域 Z={z_id}: {domain_info['name']}】")
        print(f"   {domain_info['description']}")
        for table_name in domain_info['tables']:
            lifecycle = result['lifecycle_mapping'].get(table_name, 'unknown')
            meta = next((m for m in metadata_list if m.table_name == table_name), None)
            if meta:
                print(f"     - {table_name:25s} ({lifecycle}, {meta.row_count:,}行, {meta.column_count}列)")
    
    # 构建四维矩阵
    print("\n" + "-" * 70)
    print("正在构建四维矩阵...")
    print("-" * 70)
    
    hypercube = HyperCube()
    lineage = LineageTracker()
    
    stage_to_x = {"new": 20, "growth": 50, "mature": 80, "legacy": 110}
    
    for meta in metadata_list:
        table_name = meta.table_name
        
        z = result['domain_mapping'].get(table_name, 0)
        stage = result['lifecycle_mapping'].get(table_name, "mature")
        x = stage_to_x.get(stage, 50)
        
        # 自适应Y轴归一化
        y = 0
        if meta.row_count > 0:
            max_rows = max(m.row_count for m in metadata_list)
            y = min(255, max(1, int(meta.row_count / max(max_rows / 255, 1))))
        
        # 创建溯源
        loc = PhysicalLocation(
            db_type="mysql",
            host=host,
            port=port,
            database=database,
            schema=database,
            table=table_name,
            snapshot_at=datetime.now(),
        )
        cell_id = f"{database}_{table_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        prov = lineage.register_first_stage(cell_id, loc)
        
        domain_name = result['domains'].get(z, {}).get('name', 'unknown')
        
        cell = DataCell(
            t=datetime.now(),
            x=x,
            y=y,
            z=z,
            table_name=table_name,
            schema_name=database,
            column_count=meta.column_count,
            row_count=meta.row_count,
            size_bytes=meta.size_bytes,
            business_domain=domain_name,
            lifecycle_stage=stage,
            tags=[],
            payload={
                "columns": meta.columns,
                "indexes": meta.indexes,
                "primary_key": meta.primary_key,
            },
            provenance=prov,
        )
        
        hypercube.add_cell(cell, compute_color=True)
    
    hypercube.sync_color_matrix()
    
    print(f"\n✓ 四维矩阵构建完成")
    print(f"  数据单元格: {len(hypercube.data_matrix.cells)}")
    print(f"  颜色单元格: {len(hypercube.color_matrix.cells)}")
    
    # 质量评估
    print("\n" + "-" * 70)
    print("正在进行质量评估...")
    print("-" * 70)
    
    quality_engine = QualityEngine()
    scores = quality_engine.evaluate(hypercube)
    report = quality_engine.generate_report(scores)
    
    print(f"\n质量概览:")
    print(f"  平均质量分: {report['summary']['average_score']:.1f}/100")
    print(f"  发现问题: {report['summary']['total_issues']} 个")
    
    if report['low_scoring_cells']:
        print(f"\n  需要关注的表:")
        for item in report['low_scoring_cells'][:3]:
            print(f"    - {item['cell_id']}: {item['score']:.1f}分")
    
    # 导出结果
    print("\n" + "-" * 70)
    print("正在导出结果...")
    print("-" * 70)
    
    # 导出可视化数据
    viz_data = hypercube.export_for_visualization()
    viz_file = f"{output_dir}/{database}_hypercube.json"
    with open(viz_file, "w") as f:
        json.dump(viz_data, f, indent=2, default=str)
    print(f"✓ 可视化数据: {viz_file}")
    
    # 导出分析报告
    analysis_report = {
        "database": database,
        "scan_time": datetime.now().isoformat(),
        "summary": result['stats'],
        "domains": result['domains'],
        "lifecycle": result['lifecycle_mapping'],
        "quality": report['summary'],
    }
    report_file = f"{output_dir}/{database}_analysis_report.json"
    with open(report_file, "w") as f:
        json.dump(analysis_report, f, indent=2, default=str)
    print(f"✓ 分析报告: {report_file}")
    
    print("\n" + "=" * 70)
    print("分析完成")
    print("=" * 70)
    print(f"\n可以使用以下命令启动可视化:")
    print(f"  python -m hypercube.cli visualize --input {viz_file}")
    
    return hypercube, result, report


def main():
    parser = argparse.ArgumentParser(description="分析OA数据库并生成四维矩阵")
    parser.add_argument("--host", default="localhost", help="MySQL主机")
    parser.add_argument("--port", type=int, default=3306, help="MySQL端口")
    parser.add_argument("--user", default="root", help="用户名")
    parser.add_argument("--password", default="", help="密码")
    parser.add_argument("--database", default="oa", help="数据库名")
    parser.add_argument("--output", default="/Users/blue/seebook", help="输出目录")
    
    args = parser.parse_args()
    
    analyze_oa_db(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.database,
        output_dir=args.output,
    )


if __name__ == "__main__":
    main()
