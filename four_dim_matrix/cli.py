"""
命令行工具

Usage:
    python -m hypercube.cli scan --db postgres --host localhost --database mydb
    python -m hypercube.cli visualize --input hypercube.json
"""

import argparse
import json
import sys
from datetime import datetime
from typing import Dict, Any

from four_dim_matrix.hypercube import HyperCube
from four_dim_matrix.data_matrix import DataCell
from four_dim_matrix.lineage import LineageTracker, PhysicalLocation
from four_dim_matrix.dynamic_classifier import UnknownDatabaseProcessor
from four_dim_matrix.demo import build_hypercube_from_adapter
from four_dim_matrix.db_adapter import DatabaseAdapter


def _get_connector(db_type: str, conn_params: dict):
    """Lazily import and return the appropriate database connector."""
    if db_type == "postgres":
        from four_dim_matrix.connectors.postgres import PostgresConnector
        return PostgresConnector(conn_params)
    from four_dim_matrix.connectors.mysql import MySQLConnector
    return MySQLConnector(conn_params)


def _start_dashboard(hypercube: HyperCube, port: int) -> None:
    """Lazily import Dash dashboard and start the server."""
    from four_dim_matrix.dashboard import create_hypercube_dashboard
    app = create_hypercube_dashboard(hypercube)
    app.run(debug=True, port=port)


def scan_database(args):
    """扫描数据库并生成四维矩阵（支持未知数据库结构）"""

    # ----------------------------------------------------------------
    # SQLite path – uses the lightweight DatabaseAdapter directly
    # ----------------------------------------------------------------
    if args.db == "sqlite":
        if not args.sqlite_path:
            print("错误: 使用 --db sqlite 时必须提供 --sqlite-path 参数")
            sys.exit(1)
        print(f"正在打开 SQLite 数据库: {args.sqlite_path}")
        adapter = DatabaseAdapter.from_sqlite(args.sqlite_path)
        hypercube = build_hypercube_from_adapter(adapter, args.sqlite_path)
        print("\n四维矩阵生成完成!")
        print(f"数据单元格: {len(hypercube.data_matrix.cells)}")
        print(f"颜色单元格: {len(hypercube.color_matrix.cells)}")
        if args.output:
            export_data = hypercube.export_for_visualization()
            with open(args.output, "w", encoding="utf-8") as f:
                json.dump(export_data, f, ensure_ascii=False, indent=2)
            print(f"\n已导出到: {args.output}")
        if args.visualize:
            print("\n启动可视化仪表盘...")
            print("  访问 http://127.0.0.1:{}".format(args.viz_port))
            print("  按 Ctrl+C 停止")
            _start_dashboard(hypercube, args.viz_port)
        return hypercube

    # ----------------------------------------------------------------
    # PostgreSQL / MySQL path – uses the native connectors
    # ----------------------------------------------------------------
    print(f"正在连接 {args.db} 数据库...")

    conn_params = {
        "host": args.host,
        "port": args.db_port,
        "user": args.user,
        "password": args.password,
        "database": args.database,
    }

    if args.db == "postgres":
        connector = _get_connector("postgres", conn_params)
    elif args.db == "mysql":
        connector = _get_connector("mysql", conn_params)
    else:
        print(f"不支持的数据库类型: {args.db}")
        sys.exit(1)
    
    # 连接并获取元数据
    connector.connect()
    print("正在扫描表结构...")
    
    tables_metadata = connector.get_all_tables_metadata(args.schema)
    print(f"发现 {len(tables_metadata)} 个表")
    
    # 转换为标准格式
    raw_metadata = []
    for meta in tables_metadata:
        raw_metadata.append({
            "table_name": meta.table_name,
            "schema_name": meta.schema_name,
            "columns": meta.columns,
            "indexes": [{"name": i.get("name", "")} for i in meta.indexes],
            "primary_key": meta.primary_key,
            "foreign_keys": [],  # 可以从元数据中提取
            "row_count": meta.row_count,
            "column_count": meta.column_count,
            "size_bytes": meta.size_bytes,
        })
    
    # 使用动态分类器处理未知数据库
    print("正在分析数据库结构（动态主题域发现）...")
    processor = UnknownDatabaseProcessor()
    result = processor.process(raw_metadata)
    
    # 显示动态发现的统计
    print(f"\n动态分析结果:")
    print(f"  - 发现 {result['stats']['domain_count']} 个主题域")
    print(f"  - 总表数: {result['stats']['total_tables']}")
    print(f"  - 总列数: {result['stats']['total_columns']}")
    print(f"  - 总数据量: {result['stats']['total_rows']:,} 行")
    if result['stats']['relationships'] > 0:
        print(f"  - 发现 {result['stats']['relationships']} 个表间关联")
    
    # 显示发现的域
    print(f"\n动态发现的业务域:")
    for z_id, domain_info in result['domains'].items():
        print(f"  Z={z_id}: {domain_info['name']} ({domain_info['table_count']} 表)")
        print(f"       {domain_info['description']}")
    
    # 创建超立方体
    hypercube = HyperCube()
    lineage = LineageTracker()
    
    # 阶段到X轴的映射（自适应）
    stage_to_x = {
        "new": 20,
        "growth": 50,
        "mature": 80,
        "legacy": 110,
    }
    
    # 填充数据矩阵
    for sig in result['signatures']:
        table_name = sig.table_name
        
        # 获取动态分类结果
        z = result['domain_mapping'].get(table_name, 0)
        stage = result['lifecycle_mapping'].get(table_name, "mature")
        x = stage_to_x.get(stage, 50)
        
        # y轴：对数压缩的行数（自适应）
        y = 0
        if sig.row_count > 0:
            # 使用相对值而不是固定阈值
            max_rows = max(s.row_count for s in result['signatures']) if result['signatures'] else 1
            y = min(255, max(1, int(sig.row_count / max(max_rows / 255, 1))))
        
        # 创建溯源
        loc = PhysicalLocation(
            db_type=args.db,
            host=args.host,
            port=args.db_port,
            database=args.database,
            schema=sig.schema_name,
            table=table_name,
            snapshot_at=datetime.now(),
        )
        cell_id = f"scan_{table_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        prov = lineage.register_first_stage(cell_id, loc)
        
        # 创建DataCell
        domain_name = result['domains'].get(z, {}).get('name', 'unknown')
        
        cell = DataCell(
            t=datetime.now(),
            x=x,
            y=y,
            z=z,
            table_name=table_name,
            schema_name=sig.schema_name,
            column_count=sig.column_count,
            row_count=sig.row_count,
            size_bytes=sig.row_count * 100,
            business_domain=domain_name,
            lifecycle_stage=stage,
            tags=list(sig.name_tokens),
            payload={
                "columns": [{"name": c} for c in sig.column_names],
                "indexes": sig.indexes,
                "primary_key": sig.primary_key,
                "foreign_keys": sig.foreign_keys,
                "has_timestamp": sig.has_timestamp,
                "has_soft_delete": sig.has_soft_delete,
            },
            provenance=prov,
        )
        
        hypercube.add_cell(cell, compute_color=True)
    
    # 同步颜色矩阵
    hypercube.sync_color_matrix()
    
    print("\n四维矩阵生成完成!")
    print(f"数据单元格: {len(hypercube.data_matrix.cells)}")
    print(f"颜色单元格: {len(hypercube.color_matrix.cells)}")
    
    # 导出
    if args.output:
        export_data = hypercube.export_for_visualization()
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(export_data, f, ensure_ascii=False, indent=2)
        print(f"\n已导出到: {args.output}")
    
    # 启动可视化
    if args.visualize:
        print("\n启动可视化仪表盘...")
        print("  访问 http://127.0.0.1:8050")
        print("  按 Ctrl+C 停止")
        _start_dashboard(hypercube, args.viz_port)

    return hypercube


def visualize_data(args):
    """从文件加载并可视化"""
    print(f"正在加载数据: {args.input}")

    with open(args.input, "r", encoding="utf-8") as f:
        data = json.load(f)

    # 重建超立方体
    hypercube = HyperCube()

    # 这里简化处理，实际应该从JSON重建完整对象
    print(f"数据点数量: {len(data.get('data_points', []))}")

    # 启动可视化
    _start_dashboard(hypercube, args.port)


def query_by_color(args):
    """通过颜色查询"""
    with open(args.input, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    # 简化实现：直接从颜色查找数据点
    target_color = args.color.lower()
    threshold = args.threshold
    
    print(f"查询颜色: {target_color} (阈值: {threshold})")
    
    matching = []
    for point in data.get("data_points", []):
        point_color = point["color"]["hex"].lower()
        # 计算颜色距离（简化版）
        distance = hex_color_distance(target_color, point_color)
        if distance < threshold:
            matching.append({
                "table": point["data"]["table_name"],
                "color": point_color,
                "distance": distance,
                "domain": point["data"]["business_domain"],
            })
    
    matching.sort(key=lambda x: x["distance"])
    
    print(f"\n找到 {len(matching)} 个匹配:")
    for m in matching[:20]:  # 限制显示数量
        print(f"  {m['table']}: {m['color']} (距离: {m['distance']:.2f}, 域: {m['domain']})")


def hex_color_distance(c1: str, c2: str) -> float:
    """计算十六进制颜色距离"""
    c1 = c1.lstrip("#")
    c2 = c2.lstrip("#")
    
    r1, g1, b1 = int(c1[0:2], 16), int(c1[2:4], 16), int(c1[4:6], 16)
    r2, g2, b2 = int(c2[0:2], 16), int(c2[2:4], 16), int(c2[4:6], 16)
    
    return ((r1-r2)**2 + (g1-g2)**2 + (b1-b2)**2) ** 0.5


def main():
    parser = argparse.ArgumentParser(description="四维矩阵数据库可视化工具")
    subparsers = parser.add_subparsers(dest="command", help="可用命令")

    # scan 命令
    scan_parser = subparsers.add_parser("scan", help="扫描数据库并生成四维矩阵")
    scan_parser.add_argument(
        "--db", choices=["postgres", "mysql", "sqlite"], required=True,
        help="数据库类型 (postgres / mysql / sqlite)",
    )
    # SQLite-specific
    scan_parser.add_argument(
        "--sqlite-path", metavar="FILE",
        help="SQLite 数据库文件路径 (仅 --db sqlite 时使用)",
    )
    # PostgreSQL / MySQL connection parameters
    scan_parser.add_argument("--host", default="localhost", help="主机地址")
    scan_parser.add_argument("--db-port", type=int, dest="db_port", help="数据库端口")
    scan_parser.add_argument("--user", help="用户名")
    scan_parser.add_argument("--password", help="密码")
    scan_parser.add_argument("--database", help="数据库名")
    scan_parser.add_argument("--schema", help="Schema 名称")
    # Output / visualisation
    scan_parser.add_argument("--output", "-o", help="输出 JSON 文件路径")
    scan_parser.add_argument("--visualize", "-v", action="store_true", help="启动可视化")
    scan_parser.add_argument(
        "--viz-port", dest="viz_port", type=int, default=8050,
        help="可视化服务端口 (默认: 8050)",
    )
    
    # visualize 命令
    viz_parser = subparsers.add_parser("visualize", help="可视化数据文件")
    viz_parser.add_argument("--input", "-i", required=True, help="输入JSON文件路径")
    viz_parser.add_argument("--port", type=int, default=8050, help="服务端口")
    
    # query 命令
    query_parser = subparsers.add_parser("query", help="通过颜色查询")
    query_parser.add_argument("--input", "-i", required=True, help="输入JSON文件路径")
    query_parser.add_argument("--color", "-c", required=True, help="查询颜色(如 #FF5733)")
    query_parser.add_argument("--threshold", "-t", type=float, default=50, help="颜色相似度阈值")
    
    args = parser.parse_args()

    if args.command == "scan":
        scan_database(args)
    elif args.command == "visualize":
        visualize_data(args)
    elif args.command == "query":
        query_by_color(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
