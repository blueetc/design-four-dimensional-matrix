"""
演示：使用模拟数据测试四维矩阵系统

无需真实数据库即可体验完整功能
"""

import sys
sys.path.insert(0, "/Users/blue/seebook/src")

from datetime import datetime, timedelta
import random

from hypercube.core.hypercube import HyperCube
from hypercube.core.data_matrix import DataCell
from hypercube.core.color_matrix import ColorScheme
from hypercube.visualization.dashboard import create_hypercube_dashboard


def generate_mock_tables():
    """生成模拟表数据"""
    domains = {
        "user": ["users", "profiles", "sessions", "auth_tokens", "permissions"],
        "revenue": ["orders", "payments", "invoices", "refunds", "subscriptions"],
        "product": ["products", "categories", "inventory", "skus", "variants"],
        "tech": ["logs", "events", "metrics", "traces", "alerts"],
        "marketing": ["campaigns", "ads", "coupons", "referrals", "channels"],
    }
    
    stages = ["new", "growth", "mature", "legacy"]
    
    tables = []
    base_time = datetime.now() - timedelta(days=365)
    
    for domain_id, (domain, table_names) in enumerate(domains.items()):
        for i, table_name in enumerate(table_names):
            # 随机生成表属性
            stage = random.choice(stages)
            row_count = random.randint(1000, 100000000)
            size_bytes = row_count * random.randint(50, 500)
            column_count = random.randint(5, 50)
            
            # 时间维度：最近一年内
            t = base_time + timedelta(days=random.randint(0, 365))
            
            # x轴：根据阶段映射
            stage_to_x = {"new": 20, "growth": 50, "mature": 80, "legacy": 110}
            x = stage_to_x[stage]
            
            # y轴：对数压缩的行数
            y = min(255, max(1, int(row_count / 100000)))
            
            # z轴：业务域
            z = domain_id
            
            tables.append({
                "t": t,
                "x": x,
                "y": y,
                "z": z,
                "table_name": table_name,
                "schema_name": domain,
                "row_count": row_count,
                "size_bytes": size_bytes,
                "column_count": column_count,
                "business_domain": domain,
                "lifecycle_stage": stage,
            })
    
    return tables


def main():
    print("=" * 60)
    print("四维矩阵数据库可视化系统 - 演示")
    print("=" * 60)
    
    # 生成模拟数据
    print("\n1. 生成模拟表数据...")
    mock_tables = generate_mock_tables()
    print(f"   生成了 {len(mock_tables)} 个模拟表")
    
    # 创建超立方体
    print("\n2. 构建四维超立方体...")
    hypercube = HyperCube()
    
    for table_data in mock_tables:
        cell = DataCell(
            t=table_data["t"],
            x=table_data["x"],
            y=table_data["y"],
            z=table_data["z"],
            table_name=table_data["table_name"],
            schema_name=table_data["schema_name"],
            row_count=table_data["row_count"],
            size_bytes=table_data["size_bytes"],
            column_count=table_data["column_count"],
            business_domain=table_data["business_domain"],
            lifecycle_stage=table_data["lifecycle_stage"],
            tags=[table_data["business_domain"], table_data["lifecycle_stage"]],
            payload={
                "description": f"Mock table for {table_data['business_domain']}",
            }
        )
        hypercube.add_cell(cell, compute_color=True)
    
    # 同步颜色矩阵
    hypercube.sync_color_matrix()
    
    print(f"   数据单元格: {len(hypercube.data_matrix.cells)}")
    print(f"   颜色单元格: {len(hypercube.color_matrix.cells)}")
    
    # 显示分类统计
    print("\n3. 主题分类统计:")
    for z_id, domain in hypercube.data_matrix.z_categories.items():
        cells = hypercube.data_matrix.slice_by_z(z_id)
        total_rows = sum(c.row_count for c in cells)
        print(f"   Z={z_id} ({domain:12s}): {len(cells):2d} 个表, 共 {total_rows:>12,} 行")
    
    # 颜色查询示例
    print("\n4. 颜色查询示例:")
    sample_color = "#3498db"  # 蓝色
    results = hypercube.query_by_color(sample_color, threshold=100)
    print(f"   查询颜色 {sample_color} 的近似匹配:")
    for r in results[:5]:
        print(f"     - {r['data']['table_name']}: {r['color_match']['matched']}")
    
    # 区域查询示例
    print("\n5. 区域查询示例 (Z=0, X=30-60):")
    region_results = hypercube.query_by_visual_region(z=0, x_range=(30, 60))
    print(f"   找到 {region_results['statistics']['count']} 个表")
    print(f"   主要颜色: {region_results['dominant_colors'][:3]}")
    
    # 导出数据
    print("\n6. 导出可视化数据...")
    viz_data = hypercube.export_for_visualization()
    print(f"   导出 {len(viz_data['data_points'])} 个数据点")
    
    # 启动可视化
    print("\n7. 启动可视化仪表盘...")
    print("   请访问 http://127.0.0.1:8050")
    print("   按 Ctrl+C 停止服务")
    print("-" * 60)
    
    app = create_hypercube_dashboard(hypercube)
    app.run(debug=True, port=8050)


if __name__ == "__main__":
    main()
