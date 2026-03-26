"""
完整工作流演示

展示系统的全部核心功能：
1. 数据库扫描与第一阶段矩阵构建
2. AI辅助分类优化
3. 质量评分与异常检测
4. 增量更新与变更追踪
5. 版本对比与演进分析
6. 生成优化后的第二阶段矩阵
"""

import sys
sys.path.insert(0, "/Users/blue/seebook/src")

from datetime import datetime, timedelta
import random
import json

from hypercube.core.hypercube import HyperCube
from hypercube.core.data_matrix import DataCell
from hypercube.core.lineage import (
    LineageTracker, PhysicalLocation, Provenance, LineageEdge
)
from hypercube.core.optimizer import MatrixOptimizer
from hypercube.core.changelog import ChangeTracker, ChangeType
from hypercube.core.quality import QualityEngine
from hypercube.core.ai_classifier import AIClassifier, ClassificationContext


def simulate_initial_scan():
    """模拟初始数据库扫描"""
    print("=" * 80)
    print("第一步：初始数据库扫描（Day 0）")
    print("=" * 80)
    
    # 模拟数据库表
    initial_tables = [
        # 用户域
        {"name": "users", "domain": "user", "rows": 1000000, "stage": "mature", 
         "columns": [{"name": "user_id", "type": "bigint"}, {"name": "email", "type": "varchar"}]},
        {"name": "user_profiles", "domain": "user", "rows": 980000, "stage": "mature",
         "columns": [{"name": "user_id", "type": "bigint"}, {"name": "nickname", "type": "varchar"}]},
        {"name": "user_sessions", "domain": "user", "rows": 5000000, "stage": "growth",
         "columns": [{"name": "session_id", "type": "varchar"}, {"name": "user_id", "type": "bigint"}]},
        
        # 营收域
        {"name": "orders", "domain": "revenue", "rows": 5000000, "stage": "mature",
         "columns": [{"name": "order_id", "type": "bigint"}, {"name": "amount", "type": "decimal"}]},
        {"name": "order_items", "domain": "revenue", "rows": 15000000, "stage": "mature",
         "columns": [{"name": "item_id", "type": "bigint"}, {"name": "order_id", "type": "bigint"}]},
        
        # 技术域（user_logs 实际上应该属于用户域 - 模拟分类错误）
        {"name": "user_logs", "domain": "tech", "rows": 100000000, "stage": "growth",
         "columns": [{"name": "log_id", "type": "bigint"}, {"name": "user_id", "type": "bigint"}, 
                    {"name": "action", "type": "varchar"}]},
    ]
    
    # 构建第一阶段矩阵
    cube_v1 = HyperCube()
    lineage = LineageTracker()
    change_tracker = ChangeTracker()
    
    db_loc = PhysicalLocation(
        db_type="postgres",
        host="prod-db.company.com",
        port=5432,
        database="business_db",
        schema="public",
        table="",  # 将在每个cell中覆盖
        snapshot_at=datetime.now(),
    )
    
    domain_to_z = {"user": 0, "revenue": 1, "tech": 2, "marketing": 3}
    
    for table in initial_tables:
        # 创建溯源
        loc = PhysicalLocation(
            **{**db_loc.__dict__, "table": table["name"]}
        )
        cell_id = f"v1_{table['name']}_{datetime.now().strftime('%Y%m%d')}"
        prov = lineage.register_first_stage(cell_id, loc)
        
        # 创建DataCell
        cell = DataCell(
            t=datetime.now(),
            x=80 if table["stage"] == "mature" else 50,
            y=table["rows"] / 1000000,
            z=domain_to_z[table["domain"]],
            table_name=table["name"],
            schema_name="public",
            row_count=table["rows"],
            size_bytes=table["rows"] * 200,
            column_count=len(table["columns"]),
            business_domain=table["domain"],
            lifecycle_stage=table["stage"],
            payload={"columns": table["columns"]},
            provenance=prov,
        )
        cube_v1.add_cell(cell, compute_color=True)
        print(f"  ✓ {table['name']:20s} | Z={cell.z} | 行数={table['rows']:>10,}")
    
    cube_v1.sync_color_matrix()
    
    # 创建初始版本快照
    snapshot = change_tracker.create_snapshot(
        cell_count=len(cube_v1.data_matrix.cells),
        domain_dist={"user": 3, "revenue": 2, "tech": 1},
        stage_dist={"mature": 4, "growth": 2},
        changes=[],
        description="初始扫描"
    )
    
    print(f"\n  初始状态:")
    print(f"    - 表数量: {len(cube_v1.data_matrix.cells)}")
    print(f"    - 版本ID: {snapshot.version_id}")
    
    return cube_v1, lineage, change_tracker


def ai_classification_optimization(cube):
    """AI辅助分类优化"""
    print("\n" + "=" * 80)
    print("第二步：AI辅助分类优化")
    print("=" * 80)
    
    classifier = AIClassifier(use_llm=False)  # 使用规则模式演示
    
    print("\n  分析表分类准确性...")
    
    for key, cell in cube.data_matrix.cells.items():
        ctx = ClassificationContext(
            table_name=cell.table_name,
            schema_name=cell.schema_name,
            columns=cell.payload.get("columns", []),
            existing_tags=cell.tags,
        )
        
        result = classifier.classify_table(ctx)
        
        # 检查是否需要修正
        if result["domain"] != cell.business_domain:
            print(f"\n  ⚠️  发现分类错误: {cell.table_name}")
            print(f"      当前分类: {cell.business_domain}")
            print(f"      AI建议: {result['domain']} (置信度: {result['domain_confidence']:.0%})")
            print(f"      原因: {result['reasoning']}")
            
            if result['domain_confidence'] > 0.7:
                # 自动修正
                old_domain = cell.business_domain
                cell.business_domain = result["domain"]
                cell.z = {"user": 0, "revenue": 1, "tech": 2}.get(result["domain"], 3)
                print(f"      ✅ 已自动修正到 Z={cell.z}")
        else:
            if result['domain_confidence'] > 0.8:
                print(f"  ✓ {cell.table_name}: 分类正确 ({result['domain']}, 置信度{result['domain_confidence']:.0%})")
    
    # 重新同步颜色（因为Z轴可能变化）
    cube.sync_color_matrix()
    
    return cube


def quality_evaluation(cube):
    """质量评估"""
    print("\n" + "=" * 80)
    print("第三步：数据质量评估")
    print("=" * 80)
    
    engine = QualityEngine()
    scores = engine.evaluate(cube)
    
    report = engine.generate_report(scores)
    
    print(f"\n  质量概览:")
    print(f"    - 评估单元格: {report['summary']['total_cells']}")
    print(f"    - 发现问题: {report['summary']['total_issues']}")
    print(f"    - 平均质量分: {report['summary']['average_score']:.1f}/100")
    
    print(f"\n  问题分布:")
    for severity, count in report['summary']['severity_distribution'].items():
        print(f"    - {severity}: {count} 个")
    
    print(f"\n  低分单元格 (Top 3):")
    for item in report['low_scoring_cells'][:3]:
        print(f"    - {item['cell_id']}: {item['score']:.1f}分 ({item['issues']}个问题)")
    
    return scores


def simulate_day7_changes(cube_v1, lineage, change_tracker):
    """模拟Day 7的变更"""
    print("\n" + "=" * 80)
    print("第四步：增量更新（Day 7）")
    print("=" * 80)
    
    # 模拟变更后的表状态
    day7_tables = [
        # 1. 数据量增长
        {"name": "users", "rows": 1050000, "event": "growth"},  # +5万
        {"name": "orders", "rows": 5500000, "event": "growth"},  # +50万
        
        # 2. 新增表
        {"name": "payments", "rows": 5000000, "event": "added"},
        
        # 3. 删除表（模拟user_profiles被合并）
        # user_profiles 不再独立存在
    ]
    
    print("\n  Day 7 变更事件:")
    for event in day7_tables:
        if event["event"] == "growth":
            print(f"    📈 {event['name']}: 数据量增长至 {event['rows']:,}")
        elif event["event"] == "added":
            print(f"    ➕ {event['name']}: 新增表")
    print(f"    ➖ user_profiles: 已合并到 users")
    
    # 构建新的矩阵状态
    cube_v2 = HyperCube()
    
    # 复制并更新（简化处理）
    for key, cell in cube_v1.data_matrix.cells.items():
        if cell.table_name == "user_profiles":
            continue  # 跳过已合并的表
        
        # 更新数据量
        new_row_count = cell.row_count
        if cell.table_name == "users":
            new_row_count = 1050000
        elif cell.table_name == "orders":
            new_row_count = 5500000
        
        new_cell = DataCell(
            t=datetime.now(),
            x=cell.x,
            y=new_row_count / 1000000,
            z=cell.z,
            table_name=cell.table_name,
            schema_name=cell.schema_name,
            row_count=new_row_count,
            size_bytes=new_row_count * 200,
            column_count=cell.column_count,
            business_domain=cell.business_domain,
            lifecycle_stage=cell.lifecycle_stage,
            payload=cell.payload,
            provenance=cell.provenance,
        )
        cube_v2.add_cell(new_cell, compute_color=True)
    
    # 添加新表
    new_cell = DataCell(
        t=datetime.now(),
        x=80,
        y=5.0,
        z=1,  # revenue
        table_name="payments",
        schema_name="public",
        row_count=5000000,
        size_bytes=5000000 * 200,
        column_count=8,
        business_domain="revenue",
        lifecycle_stage="mature",
        payload={"columns": [{"name": "payment_id"}, {"name": "order_id"}]},
    )
    
    # 注册新表溯源
    loc = PhysicalLocation(
        db_type="postgres",
        host="prod-db.company.com",
        port=5432,
        database="business_db",
        schema="public",
        table="payments",
    )
    prov = lineage.register_first_stage(f"v2_payments_{datetime.now().strftime('%Y%m%d')}", loc)
    new_cell.provenance = prov
    cube_v2.add_cell(new_cell, compute_color=True)
    
    cube_v2.sync_color_matrix()
    
    # 检测变更
    print("\n  变更检测结果:")
    
    # 模拟变更检测（简化）
    changes_detected = [
        {"type": "growth", "table": "users", "delta": 50000},
        {"type": "growth", "table": "orders", "delta": 500000},
        {"type": "added", "table": "payments"},
        {"type": "removed", "table": "user_profiles", "reason": "合并到users"},
    ]
    
    for ch in changes_detected:
        if ch["type"] == "growth":
            print(f"    📊 {ch['table']}: +{ch['delta']:,} 行")
        elif ch["type"] == "added":
            print(f"    ✨ {ch['table']}: 新增")
        elif ch["type"] == "removed":
            print(f"    🗑️  {ch['table']}: 删除 ({ch['reason']})")
    
    return cube_v2


def generate_optimized_matrix(cube_v2, lineage):
    """生成优化的第二阶段矩阵"""
    print("\n" + "=" * 80)
    print("第五步：生成优化矩阵（第二阶段）")
    print("=" * 80)
    
    optimizer = MatrixOptimizer(cube_v2, lineage)
    
    # 分析
    suggestions = optimizer.analyze()
    
    print(f"\n  发现 {len(suggestions)} 个优化机会:")
    
    for s in suggestions[:3]:  # 只显示前3个
        target_names = [c.table_name for c in s.target_cells]
        print(f"\n    📋 {s.suggestion_id}")
        print(f"       类型: {s.type}")
        print(f"       目标: {', '.join(target_names)}")
        print(f"       原因: {s.reason}")
        print(f"       建议: {s.new_structure.get('new_table_name', 'N/A')}")
    
    # 应用优化
    optimized_cube = optimizer.apply_suggestions(auto_only=True)
    
    print(f"\n  优化后矩阵:")
    print(f"    - 原表数量: {len(cube_v2.data_matrix.cells)}")
    print(f"    - 优化后: {len(optimized_cube.data_matrix.cells)}")
    
    # 显示血缘
    print(f"\n  血缘示例 (user_full):")
    for key, cell in optimized_cube.data_matrix.cells.items():
        if cell.table_name == "user_full" and cell.provenance:
            upstream = lineage.get_upstream(cell.provenance.cell_id, recursive=True)
            print(f"    来源表:")
            for prov in upstream[:3]:
                if prov.physical_location:
                    print(f"      - {prov.physical_location.table}")
    
    return optimized_cube


def version_comparison(change_tracker):
    """版本对比"""
    print("\n" + "=" * 80)
    print("第六步：版本对比与演进分析")
    print("=" * 80)
    
    if len(change_tracker.versions) >= 2:
        v1 = change_tracker.versions[0]
        v2 = change_tracker.versions[-1]
        
        comparison = change_tracker.compare_versions(v1.version_id, v2.version_id)
        
        print(f"\n  版本对比:")
        print(f"    V1 ({v1.timestamp.strftime('%Y-%m-%d')}): {v1.cell_count} 表")
        print(f"    V2 ({v2.timestamp.strftime('%Y-%m-%d')}): {v2.cell_count} 表")
        print(f"    变化: {comparison['cell_count_delta']:+d} 表")
    
    # 变更日志
    changelog = change_tracker.export_changelog()
    print(f"\n  近期变更日志 ({len(changelog)} 条):")
    for entry in changelog[-5:]:
        print(f"    [{entry['timestamp'][:10]}] {entry['type']}: {entry['cell_id']}")


def main():
    print("\n" + "=" * 80)
    print("四维矩阵完整工作流演示")
    print("=" * 80)
    print("\n本演示展示系统的完整能力：")
    print("  1. 初始扫描构建第一阶段矩阵")
    print("  2. AI辅助分类优化（发现并修正user_logs分类错误）")
    print("  3. 质量评估（颜色异常、结构问题检测）")
    print("  4. 增量更新与变更追踪（Day 7的变化）")
    print("  5. 生成优化后的第二阶段矩阵（宽表合并）")
    print("  6. 版本对比与演进分析")
    
    # 1. 初始扫描
    cube_v1, lineage, change_tracker = simulate_initial_scan()
    
    # 2. AI分类优化
    cube_v1 = ai_classification_optimization(cube_v1)
    
    # 3. 质量评估
    scores = quality_evaluation(cube_v1)
    
    # 4. 增量更新（Day 7）
    cube_v2 = simulate_day7_changes(cube_v1, lineage, change_tracker)
    
    # 5. 生成优化矩阵
    optimized = generate_optimized_matrix(cube_v2, lineage)
    
    # 6. 版本对比
    version_comparison(change_tracker)
    
    print("\n" + "=" * 80)
    print("演示完成 - 系统能力总结")
    print("=" * 80)
    print("""
✅ 已完成的核心功能:
   
   1. 双矩阵架构
      - DataMatrix: 完整元数据存储
      - ColorMatrix: 视觉编码与模式发现
   
   2. 溯源系统
      - 第一阶段: 直接到物理库的URI溯源
      - 第二阶段: 完整的转换血缘链
      - 影响分析: 变更影响范围追踪
   
   3. 智能优化
      - AI分类: 规则+启发式+LLM支持
      - 质量评分: 多维度质量检测
      - 自动建议: 宽表合并、归档、重构
   
   4. 变更管理
      - 增量更新: 高效同步数据库变更
      - 版本控制: 历史版本对比与回滚
      - 变更追踪: 详细的变更日志
   
   5. 可视化
      - 交互式4D可视化
      - 颜色流动趋势
      - 血缘图谱展示

📋 待扩展功能:
   - LLM API集成（目前为模拟）
   - 实时流数据支持
   - 更多数据库连接器（Oracle、SQLServer等）
   - 权限与敏感数据自动发现
   - 与DBT/SQLMesh集成
""")


if __name__ == "__main__":
    main()
