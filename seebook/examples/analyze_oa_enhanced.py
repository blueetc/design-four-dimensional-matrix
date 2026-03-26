#!/usr/bin/env python3
"""
OA数据库增强版分析

集成新完成的功能：
1. 真实分类字段分析（查询实际值分布）
2. LLM智能分类
3. 外键关系自动提取
"""

import sys
sys.path.insert(0, "/Users/blue/seebook/src")

from hypercube.connectors.mysql import MySQLConnector
from hypercube.core.real_category_analyzer import RealCategoryAnalyzer, generate_category_report
from hypercube.core.llm_classifier import LLMClassifier, create_llm_classifier
from hypercube.core.relationship_extractor import RelationshipExtractor, extract_and_print_relationships
from hypercube.core.hypercube import HyperCube
from hypercube.core.data_matrix import DataCell
from hypercube.core.lineage import LineageTracker, PhysicalLocation
from datetime import datetime
import json
import os


def analyze_oa_enhanced():
    """增强版OA数据库分析"""
    
    print("=" * 80)
    print("OA数据库增强版分析")
    print("=" * 80)
    print("\n本分析集成以下新功能：")
    print("  ✓ 真实分类字段分析（查询实际值分布）")
    print("  ✓ LLM智能分类（OpenAI/Claude）")
    print("  ✓ 外键关系自动提取")
    print()
    
    # 连接数据库
    conn_params = {
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "password": "Tdsipass@@1234",
        "database": "oa",
    }
    
    print("正在连接数据库...")
    connector = MySQLConnector(conn_params)
    connector.connect()
    engine = connector.engine
    
    # 获取基础元数据
    print("\n获取基础元数据...")
    metadata_list = connector.get_all_tables_metadata()
    
    tables_info = []
    for meta in metadata_list:
        tables_info.append({
            "table_name": meta.table_name,
            "schema_name": meta.schema_name or "oa",
            "columns": [{"name": c["name"], "type": c["type"]} for c in meta.columns],
            "row_count": meta.row_count,
            "column_count": meta.column_count,
        })
    
    print(f"发现 {len(tables_info)} 个表")
    
    # =========================================================================
    # 功能1: 外键关系提取
    # =========================================================================
    print("\n" + "=" * 80)
    print("功能1: 外键关系自动提取")
    print("=" * 80)
    
    relationship_result = extract_and_print_relationships(engine, tables_info)
    
    # =========================================================================
    # 功能2: 真实分类字段分析
    # =========================================================================
    print("\n" + "=" * 80)
    print("功能2: 真实分类字段分析（查询实际值分布）")
    print("=" * 80)
    
    category_analyzer = RealCategoryAnalyzer(uniqueness_threshold=0.3)
    category_result = category_analyzer.analyze_database(
        engine, 
        tables=[t["table_name"] for t in tables_info]
    )
    
    # 生成分类报告
    category_report = generate_category_report(category_result)
    
    print(f"\n分类字段汇总:")
    print(f"  共发现 {category_report['summary']['total_category_fields']} 个分类字段")
    
    print(f"\n业务类型分布:")
    for biz_type, count in category_report.get("business_type_distribution", {}).items():
        print(f"  - {biz_type}: {count} 个字段")
    
    print(f"\n各表分类详情:")
    for table_info in category_report["tables"]:
        print(f"\n  【{table_info['table_name']}】")
        print(f"    分类字段数: {table_info['category_fields_count']}")
        for field in table_info["fields"][:3]:  # 只显示前3个
            print(f"    - {field['field_name']}: {field['inferred_business_type']}")
            if field['top_values']:
                top3 = ", ".join([
                    f"{v['value']}({v['percentage']:.0f}%)" 
                    for v in field['top_values'][:3]
                ])
                print(f"      分布: {top3}")
    
    # =========================================================================
    # 功能3: LLM智能分类
    # =========================================================================
    print("\n" + "=" * 80)
    print("功能3: LLM智能分类")
    print("=" * 80)
    
    # 检查是否有API密钥
    has_openai = os.getenv("OPENAI_API_KEY")
    has_anthropic = os.getenv("ANTHROPIC_API_KEY")
    
    if has_openai or has_anthropic:
        print(f"检测到API密钥，使用真实LLM...")
        print(f"  OpenAI: {'✓' if has_openai else '✗'}")
        print(f"  Anthropic: {'✓' if has_anthropic else '✗'}")
        
        try:
            llm_classifier = create_llm_classifier()
            use_llm = True
        except Exception as e:
            print(f"  警告: 初始化LLM失败: {e}")
            print("  将使用模拟模式")
            use_llm = False
    else:
        print("未检测到API密钥，使用模拟模式（基于规则）")
        print("  如需使用真实LLM，请设置环境变量:")
        print("    export OPENAI_API_KEY='your-key'")
        print("    export ANTHROPIC_API_KEY='your-key'")
        use_llm = False
        llm_classifier = LLMClassifier()  # 模拟模式
    
    print("\nLLM分类结果:")
    llm_results = []
    for table_info in tables_info:
        result = llm_classifier.classify_table(table_info)
        llm_results.append(result)
        
        confidence_emoji = "🟢" if result.confidence > 0.8 else "🟡" if result.confidence > 0.5 else "🔴"
        print(f"\n  {confidence_emoji} {result.table_name}")
        print(f"    业务域: {result.business_domain}")
        print(f"    置信度: {result.confidence:.2%}")
        print(f"    标签: {', '.join(result.suggested_tags)}")
        print(f"    推理: {result.reasoning[:60]}...")
    
    # =========================================================================
    # 整合分析结果，构建增强版四维矩阵
    # =========================================================================
    print("\n" + "=" * 80)
    print("整合分析结果，构建增强版四维矩阵")
    print("=" * 80)
    
    hypercube = HyperCube()
    lineage = LineageTracker()
    
    # 阶段到X轴的映射
    stage_to_x = {"new": 20, "growth": 50, "mature": 80, "legacy": 110}
    
    for i, table_info in enumerate(tables_info):
        table_name = table_info["table_name"]
        
        # 获取LLM分类结果
        llm_result = llm_results[i]
        
        # 获取分类复杂度
        category_fields = category_result.get(table_name, {})
        category_complexity = sum(
            f.category_confidence * 100 
            for f in category_fields.values()
        ) / max(len(category_fields), 1)
        
        # 获取外键关系
        foreign_keys = relationship_result["foreign_keys"].get(table_name, [])
        
        # 计算Z轴坐标（整合LLM分类和分类复杂度）
        # 基础：LLM分类的业务域
        # 增强：加入分类复杂度作为微调
        domain_mapping = {
            "用户管理": 0,
            "订单交易": 1,
            "商品管理": 2,
            "系统日志": 3,
            "其他": 4,
        }
        z = domain_mapping.get(llm_result.business_domain, 4)
        
        # X轴：生命周期
        x = stage_to_x.get(llm_result.lifecycle_stage, 50)
        
        # Y轴：数据量级（自适应）
        y = 0
        if table_info["row_count"] > 0:
            max_rows = max(t["row_count"] for t in tables_info)
            y = min(255, max(1, int(table_info["row_count"] / max(max_rows / 255, 1))))
        
        # 创建溯源
        loc = PhysicalLocation(
            db_type="mysql",
            host="localhost",
            port=3306,
            database="oa",
            schema="oa",
            table=table_name,
            snapshot_at=datetime.now(),
        )
        cell_id = f"oa_enhanced_{table_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        prov = lineage.register_first_stage(cell_id, loc)
        
        # 创建DataCell（增强版payload）
        cell = DataCell(
            t=datetime.now(),
            x=x,
            y=y,
            z=z,
            table_name=table_name,
            schema_name="oa",
            column_count=table_info["column_count"],
            row_count=table_info["row_count"],
            size_bytes=table_info["row_count"] * 200,
            business_domain=llm_result.business_domain,
            lifecycle_stage=llm_result.lifecycle_stage or "mature",
            tags=llm_result.suggested_tags + [f"复杂度{category_complexity:.0f}"],
            payload={
                "llm_classification": llm_result.to_dict(),
                "category_fields": {k: v.to_dict() for k, v in category_fields.items()},
                "foreign_keys": [fk.to_dict() for fk in foreign_keys],
                "category_complexity": category_complexity,
            },
            provenance=prov,
        )
        
        hypercube.add_cell(cell, compute_color=True)
        
        print(f"\n  {table_name}:")
        print(f"    Z轴: {z} ({llm_result.business_domain})")
        print(f"    分类复杂度: {category_complexity:.1f}")
        print(f"    外键关系: {len(foreign_keys)} 个")
        if foreign_keys:
            for fk in foreign_keys[:2]:
                inferred_mark = "(推断)" if fk.inferred else ""
                print(f"      - {fk.column_name} → {fk.ref_table_name} {inferred_mark}")
    
    # 同步颜色矩阵
    hypercube.sync_color_matrix()
    
    print("\n✓ 增强版四维矩阵构建完成")
    
    # 导出结果
    print("\n" + "=" * 80)
    print("导出分析结果")
    print("=" * 80)
    
    # 导出可视化数据
    viz_data = hypercube.export_for_visualization()
    viz_file = "/Users/blue/seebook/oa_enhanced_hypercube.json"
    with open(viz_file, "w") as f:
        json.dump(viz_data, f, indent=2, default=str)
    print(f"✓ 可视化数据: {viz_file}")
    
    # 导出完整报告
    full_report = {
        "database": "oa",
        "analysis_time": datetime.now().isoformat(),
        "features": {
            "real_category_analysis": True,
            "llm_classification": use_llm,
            "foreign_key_extraction": True,
        },
        "summary": {
            "total_tables": len(tables_info),
            "total_category_fields": category_report['summary']['total_category_fields'],
            "total_foreign_keys": sum(len(fks) for fks in relationship_result["foreign_keys"].values()),
        },
        "llm_classifications": [r.to_dict() for r in llm_results],
        "category_analysis": category_report,
        "relationships": {
            "foreign_keys": {k: [fk.to_dict() for fk in v] for k, v in relationship_result["foreign_keys"].items()},
            "relationship_graph": relationship_result["relationship_graph"],
        },
    }
    
    report_file = "/Users/blue/seebook/oa_enhanced_report.json"
    with open(report_file, "w") as f:
        json.dump(full_report, f, indent=2, ensure_ascii=False, default=str)
    print(f"✓ 完整报告: {report_file}")
    
    # 关闭连接
    connector.disconnect()
    
    print("\n" + "=" * 80)
    print("增强版分析完成！")
    print("=" * 80)
    print("\n新增功能验证:")
    print(f"  ✓ 真实分类字段分析: {category_report['summary']['total_category_fields']} 个分类字段")
    print(f"  ✓ LLM智能分类: {'真实LLM' if use_llm else '模拟模式'}")
    print(f"  ✓ 外键关系提取: {full_report['summary']['total_foreign_keys']} 个外键关系")
    print("\n后续建议:")
    print("  1. 设置 OPENAI_API_KEY 或 ANTHROPIC_API_KEY 使用真实LLM")
    print("  2. 查看 oa_enhanced_report.json 获取完整分析结果")
    print("  3. 使用可视化工具查看 oa_enhanced_hypercube.json")


if __name__ == "__main__":
    analyze_oa_enhanced()
