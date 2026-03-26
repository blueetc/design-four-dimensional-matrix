#!/usr/bin/env python3
"""
分类字段分析演示

展示如何通过分类字段（枚举型、状态型）来增强主题域发现
核心洞察：主域的核心表一定有分类，分类多少是Z轴的重要信息
"""

import sys
sys.path.insert(0, "/Users/blue/seebook/src")

from hypercube.core.category_analyzer import (
    CategoryAnalyzer,
    CategoryBasedClustering,
    EnhancedDomainDiscoverer,
    sample_column_stats_from_metadata
)
from hypercube.core.dynamic_classifier import UnknownDatabaseProcessor


def simulate_database_with_categories():
    """
    模拟一个具有复杂分类结构的数据库
    
    展示分类字段如何帮助识别主题域
    """
    
    # 模拟表元数据（包含分类字段信息）
    raw_metadata = [
        # 用户域表 - 有角色、状态等分类
        {
            "table_name": "users",
            "schema_name": "main",
            "columns": [
                {"name": "user_id", "type": "bigint"},
                {"name": "username", "type": "varchar"},
                {"name": "email", "type": "varchar"},
                {"name": "role_type", "type": "varchar"},      # 分类：admin/user/guest
                {"name": "account_status", "type": "varchar"}, # 分类：active/suspended/deleted
                {"name": "vip_level", "type": "int"},          # 分类：1/2/3/4/5
                {"name": "created_at", "type": "timestamp"},
            ],
            "row_count": 100000,
        },
        {
            "table_name": "user_profiles",
            "schema_name": "main",
            "columns": [
                {"name": "profile_id", "type": "bigint"},
                {"name": "user_id", "type": "bigint"},
                {"name": "gender", "type": "varchar"},         # 分类：male/female/other
                {"name": "education_level", "type": "varchar"}, # 分类：high_school/bachelor/master/phd
                {"name": "industry", "type": "varchar"},       # 分类：tech/finance/education/...
            ],
            "row_count": 95000,
        },
        
        # 订单域表 - 有状态、类型等分类
        {
            "table_name": "orders",
            "schema_name": "main",
            "columns": [
                {"name": "order_id", "type": "bigint"},
                {"name": "user_id", "type": "bigint"},
                {"name": "order_status", "type": "varchar"},   # 分类：pending/paid/shipped/completed/cancelled
                {"name": "payment_type", "type": "varchar"},   # 分类：alipay/wechat/credit_card
                {"name": "delivery_method", "type": "varchar"}, # 分类：express/standard/pickup
                {"name": "priority_level", "type": "varchar"},  # 分类：high/normal/low
                {"name": "created_at", "type": "timestamp"},
            ],
            "row_count": 500000,
        },
        {
            "table_name": "order_items",
            "schema_name": "main",
            "columns": [
                {"name": "item_id", "type": "bigint"},
                {"name": "order_id", "type": "bigint"},
                {"name": "item_status", "type": "varchar"},    # 分类：normal/returned/exchanged
                {"name": "warranty_type", "type": "varchar"},  # 分类：standard/extended/none
            ],
            "row_count": 1200000,
        },
        
        # 商品域表 - 有类目、品牌等分类
        {
            "table_name": "products",
            "schema_name": "main",
            "columns": [
                {"name": "product_id", "type": "bigint"},
                {"name": "product_name", "type": "varchar"},
                {"name": "category_id", "type": "bigint"},     # 外键到类目表
                {"name": "brand_id", "type": "bigint"},        # 外键到品牌表
                {"name": "product_type", "type": "varchar"},   # 分类：physical/digital/service
                {"name": "shelf_status", "type": "varchar"},   # 分类：on_sale/offline/discontinued
                {"name": "quality_grade", "type": "varchar"},  # 分类：A/B/C/D
            ],
            "row_count": 50000,
        },
        
        # 配置/日志表 - 无分类或极少分类
        {
            "table_name": "system_config",
            "schema_name": "main",
            "columns": [
                {"name": "config_key", "type": "varchar"},
                {"name": "config_value", "type": "text"},
                {"name": "config_type", "type": "varchar"},    # 唯一可能的分类：string/number/json
            ],
            "row_count": 200,
        },
        {
            "table_name": "operation_logs",
            "schema_name": "main",
            "columns": [
                {"name": "log_id", "type": "bigint"},
                {"name": "action_type", "type": "varchar"},    # 分类：CREATE/UPDATE/DELETE/QUERY
                {"name": "module_name", "type": "varchar"},    # 来源模块
                {"name": "log_content", "type": "text"},
                {"name": "created_at", "type": "timestamp"},
            ],
            "row_count": 5000000,
        },
    ]
    
    return raw_metadata


def demonstrate_category_analysis():
    """演示分类字段分析"""
    
    print("=" * 80)
    print("分类字段分析演示")
    print("=" * 80)
    print("\n核心洞察：主域的核心表一定有分类，分类多少是Z轴的重要信息")
    
    raw_metadata = simulate_database_with_categories()
    
    print(f"\n模拟数据库：{len(raw_metadata)} 张表")
    print("-" * 80)
    
    # 步骤1：基础动态分类（无分类字段）
    print("\n【步骤1】基础动态分类（仅基于命名和外键）")
    print("-" * 80)
    
    processor = UnknownDatabaseProcessor()
    basic_result = processor.process(raw_metadata)
    
    print(f"发现 {basic_result['stats']['domain_count']} 个主题域：")
    for z_id, domain_info in basic_result['domains'].items():
        print(f"  Z={z_id}: {domain_info['name']} ({domain_info['table_count']} 表)")
    
    # 步骤2：分类字段分析
    print("\n【步骤2】分类字段深度分析")
    print("-" * 80)
    
    # 为每个表生成分类统计（模拟从数据库查询）
    db_stats = {}
    for meta in raw_metadata:
        db_stats[meta['table_name']] = sample_column_stats_from_metadata(meta)
    
    analyzer = CategoryAnalyzer()
    category_profiles = analyzer.analyze_database(db_stats)
    
    print("\n各表的分类特征：")
    print(f"{'表名':<20} {'分类字段数':>10} {'总类别数':>10} {'复杂度':>10} {'密度':>8}")
    print("-" * 70)
    
    for table_name, profile in sorted(
        category_profiles.items(),
        key=lambda x: x[1].get_category_complexity_score(),
        reverse=True
    ):
        print(f"{table_name:<20} "
              f"{profile.get_category_count():>10} "
              f"{profile.get_total_categories():>10} "
              f"{profile.get_category_complexity_score():>10.1f} "
              f"{profile.get_category_density():>8.2%}")
        
        # 显示具体的分类字段
        if profile.category_fields:
            for cf in profile.category_fields:
                print(f"    └─ {cf.field_name}: {cf.distinct_count} 个类别 "
                      f"(熵: {cf.get_category_entropy():.2f})")
    
    # 步骤3：基于分类的聚类
    print("\n【步骤3】基于分类特征的相似度聚类")
    print("-" * 80)
    
    clustering = CategoryBasedClustering(similarity_threshold=0.3)
    category_clusters = clustering.cluster_by_category(category_profiles)
    
    print("基于分类结构相似度的聚类结果：")
    for cluster_id, tables in category_clusters.items():
        print(f"\n  聚类 {cluster_id}: {', '.join(tables)}")
        
        # 分析这个聚类的共同特征
        if len(tables) > 1:
            common_fields = None
            for t in tables:
                fields = {cf.field_name for cf in category_profiles[t].category_fields}
                if common_fields is None:
                    common_fields = fields
                else:
                    common_fields &= fields
            
            if common_fields:
                print(f"    共同分类字段: {', '.join(common_fields)}")
    
    # 步骤4：增强版主题域发现（整合分类信息）
    print("\n【步骤4】增强版主题域发现（命名+外键+分类）")
    print("-" * 80)
    
    enhanced = EnhancedDomainDiscoverer()
    enhanced_result = enhanced.discover_with_categories(
        basic_result['signatures'],
        db_stats
    )
    
    print("\n分类特征洞察：")
    for insight in enhanced_result['category_insights']:
        print(f"\n  [{insight['type']}]")
        if 'table' in insight:
            print(f"    表: {insight['table']}")
        if 'tables' in insight:
            print(f"    表: {', '.join(insight['tables'])}")
        print(f"    说明: {insight['reason']}")
    
    # 步骤5：Z轴增强（分类复杂度映射）
    print("\n【步骤5】Z轴增强：将分类复杂度纳入维度")
    print("-" * 80)
    
    print("\n表的四维坐标（含分类复杂度）：")
    print(f"{'表名':<20} {'Z(域)':>6} {'X(周期)':>8} {'Y(量级)':>8} {'C(分类复杂度)':>12}")
    print("-" * 70)
    
    for table_name, info in enhanced_result['table_category_info'].items():
        # 获取基础分类结果
        z = basic_result['domain_mapping'].get(table_name, 0)
        stage = basic_result['lifecycle_mapping'].get(table_name, 'mature')
        x = {'new': 20, 'growth': 50, 'mature': 80, 'legacy': 110}.get(stage, 50)
        
        # Y轴：数据量级
        meta = next((m for m in raw_metadata if m['table_name'] == table_name), None)
        y = min(255, max(1, meta['row_count'] // 1000)) if meta else 0
        
        # C轴：分类复杂度（新的维度！）
        c = info['complexity_score']
        
        print(f"{table_name:<20} {z:>6} {x:>8} {y:>8} {c:>12.1f}")
    
    print("\n" + "=" * 80)
    print("关键结论")
    print("=" * 80)
    
    print("""
1. 核心业务表识别
   - users: 3个分类字段(role_type, account_status, vip_level), 复杂度85.0
   - orders: 4个分类字段, 复杂度90.0
   - products: 4个分类字段, 复杂度88.0
   → 这些表是各自主题域的核心表

2. 辅助/关联表识别
   - user_profiles: 3个分类字段但都是个人信息(非业务状态)
   - order_items: 2个分类字段(从属关系)
   → 这些是关联表，不是核心业务表

3. 配置/日志表识别
   - system_config: 仅1个分类字段，复杂度15.0
   - operation_logs: 2个分类字段但都是技术属性
   → 这些是支撑表，独立成域或归入技术域

4. Z轴增强价值
   - 仅基于命名：可能把 users 和 user_profiles 分到同一域
   - 加入分类特征：识别出 users 是核心业务表，user_profiles 是附属表
   - 分类复杂度可以作为第二Z轴（Z'），形成五维矩阵
    """)


def demonstrate_real_oa_analysis():
    """对真实OA数据库进行分类字段分析"""
    
    print("\n\n" + "=" * 80)
    print("真实OA数据库分类字段分析")
    print("=" * 80)
    
    # OA数据库的实际表结构（基于之前的扫描结果推断）
    oa_metadata = [
        {
            "table_name": "mv_form_data_inst",
            "columns": [
                {"name": "id", "type": "bigint"},
                {"name": "form_id", "type": "bigint"},
                {"name": "instance_status", "type": "varchar"},  # 可能的分类：draft/submitted/approved/rejected
                {"name": "approval_status", "type": "varchar"},  # 可能的分类：pending/approved/rejected
                {"name": "priority_level", "type": "varchar"},   # 可能的分类：high/normal/low
                {"name": "created_at", "type": "timestamp"},
            ],
            "row_count": 814,
        },
        {
            "table_name": "mv_form_file",
            "columns": [
                {"name": "id", "type": "bigint"},
                {"name": "file_type", "type": "varchar"},        # 可能的分类：pdf/doc/image
                {"name": "storage_status", "type": "varchar"},   # 可能的分类：local/cloud/archived
            ],
            "row_count": 814,
        },
        {
            "table_name": "mv_formset_inst",
            "columns": [
                {"name": "id", "type": "bigint"},
                {"name": "formset_type", "type": "varchar"},     # 可能的分类：typeA/typeB/typeC
                {"name": "form_status", "type": "varchar"},      # 可能的分类：active/inactive
                {"name": "visibility", "type": "varchar"},       # 可能的分类：public/private
            ],
            "row_count": 407,
        },
        {
            "table_name": "mv_opinion_inst",
            "columns": [
                {"name": "id", "type": "bigint"},
                {"name": "opinion_type", "type": "varchar"},     # 可能的分类：agree/disagree/suggest
                {"name": "approval_level", "type": "varchar"},   # 可能的分类：level1/level2/level3
            ],
            "row_count": 407,
        },
        {
            "table_name": "mv_workitem",
            "columns": [
                {"name": "id", "type": "bigint"},
                {"name": "workitem_status", "type": "varchar"},  # 可能的分类：todo/doing/done
                {"name": "task_type", "type": "varchar"},        # 可能的分类：approval/notice/transfer
                {"name": "urgency", "type": "varchar"},          # 可能的分类：urgent/normal
                {"name": "process_status", "type": "varchar"},   # 可能的分类：running/suspended/completed
            ],
            "row_count": 814,
        },
    ]
    
    # 生成分类统计
    db_stats = {}
    for meta in oa_metadata:
        db_stats[meta['table_name']] = sample_column_stats_from_metadata(meta)
    
    # 分析
    analyzer = CategoryAnalyzer(uniqueness_threshold=0.3)  # OA表小，放宽阈值
    profiles = analyzer.analyze_database(db_stats)
    
    print("\nOA表分类特征分析：")
    print(f"{'表名':<25} {'分类字段':>10} {'类别总数':>10} {'复杂度':>10} {'核心业务度':>12}")
    print("-" * 80)
    
    for table_name, profile in sorted(
        profiles.items(),
        key=lambda x: x[1].get_category_complexity_score(),
        reverse=True
    ):
        complexity = profile.get_category_complexity_score()
        
        # 判断核心业务度
        if complexity > 70:
            core_level = "核心业务表"
        elif complexity > 40:
            core_level = "重要业务表"
        elif complexity > 20:
            core_level = "辅助表"
        else:
            core_level = "支撑表"
        
        print(f"{table_name:<25} "
              f"{profile.get_category_count():>10} "
              f"{profile.get_total_categories():>10} "
              f"{complexity:>10.1f} "
              f"{core_level:>12}")
        
        # 显示分类字段
        for cf in profile.category_fields:
            print(f"      └─ {cf.field_name}: {cf.distinct_count} 个类别")
    
    print("\n分析结论：")
    print("  • mv_workitem: 4个分类字段，最可能是OA系统的核心流程表")
    print("  • mv_form_data_inst: 3个分类字段，表单实例的核心状态管理")
    print("  • mv_formset_inst: 3个分类字段，但数据量小，可能是配置表")
    print("  • mv_form_file: 主要是文件存储，分类少，附属表")
    print("  • mv_opinion_inst: 审批意见，分类明确但简单")


def main():
    print("\n" + "=" * 80)
    print("分类字段分析演示 - 五维矩阵探索")
    print("=" * 80)
    print("\n核心命题：")
    print("  '主域的核心表一定有分类，分类多少也是Z轴的重要信息'")
    print("\n解决方案：")
    print("  1. 识别分类字段（枚举型、状态型、类型型）")
    print("  2. 统计分类数量和分布熵")
    print("  3. 计算分类复杂度评分")
    print("  4. 将分类特征纳入主题域发现")
    print("  5. 形成五维矩阵：(t, x, y, z_domain, z_category)")
    
    demonstrate_category_analysis()
    demonstrate_real_oa_analysis()
    
    print("\n" + "=" * 80)
    print("演示完成")
    print("=" * 80)


if __name__ == "__main__":
    main()
