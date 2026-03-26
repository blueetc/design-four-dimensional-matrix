"""
未知数据库演示 - 展示系统如何处理任意结构的数据库

本演示展示系统如何在不预设任何业务域的情况下，
完全基于数据本身的特征动态发现主题域。
"""

import sys
sys.path.insert(0, "/Users/blue/seebook/src")

from hypercube.core.dynamic_classifier import (
    UnknownDatabaseProcessor, 
    TableSignature,
    DynamicDomainDiscoverer
)


def simulate_unknown_database():
    """
    模拟一个完全未知的、混乱的数据库
    
    表名使用各种命名规范，没有明显的业务域标识
    """
    
    # 完全随机的表名，没有预设的业务域
    raw_metadata = [
        # 第一组：看起来像是用户相关（通过外键和列名暗示）
        {
            "table_name": "tbl_usr",
            "schema_name": "main",
            "columns": [
                {"name": "uid", "type": "bigint"},
                {"name": "uname", "type": "varchar"},
                {"name": "email_addr", "type": "varchar"},
                {"name": "created_dt", "type": "timestamp"},
            ],
            "indexes": [{"name": "idx_uid"}],
            "primary_key": "uid",
            "foreign_keys": [],
            "row_count": 5000000,
        },
        {
            "table_name": "usr_profile_data",
            "schema_name": "main",
            "columns": [
                {"name": "profile_id", "type": "bigint"},
                {"name": "usr_uid", "type": "bigint"},  # 外键暗示
                {"name": "avatar_url", "type": "varchar"},
                {"name": "bio_txt", "type": "text"},
            ],
            "indexes": [{"name": "idx_usr"}],
            "primary_key": "profile_id",
            "foreign_keys": [{"column": "usr_uid", "ref_table": "tbl_usr", "ref_column": "uid"}],
            "row_count": 4800000,
        },
        {
            "table_name": "loginHistory",
            "schema_name": "main",
            "columns": [
                {"name": "loginId", "type": "bigint"},
                {"name": "userId", "type": "bigint"},  # 外键暗示
                {"name": "ipAddress", "type": "varchar"},
                {"name": "loginTime", "type": "timestamp"},
            ],
            "indexes": [{"name": "idx_user_time"}],
            "primary_key": "loginId",
            "foreign_keys": [{"column": "userId", "ref_table": "tbl_usr", "ref_column": "uid"}],
            "row_count": 50000000,
        },
        
        # 第二组：看起来像是交易相关
        {
            "table_name": "txn_main",
            "schema_name": "main",
            "columns": [
                {"name": "txn_id", "type": "bigint"},
                {"name": "buyer_uid", "type": "bigint"},  # 关联到用户
                {"name": "amt", "type": "decimal"},
                {"name": "status_cd", "type": "varchar"},
                {"name": "created_at", "type": "timestamp"},
            ],
            "indexes": [{"name": "idx_txn_buyer"}],
            "primary_key": "txn_id",
            "foreign_keys": [{"column": "buyer_uid", "ref_table": "tbl_usr", "ref_column": "uid"}],
            "row_count": 10000000,
        },
        {
            "table_name": "txn_items",
            "schema_name": "main",
            "columns": [
                {"name": "item_id", "type": "bigint"},
                {"name": "parent_txn_id", "type": "bigint"},
                {"name": "sku_code", "type": "varchar"},
                {"name": "qty", "type": "int"},
                {"name": "unit_price", "type": "decimal"},
            ],
            "indexes": [{"name": "idx_txn"}],
            "primary_key": "item_id",
            "foreign_keys": [{"column": "parent_txn_id", "ref_table": "txn_main", "ref_column": "txn_id"}],
            "row_count": 25000000,
        },
        {
            "table_name": "pay_record",
            "schema_name": "main",
            "columns": [
                {"name": "pay_id", "type": "bigint"},
                {"name": "rel_txn_id", "type": "bigint"},
                {"name": "pay_method", "type": "varchar"},
                {"name": "pay_amount", "type": "decimal"},
            ],
            "indexes": [{"name": "idx_pay_txn"}],
            "primary_key": "pay_id",
            "foreign_keys": [{"column": "rel_txn_id", "ref_table": "txn_main", "ref_column": "txn_id"}],
            "row_count": 9500000,
        },
        
        # 第三组：商品/库存相关
        {
            "table_name": "sku_master",
            "schema_name": "main",
            "columns": [
                {"name": "sku_id", "type": "bigint"},
                {"name": "sku_name", "type": "varchar"},
                {"name": "category_code", "type": "varchar"},
                {"name": "price", "type": "decimal"},
            ],
            "indexes": [{"name": "idx_sku_cat"}],
            "primary_key": "sku_id",
            "foreign_keys": [],
            "row_count": 50000,
        },
        {
            "table_name": "inv_stock",
            "schema_name": "main",
            "columns": [
                {"name": "stock_id", "type": "bigint"},
                {"name": "sku_ref_id", "type": "bigint"},
                {"name": "warehouse_loc", "type": "varchar"},
                {"name": "qty_on_hand", "type": "int"},
            ],
            "indexes": [{"name": "idx_sku_wh"}],
            "primary_key": "stock_id",
            "foreign_keys": [{"column": "sku_ref_id", "ref_table": "sku_master", "ref_column": "sku_id"}],
            "row_count": 200000,
        },
        
        # 第四组：孤立表（可能不属于任何域）
        {
            "table_name": "sys_config",
            "schema_name": "main",
            "columns": [
                {"name": "cfg_key", "type": "varchar"},
                {"name": "cfg_val", "type": "varchar"},
            ],
            "indexes": [{"name": "idx_key"}],
            "primary_key": "cfg_key",
            "foreign_keys": [],
            "row_count": 100,
        },
        
        # 第五组：遗留表（数据量小，无关联）
        {
            "table_name": "old_data_2020",
            "schema_name": "archive",
            "columns": [
                {"name": "id", "type": "bigint"},
                {"name": "data", "type": "text"},
            ],
            "indexes": [],
            "primary_key": "id",
            "foreign_keys": [],
            "row_count": 5000,
        },
    ]
    
    return raw_metadata


def demonstrate_dynamic_classification():
    """演示动态分类"""
    
    print("=" * 80)
    print("未知数据库动态分类演示")
    print("=" * 80)
    print("\n假设场景：")
    print("  你接手了一个遗留系统，数据库表命名混乱：")
    print("  - tbl_usr, usr_profile_data, loginHistory")
    print("  - txn_main, txn_items, pay_record")
    print("  - sku_master, inv_stock")
    print("  - sys_config, old_data_2020")
    print("\n  没有任何文档说明这些表属于什么业务域。")
    print("\n系统会如何自动发现业务结构？")
    
    # 获取模拟数据
    raw_metadata = simulate_unknown_database()
    
    print(f"\n" + "-" * 80)
    print("原始数据:")
    print("-" * 80)
    for meta in raw_metadata:
        print(f"  {meta['schema_name']}.{meta['table_name']}")
        print(f"    列: {', '.join(c['name'] for c in meta['columns'][:3])}...")
        print(f"    行数: {meta['row_count']:,}")
        if meta['foreign_keys']:
            for fk in meta['foreign_keys']:
                print(f"    外键: {fk['column']} → {fk['ref_table']}.{fk['ref_column']}")
    
    # 运行动态分类
    print(f"\n" + "=" * 80)
    print("动态分析中...")
    print("=" * 80)
    
    processor = UnknownDatabaseProcessor()
    result = processor.process(raw_metadata)
    
    # 展示结果
    print(f"\n✓ 分析完成！")
    print(f"\n统计信息:")
    print(f"  - 发现 {result['stats']['domain_count']} 个主题域")
    print(f"  - 总表数: {result['stats']['total_tables']}")
    print(f"  - 总列数: {result['stats']['total_columns']}")
    print(f"  - 总数据量: {result['stats']['total_rows']:,} 行")
    print(f"  - 表间关联: {result['stats']['relationships']} 个外键关系")
    
    print(f"\n动态发现的业务域:")
    print("-" * 80)
    
    for z_id, domain_info in result['domains'].items():
        print(f"\n【主题域 Z={z_id}: {domain_info['name']}】")
        print(f"  描述: {domain_info['description']}")
        print(f"  包含表:")
        for table_name in domain_info['tables']:
            lifecycle = result['lifecycle_mapping'].get(table_name, 'unknown')
            print(f"    - {table_name:25s} (生命周期: {lifecycle})")
    
    # 分析洞察
    print(f"\n" + "=" * 80)
    print("洞察分析:")
    print("=" * 80)
    
    # 找出命名相似但分在不同域的表
    print("\n1. 跨域命名相似度检查:")
    for z1, d1 in result['domains'].items():
        for z2, d2 in result['domains'].items():
            if z1 >= z2:
                continue
            # 检查是否有相似表名
            for t1 in d1['tables']:
                for t2 in d2['tables']:
                    tokens1 = set(t1.lower().split('_'))
                    tokens2 = set(t2.lower().split('_'))
                    common = tokens1 & tokens2
                    if common and len(common) >= 1:
                        print(f"   ⚠️  {t1} (Z={z1}) 与 {t2} (Z={z2}) 有共同token: {common}")
    
    # 找出孤立的表
    print("\n2. 孤立表识别:")
    all_related = set()
    for sig in result['signatures']:
        for fk in sig.foreign_keys:
            all_related.add(sig.table_name)
            all_related.add(fk.get('ref_table', ''))
    
    for sig in result['signatures']:
        if sig.table_name not in all_related and sig.row_count < 10000:
            print(f"   ⚠️  {sig.table_name}: 无关联且数据量小，可能是配置表或遗留表")
    
    # 生命周期分布
    print("\n3. 生命周期分布:")
    stage_count = {}
    for stage in result['lifecycle_mapping'].values():
        stage_count[stage] = stage_count.get(stage, 0) + 1
    for stage, count in sorted(stage_count.items()):
        print(f"   - {stage}: {count} 个表")
    
    return result


def demonstrate_adaptability():
    """展示对不同数据库结构的适应性"""
    
    print(f"\n\n" + "=" * 80)
    print("不同命名规范测试")
    print("=" * 80)
    
    test_cases = [
        {
            "name": "电商系统（snake_case）",
            "tables": [
                {"table_name": "customers", "columns": [{"name": "id"}, {"name": "email"}], "row_count": 10000, "foreign_keys": []},
                {"table_name": "customer_addresses", "columns": [{"name": "id"}, {"name": "customer_id"}], "row_count": 25000, 
                 "foreign_keys": [{"column": "customer_id", "ref_table": "customers", "ref_column": "id"}]},
                {"table_name": "orders", "columns": [{"name": "id"}, {"name": "customer_id"}], "row_count": 50000,
                 "foreign_keys": [{"column": "customer_id", "ref_table": "customers", "ref_column": "id"}]},
                {"table_name": "order_items", "columns": [{"name": "id"}, {"name": "order_id"}], "row_count": 150000,
                 "foreign_keys": [{"column": "order_id", "ref_table": "orders", "ref_column": "id"}]},
            ]
        },
        {
            "name": "金融系统（camelCase）",
            "tables": [
                {"table_name": "AccountMaster", "columns": [{"name": "accountId"}, {"name": "holderName"}], "row_count": 5000, "foreign_keys": []},
                {"table_name": "TransactionLog", "columns": [{"name": "txnId"}, {"name": "accountId"}], "row_count": 1000000,
                 "foreign_keys": [{"column": "accountId", "ref_table": "AccountMaster", "ref_column": "accountId"}]},
                {"table_name": "AccountBalance", "columns": [{"name": "balanceId"}, {"name": "accountId"}], "row_count": 5000,
                 "foreign_keys": [{"column": "accountId", "ref_table": "AccountMaster", "ref_column": "accountId"}]},
            ]
        },
        {
            "name": "遗留系统（混乱命名）",
            "tables": [
                {"table_name": "T001", "columns": [{"name": "F001"}, {"name": "F002"}], "row_count": 100000, "foreign_keys": []},
                {"table_name": "T002", "columns": [{"name": "F001"}, {"name": "REF_T001"}], "row_count": 500000,
                 "foreign_keys": [{"column": "REF_T001", "ref_table": "T001", "ref_column": "F001"}]},
                {"table_name": "CONFIG", "columns": [{"name": "KEY"}, {"name": "VAL"}], "row_count": 50, "foreign_keys": []},
            ]
        },
    ]
    
    for test_case in test_cases:
        print(f"\n测试: {test_case['name']}")
        print("-" * 40)
        
        # 格式转换
        raw_metadata = []
        for t in test_case['tables']:
            raw_metadata.append({
                "table_name": t['table_name'],
                "schema_name": "test",
                "columns": [{"name": c['name'], "type": "varchar"} for c in t['columns']],
                "indexes": [],
                "primary_key": None,
                "foreign_keys": t.get('foreign_keys', []),
                "row_count": t['row_count'],
                "column_count": len(t['columns']),
            })
        
        processor = UnknownDatabaseProcessor()
        result = processor.process(raw_metadata)
        
        print(f"  发现 {result['stats']['domain_count']} 个主题域:")
        for z_id, domain_info in result['domains'].items():
            print(f"    Z={z_id}: {domain_info['name']} ({domain_info['table_count']} 表)")


def main():
    print("\n" + "=" * 80)
    print("四维矩阵系统 - 未知数据库自适应演示")
    print("=" * 80)
    print("\n本演示展示系统如何处理完全未知结构的数据库：")
    print("  • 不预设任何业务域（没有user/revenue/product等）")
    print("  • 不依赖特定的表名模式")
    print("  • 通过外键关系、命名相似度、结构特征动态聚类")
    print("  • 自适应的生命周期分类")
    
    # 主演示
    result = demonstrate_dynamic_classification()
    
    # 适应性测试
    demonstrate_adaptability()
    
    print(f"\n" + "=" * 80)
    print("演示完成")
    print("=" * 80)
    print("\n关键结论:")
    print("  1. 系统能够完全基于数据特征动态发现主题域，无需预设")
    print("  2. 外键关系是发现业务域边界的最强信号")
    print("  3. 命名相似度可以辅助验证聚类结果")
    print("  4. 自适应的生命周期分类适用于任何规模的数据库")


if __name__ == "__main__":
    main()
