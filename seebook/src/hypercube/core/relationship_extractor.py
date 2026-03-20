"""
外键关系提取器

从数据库元数据中提取真实的外键关系
"""

from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from collections import defaultdict
from sqlalchemy import text, inspect


@dataclass
class ForeignKey:
    """外键关系"""
    # 当前表（从表）
    table_name: str
    column_name: str
    
    # 引用的表（主表）
    ref_table_name: str
    ref_column_name: str
    
    # 约束信息
    constraint_name: Optional[str] = None
    
    # 推断信息
    relationship_type: str = "many_to_one"  # many_to_one / one_to_many
    inferred: bool = False  # True表示基于命名推断，False表示真实外键
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "table": self.table_name,
            "column": self.column_name,
            "ref_table": self.ref_table_name,
            "ref_column": self.ref_column_name,
            "constraint": self.constraint_name,
            "type": self.relationship_type,
            "inferred": self.inferred,
        }


@dataclass
class TableRelationship:
    """表间关系"""
    table_a: str
    table_b: str
    relationship: str  # one_to_one, one_to_many, many_to_many
    via_table: Optional[str] = None  # 多对多的中间表
    foreign_keys: List[ForeignKey] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "table_a": self.table_a,
            "table_b": self.table_b,
            "relationship": self.relationship,
            "via_table": self.via_table,
            "foreign_keys": [fk.to_dict() for fk in self.foreign_keys],
        }


class RelationshipExtractor:
    """
    外键关系提取器
    
    提取数据库中真实的表间关系
    """
    
    def __init__(self, infer_missing: bool = True):
        """
        初始化
        
        Args:
            infer_missing: 是否基于命名推断缺失的外键
        """
        self.infer_missing = infer_missing
    
    def extract_from_mysql(self, engine) -> Dict[str, List[ForeignKey]]:
        """
        从MySQL提取外键关系
        
        Returns:
            {table_name: [ForeignKey, ...]}
        """
        result = defaultdict(list)
        
        try:
            with engine.connect() as conn:
                # 查询information_schema获取外键信息
                query = """
                    SELECT 
                        k.TABLE_NAME,
                        k.COLUMN_NAME,
                        k.CONSTRAINT_NAME,
                        k.REFERENCED_TABLE_NAME,
                        k.REFERENCED_COLUMN_NAME
                    FROM information_schema.KEY_COLUMN_USAGE k
                    WHERE k.TABLE_SCHEMA = DATABASE()
                        AND k.REFERENCED_TABLE_NAME IS NOT NULL
                """
                
                fk_result = conn.execute(text(query))
                
                for row in fk_result:
                    fk = ForeignKey(
                        table_name=row.TABLE_NAME,
                        column_name=row.COLUMN_NAME,
                        ref_table_name=row.REFERENCED_TABLE_NAME,
                        ref_column_name=row.REFERENCED_COLUMN_NAME,
                        constraint_name=row.CONSTRAINT_NAME,
                        inferred=False,
                    )
                    result[row.TABLE_NAME].append(fk)
                
                print(f"从MySQL提取到 {sum(len(fks) for fks in result.values())} 个外键关系")
                
        except Exception as e:
            print(f"从MySQL提取外键失败: {e}")
        
        return dict(result)
    
    def extract_from_postgres(self, engine) -> Dict[str, List[ForeignKey]]:
        """
        从PostgreSQL提取外键关系
        """
        result = defaultdict(list)
        
        try:
            with engine.connect() as conn:
                query = """
                    SELECT
                        tc.table_name,
                        kcu.column_name,
                        tc.constraint_name,
                        ccu.table_name AS referenced_table_name,
                        ccu.column_name AS referenced_column_name
                    FROM information_schema.table_constraints AS tc
                    JOIN information_schema.key_column_usage AS kcu
                        ON tc.constraint_name = kcu.constraint_name
                        AND tc.table_schema = kcu.table_schema
                    JOIN information_schema.constraint_column_usage AS ccu
                        ON ccu.constraint_name = tc.constraint_name
                        AND ccu.table_schema = tc.table_schema
                    WHERE tc.constraint_type = 'FOREIGN KEY'
                        AND tc.table_schema = 'public'
                """
                
                fk_result = conn.execute(text(query))
                
                for row in fk_result:
                    fk = ForeignKey(
                        table_name=row.table_name,
                        column_name=row.column_name,
                        ref_table_name=row.referenced_table_name,
                        ref_column_name=row.referenced_column_name,
                        constraint_name=row.constraint_name,
                        inferred=False,
                    )
                    result[row.table_name].append(fk)
                
                print(f"从PostgreSQL提取到 {sum(len(fks) for fks in result.values())} 个外键关系")
                
        except Exception as e:
            print(f"从PostgreSQL提取外键失败: {e}")
        
        return dict(result)
    
    def infer_from_naming(self, 
                          table_name: str,
                          columns: List[Dict],
                          all_tables: List[str]) -> List[ForeignKey]:
        """
        基于命名约定推断外键关系
        
        例如：
        - user_id → users.id
        - order_id → orders.id
        - parent_id → same_table.id (自引用)
        """
        inferred_fks = []
        
        for col in columns:
            col_name = col.get("name", "").lower()
            
            # 模式1: xxx_id → xxxs.id
            if col_name.endswith("_id") and col_name != "id":
                potential_table = col_name[:-3]  # 去掉 _id
                
                # 尝试复数形式匹配
                candidates = [
                    potential_table,           # user
                    potential_table + "s",     # users
                    potential_table + "es",    # boxes
                    potential_table[:-1] + "ies" if potential_table.endswith("y") else "",  # category -> categories
                ]
                
                for candidate in candidates:
                    if candidate and candidate in [t.lower() for t in all_tables]:
                        # 找到匹配的表
                        matched_table = next(
                            t for t in all_tables if t.lower() == candidate
                        )
                        
                        # 排除自引用（除非是parent_id）
                        if matched_table.lower() == table_name.lower() and col_name != "parent_id":
                            continue
                        
                        fk = ForeignKey(
                            table_name=table_name,
                            column_name=col.get("name"),
                            ref_table_name=matched_table,
                            ref_column_name="id",
                            inferred=True,
                            relationship_type="many_to_one",
                        )
                        inferred_fks.append(fk)
                        break
            
            # 模式2: parent_id → 自引用
            if col_name == "parent_id":
                fk = ForeignKey(
                    table_name=table_name,
                    column_name=col.get("name"),
                    ref_table_name=table_name,  # 自引用
                    ref_column_name="id",
                    inferred=True,
                    relationship_type="many_to_one",
                )
                inferred_fks.append(fk)
        
        return inferred_fks
    
    def extract_all_relationships(self,
                                   engine,
                                   tables_metadata: List[Dict]) -> Dict[str, Any]:
        """
        提取所有表间关系
        
        Returns:
            {
                "foreign_keys": {table: [ForeignKey, ...]},
                "table_relationships": [TableRelationship, ...],
                "relationship_graph": {table: [related_tables]},
            }
        """
        # 1. 提取真实外键
        db_type = engine.url.drivername
        
        if "mysql" in db_type:
            real_fks = self.extract_from_mysql(engine)
        elif "postgres" in db_type:
            real_fks = self.extract_from_postgres(engine)
        else:
            real_fks = {}
        
        # 2. 基于命名推断（补充缺失的）
        all_table_names = [m["table_name"] for m in tables_metadata]
        
        if self.infer_missing:
            for meta in tables_metadata:
                table_name = meta["table_name"]
                columns = meta.get("columns", [])
                
                # 如果该表还没有外键，尝试推断
                if table_name not in real_fks or not real_fks[table_name]:
                    inferred = self.infer_from_naming(
                        table_name, columns, all_table_names
                    )
                    if inferred:
                        real_fks[table_name] = inferred
                        print(f"  推断 {table_name} 的外键: {[fk.column_name for fk in inferred]}")
        
        # 3. 构建关系图
        relationship_graph = defaultdict(set)
        for table, fks in real_fks.items():
            for fk in fks:
                relationship_graph[table].add(fk.ref_table_name)
                relationship_graph[fk.ref_table_name].add(table)
        
        # 4. 识别表间关系类型
        table_relationships = self._identify_relationships(real_fks)
        
        return {
            "foreign_keys": real_fks,
            "table_relationships": table_relationships,
            "relationship_graph": dict(relationship_graph),
        }
    
    def _identify_relationships(self, 
                                foreign_keys: Dict[str, List[ForeignKey]]
                                ) -> List[TableRelationship]:
        """
        识别表间关系类型（一对一、一对多、多对多）
        """
        relationships = []
        processed_pairs = set()
        
        # 收集所有关系对
        for table, fks in foreign_keys.items():
            for fk in fks:
                pair = tuple(sorted([table, fk.ref_table_name]))
                if pair in processed_pairs:
                    continue
                processed_pairs.add(pair)
                
                # 判断关系类型
                # A有外键指向B：多对一
                # B有外键指向A：多对一
                # 都有外键指向对方：多对多（可能有中间表）
                
                a_to_b = any(
                    fk.ref_table_name == pair[1] 
                    for fk in foreign_keys.get(pair[0], [])
                )
                b_to_a = any(
                    fk.ref_table_name == pair[0]
                    for fk in foreign_keys.get(pair[1], [])
                )
                
                if a_to_b and b_to_a:
                    rel_type = "many_to_many"
                elif a_to_b:
                    rel_type = "many_to_one"  # pair[0] 多，pair[1] 一
                elif b_to_a:
                    rel_type = "one_to_many"
                else:
                    continue
                
                rel = TableRelationship(
                    table_a=pair[0],
                    table_b=pair[1],
                    relationship=rel_type,
                    foreign_keys=[
                        fk for fk in foreign_keys.get(pair[0], []) 
                        if fk.ref_table_name == pair[1]
                    ] + [
                        fk for fk in foreign_keys.get(pair[1], [])
                        if fk.ref_table_name == pair[0]
                    ]
                )
                relationships.append(rel)
        
        return relationships
    
    def find_circular_references(self, 
                                  relationship_graph: Dict[str, set]) -> List[List[str]]:
        """
        查找循环引用
        
        例如: A → B → C → A
        """
        circles = []
        visited = set()
        
        def dfs(node, path):
            if node in path:
                # 发现循环
                circle_start = path.index(node)
                circle = path[circle_start:] + [node]
                circles.append(circle)
                return
            
            if node in visited:
                return
            
            visited.add(node)
            path.append(node)
            
            for neighbor in relationship_graph.get(node, []):
                dfs(neighbor, path)
            
            path.pop()
        
        for node in relationship_graph:
            dfs(node, [])
        
        # 去重
        unique_circles = []
        seen = set()
        for circle in circles:
            key = tuple(sorted(circle))
            if key not in seen:
                seen.add(key)
                unique_circles.append(circle)
        
        return unique_circles
    
    def find_orphan_tables(self,
                          all_tables: List[str],
                          relationship_graph: Dict[str, set]) -> List[str]:
        """
        查找孤立表（没有外键关系的表）
        """
        orphans = []
        for table in all_tables:
            if table not in relationship_graph or not relationship_graph[table]:
                orphans.append(table)
        return orphans


def extract_and_print_relationships(engine, tables_metadata):
    """
    便捷函数：提取并打印关系
    """
    extractor = RelationshipExtractor(infer_missing=True)
    result = extractor.extract_all_relationships(engine, tables_metadata)
    
    print("\n" + "="*60)
    print("表间关系分析")
    print("="*60)
    
    # 外键关系
    print("\n【外键关系】")
    for table, fks in result["foreign_keys"].items():
        if fks:
            print(f"\n  {table}:")
            for fk in fks:
                inferred_mark = "(推断)" if fk.inferred else ""
                print(f"    └─ {fk.column_name} → {fk.ref_table_name}.{fk.ref_column_name} {inferred_mark}")
    
    # 关系图
    print("\n【关系图】")
    for table, related in result["relationship_graph"].items():
        if related:
            print(f"  {table} ↔ {', '.join(related)}")
    
    # 孤立表
    all_tables = [m["table_name"] for m in tables_metadata]
    orphans = extractor.find_orphan_tables(all_tables, result["relationship_graph"])
    if orphans:
        print("\n【孤立表（无外键关系）】")
        for table in orphans:
            print(f"  - {table}")
    
    # 循环引用
    circles = extractor.find_circular_references(result["relationship_graph"])
    if circles:
        print("\n【循环引用警告】")
        for circle in circles:
            print(f"  {' → '.join(circle)}")
    
    return result
