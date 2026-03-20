"""
真实分类字段分析器

解决原有问题：之前基于字段名推断分类，现在查询真实值分布
"""

from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from collections import Counter
import math
from sqlalchemy import text, inspect


@dataclass
class RealCategoryField:
    """基于真实数据值的分类字段"""
    field_name: str
    field_comment: str  # 字段备注/中文名
    
    # 真实值分布
    distinct_count: int
    total_rows: int
    uniqueness_ratio: float
    
    # Top值及占比
    top_values: List[Dict[str, Any]]  # [{"value": "xx", "count": 100, "percentage": 50%}, ...]
    
    # 分类判断
    is_category: bool
    category_confidence: float  # 置信度 0-1
    
    # 业务推断
    inferred_business_type: str  # 状态/类型/级别/...
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "field_name": self.field_name,
            "field_comment": self.field_comment,
            "distinct_count": self.distinct_count,
            "total_rows": self.total_rows,
            "uniqueness_ratio": round(self.uniqueness_ratio, 4),
            "is_category": self.is_category,
            "category_confidence": round(self.category_confidence, 4),
            "inferred_business_type": self.inferred_business_type,
            "top_values": self.top_values[:5],  # 只显示前5
        }


class RealCategoryAnalyzer:
    """
    真实分类字段分析器
    
    通过查询数据库获取真实值分布，而非推断
    """
    
    # 业务类型推断词典
    BUSINESS_TYPE_PATTERNS = {
        "状态": ["status", "state", "stat", "审批", "审核", "流程"],
        "类型": ["type", "category", "class", "kind", "类型", "类别", "分类"],
        "级别": ["level", "grade", "rank", "priority", "级别", "等级", "优先级"],
        "角色": ["role", "position", "duty", "角色", "职位", "职责"],
        "部门": ["dept", "department", "org", "organization", "部门", "组织"],
        "地区": ["region", "area", "zone", "district", "地区", "区域"],
        "方式": ["method", "way", "mode", "manner", "方式", "方法"],
        "结果": ["result", "outcome", "conclusion", "结果", "结论"],
    }
    
    _IDENTIFIER_RE = __import__('re').compile(r'^[A-Za-z_][A-Za-z0-9_]*$')

    @classmethod
    def _validate_identifier(cls, name: str) -> str:
        """Validate a SQL identifier to prevent injection."""
        if not cls._IDENTIFIER_RE.match(name):
            raise ValueError(f"Invalid SQL identifier: {name!r}")
        return name

    def __init__(self, uniqueness_threshold: float = 0.1, min_rows: int = 10):
        self.uniqueness_threshold = uniqueness_threshold
        self.min_rows = min_rows

    def analyze_column(self, 
                       engine,
                       table_name: str, 
                       column_name: str,
                       column_comment: str = "",
                       limit: int = 1000) -> Optional[RealCategoryField]:
        """
        分析单个列的真实值分布
        
        Args:
            engine: SQLAlchemy引擎
            table_name: 表名
            column_name: 列名
            column_comment: 列备注
            limit: 采样行数限制（大表采样）
        """
        table_name = self._validate_identifier(table_name)
        column_name = self._validate_identifier(column_name)
        try:
            with engine.connect() as conn:
                # 1. 获取总行数
                count_result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                total_rows = count_result.scalar()
                
                if total_rows < self.min_rows:
                    return None  # 数据量太小，不分析
                
                # 2. 获取不同值数量
                distinct_result = conn.execute(text(f"""
                    SELECT COUNT(DISTINCT `{column_name}`) 
                    FROM {table_name}
                    WHERE `{column_name}` IS NOT NULL
                """))
                distinct_count = distinct_result.scalar()
                
                # 3. 获取值分布（Top 20）
                # 对于大表，使用采样
                if total_rows > limit:
                    # 采样查询
                    value_result = conn.execute(text(f"""
                        SELECT `{column_name}` as val, COUNT(*) as cnt
                        FROM (
                            SELECT `{column_name}`
                            FROM {table_name}
                            WHERE `{column_name}` IS NOT NULL
                            LIMIT {limit}
                        ) sampled
                        GROUP BY `{column_name}`
                        ORDER BY cnt DESC
                        LIMIT 20
                    """))
                else:
                    # 全量查询
                    value_result = conn.execute(text(f"""
                        SELECT `{column_name}` as val, COUNT(*) as cnt
                        FROM {table_name}
                        WHERE `{column_name}` IS NOT NULL
                        GROUP BY `{column_name}`
                        ORDER BY cnt DESC
                        LIMIT 20
                    """))
                
                value_counts = [(row.val, row.cnt) for row in value_result]
                
                if not value_counts:
                    return None
                
                # 4. 计算指标
                uniqueness_ratio = distinct_count / total_rows
                
                # 转换为百分比
                total_in_sample = sum(cnt for _, cnt in value_counts)
                top_values = []
                for val, cnt in value_counts[:10]:  # 只保留Top 10
                    percentage = (cnt / total_in_sample) * 100 if total_in_sample > 0 else 0
                    top_values.append({
                        "value": str(val)[:50],  # 截断长字符串
                        "count": cnt,
                        "percentage": round(percentage, 2)
                    })
                
                # 5. 判断是否为分类字段
                is_category = self._is_category_field(
                    distinct_count, total_rows, uniqueness_ratio, value_counts
                )
                
                # 6. 计算置信度
                confidence = self._calculate_confidence(
                    distinct_count, total_rows, uniqueness_ratio, value_counts
                )
                
                # 7. 推断业务类型
                business_type = self._infer_business_type(column_name, column_comment)
                
                return RealCategoryField(
                    field_name=column_name,
                    field_comment=column_comment,
                    distinct_count=distinct_count,
                    total_rows=total_rows,
                    uniqueness_ratio=uniqueness_ratio,
                    top_values=top_values,
                    is_category=is_category,
                    category_confidence=confidence,
                    inferred_business_type=business_type,
                )
                
        except Exception as e:
            print(f"  警告: 分析列 {table_name}.{column_name} 失败: {e}")
            return None
    
    def analyze_table(self, 
                      engine,
                      table_name: str,
                      schema: str = None) -> Dict[str, RealCategoryField]:
        """
        分析整张表的分类字段
        
        Returns:
            {column_name: RealCategoryField}
        """
        results = {}
        
        try:
            # 获取表结构信息（包括备注）
            inspector = inspect(engine)
            columns = inspector.get_columns(table_name, schema=schema)
            
            print(f"\n分析表: {table_name}")
            print(f"  总列数: {len(columns)}")
            
            category_count = 0
            for col in columns:
                col_name = col['name']
                col_comment = col.get('comment', '')
                
                # 分析该列
                field = self.analyze_column(
                    engine, table_name, col_name, col_comment
                )
                
                if field and field.is_category:
                    results[col_name] = field
                    category_count += 1
                    print(f"  ✓ 发现分类字段: {col_name}")
                    print(f"    业务类型: {field.inferred_business_type}")
                    print(f"    不同值: {field.distinct_count}")
                    print(f"    置信度: {field.category_confidence:.2%}")
                    if field.top_values:
                        top3 = ", ".join([
                            f"{v['value']}({v['percentage']:.0f}%)" 
                            for v in field.top_values[:3]
                        ])
                        print(f"    分布: {top3}")
            
            print(f"  共发现 {category_count} 个分类字段")
            
        except Exception as e:
            print(f"错误: 分析表 {table_name} 失败: {e}")
        
        return results
    
    def analyze_database(self,
                         engine,
                         tables: List[str] = None) -> Dict[str, Dict[str, RealCategoryField]]:
        """
        分析整个数据库
        
        Returns:
            {table_name: {column_name: RealCategoryField}}
        """
        results = {}
        
        if tables is None:
            # 获取所有表
            inspector = inspect(engine)
            tables = inspector.get_table_names()
        
        print(f"\n{'='*60}")
        print(f"开始分析数据库，共 {len(tables)} 个表")
        print(f"{'='*60}")
        
        for table_name in tables:
            table_result = self.analyze_table(engine, table_name)
            if table_result:
                results[table_name] = table_result
        
        # 汇总
        total_categories = sum(len(cats) for cats in results.values())
        print(f"\n{'='*60}")
        print(f"分析完成: 共发现 {total_categories} 个分类字段")
        print(f"{'='*60}")
        
        return results
    
    def _is_category_field(self, 
                          distinct_count: int, 
                          total_rows: int,
                          uniqueness_ratio: float,
                          value_counts: List[Tuple]) -> bool:
        """
        判断是否为分类字段
        
        标准：
        1. 唯一值比例 < threshold (默认10%)
        2. 不同值数量 >= 2 且 <= 100（太多就不是分类了）
        3. 有明确的分布（非均匀分布）
        """
        if uniqueness_ratio > self.uniqueness_threshold:
            return False
        
        if distinct_count < 2 or distinct_count > 100:
            return False
        
        # 检查是否有主导值（至少一个值占比>20%）
        if value_counts:
            total = sum(cnt for _, cnt in value_counts)
            if total > 0:
                max_ratio = max(cnt for _, cnt in value_counts) / total
                if max_ratio < 0.1:  # 分布太均匀，可能是随机值
                    return False
        
        return True
    
    def _calculate_confidence(self,
                             distinct_count: int,
                             total_rows: int,
                             uniqueness_ratio: float,
                             value_counts: List[Tuple]) -> float:
        """
        计算分类判断的置信度
        """
        confidence = 0.0
        
        # 1. 唯一值比例越低，置信度越高
        confidence += (1 - uniqueness_ratio) * 0.4
        
        # 2. 不同值数量适中（5-20个类别最佳）
        if 5 <= distinct_count <= 20:
            confidence += 0.3
        elif 3 <= distinct_count <= 50:
            confidence += 0.2
        else:
            confidence += 0.1
        
        # 3. 分布集中度（是否有主导类别）
        if value_counts:
            total = sum(cnt for _, cnt in value_counts)
            if total > 0:
                max_ratio = max(cnt for _, cnt in value_counts) / total
                confidence += max_ratio * 0.3
        
        return min(1.0, confidence)
    
    def _infer_business_type(self, column_name: str, column_comment: str) -> str:
        """
        推断字段的业务类型
        """
        search_text = f"{column_name} {column_comment}".lower()
        
        for biz_type, patterns in self.BUSINESS_TYPE_PATTERNS.items():
            for pattern in patterns:
                if pattern.lower() in search_text:
                    return biz_type
        
        return "其他"


def generate_category_report(analysis_result: Dict) -> Dict[str, Any]:
    """
    生成分类字段分析报告
    """
    report = {
        "summary": {
            "total_tables": len(analysis_result),
            "total_category_fields": sum(len(cats) for cats in analysis_result.values()),
        },
        "tables": [],
        "insights": []
    }
    
    # 按表汇总
    for table_name, fields in analysis_result.items():
        if not fields:
            continue
        
        table_info = {
            "table_name": table_name,
            "category_fields_count": len(fields),
            "fields": []
        }
        
        for field_name, field in fields.items():
            table_info["fields"].append(field.to_dict())
        
        report["tables"].append(table_info)
    
    # 生成洞察
    # 1. 找出分类最丰富的表
    sorted_tables = sorted(
        report["tables"],
        key=lambda x: x["category_fields_count"],
        reverse=True
    )
    
    if sorted_tables:
        top_table = sorted_tables[0]
        report["insights"].append({
            "type": "most_categories",
            "table": top_table["table_name"],
            "count": top_table["category_fields_count"],
            "description": f"{top_table['table_name']} 有 {top_table['category_fields_count']} 个分类字段，业务逻辑最复杂"
        })
    
    # 2. 统计各业务类型的字段数量
    biz_type_count = {}
    for table in report["tables"]:
        for field in table["fields"]:
            biz_type = field["inferred_business_type"]
            biz_type_count[biz_type] = biz_type_count.get(biz_type, 0) + 1
    
    report["business_type_distribution"] = biz_type_count
    
    return report
