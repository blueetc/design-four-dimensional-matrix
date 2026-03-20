"""
分类字段分析器

核心思想：主域的核心表一定有分类字段，分类数量是主题域的重要特征

解决方案：
1. 识别分类字段（枚举型、值域有限、重复度高）
2. 提取分类数量和分布特征
3. 基于分类结构相似度进行聚类
4. 将分类复杂度作为Z轴的辅助维度
"""

from typing import Dict, List, Optional, Any, Tuple, Set
from dataclasses import dataclass, field
from collections import Counter, defaultdict
import math


@dataclass
class CategoryField:
    """分类字段特征"""
    field_name: str
    distinct_count: int              # 不同值的数量
    total_rows: int
    uniqueness_ratio: float          # 唯一值比例 = distinct/total
    top_values: List[Tuple[str, int]]  # 最常见的值及其频次 [(value, count), ...]
    value_distribution: Dict[str, int]  # 完整的值分布
    
    def is_classification_field(self, threshold: float = 0.1) -> bool:
        """
        判断是否为分类字段
        
        标准：
        - 唯一值比例 < threshold (默认10%)
        - 有明确的类别分布（不是完全随机）
        """
        if self.total_rows == 0:
            return False
        
        # 唯一值比例低，说明是分类/枚举类型
        if self.uniqueness_ratio > threshold:
            return False
        
        # 至少有一些重复值（不是全是NULL）
        if self.distinct_count < 2:
            return False
        
        return True
    
    def get_category_entropy(self) -> float:
        """
        计算分类熵，衡量分类的均匀程度
        
        熵低 = 有主导类别（如90%是"已完成"）
        熵高 = 分布均匀（如各占25%）
        """
        if not self.value_distribution:
            return 0.0
        
        entropy = 0.0
        total = sum(self.value_distribution.values())
        
        for count in self.value_distribution.values():
            if count > 0:
                p = count / total
                entropy -= p * math.log2(p)
        
        return entropy
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "field_name": self.field_name,
            "distinct_count": self.distinct_count,
            "uniqueness_ratio": round(self.uniqueness_ratio, 4),
            "is_category": self.is_classification_field(),
            "entropy": round(self.get_category_entropy(), 4),
            "top_values": self.top_values[:5],  # 只显示前5
        }


@dataclass
class TableCategoryProfile:
    """表的分类特征画像"""
    table_name: str
    total_columns: int
    category_fields: List[CategoryField] = field(default_factory=list)
    
    def get_category_count(self) -> int:
        """分类字段数量"""
        return len(self.category_fields)
    
    def get_total_categories(self) -> int:
        """所有分类字段的类别总数"""
        return sum(cf.distinct_count for cf in self.category_fields)
    
    def get_avg_categories_per_field(self) -> float:
        """平均每个分类字段的类别数"""
        if not self.category_fields:
            return 0.0
        return self.get_total_categories() / len(self.category_fields)
    
    def get_category_density(self) -> float:
        """分类字段密度 = 分类字段数/总列数"""
        if self.total_columns == 0:
            return 0.0
        return len(self.category_fields) / self.total_columns
    
    def get_category_complexity_score(self) -> float:
        """
        分类复杂度评分 0-100
        
        考虑因素：
        - 分类字段数量
        - 分类密度
        - 类别总数的对数
        """
        if not self.category_fields:
            return 0.0
        
        # 基础分：分类字段数 * 10
        base_score = min(50, len(self.category_fields) * 10)
        
        # 密度加分
        density_bonus = self.get_category_density() * 20
        
        # 类别复杂度加分（对数压缩）
        total_cats = self.get_total_categories()
        complexity_bonus = min(30, math.log2(total_cats + 1) * 5)
        
        return min(100, base_score + density_bonus + complexity_bonus)
    
    def get_signature(self) -> str:
        """
        生成分类特征签名，用于相似度比较
        
        格式: "field1:count1,field2:count2,..."
        """
        sig_parts = []
        for cf in sorted(self.category_fields, key=lambda x: x.field_name):
            sig_parts.append(f"{cf.field_name}:{cf.distinct_count}")
        return "|".join(sig_parts)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "table_name": self.table_name,
            "total_columns": self.total_columns,
            "category_fields_count": self.get_category_count(),
            "total_categories": self.get_total_categories(),
            "avg_categories_per_field": round(self.get_avg_categories_per_field(), 2),
            "category_density": round(self.get_category_density(), 4),
            "complexity_score": round(self.get_category_complexity_score(), 2),
            "category_fields": [cf.to_dict() for cf in self.category_fields],
        }


class CategoryAnalyzer:
    """
    分类字段分析器
    
    分析表中哪些字段是分类字段，提取分类特征
    """
    
    def __init__(self, uniqueness_threshold: float = 0.1):
        self.uniqueness_threshold = uniqueness_threshold
    
    def analyze_table(self, 
                      table_name: str,
                      column_stats: Dict[str, Dict]) -> TableCategoryProfile:
        """
        分析单表的分类特征
        
        Args:
            table_name: 表名
            column_stats: 列统计信息 {
                column_name: {
                    "distinct_count": int,
                    "total_rows": int,
                    "top_values": [(value, count), ...]
                }
            }
        """
        category_fields = []
        
        for col_name, stats in column_stats.items():
            distinct = stats.get("distinct_count", 0)
            total = stats.get("total_rows", 0)
            
            if total == 0:
                continue
            
            uniqueness = distinct / total
            
            cf = CategoryField(
                field_name=col_name,
                distinct_count=distinct,
                total_rows=total,
                uniqueness_ratio=uniqueness,
                top_values=stats.get("top_values", []),
                value_distribution=stats.get("value_distribution", {}),
            )
            
            # 只保留真正的分类字段
            if cf.is_classification_field(self.uniqueness_threshold):
                category_fields.append(cf)
        
        return TableCategoryProfile(
            table_name=table_name,
            total_columns=len(column_stats),
            category_fields=category_fields,
        )
    
    def analyze_database(self, 
                         db_stats: Dict[str, Dict[str, Dict]]) -> Dict[str, TableCategoryProfile]:
        """
        分析整个数据库的分类特征
        
        Args:
            db_stats: {
                table_name: {
                    column_name: {
                        "distinct_count": int,
                        "total_rows": int,
                        ...
                    }
                }
            }
        """
        profiles = {}
        for table_name, column_stats in db_stats.items():
            profiles[table_name] = self.analyze_table(table_name, column_stats)
        return profiles


class CategoryBasedClustering:
    """
    基于分类特征的聚类
    
    将分类结构相似的表聚为一类
    """
    
    def __init__(self, similarity_threshold: float = 0.3):
        self.similarity_threshold = similarity_threshold
    
    def calculate_similarity(self, 
                            profile1: TableCategoryProfile,
                            profile2: TableCategoryProfile) -> float:
        """
        计算两张表的分类相似度
        
        算法：
        1. 分类字段名称的Jaccard相似度
        2. 分类数量分布的余弦相似度
        3. 综合得分
        """
        # 1. 字段名称相似度
        fields1 = {cf.field_name for cf in profile1.category_fields}
        fields2 = {cf.field_name for cf in profile2.category_fields}
        
        if not fields1 or not fields2:
            name_sim = 0.0
        else:
            intersection = len(fields1 & fields2)
            union = len(fields1 | fields2)
            name_sim = intersection / union if union > 0 else 0.0
        
        # 2. 分类复杂度相似度
        comp1 = profile1.get_category_complexity_score()
        comp2 = profile2.get_category_complexity_score()
        
        # 复杂度差异越小，相似度越高
        max_comp = max(comp1, comp2)
        if max_comp == 0:
            comp_sim = 1.0 if comp1 == comp2 else 0.0
        else:
            comp_sim = 1.0 - abs(comp1 - comp2) / max_comp
        
        # 3. 综合相似度 (加权平均)
        similarity = name_sim * 0.6 + comp_sim * 0.4
        
        return similarity
    
    def cluster_by_category(self, 
                           profiles: Dict[str, TableCategoryProfile]) -> Dict[int, List[str]]:
        """
        基于分类特征进行聚类
        
        Returns:
            {cluster_id: [table_name, ...]}
        """
        tables = list(profiles.keys())
        n = len(tables)
        
        if n == 0:
            return {}
        
        # 计算相似度矩阵
        similarity_matrix = {}
        for i, t1 in enumerate(tables):
            for j, t2 in enumerate(tables):
                if i <= j:
                    sim = self.calculate_similarity(profiles[t1], profiles[t2])
                    similarity_matrix[(t1, t2)] = sim
                    similarity_matrix[(t2, t1)] = sim
        
        # 层次聚类（简化版）
        clusters = []
        assigned = set()
        
        for table in tables:
            if table in assigned:
                continue
            
            cluster = [table]
            assigned.add(table)
            
            # 找到所有相似度超过阈值的表
            for other in tables:
                if other not in assigned:
                    sim = similarity_matrix.get((table, other), 0)
                    if sim > self.similarity_threshold:
                        cluster.append(other)
                        assigned.add(other)
            
            clusters.append(cluster)
        
        return {i: cluster for i, cluster in enumerate(clusters)}


class EnhancedDomainDiscoverer:
    """
    增强版主题域发现器
    
    结合：
    1. 原始动态分类（命名、外键、结构）
    2. 分类字段特征
    """
    
    def __init__(self):
        self.category_analyzer = CategoryAnalyzer()
        self.category_clustering = CategoryBasedClustering()
    
    def discover_with_categories(self,
                                  signatures: List[Any],
                                  db_stats: Dict[str, Dict[str, Dict]]) -> Dict[str, Any]:
        """
        结合分类特征进行主题域发现
        
        Args:
            signatures: 表签名列表（来自dynamic_classifier）
            db_stats: 数据库统计信息 {table: {column: stats}}
        
        Returns:
            增强的分析结果
        """
        # 1. 分析分类特征
        category_profiles = self.category_analyzer.analyze_database(db_stats)
        
        # 2. 基于分类特征聚类
        category_clusters = self.category_clustering.cluster_by_category(category_profiles)
        
        # 3. 整合结果
        table_category_info = {}
        for table_name, profile in category_profiles.items():
            table_category_info[table_name] = {
                "profile": profile.to_dict(),
                "complexity_score": profile.get_category_complexity_score(),
                "category_fields": [cf.field_name for cf in profile.category_fields],
            }
        
        return {
            "category_profiles": {name: prof.to_dict() for name, prof in category_profiles.items()},
            "category_clusters": category_clusters,
            "table_category_info": table_category_info,
            "category_insights": self._generate_insights(category_profiles),
        }
    
    def _generate_insights(self, 
                          profiles: Dict[str, TableCategoryProfile]) -> List[Dict]:
        """生成分类相关的洞察"""
        insights = []
        
        # 1. 找出分类最复杂的表（可能是核心业务表）
        sorted_by_complexity = sorted(
            profiles.items(),
            key=lambda x: x[1].get_category_complexity_score(),
            reverse=True
        )
        
        if sorted_by_complexity:
            top_table, top_profile = sorted_by_complexity[0]
            if top_profile.get_category_complexity_score() > 50:
                insights.append({
                    "type": "core_table_candidate",
                    "table": top_table,
                    "score": top_profile.get_category_complexity_score(),
                    "reason": f"该表有{top_profile.get_category_count()}个分类字段，{top_profile.get_total_categories()}个类别，可能是核心业务表",
                })
        
        # 2. 找出分类结构相似的表组
        signatures = {}
        for name, prof in profiles.items():
            sig = prof.get_signature()
            if sig:
                signatures.setdefault(sig, []).append(name)
        
        for sig, tables in signatures.items():
            if len(tables) > 1:
                insights.append({
                    "type": "similar_category_structure",
                    "tables": tables,
                    "reason": f"这些表有相似的分类结构: {sig[:50]}...",
                })
        
        # 3. 找出无分类的表（可能是日志/配置表）
        no_category_tables = [
            name for name, prof in profiles.items()
            if prof.get_category_count() == 0
        ]
        
        if no_category_tables:
            insights.append({
                "type": "no_category_tables",
                "tables": no_category_tables,
                "reason": "这些表没有分类字段，可能是日志表、配置表或关联表",
            })
        
        return insights


def sample_column_stats_from_metadata(metadata: Dict) -> Dict[str, Dict]:
    """
    从元数据中提取列统计信息（模拟）
    
    实际应用中应该从数据库查询真实的值分布
    """
    column_stats = {}
    
    for col in metadata.get("columns", []):
        col_name = col.get("name", "").lower()
        col_type = col.get("type", "").lower()
        
        # 启发式推断哪些可能是分类字段
        # 实际应该从数据库查询 distinct count
        
        # 常见的分类字段命名模式
        category_indicators = [
            "status", "type", "category", "state", "level",
            "flag", "is_", "has_", "mode", "priority",
            "role", "group", "tag", "label"
        ]
        
        is_likely_category = any(ind in col_name for ind in category_indicators)
        
        if is_likely_category:
            # 模拟分类字段的统计
            column_stats[col_name] = {
                "distinct_count": 5,  # 假设有5个分类
                "total_rows": metadata.get("row_count", 1000),
                "top_values": [
                    ("active", 500),
                    ("inactive", 300),
                    ("pending", 150),
                    ("deleted", 40),
                    ("archived", 10),
                ],
                "value_distribution": {
                    "active": 500,
                    "inactive": 300,
                    "pending": 150,
                    "deleted": 40,
                    "archived": 10,
                }
            }
        else:
            # 非分类字段（如ID、时间、文本）
            column_stats[col_name] = {
                "distinct_count": metadata.get("row_count", 1000),  # 几乎唯一
                "total_rows": metadata.get("row_count", 1000),
                "top_values": [],
                "value_distribution": {}
            }
    
    return column_stats
