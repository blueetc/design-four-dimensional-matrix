"""
动态分类器 - 适应未知数据库结构

核心思想：不预设任何业务域，完全基于数据本身的特征进行动态聚类

从 seebook 吸收的关键能力：
- DynamicDomainDiscoverer：基于外键关系、名称相似度、结构相似度动态聚类
- AdaptiveLifecycleClassifier：基于数据分布自适应判断生命周期阶段
- UnknownDatabaseProcessor：端到端处理完全未知数据库
"""

from __future__ import annotations

from typing import Dict, List, Optional, Any, Tuple, Set
from dataclasses import dataclass, field
from collections import defaultdict
import re
import numpy as np


@dataclass
class TableSignature:
    """表特征签名 - 用于动态聚类"""
    table_name: str
    schema_name: str
    
    # 结构特征
    column_names: List[str]
    column_types: List[str]
    primary_key: Optional[str]
    foreign_keys: List[Dict[str, str]]  # [{column: xxx, ref_table: xxx, ref_column: xxx}]
    indexes: List[str]
    
    # 数据特征
    row_count: int
    column_count: int
    has_timestamp: bool
    has_soft_delete: bool  # is_deleted/deleted_at等
    
    # 命名特征（提取的token）
    name_tokens: List[str] = field(default_factory=list)
    
    def __post_init__(self):
        if not self.name_tokens:
            self.name_tokens = self._extract_tokens(self.table_name)
    
    def _extract_tokens(self, name: str) -> List[str]:
        """从表名提取语义token"""
        # 支持多种命名规范：snake_case, camelCase, PascalCase
        tokens = []
        
        # snake_case
        for part in name.lower().split('_'):
            if part:
                tokens.append(part)
        
        # camelCase / PascalCase
        camel_parts = re.findall(r'[A-Z]?[a-z]+|[A-Z]+(?=[A-Z]|$)', name)
        for part in camel_parts:
            if part.lower() not in tokens:
                tokens.append(part.lower())
        
        return tokens


class DynamicDomainDiscoverer:
    """
    动态主题域发现器
    
    不预设任何业务域，通过以下特征自动发现：
    1. 表名语义相似度
    2. 外键关联关系（图聚类）
    3. 列结构相似度
    4. 数据量级相似性
    """
    
    def __init__(self):
        self.domains: Dict[int, Dict[str, Any]] = {}  # z -> domain_info
        self.domain_counter = 0
    
    def discover_domains(self, signatures: List[TableSignature]) -> Dict[str, int]:
        """
        动态发现主题域
        
        Returns:
            {table_name: z_index}
        """
        if not signatures:
            return {}
        
        # 1. 基于外键关系构建图
        graph = self._build_relationship_graph(signatures)
        
        # 2. 基于名称相似度聚类
        name_clusters = self._cluster_by_name_similarity(signatures)
        
        # 3. 基于结构相似度聚类
        structure_clusters = self._cluster_by_structure(signatures)
        
        # 4. 合并聚类结果
        merged_clusters = self._merge_clusters(graph, name_clusters, structure_clusters)
        
        # 5. 为每个聚类分配Z轴索引和命名
        result = {}
        for cluster_id, table_sigs in merged_clusters.items():
            z = self.domain_counter
            self.domain_counter += 1
            
            # 动态命名：选择最具代表性的表名特征
            domain_name = self._generate_domain_name(table_sigs)
            
            self.domains[z] = {
                "name": domain_name,
                "tables": [s.table_name for s in table_sigs],
                "table_count": len(table_sigs),
                "description": self._generate_description(table_sigs),
            }
            
            for sig in table_sigs:
                result[sig.table_name] = z
        
        return result
    
    def _build_relationship_graph(self, signatures: List[TableSignature]) -> Dict[str, Set[str]]:
        """基于外键关系构建关联图"""
        graph = defaultdict(set)
        table_names = {s.table_name for s in signatures}
        
        for sig in signatures:
            for fk in sig.foreign_keys:
                ref_table = fk.get("ref_table")
                if ref_table and ref_table in table_names:
                    # 双向关联
                    graph[sig.table_name].add(ref_table)
                    graph[ref_table].add(sig.table_name)
        
        return dict(graph)
    
    def _cluster_by_name_similarity(self, signatures: List[TableSignature]) -> Dict[int, List[TableSignature]]:
        """基于表名相似度聚类"""
        if not signatures:
            return {}
        
        n = len(signatures)
        similarity_matrix = np.zeros((n, n))
        
        # 计算两两相似度
        for i in range(n):
            for j in range(i+1, n):
                sim = self._calculate_name_similarity(
                    signatures[i].name_tokens,
                    signatures[j].name_tokens
                )
                similarity_matrix[i][j] = sim
                similarity_matrix[j][i] = sim
        
        # 简单的层次聚类（可以使用更复杂的算法）
        threshold = 0.3
        clusters = []
        assigned = set()
        
        for i in range(n):
            if i in assigned:
                continue
            
            cluster = [signatures[i]]
            assigned.add(i)
            
            for j in range(i+1, n):
                if j not in assigned and similarity_matrix[i][j] > threshold:
                    cluster.append(signatures[j])
                    assigned.add(j)
            
            clusters.append(cluster)
        
        return {i: cluster for i, cluster in enumerate(clusters)}
    
    def _calculate_name_similarity(self, tokens1: List[str], tokens2: List[str]) -> float:
        """计算两个token列表的相似度"""
        if not tokens1 or not tokens2:
            return 0.0
        
        set1 = set(tokens1)
        set2 = set(tokens2)
        
        intersection = set1 & set2
        union = set1 | set2
        
        if not union:
            return 0.0
        
        return len(intersection) / len(union)
    
    def _cluster_by_structure(self, signatures: List[TableSignature]) -> Dict[int, List[TableSignature]]:
        """基于结构相似度聚类"""
        # 简化的结构相似度：列类型分布、是否有时间戳、是否有软删除
        clusters = defaultdict(list)
        
        for sig in signatures:
            # 生成结构指纹
            type_key = tuple(sorted(set(sig.column_types)))
            feature_key = (sig.has_timestamp, sig.has_soft_delete)
            
            # 使用组合特征作为聚类键
            key = (type_key, feature_key)
            clusters[key].append(sig)
        
        return {i: cluster for i, cluster in enumerate(clusters.values())}
    
    def _merge_clusters(self, 
                        graph: Dict[str, Set[str]],
                        name_clusters: Dict[int, List[TableSignature]],
                        structure_clusters: Dict[int, List[TableSignature]]) -> Dict[int, List[TableSignature]]:
        """合并多种聚类结果"""
        
        # 创建表到签名的映射
        sig_map = {}
        for cluster in name_clusters.values():
            for sig in cluster:
                sig_map[sig.table_name] = sig
        
        # 使用并查集合并相关表
        parent = {sig.table_name: sig.table_name for sig in sig_map.values()}
        
        def find(x):
            if parent[x] != x:
                parent[x] = find(parent[x])
            return parent[x]
        
        def union(x, y):
            px, py = find(x), find(y)
            if px != py:
                parent[px] = py
        
        # 1. 合并外键关联的表
        for table, refs in graph.items():
            for ref in refs:
                if table in parent and ref in parent:
                    union(table, ref)
        
        # 2. 合并名称相似的表
        for cluster in name_clusters.values():
            if len(cluster) > 1:
                first = cluster[0].table_name
                for sig in cluster[1:]:
                    union(first, sig.table_name)
        
        # 收集最终聚类
        final_clusters = defaultdict(list)
        for table, sig in sig_map.items():
            root = find(table)
            final_clusters[root].append(sig)
        
        return {i: cluster for i, cluster in enumerate(final_clusters.values())}
    
    def _generate_domain_name(self, signatures: List[TableSignature]) -> str:
        """为主题域生成名称"""
        if not signatures:
            return "unknown"
        
        # 提取公共token
        all_tokens = [set(s.name_tokens) for s in signatures]
        if not all_tokens:
            return f"domain_{self.domain_counter}"
        
        common_tokens = all_tokens[0]
        for tokens in all_tokens[1:]:
            common_tokens &= tokens
        
        if common_tokens:
            # 使用最常见的公共token
            return max(common_tokens, key=len)
        
        # 如果没有公共token，使用最长的表名前缀
        names = [s.table_name for s in signatures]
        prefix = names[0]
        for name in names[1:]:
            while not name.startswith(prefix) and prefix:
                prefix = prefix[:-1]
        
        if prefix and len(prefix) > 2:
            return prefix.rstrip('_')
        
        # 回退：使用表数量命名
        return f"cluster_{len(signatures)}_tables"
    
    def _generate_description(self, signatures: List[TableSignature]) -> str:
        """生成域描述"""
        if not signatures:
            return ""
        
        total_rows = sum(s.row_count for s in signatures)
        avg_cols = sum(s.column_count for s in signatures) / len(signatures)
        
        # 检测特征
        has_ts = any(s.has_timestamp for s in signatures)
        has_fk = any(s.foreign_keys for s in signatures)
        
        desc = f"包含{len(signatures)}张表，共{total_rows:,}行数据"
        if has_ts:
            desc += "，带时间戳"
        if has_fk:
            desc += "，存在表间关联"
        
        return desc


class AdaptiveLifecycleClassifier:
    """
    自适应生命周期分类器
    
    不预设生命周期规则，基于数据的统计特征动态判断
    """
    
    def classify(self, signatures: List[TableSignature]) -> Dict[str, str]:
        """
        基于分布动态确定生命周期阶段
        
        策略：
        - 数据量最小的20% → new（新建表）
        - 数据量最大的20% → mature（成熟表）
        - 有外键引用其他表 → growth（在发展中）
        - 未被任何表引用且数据量稳定 → legacy（遗留表）
        """
        if not signatures:
            return {}
        
        result = {}
        
        # 计算行数分布
        row_counts = [s.row_count for s in signatures]
        p20 = np.percentile(row_counts, 20) if len(row_counts) > 5 else 0
        p80 = np.percentile(row_counts, 80) if len(row_counts) > 5 else max(row_counts)
        
        # 构建被引用关系
        referenced_by = defaultdict(set)
        for sig in signatures:
            for fk in sig.foreign_keys:
                ref_table = fk.get("ref_table")
                if ref_table:
                    referenced_by[ref_table].add(sig.table_name)
        
        for sig in signatures:
            # 判断逻辑
            if sig.row_count <= p20 and sig.column_count < 10:
                stage = "new"
            elif sig.row_count >= p80:
                stage = "mature"
            elif sig.foreign_keys and not referenced_by[sig.table_name]:
                stage = "growth"  # 引用别人但不被引用 → 可能正在扩展
            elif not referenced_by[sig.table_name] and sig.row_count < p50(row_counts):
                stage = "legacy"  # 孤立且数据量小
            else:
                stage = "mature"
            
            result[sig.table_name] = stage
        
        return result


def p50(arr):
    """中位数"""
    return np.percentile(arr, 50) if arr else 0


class UnknownDatabaseProcessor:
    """
    未知数据库处理器
    
    整合动态发现流程，处理完全未知的数据库
    """
    
    def __init__(self):
        self.domain_discoverer = DynamicDomainDiscoverer()
        self.lifecycle_classifier = AdaptiveLifecycleClassifier()
    
    def process(self, 
                table_metadata_list: List[Dict]) -> Dict[str, Any]:
        """
        处理未知数据库的表元数据列表
        
        Args:
            table_metadata_list: 从数据库扫描得到的原始元数据
        
        Returns:
            {
                "signatures": [TableSignature],
                "domain_mapping": {table_name: z_index},
                "lifecycle_mapping": {table_name: stage},
                "domains": {z_index: domain_info},
                "stats": {...}
            }
        """
        # 1. 转换为特征签名
        signatures = []
        for meta in table_metadata_list:
            sig = TableSignature(
                table_name=meta.get("table_name", ""),
                schema_name=meta.get("schema_name", "public"),
                column_names=[c.get("name", "") for c in meta.get("columns", [])],
                column_types=[c.get("type", "") for c in meta.get("columns", [])],
                primary_key=meta.get("primary_key"),
                foreign_keys=meta.get("foreign_keys", []),
                indexes=[i.get("name", "") for i in meta.get("indexes", [])],
                row_count=meta.get("row_count", 0),
                column_count=len(meta.get("columns", [])),
                has_timestamp=any(
                    "time" in c.get("name", "").lower() or 
                    "date" in c.get("name", "").lower()
                    for c in meta.get("columns", [])
                ),
                has_soft_delete=any(
                    "delete" in c.get("name", "").lower() or
                    c.get("name", "").lower() in ["is_deleted", "deleted_flag"]
                    for c in meta.get("columns", [])
                ),
            )
            signatures.append(sig)
        
        # 2. 动态发现主题域
        domain_mapping = self.domain_discoverer.discover_domains(signatures)
        
        # 3. 自适应生命周期分类
        lifecycle_mapping = self.lifecycle_classifier.classify(signatures)
        
        # 4. 统计信息
        stats = {
            "total_tables": len(signatures),
            "total_columns": sum(s.column_count for s in signatures),
            "total_rows": sum(s.row_count for s in signatures),
            "domain_count": len(self.domain_discoverer.domains),
            "relationships": sum(len(s.foreign_keys) for s in signatures),
        }
        
        return {
            "signatures": signatures,
            "domain_mapping": domain_mapping,
            "lifecycle_mapping": lifecycle_mapping,
            "domains": self.domain_discoverer.domains,
            "stats": stats,
        }
