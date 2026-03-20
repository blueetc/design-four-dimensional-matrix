"""
数据血缘与溯源模块

确保两个阶段的矩阵都能追溯到真实业务数据库
"""

from typing import Dict, List, Optional, Any, Set
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


class TransformationType(Enum):
    """转换类型"""
    DIRECT = "direct"           # 直接映射，无转换
    RENAME = "rename"           # 重命名
    MERGE = "merge"             # 多表合并
    SPLIT = "split"             # 表拆分
    AGGREGATE = "aggregate"     # 聚合
    FILTER = "filter"           # 过滤
    DERIVED = "derived"         # 派生计算
    ARCHIVE = "archive"         # 归档标记


@dataclass
class PhysicalLocation:
    """物理数据源位置"""
    db_type: str                    # postgres/mysql/clickhouse
    host: str
    port: int
    database: str
    schema: str
    table: str
    
    # 精确到字段（可选）
    column: Optional[str] = None
    
    # 查询语句（用于验证）
    query_sql: Optional[str] = None
    
    # 快照时间
    snapshot_at: Optional[datetime] = None
    
    def to_uri(self) -> str:
        """生成唯一资源标识符"""
        uri = f"{self.db_type}://{self.host}:{self.port}/{self.database}/{self.schema}/{self.table}"
        if self.column:
            uri += f"#{self.column}"
        return uri
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "db_type": self.db_type,
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "schema": self.schema,
            "table": self.table,
            "column": self.column,
            "query_sql": self.query_sql,
            "snapshot_at": self.snapshot_at.isoformat() if self.snapshot_at else None,
            "uri": self.to_uri(),
        }


@dataclass
class FieldMapping:
    """字段级映射关系"""
    source_field: str
    target_field: str
    transform_type: TransformationType
    transform_logic: Optional[str] = None  # 如 "CONCAT(first_name, ' ', last_name)"
    
    # 溯源
    source_location: Optional[PhysicalLocation] = None


@dataclass
class LineageEdge:
    """血缘边 - 记录从源到目标的转换"""
    source_id: str                  # 源单元格ID
    target_id: str                  # 目标单元格ID
    transform_type: TransformationType
    transform_reason: str           # 转换原因说明
    
    # 字段级映射
    field_mappings: List[FieldMapping] = field(default_factory=list)
    
    # 元数据
    created_at: datetime = field(default_factory=datetime.now)
    confidence: float = 1.0         # 转换置信度（AI推断或人工确认）
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "source_id": self.source_id,
            "target_id": self.target_id,
            "transform_type": self.transform_type.value,
            "transform_reason": self.transform_reason,
            "field_mappings": [
                {
                    "source": fm.source_field,
                    "target": fm.target_field,
                    "type": fm.transform_type.value,
                    "logic": fm.transform_logic,
                }
                for fm in self.field_mappings
            ],
            "confidence": self.confidence,
        }


@dataclass
class Provenance:
    """
    溯源信息 - 附加到DataCell
    
    支持两个阶段的矩阵：
    - 第一阶段：直接记录物理位置
    - 第二阶段：记录来源映射（指向第一阶段或其他第二阶段单元格）
    """
    # 当前单元格ID
    cell_id: str
    
    # 如果是第一阶段：直接物理位置
    physical_location: Optional[PhysicalLocation] = None
    
    # 如果是第二阶段：来源映射（可以有多源）
    sources: List[LineageEdge] = field(default_factory=list)
    
    # 历史版本（支持矩阵迭代优化）
    previous_versions: List[str] = field(default_factory=list)
    
    # 审计信息
    created_by: str = "system"      # system/manual/ai
    created_at: datetime = field(default_factory=datetime.now)
    verified_by: Optional[str] = None
    verified_at: Optional[datetime] = None
    
    def is_first_stage(self) -> bool:
        """判断是否为第一阶段（直接来自物理库）"""
        return self.physical_location is not None and not self.sources
    
    def is_second_stage(self) -> bool:
        """判断是否为第二阶段（经过转换）"""
        return len(self.sources) > 0
    
    def get_root_sources(self) -> List[PhysicalLocation]:
        """
        递归获取所有根级物理数据源
        
        对于第二阶段单元格，追溯所有原始物理位置
        """
        roots = []
        visited = set()
        
        def traverse(edge: LineageEdge):
            if edge.source_id in visited:
                return
            visited.add(edge.source_id)
            
            # 这里假设可以通过某种方式获取源单元格的Provenance
            # 实际实现中需要配合LineageTracker
        
        if self.physical_location:
            roots.append(self.physical_location)
        
        return roots
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "cell_id": self.cell_id,
            "is_first_stage": self.is_first_stage(),
            "is_second_stage": self.is_second_stage(),
            "physical_location": self.physical_location.to_dict() if self.physical_location else None,
            "sources": [s.to_dict() for s in self.sources],
            "created_by": self.created_by,
            "created_at": self.created_at.isoformat(),
            "verified_by": self.verified_by,
        }


class LineageTracker:
    """
    血缘追踪器
    
    管理两个阶段的矩阵之间的血缘关系
    """
    
    def __init__(self):
        # 所有溯源信息的存储
        self.provenance: Dict[str, Provenance] = {}
        
        # 反向索引：物理URI → 单元格ID列表
        self.physical_index: Dict[str, List[str]] = {}
        
        # 正向索引：源单元格 → 目标单元格
        self.downstream_map: Dict[str, List[str]] = {}
    
    def register_first_stage(self, 
                             cell_id: str, 
                             location: PhysicalLocation,
                             metadata: Optional[Dict] = None) -> Provenance:
        """
        注册第一阶段单元格
        
        Args:
            cell_id: 单元格唯一ID，格式建议："{db}_{schema}_{table}_{timestamp}"
            location: 物理位置
            metadata: 额外元数据
        """
        prov = Provenance(
            cell_id=cell_id,
            physical_location=location,
            created_by="system",
        )
        
        self.provenance[cell_id] = prov
        
        # 建立物理位置索引
        uri = location.to_uri()
        if uri not in self.physical_index:
            self.physical_index[uri] = []
        self.physical_index[uri].append(cell_id)
        
        return prov
    
    def register_second_stage(self,
                              cell_id: str,
                              source_edges: List[LineageEdge],
                              created_by: str = "ai") -> Provenance:
        """
        注册第二阶段单元格
        
        Args:
            cell_id: 新单元格ID
            source_edges: 来源边（可以来自第一阶段或其他第二阶段）
            created_by: 创建方式（ai/manual/system）
        """
        prov = Provenance(
            cell_id=cell_id,
            sources=source_edges,
            created_by=created_by,
        )
        
        self.provenance[cell_id] = prov
        
        # 建立下游映射
        for edge in source_edges:
            if edge.source_id not in self.downstream_map:
                self.downstream_map[edge.source_id] = []
            self.downstream_map[edge.source_id].append(cell_id)
        
        return prov
    
    def get_upstream(self, cell_id: str, recursive: bool = False) -> List[Provenance]:
        """
        获取上游溯源
        
        Args:
            cell_id: 目标单元格
            recursive: 是否递归获取所有上游
        """
        prov = self.provenance.get(cell_id)
        if not prov:
            return []
        
        upstream = []
        
        # 直接上游
        for edge in prov.sources:
            source_prov = self.provenance.get(edge.source_id)
            if source_prov:
                upstream.append(source_prov)
                
                # 递归
                if recursive:
                    upstream.extend(self.get_upstream(edge.source_id, True))
        
        # 去重
        seen = set()
        unique = []
        for p in upstream:
            if p.cell_id not in seen:
                seen.add(p.cell_id)
                unique.append(p)
        
        return unique
    
    def get_downstream(self, cell_id: str, recursive: bool = False) -> List[Provenance]:
        """获取下游影响"""
        downstream_ids = self.downstream_map.get(cell_id, [])
        downstream = [self.provenance[id] for id in downstream_ids if id in self.provenance]
        
        if recursive:
            for id in downstream_ids:
                downstream.extend(self.get_downstream(id, True))
        
        return downstream
    
    def find_by_physical_location(self, location: PhysicalLocation) -> List[Provenance]:
        """通过物理位置查找所有相关单元格"""
        uri = location.to_uri()
        cell_ids = self.physical_index.get(uri, [])
        return [self.provenance[id] for id in cell_ids if id in self.provenance]
    
    def generate_impact_report(self, location: PhysicalLocation) -> Dict[str, Any]:
        """
        生成影响分析报告
        
        当原始表发生变更时，分析会影响哪些第二阶段结构
        """
        # 找到所有依赖该物理位置的单元格
        first_stage_ids = self.physical_index.get(location.to_uri(), [])
        
        # 找到所有下游
        all_impacts = set()
        for id in first_stage_ids:
            downstream = self.get_downstream(id, recursive=True)
            all_impacts.update([p.cell_id for p in downstream])
        
        return {
            "physical_location": location.to_dict(),
            "affected_first_stage": first_stage_ids,
            "affected_second_stage": list(all_impacts),
            "total_impact": len(all_impacts),
        }
    
    def verify_lineage(self, cell_id: str, verified_by: str):
        """人工确认血缘关系"""
        prov = self.provenance.get(cell_id)
        if prov:
            prov.verified_by = verified_by
            prov.verified_at = datetime.now()
    
    def export_lineage_graph(self) -> Dict[str, Any]:
        """导出血缘图（用于可视化）"""
        nodes = []
        edges = []
        
        for cell_id, prov in self.provenance.items():
            nodes.append({
                "id": cell_id,
                "type": "first_stage" if prov.is_first_stage() else "second_stage",
                **prov.to_dict(),
            })
            
            for edge in prov.sources:
                edges.append({
                    "source": edge.source_id,
                    "target": cell_id,
                    "type": edge.transform_type.value,
                    "confidence": edge.confidence,
                })
        
        return {
            "nodes": nodes,
            "edges": edges,
            "stats": {
                "total_nodes": len(nodes),
                "total_edges": len(edges),
                "first_stage": sum(1 for n in nodes if n["type"] == "first_stage"),
                "second_stage": sum(1 for n in nodes if n["type"] == "second_stage"),
            }
        }
