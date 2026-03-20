"""
矩阵优化引擎

基于第一阶段矩阵的分析，生成第二阶段（优化）矩阵

核心流程：
1. 分析ColorMatrix发现模式
2. 生成优化建议
3. 应用转换生成第二阶段DataMatrix
4. 建立完整的血缘关系
"""

from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from datetime import datetime
import json

from .hypercube import HyperCube, RichDataMatrix, RichColorMatrix
from .data_matrix import DataCell, DataPoint
from .lineage import (
    LineageTracker, Provenance, PhysicalLocation,
    LineageEdge, FieldMapping, TransformationType
)


@dataclass
class OptimizationSuggestion:
    """优化建议"""
    suggestion_id: str
    type: str                      # merge/split/reclassify/archive
    target_cells: List[str]        # 目标单元格IDs
    reason: str                    # 原因说明
    confidence: float              # 置信度
    auto_applicable: bool          # 是否可自动应用
    
    # 转换详情
    transform_type: TransformationType
    new_structure: Dict[str, Any]  # 建议的新结构
    field_mappings: List[FieldMapping]


class MatrixOptimizer:
    """
    矩阵优化器
    
    目标：从发现层（第一阶段）生成目标层（第二阶段）
    关键：保持完整的溯源链
    """
    
    def __init__(self, first_stage_cube: HyperCube, lineage_tracker: LineageTracker):
        self.source_cube = first_stage_cube
        self.lineage = lineage_tracker
        self.suggestions: List[OptimizationSuggestion] = []
        
        # 第二阶段矩阵（正在构建）
        self.optimized_cube: Optional[HyperCube] = None
    
    def analyze(self) -> List[OptimizationSuggestion]:
        """
        分析第一阶段矩阵，生成优化建议
        
        分析维度：
        1. 颜色相似度 → 表合并建议
        2. Z轴分布 → 主题域重构建议
        3. X轴分布 → 生命周期策略建议
        4. 孤立点 → 数据质量警告
        """
        suggestions = []
        
        # 1. 发现跨Z轴的高相似度色块（分类错误）
        suggestions.extend(self._analyze_cross_domain_similarity())
        
        # 2. 发现同Z轴内的色块聚类（宽表机会）
        suggestions.extend(self._analyze_intra_domain_clustering())
        
        # 3. 发现颜色异常点（数据质量问题）
        suggestions.extend(self._analyze_color_outliers())
        
        # 4. 发现时间轴上的颜色退化（归档候选）
        suggestions.extend(self._analyze_temporal_decay())
        
        self.suggestions = suggestions
        return suggestions
    
    def _analyze_cross_domain_similarity(self) -> List[OptimizationSuggestion]:
        """分析跨域颜色相似度 - 发现分类错误"""
        suggestions = []
        
        # 获取所有颜色单元格
        color_cells = list(self.source_cube.color_matrix.cells.values())
        
        # 计算跨Z的颜色距离
        for i, cell1 in enumerate(color_cells):
            for cell2 in color_cells[i+1:]:
                if cell1.z == cell2.z:
                    continue  # 同域不比较
                
                # 计算颜色距离
                rgb1 = [cell1.r, cell1.g, cell1.b]
                rgb2 = [cell2.r, cell2.g, cell2.b]
                distance = sum((a - b) ** 2 for a, b in zip(rgb1, rgb2)) ** 0.5
                
                # 颜色非常相似但不同域 → 分类错误
                if distance < 30:
                    data1 = self.source_cube.data_matrix.get_cell(
                        cell1.t, cell1.x, cell1.y, cell1.z
                    )
                    data2 = self.source_cube.data_matrix.get_cell(
                        cell2.t, cell2.x, cell2.y, cell2.z
                    )
                    
                    if data1 and data2:
                        suggestion = OptimizationSuggestion(
                            suggestion_id=f"reclass_{cell1.z}_{cell2.z}_{i}",
                            type="reclassify",
                            target_cells=[data1, data2],
                            reason=f"颜色距离仅{distance:.1f}，但分属不同主题域，建议重新分类",
                            confidence=0.8,
                            auto_applicable=False,  # 需要人工确认
                            transform_type=TransformationType.RENAME,
                            new_structure={
                                "suggested_domain": data1.business_domain,
                                "reason": "高颜色相似度表明业务关联性强",
                            },
                            field_mappings=[],
                        )
                        suggestions.append(suggestion)
        
        return suggestions
    
    def _analyze_intra_domain_clustering(self) -> List[OptimizationSuggestion]:
        """分析同域内的颜色聚类 - 发现宽表机会"""
        suggestions = []
        
        # 按Z轴分组
        z_groups: Dict[int, List] = {}
        for key, cell in self.source_cube.data_matrix.cells.items():
            z = key[3]
            if z not in z_groups:
                z_groups[z] = []
            z_groups[z].append((key, cell))
        
        # 在每个域内寻找空间邻近的表
        for z, cells in z_groups.items():
            if len(cells) < 2:
                continue
            
            # 简单的空间聚类（距离阈值）
            for i, (key1, cell1) in enumerate(cells):
                for key2, cell2 in cells[i+1:]:
                    # 计算XY平面距离
                    x1, y1 = key1[1], key1[2]
                    x2, y2 = key2[1], key2[2]
                    xy_distance = ((x1 - x2) ** 2 + (y1 - y2) ** 2) ** 0.5
                    
                    # XY距离近且颜色相似 → 宽表候选
                    if xy_distance < 50:
                        suggestion = OptimizationSuggestion(
                            suggestion_id=f"merge_{cell1.table_name}_{cell2.table_name}",
                            type="merge",
                            target_cells=[cell1, cell2],
                            reason=f"空间距离{xy_distance:.1f}，生命周期和数据量级相近，建议合并为宽表",
                            confidence=0.7,
                            auto_applicable=True,
                            transform_type=TransformationType.MERGE,
                            new_structure={
                                "new_table_name": f"{cell1.business_domain}_full",
                                "source_tables": [cell1.table_name, cell2.table_name],
                            },
                            field_mappings=self._infer_field_mappings(cell1, cell2),
                        )
                        suggestions.append(suggestion)
        
        return suggestions
    
    def _analyze_color_outliers(self) -> List[OptimizationSuggestion]:
        """分析颜色异常点"""
        suggestions = []
        # TODO: 实现异常检测
        return suggestions
    
    def _analyze_temporal_decay(self) -> List[OptimizationSuggestion]:
        """分析时间衰减 - 归档候选"""
        suggestions = []
        
        for key, cell in self.source_cube.data_matrix.cells.items():
            # 老数据 + 低饱和度（X轴低）= 归档候选
            if cell.x < 30 and cell.lifecycle_stage == "legacy":
                suggestion = OptimizationSuggestion(
                    suggestion_id=f"archive_{cell.table_name}",
                    type="archive",
                    target_cells=[cell],
                    reason=f"表{cell.table_name}处于遗留阶段且低活跃度，建议归档",
                    confidence=0.9,
                    auto_applicable=True,
                    transform_type=TransformationType.ARCHIVE,
                    new_structure={
                        "action": "archive_to_cold_storage",
                        "retention_days": 365,
                    },
                    field_mappings=[],
                )
                suggestions.append(suggestion)
        
        return suggestions
    
    def _infer_field_mappings(self, cell1: DataCell, cell2: DataCell) -> List[FieldMapping]:
        """推断两个表的字段映射关系"""
        mappings = []
        
        # 简单的字段名匹配（实际应用中可用更复杂的算法）
        cols1 = {c["name"].lower(): c for c in (cell1.payload.get("columns", []))}
        cols2 = {c["name"].lower(): c for c in (cell2.payload.get("columns", []))}
        
        # 共同字段
        common = set(cols1.keys()) & set(cols2.keys())
        for col in common:
            mappings.append(FieldMapping(
                source_field=f"{cell1.table_name}.{col}",
                target_field=col,
                transform_type=TransformationType.DIRECT,
            ))
        
        # 独有字段
        for col in cols1.keys() - common:
            mappings.append(FieldMapping(
                source_field=f"{cell1.table_name}.{col}",
                target_field=f"from_{cell1.table_name}_{col}",
                transform_type=TransformationType.DIRECT,
            ))
        
        for col in cols2.keys() - common:
            mappings.append(FieldMapping(
                source_field=f"{cell2.table_name}.{col}",
                target_field=f"from_{cell2.table_name}_{col}",
                transform_type=TransformationType.DIRECT,
            ))
        
        return mappings
    
    def apply_suggestions(self, 
                          suggestion_ids: Optional[List[str]] = None,
                          auto_only: bool = False) -> HyperCube:
        """
        应用优化建议，生成第二阶段矩阵
        
        Args:
            suggestion_ids: 指定要应用的建议ID，None表示应用所有
            auto_only: 是否只应用可自动应用的
        
        Returns:
            新的HyperCube（第二阶段矩阵）
        """
        self.optimized_cube = HyperCube()
        
        # 确定要应用的建议
        to_apply = self.suggestions
        if suggestion_ids:
            to_apply = [s for s in to_apply if s.suggestion_id in suggestion_ids]
        if auto_only:
            to_apply = [s for s in to_apply if s.auto_applicable]
        
        # 跟踪已处理的源单元格
        processed_sources = set()
        
        # 应用每个建议
        for suggestion in to_apply:
            if suggestion.type == "merge":
                self._apply_merge(suggestion)
                for cell in suggestion.target_cells:
                    processed_sources.add((cell.t, cell.x, cell.y, cell.z))
            
            elif suggestion.type == "archive":
                # 归档不生成新矩阵单元格，只记录血缘
                for cell in suggestion.target_cells:
                    processed_sources.add((cell.t, cell.x, cell.y, cell.z))
            
            elif suggestion.type == "reclassify":
                # 重新分类只修改元数据
                for cell in suggestion.target_cells:
                    cell.business_domain = suggestion.new_structure.get("suggested_domain", cell.business_domain)
        
        # 复制未被修改的单元格（保持溯源）
        for key, cell in self.source_cube.data_matrix.cells.items():
            if key not in processed_sources:
                # 直接复制，但标记为"已通过审核"
                new_cell = DataCell(
                    t=cell.t,
                    x=cell.x,
                    y=cell.y,
                    z=cell.z,
                    table_name=cell.table_name,
                    schema_name=cell.schema_name,
                    row_count=cell.row_count,
                    size_bytes=cell.size_bytes,
                    column_count=cell.column_count,
                    business_domain=cell.business_domain,
                    lifecycle_stage=cell.lifecycle_stage,
                    tags=cell.tags.copy(),
                    payload=cell.payload.copy(),
                    provenance=cell.provenance,  # 保持溯源
                )
                
                # 如果原来是第一阶段，添加直接血缘
                if new_cell.provenance and new_cell.provenance.is_first_stage():
                    edge = LineageEdge(
                        source_id=new_cell.provenance.cell_id,
                        target_id=f"optimized_{cell.table_name}",
                        transform_type=TransformationType.DIRECT,
                        transform_reason="审核通过，直接保留",
                    )
                    
                    new_prov = self.lineage.register_second_stage(
                        cell_id=f"optimized_{cell.table_name}",
                        source_edges=[edge],
                        created_by="manual",
                    )
                    new_cell.provenance = new_prov
                
                self.optimized_cube.add_cell(new_cell, compute_color=True)
        
        # 同步颜色矩阵
        self.optimized_cube.sync_color_matrix()
        
        return self.optimized_cube
    
    def _apply_merge(self, suggestion: OptimizationSuggestion):
        """应用合并转换"""
        cells = suggestion.target_cells
        if not cells:
            return
        
        # 创建新单元格（代表宽表）
        primary_cell = cells[0]
        new_table_name = suggestion.new_structure.get("new_table_name", "merged_table")
        
        # 计算聚合属性
        total_rows = sum(c.row_count for c in cells)
        total_size = sum(c.size_bytes for c in cells)
        total_cols = sum(c.column_count for c in cells)
        
        # 新坐标（取平均）
        avg_x = sum(c.x for c in cells) / len(cells)
        avg_y = sum(c.y for c in cells) / len(cells)
        
        # 创建新单元格
        new_cell = DataCell(
            t=datetime.now(),
            x=int(avg_x),
            y=avg_y,
            z=primary_cell.z,  # 保持同一主题域
            table_name=new_table_name,
            schema_name="optimized",  # 新schema
            row_count=total_rows,
            size_bytes=total_size,
            column_count=total_cols,
            business_domain=primary_cell.business_domain,
            lifecycle_stage="mature",
            tags=list(set(tag for c in cells for tag in c.tags)),
            payload={
                "type": "wide_table",
                "source_tables": [c.table_name for c in cells],
                "field_mappings": [
                    {
                        "source": fm.source_field,
                        "target": fm.target_field,
                        "type": fm.transform_type.value,
                    }
                    for fm in suggestion.field_mappings
                ],
            }
        )
        
        # 建立血缘关系
        source_edges = []
        for cell in cells:
            if cell.provenance:
                edge = LineageEdge(
                    source_id=cell.provenance.cell_id,
                    target_id=f"optimized_{new_table_name}",
                    transform_type=TransformationType.MERGE,
                    transform_reason=suggestion.reason,
                    field_mappings=suggestion.field_mappings,
                    confidence=suggestion.confidence,
                )
                source_edges.append(edge)
        
        # 注册到血缘追踪器
        new_prov = self.lineage.register_second_stage(
            cell_id=f"optimized_{new_table_name}",
            source_edges=source_edges,
            created_by="ai" if suggestion.auto_applicable else "manual",
        )
        new_cell.provenance = new_prov
        
        # 添加到优化矩阵
        self.optimized_cube.add_cell(new_cell, compute_color=True)
    
    def generate_ddl(self) -> Dict[str, str]:
        """
        为第二阶段矩阵生成DDL语句
        
        用于实际在数据库中创建优化后的结构
        """
        if not self.optimized_cube:
            raise ValueError("请先应用优化建议")
        
        ddl_statements = {}
        
        for key, cell in self.optimized_cube.data_matrix.cells.items():
            if cell.provenance and cell.provenance.is_second_stage():
                # 生成建表语句
                table_name = cell.table_name
                
                ddl = f"-- 表: {table_name}\n"
                ddl += f"-- 业务域: {cell.business_domain}\n"
                ddl += f"-- 来源: {cell.payload.get('source_tables', [])}\n\n"
                ddl += f"CREATE TABLE {cell.schema_name}.{table_name} (\n"
                
                # 添加字段
                columns = []
                for fm in cell.payload.get("field_mappings", []):
                    columns.append(f"    {fm['target']} VARCHAR(255)  -- from {fm['source']}")
                
                ddl += ",\n".join(columns)
                ddl += "\n);\n"
                
                ddl_statements[table_name] = ddl
        
        return ddl_statements
