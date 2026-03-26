"""
四维数据矩阵 - 矩阵一
存储数据库主题库的完整元数据信息

维度定义：
- t: 时间维度（数据更新时间、创建时间等）
- x: 业务时间/阶段（表的生命周期阶段）
- y: 量级（表大小、行数、重要性评分）
- z: 主题分类（业务域、schema分类）
"""

from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from datetime import datetime
import json
import numpy as np
import pandas as pd

from hypercube.core.lineage import Provenance


@dataclass
class DataCell:
    """单个数据单元格，存储完整的JSON记录"""
    t: datetime          # 时间维度
    x: int               # 业务阶段（0-255）
    y: float             # 量级指标（归一化或对数压缩）
    z: int               # 主题分类ID
    
    # 核心数据载荷
    payload: Dict[str, Any] = field(default_factory=dict)
    
    # 元数据
    table_name: str = ""
    schema_name: str = ""
    column_count: int = 0
    row_count: int = 0
    size_bytes: int = 0
    last_updated: Optional[datetime] = None
    
    # 业务标签
    tags: List[str] = field(default_factory=list)
    business_domain: str = ""  # 业务域（用于z轴分类）
    lifecycle_stage: str = ""  # 生命周期阶段（用于x轴）
    
    # 溯源信息（关键：支持两个阶段）
    provenance: Optional[Provenance] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """序列化为字典"""
        return {
            "coordinates": {"t": self.t.isoformat(), "x": self.x, "y": self.y, "z": self.z},
            "table_name": self.table_name,
            "schema_name": self.schema_name,
            "metrics": {
                "column_count": self.column_count,
                "row_count": self.row_count,
                "size_bytes": self.size_bytes,
                "size_human": self._human_readable_size(),
            },
            "classification": {
                "business_domain": self.business_domain,
                "lifecycle_stage": self.lifecycle_stage,
                "tags": self.tags,
            },
            "payload": self.payload,
        }
    
    def _human_readable_size(self) -> str:
        """转换字节为人类可读格式"""
        size = self.size_bytes
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size < 1024.0:
                return f"{size:.2f} {unit}"
            size /= 1024.0
        return f"{size:.2f} PB"


class DataMatrix:
    """
    四维数据矩阵
    
    存储结构：
    - 稀疏矩阵存储，使用字典 {(t,x,y,z): DataCell}
    - 支持按任意维度切片查询
    """
    
    def __init__(self):
        self.cells: Dict[Tuple, DataCell] = {}
        self.z_categories: Dict[int, str] = {}  # z轴分类映射
        self.x_stages: Dict[int, str] = {}      # x轴阶段映射
        self.metadata: Dict[str, Any] = {
            "created_at": datetime.now().isoformat(),
            "total_cells": 0,
            "total_tables": 0,
        }
    
    def add_cell(self, cell: DataCell) -> None:
        """添加数据单元格"""
        key = (cell.t, cell.x, cell.y, cell.z)
        self.cells[key] = cell
        self.metadata["total_cells"] = len(self.cells)
        
        # 更新分类映射
        if cell.z not in self.z_categories and cell.business_domain:
            self.z_categories[cell.z] = cell.business_domain
        if cell.x not in self.x_stages and cell.lifecycle_stage:
            self.x_stages[cell.x] = cell.lifecycle_stage
    
    def get_cell(self, t, x, y, z) -> Optional[DataCell]:
        """获取指定坐标的单元格"""
        return self.cells.get((t, x, y, z))
    
    def slice_by_z(self, z: int) -> List[DataCell]:
        """按z轴切片（获取特定主题的所有表）"""
        return [cell for key, cell in self.cells.items() if key[3] == z]
    
    def slice_by_t(self, t: datetime) -> List[DataCell]:
        """按t轴切片（获取特定时间点的所有表）"""
        return [cell for key, cell in self.cells.items() if key[0] == t]
    
    def slice_by_xy(self, x_range: Tuple[int, int], y_range: Tuple[float, float]) -> List[DataCell]:
        """按xy平面切片（获取特定阶段和量级的表）"""
        results = []
        for key, cell in self.cells.items():
            _, x, y, _ = key
            if x_range[0] <= x <= x_range[1] and y_range[0] <= y <= y_range[1]:
                results.append(cell)
        return results
    
    def get_trend(self, z: int, x: Optional[int] = None) -> pd.DataFrame:
        """
        获取趋势数据
        
        Args:
            z: 主题分类ID
            x: 可选，特定业务阶段
        
        Returns:
            DataFrame包含时间序列数据
        """
        cells = self.slice_by_z(z)
        if x is not None:
            cells = [c for c in cells if c.x == x]
        
        if not cells:
            return pd.DataFrame()
        
        data = []
        for cell in cells:
            data.append({
                "t": cell.t,
                "x": cell.x,
                "y": cell.y,
                "row_count": cell.row_count,
                "size_bytes": cell.size_bytes,
                "table_name": cell.table_name,
            })
        
        return pd.DataFrame(data).sort_values("t")
    
    def query_by_tags(self, tags: List[str]) -> List[DataCell]:
        """按标签查询"""
        tag_set = set(tags)
        return [cell for cell in self.cells.values() if tag_set & set(cell.tags)]
    
    def to_dataframe(self) -> pd.DataFrame:
        """导出为DataFrame"""
        data = [cell.to_dict() for cell in self.cells.values()]
        return pd.json_normalize(data)
    
    def get_summary(self) -> Dict[str, Any]:
        """获取矩阵摘要统计"""
        if not self.cells:
            return {"empty": True}
        
        domains = list(self.z_categories.values())
        stages = list(self.x_stages.values())
        
        total_rows = sum(c.row_count for c in self.cells.values())
        total_size = sum(c.size_bytes for c in self.cells.values())
        
        return {
            "empty": False,
            "total_cells": len(self.cells),
            "unique_tables": len(set(c.table_name for c in self.cells.values())),
            "domains": domains,
            "stages": stages,
            "total_rows": total_rows,
            "total_size_bytes": total_size,
            "time_range": {
                "min": min(c.t for c in self.cells.values()),
                "max": max(c.t for c in self.cells.values()),
            },
            "y_range": {
                "min": min(c.y for c in self.cells.values()),
                "max": max(c.y for c in self.cells.values()),
            }
        }
