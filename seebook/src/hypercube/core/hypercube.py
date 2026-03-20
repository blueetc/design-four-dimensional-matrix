"""
超立方体 - 双矩阵管理器

整合 DataMatrix 和 ColorMatrix，提供统一的查询和可视化接口
"""

from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
import pandas as pd

from hypercube.core.data_matrix import DataMatrix, DataCell
from hypercube.core.color_matrix import ColorMatrix, ColorCell, ColorScheme


class HyperCube:
    """
    四维超立方体
    
    同时维护两个矩阵：
    - data_matrix: 存储完整数据
    - color_matrix: 存储视觉编码
    """
    
    def __init__(self, color_scheme: Optional[ColorScheme] = None):
        self.data_matrix = DataMatrix()
        self.color_matrix = ColorMatrix(scheme=color_scheme)
        self.synced = False
    
    def add_cell(self, data_cell: DataCell, compute_color: bool = True) -> Optional[ColorCell]:
        """
        添加数据单元格，可选同时计算颜色
        
        Args:
            data_cell: 数据单元格
            compute_color: 是否立即计算对应颜色
        
        Returns:
            如果compute_color=True，返回ColorCell
        """
        self.data_matrix.add_cell(data_cell)
        
        color_cell = None
        if compute_color:
            # 确保颜色矩阵有正确的分类映射
            self._sync_metadata()
            color_cell = self.color_matrix.add_cell(
                data_cell.t, data_cell.x, data_cell.y, data_cell.z
            )
        
        return color_cell
    
    def _sync_metadata(self):
        """同步元数据到颜色矩阵"""
        self.color_matrix.set_categories(
            self.data_matrix.z_categories,
            self.data_matrix.x_stages
        )
        
        # 同步时间范围
        if self.data_matrix.cells:
            times = [key[0] for key in self.data_matrix.cells.keys()]
            if times:
                self.color_matrix.set_time_range(min(times), max(times))
    
    def sync_color_matrix(self):
        """
        全量同步颜色矩阵
        
        当数据矩阵更新后，重新计算所有颜色
        """
        self._sync_metadata()
        
        # 清空并重建颜色矩阵
        self.color_matrix.cells = {}
        
        for key, data_cell in self.data_matrix.cells.items():
            t, x, y, z = key
            self.color_matrix.add_cell(t, x, y, z)
        
        self.synced = True
    
    def get_dual_cell(self, t, x, y, z) -> Tuple[Optional[DataCell], Optional[ColorCell]]:
        """
        获取双矩阵的对应单元格
        
        Returns:
            (DataCell, ColorCell) 元组
        """
        data_cell = self.data_matrix.get_cell(t, x, y, z)
        color_cell = self.color_matrix.get_cell(t, x, y, z)
        return data_cell, color_cell
    
    def query_by_color(self, hex_color: str, threshold: int = 30) -> List[Dict[str, Any]]:
        """
        通过颜色查询数据
        
        这是系统的核心功能：颜色 → 数据
        
        Args:
            hex_color: 十六进制颜色，如 "#FF5733"
            threshold: 颜色相似度阈值
        
        Returns:
            匹配的数据记录列表
        """
        # 解析目标颜色
        hex_color = hex_color.lstrip('#')
        target_rgb = tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))
        
        # 查找相似颜色
        results = []
        for key, color_cell in self.color_matrix.cells.items():
            rgb = (color_cell.r, color_cell.g, color_cell.b)
            distance = sum((a - b) ** 2 for a, b in zip(target_rgb, rgb)) ** 0.5
            
            if distance < threshold:
                data_cell = self.data_matrix.get_cell(*key)
                if data_cell:
                    results.append({
                        "color_match": {
                            "target": f"#{hex_color}",
                            "matched": color_cell.to_hex(),
                            "distance": distance,
                        },
                        "data": data_cell.to_dict(),
                    })
        
        # 按相似度排序
        results.sort(key=lambda x: x["color_match"]["distance"])
        return results
    
    def query_by_visual_region(self, 
                                z: int,
                                x_range: Optional[Tuple[int, int]] = None,
                                y_range: Optional[Tuple[float, float]] = None) -> Dict[str, Any]:
        """
        通过视觉区域查询
        
        模拟用户在可视化界面上框选区域
        
        Args:
            z: 主题分类（z轴切片）
            x_range: x轴范围（可选）
            y_range: y轴范围（可选）
        
        Returns:
            区域内的数据和颜色分布
        """
        # 获取z轴切片
        data_cells = self.data_matrix.slice_by_z(z)
        color_cells = self.color_matrix.slice_by_z(z)
        
        # 应用范围过滤
        if x_range:
            data_cells = [c for c in data_cells if x_range[0] <= c.x <= x_range[1]]
            color_cells = [c for c in color_cells if x_range[0] <= c.x <= x_range[1]]
        
        if y_range:
            data_cells = [c for c in data_cells if y_range[0] <= c.y <= y_range[1]]
            color_cells = [c for c in color_cells if y_range[0] <= c.y <= y_range[1]]
        
        # 分析颜色分布
        color_distribution = {}
        for cell in color_cells:
            hex_color = cell.to_hex()
            color_distribution[hex_color] = color_distribution.get(hex_color, 0) + 1
        
        return {
            "query_params": {
                "z": z,
                "z_name": self.data_matrix.z_categories.get(z, "unknown"),
                "x_range": x_range,
                "y_range": y_range,
            },
            "statistics": {
                "count": len(data_cells),
                "total_rows": sum(c.row_count for c in data_cells),
                "total_size": sum(c.size_bytes for c in data_cells),
            },
            "color_distribution": color_distribution,
            "dominant_colors": sorted(
                color_distribution.items(),
                key=lambda x: x[1],
                reverse=True
            )[:5],
            "data": [c.to_dict() for c in data_cells],
        }
    
    def get_business_trend(self, z: int, metric: str = "row_count") -> pd.DataFrame:
        """
        获取业务趋势
        
        用于分析特定主题随时间的演变
        
        Args:
            z: 主题分类ID
            metric: 指标字段（row_count, size_bytes, y）
        
        Returns:
            DataFrame with trend data
        """
        df = self.data_matrix.get_trend(z)
        if df.empty:
            return df
        
        # 按时间聚合
        if metric == "row_count":
            trend = df.groupby("t")["row_count"].sum().reset_index()
        elif metric == "size_bytes":
            trend = df.groupby("t")["size_bytes"].sum().reset_index()
        else:
            trend = df.groupby("t")["y"].mean().reset_index()
        
        return trend
    
    def get_color_flow(self, z: int) -> List[Dict[str, Any]]:
        """
        获取颜色流动序列
        
        用于可视化时间维度上的颜色变化
        """
        color_trend = self.color_matrix.get_color_trend(z)
        
        flow = []
        for t, hex_color in color_trend:
            # 查找对应数据
            data_cells = [
                c for key, c in self.data_matrix.cells.items()
                if key[0] == t and key[3] == z
            ]
            
            total_rows = sum(c.row_count for c in data_cells)
            
            flow.append({
                "time": t.isoformat(),
                "color": hex_color,
                "metrics": {
                    "table_count": len(data_cells),
                    "total_rows": total_rows,
                }
            })
        
        return flow
    
    def export_for_visualization(self) -> Dict[str, Any]:
        """
        导出为可视化格式
        
        供前端可视化组件使用
        """
        if not self.synced:
            self.sync_color_matrix()
        
        data_points = []
        for key, data_cell in self.data_matrix.cells.items():
            color_cell = self.color_matrix.get_cell(*key)
            if color_cell:
                t, x, y, z = key
                data_points.append({
                    "coordinates": {
                        "t": t.isoformat() if isinstance(t, datetime) else t,
                        "x": x, "y": y, "z": z,
                    },
                    "data": {
                        "table_name": data_cell.table_name,
                        "schema_name": data_cell.schema_name,
                        "row_count": data_cell.row_count,
                        "size_bytes": data_cell.size_bytes,
                        "business_domain": data_cell.business_domain,
                        "lifecycle_stage": data_cell.lifecycle_stage,
                    },
                    "color": color_cell.to_dict()["color"],
                })
        
        return {
            "metadata": self.data_matrix.metadata,
            "categories": {
                "z": self.data_matrix.z_categories,
                "x": self.data_matrix.x_stages,
            },
            "data_points": data_points,
        }
    
    def get_summary(self) -> Dict[str, Any]:
        """获取双矩阵摘要"""
        data_summary = self.data_matrix.get_summary()
        
        return {
            "data_matrix": data_summary,
            "color_matrix": {
                "total_cells": len(self.color_matrix.cells),
                "color_categories": len(set(c.to_hex() for c in self.color_matrix.cells.values())),
            },
            "sync_status": self.synced,
        }
