"""HyperCube – 双矩阵管理器（吸收自 seebook）

整合 RichDataMatrix（DataCell 字典型稀疏矩阵）和 RichColorMatrix（ColorCell
字典型颜色矩阵），提供统一的查询、可视化导出和趋势分析接口。

相比原有 KnowledgeBase，HyperCube 新增：
* 基于外键/命名/结构的动态主题域发现（无需预设）
* 数据血缘 / 溯源信息随单元格共同存储
* 颜色异常检测与数据质量评分
* 矩阵优化（合并/归档/重分类建议）
"""

from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

try:
    from colorspacious import cspace_convert  # type: ignore
    _HAS_COLORSPACIOUS = True
except ImportError:
    _HAS_COLORSPACIOUS = False

try:
    import pandas as pd  # type: ignore
    _HAS_PANDAS = True
except ImportError:
    _HAS_PANDAS = False

from .data_matrix import DataCell


# ---------------------------------------------------------------------------
# ColorCell  &  ColorScheme
# ---------------------------------------------------------------------------

@dataclass
class ColorCell:
    """单个颜色单元格（与 DataCell 一一对应）。"""

    t: datetime
    x: int
    y: float
    z: int

    r: int
    g: int
    b: int
    h: float
    s: float
    l: float
    alpha: float = 1.0
    pulse: bool = False
    pulse_rate: float = 0.0
    source_coordinates: Tuple = ()

    def to_hex(self) -> str:
        return f"#{self.r:02x}{self.g:02x}{self.b:02x}"

    def to_rgba(self) -> Tuple[int, int, int, float]:
        return (self.r, self.g, self.b, self.alpha)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "coordinates": {"t": self.t.isoformat(), "x": self.x, "y": self.y, "z": self.z},
            "color": {
                "hex": self.to_hex(),
                "rgb": [self.r, self.g, self.b],
                "hsl": [self.h, self.s, self.l],
            },
            "alpha": self.alpha,
            "pulse": self.pulse,
        }


class ColorScheme:
    """颜色方案：将四维坐标映射到 HSL 颜色。"""

    DOMAIN_HUES: Dict[str, int] = {
        "user": 200,
        "revenue": 120,
        "product": 280,
        "tech": 30,
        "marketing": 340,
        "operations": 60,
        "default": 210,
    }

    STAGE_SATURATION: Dict[str, float] = {
        "new": 0.3,
        "growth": 0.6,
        "mature": 0.9,
        "legacy": 0.5,
        "deprecated": 0.2,
        "default": 0.7,
    }

    def __init__(self) -> None:
        self.domain_hues = self.DOMAIN_HUES.copy()
        self.stage_saturation = self.STAGE_SATURATION.copy()
        self.y_log_scale = True
        self.y_range = (0.0, 1.0)
        self.time_shift_max = 30.0

    def get_hue_for_z(self, z: int, z_categories: Dict[int, str]) -> float:
        domain = z_categories.get(z, "default")
        for key, hue in self.domain_hues.items():
            if key in domain.lower():
                return float(hue)
        return float((z * 60) % 360)

    def get_saturation_for_x(self, x: int, x_stages: Dict[int, str]) -> float:
        stage = x_stages.get(x, "default")
        for key, sat in self.stage_saturation.items():
            if key in stage.lower():
                return sat
        return self.stage_saturation["default"]

    def get_lightness_for_y(self, y: float) -> float:
        if self.y_log_scale and y > 0:
            y_norm = math.log10(y + 1) / 10.0
        else:
            y_norm = float(y)
        lightness = 0.15 + y_norm * 0.7
        return min(max(lightness, 0.15), 0.85)

    def get_time_shift(self, t: datetime, t_start: datetime, t_end: datetime) -> float:
        if t_start == t_end:
            return 0.0
        ratio = (t - t_start).total_seconds() / (t_end - t_start).total_seconds()
        return ratio * self.time_shift_max


# ---------------------------------------------------------------------------
# RichColorMatrix – dict-based color store keyed by (t, x, y, z)
# ---------------------------------------------------------------------------

class RichColorMatrix:
    """字典型四维颜色矩阵（与 RichDataMatrix 一一对应）。"""

    def __init__(self, scheme: Optional[ColorScheme] = None) -> None:
        self.cells: Dict[Tuple, ColorCell] = {}
        self.scheme = scheme or ColorScheme()
        self.z_categories: Dict[int, str] = {}
        self.x_stages: Dict[int, str] = {}
        self.t_start: Optional[datetime] = None
        self.t_end: Optional[datetime] = None

    def set_time_range(self, t_start: datetime, t_end: datetime) -> None:
        self.t_start = t_start
        self.t_end = t_end

    def set_categories(self, z_categories: Dict[int, str], x_stages: Dict[int, str]) -> None:
        self.z_categories = z_categories
        self.x_stages = x_stages

    def compute_color(self, t: datetime, x: int, y: float, z: int) -> ColorCell:
        hue = self.scheme.get_hue_for_z(z, self.z_categories)
        saturation = self.scheme.get_saturation_for_x(x, self.x_stages)
        lightness = self.scheme.get_lightness_for_y(y)
        if self.t_start and self.t_end:
            shift = self.scheme.get_time_shift(t, self.t_start, self.t_end)
            hue = (hue + shift) % 360
        r, g, b = _hsl_to_rgb(hue, saturation, lightness)
        return ColorCell(t=t, x=x, y=y, z=z, r=r, g=g, b=b,
                         h=hue, s=saturation, l=lightness,
                         source_coordinates=(t, x, y, z))

    def add_cell(self, t: datetime, x: int, y: float, z: int) -> ColorCell:
        cell = self.compute_color(t, x, y, z)
        self.cells[(t, x, y, z)] = cell
        return cell

    def get_cell(self, t: datetime, x: int, y: float, z: int) -> Optional[ColorCell]:
        return self.cells.get((t, x, y, z))

    def slice_by_z(self, z: int) -> List[ColorCell]:
        return [c for key, c in self.cells.items() if key[3] == z]

    def get_color_trend(self, z: int) -> List[Tuple[datetime, str]]:
        cells = self.slice_by_z(z)
        cells.sort(key=lambda c: c.t)
        return [(c.t, c.to_hex()) for c in cells]


def _hsl_to_rgb(h: float, s: float, l: float) -> Tuple[int, int, int]:
    """HSL → RGB conversion with optional colorspacious perceptual path."""
    if _HAS_COLORSPACIOUS:
        try:
            rgb = cspace_convert([l * 100, s * 100, h], "HSLuv", "sRGB1")
            rgb = np.clip(rgb, 0, 1)
            return (int(rgb[0] * 255), int(rgb[1] * 255), int(rgb[2] * 255))
        except Exception:
            pass
    c = (1 - abs(2 * l - 1)) * s
    x = c * (1 - abs((h / 60) % 2 - 1))
    m = l - c / 2
    if h < 60:
        r, g, b = c, x, 0.0
    elif h < 120:
        r, g, b = x, c, 0.0
    elif h < 180:
        r, g, b = 0.0, c, x
    elif h < 240:
        r, g, b = 0.0, x, c
    elif h < 300:
        r, g, b = x, 0.0, c
    else:
        r, g, b = c, 0.0, x
    return (int((r + m) * 255), int((g + m) * 255), int((b + m) * 255))


# ---------------------------------------------------------------------------
# RichDataMatrix – dict-based data store keyed by (t, x, y, z)
# ---------------------------------------------------------------------------

class RichDataMatrix:
    """字典型四维数据矩阵（使用富 DataCell，与 RichColorMatrix 配套）。"""

    def __init__(self) -> None:
        self.cells: Dict[Tuple, DataCell] = {}
        self.z_categories: Dict[int, str] = {}
        self.x_stages: Dict[int, str] = {}
        self.metadata: Dict[str, Any] = {
            "created_at": datetime.now().isoformat(),
        }

    def add_cell(self, cell: DataCell) -> None:
        key = (cell.t, cell.x, cell.y, cell.z)
        self.cells[key] = cell
        if cell.z not in self.z_categories and cell.business_domain:
            self.z_categories[cell.z] = cell.business_domain
        if cell.x not in self.x_stages and cell.lifecycle_stage:
            self.x_stages[cell.x] = cell.lifecycle_stage

    def get_cell(self, t: datetime, x: int, y: float, z: int) -> Optional[DataCell]:
        return self.cells.get((t, x, y, z))

    def slice_by_z(self, z: int) -> List[DataCell]:
        return [c for key, c in self.cells.items() if key[3] == z]

    def get_trend(self, z: int) -> Any:
        cells = self.slice_by_z(z)
        if not cells:
            if _HAS_PANDAS:
                import pandas as pd
                return pd.DataFrame()
            return []
        data = [
            {
                "t": c.t,
                "x": c.x,
                "y": c.y,
                "row_count": c.row_count,
                "size_bytes": c.size_bytes,
                "table_name": c.table_name,
            }
            for c in cells
        ]
        if _HAS_PANDAS:
            import pandas as pd
            return pd.DataFrame(data).sort_values("t")
        return sorted(data, key=lambda d: d["t"])

    def get_summary(self) -> Dict[str, Any]:
        if not self.cells:
            return {"empty": True}
        rows = sum(c.row_count for c in self.cells.values())
        size = sum(c.size_bytes for c in self.cells.values())
        times = [c.t for c in self.cells.values()]
        return {
            "empty": False,
            "total_cells": len(self.cells),
            "unique_tables": len({c.table_name for c in self.cells.values()}),
            "domains": list(self.z_categories.values()),
            "stages": list(self.x_stages.values()),
            "total_rows": rows,
            "total_size_bytes": size,
            "time_range": {"min": min(times), "max": max(times)},
        }


# ---------------------------------------------------------------------------
# HyperCube – dual-matrix manager
# ---------------------------------------------------------------------------

class HyperCube:
    """四维超立方体：同时维护 RichDataMatrix 和 RichColorMatrix。

    与原有 :class:`~four_dim_matrix.KnowledgeBase` 相比，HyperCube 保留完整
    的数据库元数据（DataCell）、业务域/生命周期标签和溯源信息，并支持：

    * :meth:`query_by_color` – 通过颜色反查数据记录
    * :meth:`query_by_visual_region` – 模拟框选区域查询
    * :meth:`get_business_trend` – 按主题域获取时间趋势
    * :meth:`get_color_flow` – 颜色流动序列（时间动画）
    * :meth:`export_for_visualization` – 导出可视化格式
    """

    def __init__(self, color_scheme: Optional[ColorScheme] = None) -> None:
        self.data_matrix = RichDataMatrix()
        self.color_matrix = RichColorMatrix(scheme=color_scheme)
        self.synced = False

    # ------------------------------------------------------------------
    # Mutation
    # ------------------------------------------------------------------

    def add_cell(self, data_cell: DataCell, compute_color: bool = True) -> Optional[ColorCell]:
        """添加数据单元格，可选同时计算颜色。"""
        self.data_matrix.add_cell(data_cell)
        color_cell = None
        if compute_color:
            self._sync_metadata()
            color_cell = self.color_matrix.add_cell(
                data_cell.t, data_cell.x, data_cell.y, data_cell.z
            )
        return color_cell

    def _sync_metadata(self) -> None:
        self.color_matrix.set_categories(
            self.data_matrix.z_categories,
            self.data_matrix.x_stages,
        )
        if self.data_matrix.cells:
            times = [key[0] for key in self.data_matrix.cells]
            if times:
                self.color_matrix.set_time_range(min(times), max(times))

    def sync_color_matrix(self) -> None:
        """重新计算所有颜色单元格（全量同步）。"""
        self._sync_metadata()
        self.color_matrix.cells = {}
        for key, dc in self.data_matrix.cells.items():
            t, x, y, z = key
            self.color_matrix.add_cell(t, x, y, z)
        self.synced = True

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    def get_dual_cell(
        self, t: datetime, x: int, y: float, z: int
    ) -> Tuple[Optional[DataCell], Optional[ColorCell]]:
        """返回同一坐标处的 (DataCell, ColorCell)。"""
        return (
            self.data_matrix.get_cell(t, x, y, z),
            self.color_matrix.get_cell(t, x, y, z),
        )

    def query_by_color(
        self, hex_color: str, threshold: int = 30
    ) -> List[Dict[str, Any]]:
        """颜色 → 数据：返回颜色相似度在 *threshold* 以内的数据记录。"""
        hex_color = hex_color.lstrip("#")
        target = tuple(int(hex_color[i : i + 2], 16) for i in (0, 2, 4))
        results = []
        for key, cc in self.color_matrix.cells.items():
            rgb = (cc.r, cc.g, cc.b)
            dist = sum((a - b) ** 2 for a, b in zip(target, rgb)) ** 0.5
            if dist < threshold:
                dc = self.data_matrix.get_cell(*key)
                if dc:
                    results.append({
                        "color_match": {
                            "target": f"#{hex_color}",
                            "matched": cc.to_hex(),
                            "distance": dist,
                        },
                        "data": dc.to_dict(),
                    })
        results.sort(key=lambda r: r["color_match"]["distance"])
        return results

    def query_by_visual_region(
        self,
        z: int,
        x_range: Optional[Tuple[int, int]] = None,
        y_range: Optional[Tuple[float, float]] = None,
    ) -> Dict[str, Any]:
        """模拟框选区域查询（z 轴切片 + 可选 XY 范围过滤）。"""
        data_cells = self.data_matrix.slice_by_z(z)
        color_cells = self.color_matrix.slice_by_z(z)
        if x_range:
            data_cells = [c for c in data_cells if x_range[0] <= c.x <= x_range[1]]
            color_cells = [c for c in color_cells if x_range[0] <= c.x <= x_range[1]]
        if y_range:
            data_cells = [c for c in data_cells if y_range[0] <= c.y <= y_range[1]]
            color_cells = [c for c in color_cells if y_range[0] <= c.y <= y_range[1]]
        color_dist: Dict[str, int] = {}
        for cc in color_cells:
            h = cc.to_hex()
            color_dist[h] = color_dist.get(h, 0) + 1
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
            "color_distribution": color_dist,
            "dominant_colors": sorted(
                color_dist.items(), key=lambda x: x[1], reverse=True
            )[:5],
            "data": [c.to_dict() for c in data_cells],
        }

    # ------------------------------------------------------------------
    # Trend & flow
    # ------------------------------------------------------------------

    def get_business_trend(self, z: int, metric: str = "row_count") -> Any:
        """返回特定主题域随时间的趋势（需 pandas）。"""
        df = self.data_matrix.get_trend(z)
        if not _HAS_PANDAS or (hasattr(df, "empty") and df.empty):
            return df
        import pandas as pd

        if metric == "row_count":
            return df.groupby("t")["row_count"].sum().reset_index()
        if metric == "size_bytes":
            return df.groupby("t")["size_bytes"].sum().reset_index()
        return df.groupby("t")["y"].mean().reset_index()

    def get_color_flow(self, z: int) -> List[Dict[str, Any]]:
        """返回颜色流动序列（时间轴上颜色的变化历史）。"""
        flow = []
        for t, hex_color in self.color_matrix.get_color_trend(z):
            dcs = [c for key, c in self.data_matrix.cells.items()
                   if key[0] == t and key[3] == z]
            flow.append({
                "time": t.isoformat(),
                "color": hex_color,
                "metrics": {
                    "table_count": len(dcs),
                    "total_rows": sum(c.row_count for c in dcs),
                },
            })
        return flow

    # ------------------------------------------------------------------
    # Export
    # ------------------------------------------------------------------

    def export_for_visualization(self) -> Dict[str, Any]:
        """导出适合前端可视化的 JSON 格式。"""
        if not self.synced:
            self.sync_color_matrix()
        points = []
        for key, dc in self.data_matrix.cells.items():
            cc = self.color_matrix.get_cell(*key)
            if cc:
                t, x, y, z = key
                points.append({
                    "coordinates": {
                        "t": t.isoformat() if isinstance(t, datetime) else t,
                        "x": x,
                        "y": y,
                        "z": z,
                    },
                    "data": {
                        "table_name": dc.table_name,
                        "schema_name": dc.schema_name,
                        "row_count": dc.row_count,
                        "size_bytes": dc.size_bytes,
                        "business_domain": dc.business_domain,
                        "lifecycle_stage": dc.lifecycle_stage,
                    },
                    "color": cc.to_dict()["color"],
                })
        return {
            "metadata": self.data_matrix.metadata,
            "categories": {
                "z": self.data_matrix.z_categories,
                "x": self.data_matrix.x_stages,
            },
            "data_points": points,
        }

    def get_summary(self) -> Dict[str, Any]:
        """返回双矩阵摘要。"""
        return {
            "data_matrix": self.data_matrix.get_summary(),
            "color_matrix": {
                "total_cells": len(self.color_matrix.cells),
                "color_categories": len(
                    {c.to_hex() for c in self.color_matrix.cells.values()}
                ),
            },
            "sync_status": self.synced,
        }
