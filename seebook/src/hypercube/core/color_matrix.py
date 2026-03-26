"""
四维颜色矩阵 - 矩阵二
存储数据矩阵的视觉编码

颜色映射策略：
- z (主题)  → 色相(Hue): 不同业务域用不同色系
- y (量级)  → 亮度(Lightness): 数值越大越亮  
- x (阶段)  → 饱和度(Saturation): 阶段特征
- t (时间)  → 色温偏移: 时间流动感
"""

from typing import Dict, Tuple, Optional, List, Any
from dataclasses import dataclass
from datetime import datetime
import numpy as np
try:
    from colorspacious import cspace_convert
    HAS_COLORSPACIOUS = True
except ImportError:
    HAS_COLORSPACIOUS = False


@dataclass
class ColorCell:
    """颜色单元格"""
    t: datetime
    x: int
    y: float
    z: int
    
    # RGB颜色值 (0-255)
    r: int
    g: int
    b: int
    
    # HSL值 (用于调试和理解)
    h: float  # 色相 0-360
    s: float  # 饱和度 0-1
    l: float  # 亮度 0-1
    
    # 透明度（可选第五维度）
    alpha: float = 1.0
    
    # 闪烁/脉冲标记（异常预警）
    pulse: bool = False
    pulse_rate: float = 0.0
    
    # 元数据
    source_coordinates: Tuple = ()  # 对应的数据矩阵坐标
    
    def to_hex(self) -> str:
        """转换为十六进制颜色"""
        return f"#{self.r:02x}{self.g:02x}{self.b:02x}"
    
    def to_rgba(self) -> Tuple[int, int, int, float]:
        """转换为RGBA元组"""
        return (self.r, self.g, self.b, self.alpha)
    
    def to_dict(self) -> Dict[str, Any]:
        """序列化"""
        return {
            "coordinates": {"t": self.t.isoformat(), "x": self.x, "y": self.y, "z": self.z},
            "color": {
                "hex": self.to_hex(),
                "rgb": [self.r, self.g, self.b],
                "hsl": [self.h, self.s, self.l],
                "rgba": self.to_rgba(),
            },
            "alpha": self.alpha,
            "pulse": self.pulse,
        }


class ColorScheme:
    """颜色方案配置"""
    
    # 业务域默认配色（色相值）
    DOMAIN_HUES = {
        "user": 200,      # 用户域 - 蓝色
        "revenue": 120,   # 营收域 - 绿色
        "product": 280,   # 产品域 - 紫色
        "tech": 30,       # 技术域 - 橙色
        "marketing": 340, # 营销域 - 粉色
        "operations": 60, # 运营域 - 黄色
        "default": 210,   # 默认 - 天蓝
    }
    
    # 生命周期阶段饱和度
    STAGE_SATURATION = {
        "new": 0.3,       # 新建 - 低饱和（柔和）
        "growth": 0.6,    # 增长 - 中饱和
        "mature": 0.9,    # 成熟 - 高饱和（醒目）
        "legacy": 0.5,    # 遗留 - 中低饱和
        "deprecated": 0.2,# 废弃 - 很低饱和
        "default": 0.7,
    }
    
    def __init__(self):
        self.domain_hues = self.DOMAIN_HUES.copy()
        self.stage_saturation = self.STAGE_SATURATION.copy()
        self.y_log_scale = True  # y轴使用对数压缩
        self.y_range = (0, 1)    # y轴归一化范围
        self.time_shift_max = 30  # 时间色温最大偏移角度
    
    def get_hue_for_z(self, z: int, z_categories: Dict[int, str]) -> float:
        """获取z轴对应的色相"""
        domain = z_categories.get(z, "default")
        # 尝试匹配已知域
        for key, hue in self.domain_hues.items():
            if key in domain.lower():
                return hue
        # 基于z值生成色相（均匀分布）
        return (z * 60) % 360
    
    def get_saturation_for_x(self, x: int, x_stages: Dict[int, str]) -> float:
        """获取x轴对应的饱和度"""
        stage = x_stages.get(x, "default")
        for key, sat in self.stage_saturation.items():
            if key in stage.lower():
                return sat
        return self.stage_saturation["default"]
    
    def get_lightness_for_y(self, y: float) -> float:
        """
        获取y轴对应的亮度
        
        y值越小（量级小）→ 越暗
        y值越大（量级大）→ 越亮
        避免全黑(0.1)和全白(0.9)
        """
        if self.y_log_scale and y > 0:
            # 对数压缩，处理数量级差异大的情况
            y_norm = np.log10(y + 1) / 10  # 假设最大10^10
        else:
            y_norm = y
        
        # 映射到 0.15 - 0.85，避免极端值
        lightness = 0.15 + y_norm * 0.7
        return min(max(lightness, 0.15), 0.85)
    
    def get_time_shift(self, t: datetime, t_start: datetime, t_end: datetime) -> float:
        """获取时间偏移量"""
        if t_start == t_end:
            return 0
        ratio = (t - t_start).total_seconds() / (t_end - t_start).total_seconds()
        return ratio * self.time_shift_max


class ColorMatrix:
    """
    四维颜色矩阵
    
    与DataMatrix一一对应，提供视觉编码
    """
    
    def __init__(self, scheme: Optional[ColorScheme] = None):
        self.cells: Dict[Tuple, ColorCell] = {}
        self.scheme = scheme or ColorScheme()
        self.z_categories: Dict[int, str] = {}
        self.x_stages: Dict[int, str] = {}
        
        # 时间范围（用于色温偏移计算）
        self.t_start: Optional[datetime] = None
        self.t_end: Optional[datetime] = None
    
    def set_time_range(self, t_start: datetime, t_end: datetime):
        """设置时间范围"""
        self.t_start = t_start
        self.t_end = t_end
    
    def set_categories(self, z_categories: Dict[int, str], x_stages: Dict[int, str]):
        """设置分类映射"""
        self.z_categories = z_categories
        self.x_stages = x_stages
    
    def compute_color(self, t, x, y, z) -> ColorCell:
        """
        计算指定坐标的颜色
        
        算法：
        1. z → 色相（业务域识别）
        2. x → 饱和度（阶段特征）
        3. y → 亮度（量级大小）
        4. t → 色温偏移（时间流动）
        """
        # 获取各维度参数
        hue = self.scheme.get_hue_for_z(z, self.z_categories)
        saturation = self.scheme.get_saturation_for_x(x, self.x_stages)
        lightness = self.scheme.get_lightness_for_y(y)
        
        # 时间偏移
        if self.t_start and self.t_end:
            time_shift = self.scheme.get_time_shift(t, self.t_start, self.t_end)
            hue = (hue + time_shift) % 360
        
        # HSL → RGB
        r, g, b = self._hsl_to_rgb(hue, saturation, lightness)
        
        return ColorCell(
            t=t, x=x, y=y, z=z,
            r=r, g=g, b=b,
            h=hue, s=saturation, l=lightness,
            source_coordinates=(t, x, y, z)
        )
    
    def _hsl_to_rgb(self, h: float, s: float, l: float) -> Tuple[int, int, int]:
        """HSL转RGB"""
        # 使用colorspacious进行更准确的转换
        try:
            # HSLuv提供感知均匀的颜色空间
            rgb = cspace_convert([l * 100, s * 100, h], "HSLuv", "sRGB1")
            # 裁剪到0-1并转换为0-255
            rgb = np.clip(rgb, 0, 1)
            return (int(rgb[0] * 255), int(rgb[1] * 255), int(rgb[2] * 255))
        except:
            # 回退到标准HSL算法
            c = (1 - abs(2 * l - 1)) * s
            x = c * (1 - abs((h / 60) % 2 - 1))
            m = l - c / 2
            
            if h < 60:
                r, g, b = c, x, 0
            elif h < 120:
                r, g, b = x, c, 0
            elif h < 180:
                r, g, b = 0, c, x
            elif h < 240:
                r, g, b = 0, x, c
            elif h < 300:
                r, g, b = x, 0, c
            else:
                r, g, b = c, 0, x
            
            return (
                int((r + m) * 255),
                int((g + m) * 255),
                int((b + m) * 255)
            )
    
    def add_cell(self, t, x, y, z, **kwargs) -> ColorCell:
        """添加颜色单元格"""
        cell = self.compute_color(t, x, y, z)
        
        # 应用自定义属性
        for key, value in kwargs.items():
            if hasattr(cell, key):
                setattr(cell, key, value)
        
        self.cells[(t, x, y, z)] = cell
        return cell
    
    def get_cell(self, t, x, y, z) -> Optional[ColorCell]:
        """获取颜色单元格"""
        return self.cells.get((t, x, y, z))
    
    def slice_by_z(self, z: int) -> List[ColorCell]:
        """按z轴切片"""
        return [cell for key, cell in self.cells.items() if key[3] == z]
    
    def get_color_trend(self, z: int) -> List[Tuple[datetime, str]]:
        """
        获取颜色趋势序列
        
        Returns:
            [(时间, 颜色hex), ...]
        """
        cells = self.slice_by_z(z)
        cells.sort(key=lambda c: c.t)
        return [(c.t, c.to_hex()) for c in cells]
    
    def find_similar_colors(self, target_cell: ColorCell, threshold: int = 30) -> List[ColorCell]:
        """
        查找相似颜色的单元格
        
        用于发现关联的数据点
        """
        similar = []
        target_rgb = np.array([target_cell.r, target_cell.g, target_cell.b])
        
        for cell in self.cells.values():
            if cell == target_cell:
                continue
            rgb = np.array([cell.r, cell.g, cell.b])
            distance = np.linalg.norm(target_rgb - rgb)
            if distance < threshold:
                similar.append(cell)
        
        return similar
    
    def to_array(self) -> np.ndarray:
        """转换为NumPy数组（用于可视化）"""
        if not self.cells:
            return np.array([])
        
        data = []
        for key, cell in self.cells.items():
            t, x, y, z = key
            # 将时间转换为时间戳
            t_ts = t.timestamp() if isinstance(t, datetime) else t
            data.append([t_ts, x, y, z, cell.r, cell.g, cell.b, cell.alpha])
        
        return np.array(data)
