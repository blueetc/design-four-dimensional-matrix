"""
四维矩阵数据库可视化认知系统

通过双矩阵架构实现数据库的快速认知和查询：
- DataMatrix: 存储完整的元数据信息
- ColorMatrix: 存储视觉编码，用于快速识别和趋势分析
"""

__version__ = "0.1.0"

from hypercube.core.data_matrix import DataMatrix
from hypercube.core.color_matrix import ColorMatrix
from hypercube.core.hypercube import HyperCube

__all__ = ["DataMatrix", "ColorMatrix", "HyperCube"]
