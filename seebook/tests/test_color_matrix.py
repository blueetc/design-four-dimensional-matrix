"""
ColorMatrix 单元测试
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from datetime import datetime
import pytest

from hypercube.core.color_matrix import ColorMatrix, ColorScheme, ColorCell


def test_color_scheme_hue_mapping():
    """测试色相映射"""
    scheme = ColorScheme()
    
    z_categories = {0: "user", 1: "revenue", 2: "product"}
    
    hue_0 = scheme.get_hue_for_z(0, z_categories)
    hue_1 = scheme.get_hue_for_z(1, z_categories)
    
    assert 0 <= hue_0 < 360
    assert 0 <= hue_1 < 360


def test_color_scheme_lightness_mapping():
    """测试亮度映射"""
    scheme = ColorScheme()
    
    # 小值应该映射到较暗
    lightness_small = scheme.get_lightness_for_y(1)
    # 大值应该映射到较亮（对数压缩后）
    lightness_large = scheme.get_lightness_for_y(100000000)  # 1亿行
    
    assert 0.1 <= lightness_small < 0.4
    assert lightness_small < lightness_large  # 大值应该更亮
    assert lightness_large <= 0.9


def test_color_matrix_compute():
    """测试颜色计算"""
    scheme = ColorScheme()
    matrix = ColorMatrix(scheme)
    
    matrix.set_categories(
        z_categories={0: "user"},
        x_stages={50: "growth"}
    )
    
    now = datetime.now()
    matrix.set_time_range(now, now)
    
    cell = matrix.compute_color(now, 50, 100, 0)
    
    assert isinstance(cell, ColorCell)
    assert 0 <= cell.r <= 255
    assert 0 <= cell.g <= 255
    assert 0 <= cell.b <= 255


def test_color_cell_to_hex():
    """测试颜色转十六进制"""
    cell = ColorCell(
        t=datetime.now(),
        x=50,
        y=100.0,
        z=1,
        r=255,
        g=100,
        b=50,
        h=0,
        s=1.0,
        l=0.5,
    )
    
    hex_color = cell.to_hex()
    assert hex_color.startswith("#")
    assert len(hex_color) == 7


def test_color_matrix_add_and_get():
    """测试颜色矩阵添加和获取"""
    matrix = ColorMatrix()
    matrix.set_categories({0: "user"}, {50: "growth"})
    
    now = datetime.now()
    matrix.set_time_range(now, now)
    
    cell = matrix.add_cell(now, 50, 100, 0)
    
    retrieved = matrix.get_cell(now, 50, 100, 0)
    assert retrieved is not None
    assert retrieved.to_hex() == cell.to_hex()


if __name__ == "__main__":
    test_color_scheme_hue_mapping()
    test_color_scheme_lightness_mapping()
    test_color_matrix_compute()
    test_color_cell_to_hex()
    test_color_matrix_add_and_get()
    print("所有测试通过!")
