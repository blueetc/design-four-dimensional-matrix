"""
DataMatrix 单元测试
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from datetime import datetime
import pytest

from hypercube.core.data_matrix import DataMatrix, DataCell


def test_data_cell_creation():
    """测试数据单元格创建"""
    cell = DataCell(
        t=datetime.now(),
        x=50,
        y=100.0,
        z=1,
        table_name="test_table",
        schema_name="public",
        row_count=10000,
        size_bytes=1024000,
    )
    
    assert cell.table_name == "test_table"
    assert cell.x == 50
    assert cell.y == 100.0


def test_data_matrix_add_and_get():
    """测试数据矩阵添加和获取"""
    matrix = DataMatrix()
    
    cell = DataCell(
        t=datetime.now(),
        x=50,
        y=100.0,
        z=1,
        table_name="test_table",
    )
    
    matrix.add_cell(cell)
    
    # 验证添加成功
    assert len(matrix.cells) == 1
    
    # 验证可以获取
    retrieved = matrix.get_cell(cell.t, cell.x, cell.y, cell.z)
    assert retrieved is not None
    assert retrieved.table_name == "test_table"


def test_data_matrix_slice_by_z():
    """测试按Z轴切片"""
    matrix = DataMatrix()
    
    now = datetime.now()
    
    # 添加不同Z值的单元格（确保坐标唯一）
    test_data = [
        (0, 10, 100.0),  # z=0
        (0, 20, 100.0),  # z=0 (不同x)
        (1, 50, 100.0),  # z=1
        (2, 50, 100.0),  # z=2
    ]
    for z, x, y in test_data:
        cell = DataCell(
            t=now,
            x=x,
            y=y,
            z=z,
            table_name=f"table_z{z}_x{x}",
        )
        matrix.add_cell(cell)
    
    # 验证切片
    z0_cells = matrix.slice_by_z(0)
    assert len(z0_cells) == 2
    
    z1_cells = matrix.slice_by_z(1)
    assert len(z1_cells) == 1


def test_data_matrix_summary():
    """测试摘要统计"""
    matrix = DataMatrix()
    
    now = datetime.now()
    cell = DataCell(
        t=now,
        x=50,
        y=100.0,
        z=1,
        table_name="test_table",
        row_count=1000,
        size_bytes=1024000,
        business_domain="user",
    )
    matrix.add_cell(cell)
    
    summary = matrix.get_summary()
    
    assert not summary["empty"]
    assert summary["total_cells"] == 1
    assert summary["total_rows"] == 1000


if __name__ == "__main__":
    test_data_cell_creation()
    test_data_matrix_add_and_get()
    test_data_matrix_slice_by_z()
    test_data_matrix_summary()
    print("所有测试通过!")
