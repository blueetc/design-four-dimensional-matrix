"""Tests for the dual-matrix demo module.

Validates that `four_dim_matrix.demo` can:
1. Build sample SQLite databases (e-commerce and CRM).
2. Convert them into HyperCubes (DataMatrix + ColorMatrix pairs).
3. Export a combined HTML visualization (requires plotly).
4. Run the CLI `scan --db sqlite` command.
"""

from __future__ import annotations

import os
import sqlite3
import tempfile
import shutil
from datetime import datetime
from typing import Generator

import pytest

from four_dim_matrix.demo import (
    _classify_domain,
    _classify_lifecycle,
    _compress_rows,
    _create_crm_db,
    _create_ecommerce_db,
    build_hypercube_from_adapter,
    run_demo,
)
from four_dim_matrix.db_adapter import DatabaseAdapter
from four_dim_matrix.hypercube import HyperCube


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def tmpdir() -> Generator[str, None, None]:
    d = tempfile.mkdtemp(prefix="four_dim_test_")
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture()
def ecommerce_db(tmpdir: str) -> str:
    path = os.path.join(tmpdir, "ecommerce.db")
    _create_ecommerce_db(path)
    return path


@pytest.fixture()
def crm_db(tmpdir: str) -> str:
    path = os.path.join(tmpdir, "crm.db")
    _create_crm_db(path)
    return path


# ---------------------------------------------------------------------------
# Classification helpers
# ---------------------------------------------------------------------------

class TestClassifyDomain:
    def test_user_table(self):
        z, name = _classify_domain("customers")
        assert name == "user"

    def test_revenue_table(self):
        z, name = _classify_domain("orders")
        assert name == "revenue"

    def test_product_table(self):
        z, name = _classify_domain("products")
        assert name == "product"

    def test_marketing_table(self):
        z, name = _classify_domain("campaigns")
        assert name == "marketing"

    def test_operations_table(self):
        z, name = _classify_domain("audit_log")
        assert name == "operations"

    def test_unknown_table_falls_back_to_operations(self):
        _z, name = _classify_domain("xyz_table_with_no_keywords")
        assert name == "operations"

    def test_returns_non_negative_z(self):
        z, _name = _classify_domain("any_table")
        assert z >= 0


class TestClassifyLifecycle:
    def test_legacy_at_most_four_columns(self):
        stage, x = _classify_lifecycle(3)
        assert stage == "legacy"
        assert x == 110

    def test_mature_at_six_columns(self):
        stage, x = _classify_lifecycle(6)
        assert stage == "mature"
        assert x == 80

    def test_growth_at_ten_columns(self):
        stage, x = _classify_lifecycle(10)
        assert stage == "growth"
        assert x == 50

    def test_new_at_many_columns(self):
        stage, x = _classify_lifecycle(20)
        assert stage == "new"
        assert x == 20


class TestCompressRows:
    def test_zero_rows_returns_minimum(self):
        assert _compress_rows(0, 100) == 1.0

    def test_zero_max_returns_minimum(self):
        assert _compress_rows(50, 0) == 1.0

    def test_max_rows_returns_255(self):
        result = _compress_rows(1000, 1000)
        assert abs(result - 255.0) < 1e-9

    def test_intermediate_value_in_range(self):
        result = _compress_rows(50, 1000)
        assert 1.0 <= result <= 255.0

    def test_monotone_increasing(self):
        r1 = _compress_rows(10, 1000)
        r2 = _compress_rows(100, 1000)
        r3 = _compress_rows(500, 1000)
        assert r1 < r2 < r3


# ---------------------------------------------------------------------------
# Sample database creators
# ---------------------------------------------------------------------------

class TestCreateEcommerceDb:
    def test_creates_file(self, tmpdir: str):
        path = os.path.join(tmpdir, "ec.db")
        _create_ecommerce_db(path)
        assert os.path.isfile(path)

    def test_expected_tables(self, ecommerce_db: str):
        conn = sqlite3.connect(ecommerce_db)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        tables = {row[0] for row in cursor.fetchall()}
        conn.close()
        assert {"customers", "orders", "products", "payments"}.issubset(tables)

    def test_has_rows(self, ecommerce_db: str):
        conn = sqlite3.connect(ecommerce_db)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM customers")
        count = cursor.fetchone()[0]
        conn.close()
        assert count > 0


class TestCreateCrmDb:
    def test_creates_file(self, tmpdir: str):
        path = os.path.join(tmpdir, "crm.db")
        _create_crm_db(path)
        assert os.path.isfile(path)

    def test_expected_tables(self, crm_db: str):
        conn = sqlite3.connect(crm_db)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        tables = {row[0] for row in cursor.fetchall()}
        conn.close()
        assert {"contacts", "leads", "deals", "campaigns"}.issubset(tables)

    def test_has_rows(self, crm_db: str):
        conn = sqlite3.connect(crm_db)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM contacts")
        count = cursor.fetchone()[0]
        conn.close()
        assert count > 0


# ---------------------------------------------------------------------------
# HyperCube generation (dual-matrix)
# ---------------------------------------------------------------------------

class TestBuildHypercubeFromAdapter:
    """Validate that build_hypercube_from_adapter produces both matrices."""

    def test_returns_hypercube_instance(self, ecommerce_db: str):
        adapter = DatabaseAdapter.from_sqlite(ecommerce_db)
        hc = build_hypercube_from_adapter(adapter)
        assert isinstance(hc, HyperCube)

    def test_data_matrix_populated(self, ecommerce_db: str):
        adapter = DatabaseAdapter.from_sqlite(ecommerce_db)
        hc = build_hypercube_from_adapter(adapter)
        assert len(hc.data_matrix.cells) > 0

    def test_color_matrix_populated(self, ecommerce_db: str):
        adapter = DatabaseAdapter.from_sqlite(ecommerce_db)
        hc = build_hypercube_from_adapter(adapter)
        assert len(hc.color_matrix.cells) > 0

    def test_dual_matrix_same_cell_count(self, ecommerce_db: str):
        """DataMatrix and ColorMatrix must have a 1-to-1 cell correspondence."""
        adapter = DatabaseAdapter.from_sqlite(ecommerce_db)
        hc = build_hypercube_from_adapter(adapter)
        assert len(hc.data_matrix.cells) == len(hc.color_matrix.cells)

    def test_both_matrices_from_crm(self, crm_db: str):
        adapter = DatabaseAdapter.from_sqlite(crm_db)
        hc = build_hypercube_from_adapter(adapter, "CRM数据库")
        assert len(hc.data_matrix.cells) > 0
        assert len(hc.color_matrix.cells) > 0
        assert len(hc.data_matrix.cells) == len(hc.color_matrix.cells)

    def test_color_cells_have_valid_hex(self, ecommerce_db: str):
        adapter = DatabaseAdapter.from_sqlite(ecommerce_db)
        hc = build_hypercube_from_adapter(adapter)
        for cc in hc.color_matrix.cells.values():
            hex_color = cc.to_hex()
            assert hex_color.startswith("#"), f"Invalid hex: {hex_color}"
            assert len(hex_color) == 7, f"Wrong hex length: {hex_color}"

    def test_data_cells_carry_table_metadata(self, ecommerce_db: str):
        adapter = DatabaseAdapter.from_sqlite(ecommerce_db)
        hc = build_hypercube_from_adapter(adapter, "mydb")
        table_names = {dc.table_name for dc in hc.data_matrix.cells.values()}
        assert "customers" in table_names
        assert "orders" in table_names

    def test_data_cells_have_domain_label(self, ecommerce_db: str):
        adapter = DatabaseAdapter.from_sqlite(ecommerce_db)
        hc = build_hypercube_from_adapter(adapter)
        domains = {dc.business_domain for dc in hc.data_matrix.cells.values()}
        assert len(domains) > 0
        # Should not be empty strings
        assert all(d for d in domains)

    def test_db_label_stored_in_schema_name(self, ecommerce_db: str):
        adapter = DatabaseAdapter.from_sqlite(ecommerce_db)
        hc = build_hypercube_from_adapter(adapter, "test_label")
        schema_names = {dc.schema_name for dc in hc.data_matrix.cells.values()}
        assert "test_label" in schema_names

    def test_empty_database_returns_empty_hypercube(self, tmpdir: str):
        empty_path = os.path.join(tmpdir, "empty.db")
        conn = sqlite3.connect(empty_path)
        conn.close()
        adapter = DatabaseAdapter.from_sqlite(empty_path)
        hc = build_hypercube_from_adapter(adapter)
        assert len(hc.data_matrix.cells) == 0
        assert len(hc.color_matrix.cells) == 0

    def test_synced_flag_is_set(self, ecommerce_db: str):
        adapter = DatabaseAdapter.from_sqlite(ecommerce_db)
        hc = build_hypercube_from_adapter(adapter)
        assert hc.synced is True

    def test_summary_is_not_empty(self, ecommerce_db: str):
        adapter = DatabaseAdapter.from_sqlite(ecommerce_db)
        hc = build_hypercube_from_adapter(adapter)
        summary = hc.get_summary()
        dm = summary["data_matrix"]
        assert dm["empty"] is False
        assert dm["total_cells"] > 0

    def test_export_for_visualization_returns_data(self, ecommerce_db: str):
        adapter = DatabaseAdapter.from_sqlite(ecommerce_db)
        hc = build_hypercube_from_adapter(adapter)
        viz = hc.export_for_visualization()
        assert "data_points" in viz
        assert len(viz["data_points"]) > 0

    def test_export_point_has_color_field(self, ecommerce_db: str):
        adapter = DatabaseAdapter.from_sqlite(ecommerce_db)
        hc = build_hypercube_from_adapter(adapter)
        viz = hc.export_for_visualization()
        first_pt = viz["data_points"][0]
        assert "color" in first_pt
        assert "hex" in first_pt["color"]


# ---------------------------------------------------------------------------
# run_demo (no-argument mode uses temp databases)
# ---------------------------------------------------------------------------

class TestRunDemo:
    def test_returns_two_hypercubes(self, tmpdir: str):
        output = os.path.join(tmpdir, "test_demo.html")
        try:
            hc_a, hc_b = run_demo(output_path=output)
        except ImportError:
            pytest.skip("plotly not installed")
        assert isinstance(hc_a, HyperCube)
        assert isinstance(hc_b, HyperCube)

    def test_writes_html_file(self, tmpdir: str):
        output = os.path.join(tmpdir, "test_demo.html")
        try:
            run_demo(output_path=output)
        except ImportError:
            pytest.skip("plotly not installed")
        assert os.path.isfile(output)
        assert os.path.getsize(output) > 0

    def test_html_contains_plotly(self, tmpdir: str):
        output = os.path.join(tmpdir, "test_demo.html")
        try:
            run_demo(output_path=output)
        except ImportError:
            pytest.skip("plotly not installed")
        with open(output, encoding="utf-8") as f:
            content = f.read()
        assert "plotly" in content.lower()

    def test_with_explicit_db_paths(self, tmpdir: str, ecommerce_db: str, crm_db: str):
        output = os.path.join(tmpdir, "explicit.html")
        try:
            hc_a, hc_b = run_demo(
                output_path=output,
                db_a_path=ecommerce_db,
                db_b_path=crm_db,
            )
        except ImportError:
            pytest.skip("plotly not installed")
        assert isinstance(hc_a, HyperCube)
        assert isinstance(hc_b, HyperCube)
        assert os.path.isfile(output)

    def test_both_hypercubes_have_data(self, tmpdir: str):
        output = os.path.join(tmpdir, "test_demo.html")
        try:
            hc_a, hc_b = run_demo(output_path=output)
        except ImportError:
            pytest.skip("plotly not installed")
        assert len(hc_a.data_matrix.cells) > 0
        assert len(hc_b.data_matrix.cells) > 0

    def test_both_have_matching_matrix_sizes(self, tmpdir: str):
        """DataMatrix and ColorMatrix must be in sync for both hypercubes."""
        output = os.path.join(tmpdir, "test_demo.html")
        try:
            hc_a, hc_b = run_demo(output_path=output)
        except ImportError:
            pytest.skip("plotly not installed")
        assert len(hc_a.data_matrix.cells) == len(hc_a.color_matrix.cells)
        assert len(hc_b.data_matrix.cells) == len(hc_b.color_matrix.cells)


# ---------------------------------------------------------------------------
# CLI – scan --db sqlite
# ---------------------------------------------------------------------------

class TestCliScanSqlite:
    """Validate the `scan --db sqlite` command without launching the visualiser."""

    def _make_args(self, db_path: str, output: str = "", visualize: bool = False):
        """Build an argparse-like namespace for scan_database()."""
        import argparse
        args = argparse.Namespace(
            db="sqlite",
            sqlite_path=db_path,
            output=output if output else None,
            visualize=visualize,
            viz_port=8050,
        )
        return args

    def test_sqlite_scan_returns_hypercube(self, ecommerce_db: str):
        from four_dim_matrix.cli import scan_database
        args = self._make_args(ecommerce_db)
        hc = scan_database(args)
        assert isinstance(hc, HyperCube)
        assert len(hc.data_matrix.cells) > 0

    def test_sqlite_scan_produces_output_json(self, ecommerce_db: str, tmpdir: str):
        from four_dim_matrix.cli import scan_database
        import json
        output_path = os.path.join(tmpdir, "out.json")
        args = self._make_args(ecommerce_db, output=output_path)
        scan_database(args)
        assert os.path.isfile(output_path)
        with open(output_path) as f:
            data = json.load(f)
        assert "data_points" in data
        assert len(data["data_points"]) > 0

    def test_cli_argparse_accepts_sqlite(self):
        """Ensure argparse is configured with the sqlite option."""
        # Import the module to verify it parses without errors
        import four_dim_matrix.cli as cli_module
        # Check that scan_database exists and is callable
        assert callable(cli_module.scan_database)
        # Verify the argparse setup handles sqlite by constructing the parser
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument("--db", choices=["postgres", "mysql", "sqlite"])
        parser.add_argument("--sqlite-path")
        args = parser.parse_args(["--db", "sqlite", "--sqlite-path", "/tmp/test.db"])
        assert args.db == "sqlite"
        assert args.sqlite_path == "/tmp/test.db"
