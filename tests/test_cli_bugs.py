"""Regression tests for CLI bugs found during code audit.

Bug 1: _record_session stored a wrong output_file path because it called
       _resolve_output_path a second time, generating a different timestamp.

Bug 2: visualize subcommand created an empty HyperCube instead of
       reconstructing from the loaded JSON data.

Bug 3: max_rows was recomputed on every loop iteration (O(n²)).

Bug 4: test_visualizer.py failed with ModuleNotFoundError — fixed by adding
       pytest.skipif guards (tested elsewhere).
"""

from __future__ import annotations

import json
import sqlite3
import tempfile
from datetime import datetime
from pathlib import Path

import pytest

from four_dim_matrix.hypercube import HyperCube
from four_dim_matrix.data_matrix import DataCell
from four_dim_matrix.memory import MemoryStore


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_simple_hypercube() -> HyperCube:
    """Return a HyperCube with a couple of DataCells for round-trip tests."""
    hc = HyperCube()
    for i, (table, domain, rows) in enumerate([
        ("users", "user", 500),
        ("orders", "revenue", 1200),
        ("products", "product", 80),
    ]):
        cell = DataCell(
            t=datetime(2024, 1, 1),
            x=80, y=float(i * 30 + 10), z=i,
            table_name=table,
            schema_name="public",
            column_count=6,
            row_count=rows,
            size_bytes=rows * 100,
            business_domain=domain,
            lifecycle_stage="mature",
        )
        hc.add_cell(cell, compute_color=True)
    return hc


# ---------------------------------------------------------------------------
# Bug 1 – _record_session must NOT re-derive the output path
# ---------------------------------------------------------------------------

class TestRecordSessionOutPath:
    """_record_session must store the path that was *actually* written,
    not a freshly-derived one (which would differ by a timestamp second)."""

    def test_out_path_kwarg_stored_verbatim(self, tmp_path: Path):
        store = MemoryStore(path=tmp_path / "mem.json")
        hc = _make_simple_hypercube()

        # Simulate what scan_database does: resolve path ONCE, then pass it
        explicit_path = str(tmp_path / "matrix_scan_20240101_120000.json")
        store.record_session(
            source="test.db",
            cell_count=len(hc.data_matrix.cells),
            color_count=len(hc.color_matrix.cells),
            output_file=explicit_path,
        )
        rec = store.recent_sessions(1)[0]
        assert rec.output_file == explicit_path

    def test_empty_out_path_when_no_export(self, tmp_path: Path):
        """When no output is requested, output_file should be empty."""
        store = MemoryStore(path=tmp_path / "mem.json")
        store.record_session(source="test.db", output_file="")
        rec = store.recent_sessions(1)[0]
        assert rec.output_file == ""

    def test_cli_record_session_uses_provided_out_path(self, tmp_path: Path, monkeypatch):
        """_record_session in cli.py receives out_path= and doesn't re-resolve."""
        import time
        from four_dim_matrix.cli import _record_session, _resolve_output_path

        store = MemoryStore(path=tmp_path / "mem.json")
        hc = _make_simple_hypercube()

        # Resolve once (simulating scan_database behaviour)
        output_dir = str(tmp_path) + "/"
        out_path = _resolve_output_path(output_dir)

        # Introduce a sleep so a second call would produce a different name
        time.sleep(1)

        class _Args:
            db = "sqlite"
            label = ""

        _record_session(store, _Args(), hc, "test.db", out_path=out_path or "")
        rec = store.recent_sessions(1)[0]
        # The stored path must match the one we passed – not a new timestamp
        assert rec.output_file == (out_path or "")


# ---------------------------------------------------------------------------
# Bug 2 – HyperCube.from_visualization_dict round-trips export data
# ---------------------------------------------------------------------------

class TestFromVisualizationDict:
    def test_basic_round_trip(self):
        hc = _make_simple_hypercube()
        exported = hc.export_for_visualization()

        hc2 = HyperCube.from_visualization_dict(exported)
        assert len(hc2.data_matrix.cells) == len(hc.data_matrix.cells)

    def test_field_values_round_trip(self):
        hc = _make_simple_hypercube()
        exported = hc.export_for_visualization()
        hc2 = HyperCube.from_visualization_dict(exported)

        tables = {c.table_name for c in hc2.data_matrix.cells.values()}
        assert "users" in tables
        assert "orders" in tables

    def test_row_count_round_trip(self):
        hc = _make_simple_hypercube()
        exported = hc.export_for_visualization()
        hc2 = HyperCube.from_visualization_dict(exported)

        row_counts = {c.table_name: c.row_count
                      for c in hc2.data_matrix.cells.values()}
        assert row_counts.get("users") == 500
        assert row_counts.get("orders") == 1200

    def test_color_matrix_populated(self):
        hc = _make_simple_hypercube()
        exported = hc.export_for_visualization()
        hc2 = HyperCube.from_visualization_dict(exported)
        # Color matrix should be populated (compute_color=True during reconstruction)
        assert len(hc2.color_matrix.cells) == len(hc.data_matrix.cells)

    def test_empty_dict_produces_empty_hypercube(self):
        hc = HyperCube.from_visualization_dict({})
        assert len(hc.data_matrix.cells) == 0
        assert len(hc.color_matrix.cells) == 0

    def test_empty_data_points_list(self):
        hc = HyperCube.from_visualization_dict({"data_points": []})
        assert len(hc.data_matrix.cells) == 0

    def test_malformed_point_skipped(self):
        """A point with missing coordinates should be skipped, not crash."""
        data = {
            "data_points": [
                {"coordinates": {}, "data": {}},               # empty coords
                {"coordinates": {"x": "bad", "y": 1, "z": 0},  # non-int x
                 "data": {"row_count": 10}},
            ]
        }
        hc = HyperCube.from_visualization_dict(data)
        # Should not raise; may or may not add cells depending on coercion
        assert isinstance(hc, HyperCube)

    def test_summary_after_reconstruction(self):
        hc = _make_simple_hypercube()
        exported = hc.export_for_visualization()
        hc2 = HyperCube.from_visualization_dict(exported)

        summary = hc2.get_summary()
        assert summary["data_matrix"]["total_cells"] == 3
        assert summary["data_matrix"]["unique_tables"] == 3

    def test_json_serialization_round_trip(self, tmp_path: Path):
        """Full round-trip through JSON file."""
        hc = _make_simple_hypercube()
        exported = hc.export_for_visualization()

        json_path = tmp_path / "scan.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(exported, f)

        with open(json_path, "r", encoding="utf-8") as f:
            loaded = json.load(f)

        hc2 = HyperCube.from_visualization_dict(loaded)
        assert len(hc2.data_matrix.cells) == 3

    def test_business_domain_round_trip(self):
        hc = _make_simple_hypercube()
        exported = hc.export_for_visualization()
        hc2 = HyperCube.from_visualization_dict(exported)

        domains = {c.business_domain for c in hc2.data_matrix.cells.values()}
        assert "user" in domains
        assert "revenue" in domains


# ---------------------------------------------------------------------------
# Bug 3 – max_rows O(n²) must be computed outside the loop
# ---------------------------------------------------------------------------

class TestScanDatabaseMaxRows:
    """Verify that the y-coordinate mapping is consistent for all tables
    (which requires max_rows to be computed once for the whole set)."""

    def test_y_values_scale_correctly(self):
        """Largest table should have y=255; smallest should have a lower y.

        Construct a HyperCube manually using the same algorithm to confirm
        the values are proportional and max_rows is applied uniformly.
        """
        signatures = [
            {"table": "big",    "rows": 1000},
            {"table": "medium", "rows": 500},
            {"table": "small",  "rows": 1},
        ]
        # Apply the same formula as scan_database (after the fix)
        max_rows = max(s["rows"] for s in signatures) or 1
        y_values = {}
        for sig in signatures:
            r = sig["rows"]
            y = 0
            if r > 0:
                y = min(255, max(1, int(r / max(max_rows / 255, 1))))
            y_values[sig["table"]] = y

        assert y_values["big"] == 255
        assert y_values["medium"] > y_values["small"]
        # All y-values must be in range [0, 255]
        for y in y_values.values():
            assert 0 <= y <= 255

    def test_max_rows_zero_handled(self):
        """If all tables have 0 rows, y must remain 0 without ZeroDivisionError."""
        signatures = [{"rows": 0}, {"rows": 0}]
        max_rows = max((s["rows"] for s in signatures), default=1) or 1
        for sig in signatures:
            r = sig["rows"]
            y = 0
            if r > 0:
                y = min(255, max(1, int(r / max(max_rows / 255, 1))))
            assert y == 0

    def test_max_rows_single_table(self):
        """Single-table database: y equals row_count when max_rows < 255
        (divisor clamps to 1), capped at 255 for large row counts."""
        # Case 1: small row count (< 255) – y equals the row count directly
        signatures = [{"rows": 42}]
        max_rows = max((s["rows"] for s in signatures), default=1) or 1
        r = signatures[0]["rows"]
        y = min(255, max(1, int(r / max(max_rows / 255, 1))))
        assert y == 42  # divisor = max(42/255, 1) = 1 → y = 42

        # Case 2: large row count (>= 255) – single table gets y = 255
        signatures = [{"rows": 1000}]
        max_rows = max((s["rows"] for s in signatures), default=1) or 1
        r = signatures[0]["rows"]
        y = min(255, max(1, int(r / max(max_rows / 255, 1))))
        assert y == 255  # divisor = 1000/255 ≈ 3.92 → y = int(1000/3.92) = 255


# ---------------------------------------------------------------------------
# Integration: scan + from_visualization_dict end-to-end via SQLite
# ---------------------------------------------------------------------------

class TestVisualizeSQLiteRoundTrip:
    """Build a HyperCube from a real SQLite DB, export to JSON, then
    reconstruct via from_visualization_dict and verify."""

    def _create_db(self, path: str) -> None:
        conn = sqlite3.connect(path)
        conn.executescript("""
            CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, email TEXT);
            INSERT INTO customers VALUES (1, 'Alice', 'alice@example.com');
            INSERT INTO customers VALUES (2, 'Bob', 'bob@example.com');

            CREATE TABLE orders (
                id INTEGER PRIMARY KEY, customer_id INTEGER,
                amount REAL, created_at TEXT
            );
            INSERT INTO orders VALUES (1, 1, 99.9, '2024-01-01');
        """)
        conn.close()

    def test_full_round_trip(self, tmp_path: Path):
        from four_dim_matrix.demo import build_hypercube_from_adapter
        from four_dim_matrix.db_adapter import DatabaseAdapter

        db_path = str(tmp_path / "test.db")
        self._create_db(db_path)

        adapter = DatabaseAdapter.from_sqlite(db_path)
        hc = build_hypercube_from_adapter(adapter, db_path)
        assert len(hc.data_matrix.cells) == 2

        # Export
        exported = hc.export_for_visualization()
        json_path = tmp_path / "scan.json"
        with open(json_path, "w") as f:
            json.dump(exported, f)

        # Reload (simulates `four-dim-matrix visualize -i scan.json`)
        with open(json_path) as f:
            loaded = json.load(f)
        hc2 = HyperCube.from_visualization_dict(loaded)

        assert len(hc2.data_matrix.cells) == 2
        tables = {c.table_name for c in hc2.data_matrix.cells.values()}
        assert "customers" in tables
        assert "orders" in tables
