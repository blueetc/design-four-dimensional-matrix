"""Tests for the multi-stream matrix comparator (StreamComparator).

Tests cover:
1. StreamCursor – movement, lifecycle names, x coordinates
2. TableChange – property change detection (domain/lifecycle/volume)
3. StreamDiff – summary, to_dict, convenience views
4. StreamComparator – add/remove streams, at_cursor, sync_filter, diff, scan, summary
5. CLI `compare` subcommand – end-to-end with saved JSON files
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import List

import pytest

from four_dim_matrix.data_matrix import DataCell
from four_dim_matrix.hypercube import HyperCube
from four_dim_matrix.stream_comparator import (
    LIFECYCLE_ORDER,
    LIFECYCLE_X,
    StreamComparator,
    StreamCursor,
    StreamDiff,
    TableChange,
)


# ============================================================
# Fixtures / helpers
# ============================================================

_T0 = datetime(2024, 1, 1, 12, 0, 0)
_T1 = datetime(2024, 6, 1, 12, 0, 0)


def _cell(
    table: str,
    domain: str = "user",
    lifecycle: str = "mature",
    rows: int = 1000,
    t: datetime = _T0,
) -> DataCell:
    """Build a minimal DataCell for testing."""
    x = LIFECYCLE_X.get(lifecycle, 80)
    return DataCell(
        t=t,
        x=x,
        y=float(rows),
        z=0,
        table_name=table,
        schema_name="test",
        row_count=rows,
        business_domain=domain,
        lifecycle_stage=lifecycle,
    )


def _hc(cells: List[DataCell]) -> HyperCube:
    """Build a HyperCube from a list of DataCells."""
    hc = HyperCube()
    for c in cells:
        hc.add_cell(c, compute_color=True)
    hc.sync_color_matrix()
    return hc


def _v1() -> HyperCube:
    """Version 1 snapshot — used as 'before'."""
    return _hc([
        _cell("users",     domain="user",       lifecycle="mature",  rows=5000),
        _cell("orders",    domain="revenue",    lifecycle="mature",  rows=20000),
        _cell("products",  domain="product",    lifecycle="mature",  rows=300),
        _cell("audit_log", domain="operations", lifecycle="legacy",  rows=100),
    ])


def _v2() -> HyperCube:
    """Version 2 snapshot — used as 'after'.

    Changes relative to v1:
      - 'payments' added
      - 'audit_log' removed
      - 'users' domain changed: user → auth
      - 'orders' lifecycle changed: mature → growth
      - 'products' row_count increased 300 → 1500 (+400% → volume_changed)
    """
    return _hc([
        _cell("users",    domain="auth",     lifecycle="mature",  rows=5100),
        _cell("orders",   domain="revenue",  lifecycle="growth",  rows=21000),
        _cell("products", domain="product",  lifecycle="mature",  rows=1500),
        _cell("payments", domain="revenue",  lifecycle="new",     rows=50),
    ])


# ============================================================
# StreamCursor
# ============================================================

class TestStreamCursor:
    def test_default_lifecycle(self):
        cursor = StreamCursor()
        assert cursor.lifecycle == "mature"

    def test_default_x(self):
        cursor = StreamCursor()
        assert cursor.x == LIFECYCLE_X["mature"]

    def test_move_to_lifecycle(self):
        cursor = StreamCursor()
        cursor.move_to_lifecycle("new")
        assert cursor.lifecycle == "new"
        assert cursor.x == LIFECYCLE_X["new"]

    def test_move_to_unknown_lifecycle_no_crash(self):
        cursor = StreamCursor()
        cursor.move_to_lifecycle("nonexistent")
        assert cursor.lifecycle == "mature"  # unchanged

    def test_advance_from_new(self):
        cursor = StreamCursor("new")
        cursor.advance()
        assert cursor.lifecycle == "growth"

    def test_advance_at_end_stays(self):
        cursor = StreamCursor("deprecated")
        cursor.advance()
        assert cursor.lifecycle == "deprecated"

    def test_rewind_from_legacy(self):
        cursor = StreamCursor("legacy")
        cursor.rewind()
        assert cursor.lifecycle == "mature"

    def test_rewind_at_start_stays(self):
        cursor = StreamCursor("new")
        cursor.rewind()
        assert cursor.lifecycle == "new"

    def test_reset(self):
        cursor = StreamCursor("legacy")
        cursor.reset()
        assert cursor.lifecycle == LIFECYCLE_ORDER[0]

    def test_index(self):
        cursor = StreamCursor("mature")
        assert cursor.index == LIFECYCLE_ORDER.index("mature")

    def test_chaining(self):
        cursor = StreamCursor("new")
        result = cursor.advance().advance()
        assert result is cursor  # same object
        assert cursor.lifecycle == "mature"

    def test_frames_yields_all_from_current(self):
        cursor = StreamCursor("growth")
        frames = list(cursor.frames())
        expected = LIFECYCLE_ORDER[LIFECYCLE_ORDER.index("growth"):]
        assert frames == expected

    def test_frames_updates_cursor_state(self):
        cursor = StreamCursor("new")
        stages = list(cursor.frames())
        assert cursor.lifecycle == stages[-1]

    def test_constructor_with_unknown_name(self):
        cursor = StreamCursor("invalid_stage")
        # Should default to first in LIFECYCLE_ORDER
        assert cursor.lifecycle == LIFECYCLE_ORDER[0]


# ============================================================
# TableChange
# ============================================================

class TestTableChange:
    def test_domain_changed_true(self):
        ch = TableChange("t", old_domain="user", new_domain="auth")
        assert ch.domain_changed is True

    def test_domain_changed_false_same(self):
        ch = TableChange("t", old_domain="user", new_domain="user")
        assert ch.domain_changed is False

    def test_domain_changed_false_empty(self):
        ch = TableChange("t", old_domain="user", new_domain="")
        assert ch.domain_changed is False

    def test_lifecycle_changed_true(self):
        ch = TableChange("t", old_lifecycle="mature", new_lifecycle="growth")
        assert ch.lifecycle_changed is True

    def test_lifecycle_changed_false_same(self):
        ch = TableChange("t", old_lifecycle="mature", new_lifecycle="mature")
        assert ch.lifecycle_changed is False

    def test_volume_changed_true_above_threshold(self):
        ch = TableChange("t", old_row_count=1000, new_row_count=1300)  # +30%
        assert ch.volume_changed is True

    def test_volume_changed_false_below_threshold(self):
        ch = TableChange("t", old_row_count=1000, new_row_count=1100)  # +10%
        assert ch.volume_changed is False

    def test_volume_changed_zero_to_nonzero(self):
        ch = TableChange("t", old_row_count=0, new_row_count=1)
        assert ch.volume_changed is True

    def test_volume_changed_both_zero(self):
        ch = TableChange("t", old_row_count=0, new_row_count=0)
        assert ch.volume_changed is False

    def test_to_dict_structure(self):
        ch = TableChange(
            "orders",
            old_domain="revenue", new_domain="finance",
            old_lifecycle="mature", new_lifecycle="growth",
            old_row_count=1000, new_row_count=2000,
        )
        d = ch.to_dict()
        assert d["table_name"] == "orders"
        assert d["domain"]["changed"] is True
        assert d["lifecycle"]["changed"] is True
        assert d["row_count"]["changed"] is True


# ============================================================
# StreamDiff
# ============================================================

class TestStreamDiff:
    def _diff(self) -> StreamDiff:
        return StreamDiff(
            stream_a="before",
            stream_b="after",
            added=["payments"],
            removed=["audit_log"],
            changes=[
                TableChange("users",    old_domain="user",    new_domain="auth"),
                TableChange("orders",   old_lifecycle="mature", new_lifecycle="growth"),
                TableChange("products", old_row_count=300, new_row_count=1500),
            ],
        )

    def test_has_differences_true(self):
        assert self._diff().has_differences is True

    def test_has_differences_false(self):
        diff = StreamDiff("a", "b")
        assert diff.has_differences is False

    def test_domain_changes(self):
        changes = self._diff().domain_changes
        assert len(changes) == 1
        assert changes[0].table_name == "users"

    def test_lifecycle_changes(self):
        changes = self._diff().lifecycle_changes
        assert len(changes) == 1
        assert changes[0].table_name == "orders"

    def test_volume_changes(self):
        changes = self._diff().volume_changes
        assert len(changes) == 1
        assert changes[0].table_name == "products"

    def test_summary_contains_stream_names(self):
        s = self._diff().summary()
        assert "before" in s
        assert "after" in s

    def test_summary_added(self):
        s = self._diff().summary()
        assert "payments" in s

    def test_summary_removed(self):
        s = self._diff().summary()
        assert "audit_log" in s

    def test_summary_no_difference(self):
        diff = StreamDiff("a", "b")
        s = diff.summary()
        assert "一致" in s or "no" in s.lower() or "0" in s

    def test_to_dict_counts(self):
        d = self._diff().to_dict()
        assert d["summary"]["added_count"] == 1
        assert d["summary"]["removed_count"] == 1
        assert d["summary"]["domain_changes"] == 1
        assert d["summary"]["lifecycle_changes"] == 1

    def test_to_dict_added_sorted(self):
        diff = StreamDiff("a", "b", added=["z_table", "a_table"])
        assert diff.to_dict()["added"] == ["a_table", "z_table"]

    def test_to_dict_streams(self):
        d = self._diff().to_dict()
        assert d["streams"]["a"] == "before"
        assert d["streams"]["b"] == "after"


# ============================================================
# StreamComparator
# ============================================================

class TestStreamComparator:
    def test_add_stream_returns_self(self):
        cmp = StreamComparator()
        result = cmp.add_stream("v1", _v1())
        assert result is cmp

    def test_stream_names(self):
        cmp = StreamComparator()
        cmp.add_stream("alpha", _v1())
        cmp.add_stream("beta", _v2())
        assert cmp.stream_names == ["alpha", "beta"]

    def test_len(self):
        cmp = StreamComparator()
        assert len(cmp) == 0
        cmp.add_stream("a", _v1())
        assert len(cmp) == 1

    def test_remove_stream(self):
        cmp = StreamComparator()
        cmp.add_stream("a", _v1())
        cmp.remove_stream("a")
        assert len(cmp) == 0

    def test_remove_nonexistent_no_crash(self):
        cmp = StreamComparator()
        cmp.remove_stream("nope")  # should not raise

    def test_at_cursor_returns_correct_lifecycle(self):
        cmp = StreamComparator()
        cmp.add_stream("v1", _v1())
        cmp.cursor.move_to_lifecycle("mature")
        view = cmp.at_cursor()
        cells = view["v1"]
        assert all(c.lifecycle_stage == "mature" for c in cells)
        assert len(cells) > 0

    def test_at_cursor_legacy(self):
        cmp = StreamComparator()
        cmp.add_stream("v1", _v1())
        view = cmp.at_cursor(lifecycle="legacy")
        cells = view["v1"]
        assert all(c.lifecycle_stage == "legacy" for c in cells)

    def test_at_cursor_empty_stage(self):
        cmp = StreamComparator()
        cmp.add_stream("v1", _v1())
        view = cmp.at_cursor(lifecycle="deprecated")
        assert view["v1"] == []

    def test_at_cursor_overrides_not_mutate_cursor(self):
        cmp = StreamComparator()
        cmp.add_stream("v1", _v1())
        cmp.cursor.move_to_lifecycle("mature")
        cmp.at_cursor(lifecycle="legacy")  # temporary override
        assert cmp.cursor.lifecycle == "mature"  # cursor not changed

    def test_sync_filter_by_domain(self):
        cmp = StreamComparator()
        cmp.add_stream("v1", _v1())
        cmp.add_stream("v2", _v2())
        view = cmp.sync_filter(domain="revenue")
        for cells in view.values():
            assert all(c.business_domain.lower() == "revenue" for c in cells)

    def test_sync_filter_by_lifecycle(self):
        cmp = StreamComparator()
        cmp.add_stream("v1", _v1())
        view = cmp.sync_filter(lifecycle="mature")
        for cells in view.values():
            assert all(c.lifecycle_stage == "mature" for c in cells)

    def test_sync_filter_both(self):
        cmp = StreamComparator()
        cmp.add_stream("v1", _v1())
        view = cmp.sync_filter(domain="user", lifecycle="mature")
        for cells in view.values():
            assert all(
                c.business_domain == "user" and c.lifecycle_stage == "mature"
                for c in cells
            )

    def test_sync_filter_case_insensitive(self):
        cmp = StreamComparator()
        cmp.add_stream("v1", _v1())
        view_lower = cmp.sync_filter(domain="user")
        view_upper = cmp.sync_filter(domain="USER")
        assert len(view_lower["v1"]) == len(view_upper["v1"])

    def test_diff_added(self):
        cmp = StreamComparator()
        cmp.add_stream("v1", _v1())
        cmp.add_stream("v2", _v2())
        diff = cmp.diff("v1", "v2")
        assert "payments" in diff.added

    def test_diff_removed(self):
        cmp = StreamComparator()
        cmp.add_stream("v1", _v1())
        cmp.add_stream("v2", _v2())
        diff = cmp.diff("v1", "v2")
        assert "audit_log" in diff.removed

    def test_diff_domain_change(self):
        cmp = StreamComparator()
        cmp.add_stream("v1", _v1())
        cmp.add_stream("v2", _v2())
        diff = cmp.diff("v1", "v2")
        domain_changed_tables = {c.table_name for c in diff.domain_changes}
        assert "users" in domain_changed_tables

    def test_diff_lifecycle_change(self):
        cmp = StreamComparator()
        cmp.add_stream("v1", _v1())
        cmp.add_stream("v2", _v2())
        diff = cmp.diff("v1", "v2")
        lc_changed_tables = {c.table_name for c in diff.lifecycle_changes}
        assert "orders" in lc_changed_tables

    def test_diff_volume_change(self):
        cmp = StreamComparator()
        cmp.add_stream("v1", _v1())
        cmp.add_stream("v2", _v2())
        diff = cmp.diff("v1", "v2")
        vol_changed_tables = {c.table_name for c in diff.volume_changes}
        assert "products" in vol_changed_tables

    def test_diff_identical_streams(self):
        hc = _v1()
        cmp = StreamComparator()
        cmp.add_stream("a", hc)
        cmp.add_stream("b", hc)
        diff = cmp.diff("a", "b")
        assert not diff.has_differences

    def test_diff_unknown_stream_raises(self):
        cmp = StreamComparator()
        cmp.add_stream("v1", _v1())
        with pytest.raises(KeyError):
            cmp.diff("v1", "nonexistent")

    def test_diff_stream_names_preserved(self):
        cmp = StreamComparator()
        cmp.add_stream("before", _v1())
        cmp.add_stream("after", _v2())
        diff = cmp.diff("before", "after")
        assert diff.stream_a == "before"
        assert diff.stream_b == "after"

    def test_scan_yields_all_from_cursor(self):
        cmp = StreamComparator()
        cmp.add_stream("v1", _v1())
        cmp.cursor.reset()  # start from "new"
        frames = list(cmp.scan())
        assert len(frames) == len(LIFECYCLE_ORDER)

    def test_scan_frame_structure(self):
        cmp = StreamComparator()
        cmp.add_stream("v1", _v1())
        cmp.cursor.reset()
        frame = next(cmp.scan())
        assert "cursor" in frame
        assert "x" in frame
        assert "streams" in frame
        assert "counts" in frame
        assert "v1" in frame["streams"]

    def test_scan_counts_match_streams(self):
        cmp = StreamComparator()
        cmp.add_stream("v1", _v1())
        cmp.cursor.reset()
        for frame in cmp.scan():
            assert frame["counts"]["v1"] == len(frame["streams"]["v1"])

    def test_scan_with_start_override(self):
        cmp = StreamComparator()
        cmp.add_stream("v1", _v1())
        frames = list(cmp.scan(start="legacy"))
        stages = [f["cursor"] for f in frames]
        expected = LIFECYCLE_ORDER[LIFECYCLE_ORDER.index("legacy"):]
        assert stages == expected

    def test_summary_contains_stream_names(self):
        cmp = StreamComparator()
        cmp.add_stream("before", _v1())
        cmp.add_stream("after", _v2())
        s = cmp.summary()
        assert "before" in s
        assert "after" in s

    def test_summary_empty(self):
        cmp = StreamComparator()
        s = cmp.summary()
        assert "没有" in s or "0" in s or "empty" in s.lower()

    def test_chained_add_streams(self):
        cmp = (
            StreamComparator()
            .add_stream("a", _v1())
            .add_stream("b", _v2())
        )
        assert len(cmp) == 2


# ============================================================
# CLI compare subcommand (end-to-end)
# ============================================================

class TestCLICompare:
    def _save_hc(self, hc: HyperCube, path: Path) -> None:
        data = hc.export_for_visualization()
        path.write_text(json.dumps(data, default=str), encoding="utf-8")

    def test_compare_prints_diff(self, tmp_path: Path, capsys):
        from four_dim_matrix.cli import compare_streams

        path_a = tmp_path / "v1.json"
        path_b = tmp_path / "v2.json"
        self._save_hc(_v1(), path_a)
        self._save_hc(_v2(), path_b)

        class _Args:
            input_a = str(path_a)
            input_b = str(path_b)
            label_a = "version1"
            label_b = "version2"
            output = None

        compare_streams(_Args())
        captured = capsys.readouterr()
        assert "version1" in captured.out
        assert "version2" in captured.out
        # diff report should mention the added/removed tables
        assert "payments" in captured.out
        assert "audit_log" in captured.out

    def test_compare_saves_json(self, tmp_path: Path, capsys):
        from four_dim_matrix.cli import compare_streams

        path_a = tmp_path / "v1.json"
        path_b = tmp_path / "v2.json"
        out_path = tmp_path / "diff.json"
        self._save_hc(_v1(), path_a)
        self._save_hc(_v2(), path_b)

        class _Args:
            input_a = str(path_a)
            input_b = str(path_b)
            label_a = ""
            label_b = ""
            output = str(out_path)

        compare_streams(_Args())
        assert out_path.exists()
        d = json.loads(out_path.read_text())
        assert "streams" in d
        assert "added" in d
        assert "removed" in d

    def test_compare_scan_shows_all_lifecycle_stages(self, tmp_path: Path, capsys):
        from four_dim_matrix.cli import compare_streams

        path_a = tmp_path / "v1.json"
        path_b = tmp_path / "v2.json"
        self._save_hc(_v1(), path_a)
        self._save_hc(_v2(), path_b)

        class _Args:
            input_a = str(path_a)
            input_b = str(path_b)
            label_a = "A"
            label_b = "B"
            output = None

        compare_streams(_Args())
        captured = capsys.readouterr()
        # All lifecycle stages should appear in the scan section
        for stage in ["new", "mature", "legacy"]:
            assert stage in captured.out
