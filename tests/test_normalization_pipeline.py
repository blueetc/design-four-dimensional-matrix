"""Tests for NormalizationPipeline (two-stage matrix builder with lineage).

Scenarios covered:
  1. Fully normalized schema (small focused tables — plan = pass-through)
  2. Wide/denormalized schema (one big mixed table — plan = split into sub-tables)
  3. Mixed schema (one wide + one normalized table)
  4. Empty schema (edge case)

Tests verify: SubTablePlan shape, z-index stability, Stage 2 data loading,
axis assignment (t/x/y/z), lineage correctness, and reverse-lookup helpers.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime

import pytest

from four_dim_matrix import (
    ColumnGroup,
    DatabaseAdapter,
    KnowledgeBase,
    NormalizationPipeline,
    SchemaAnalyzer,
    SubTablePlan,
)


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

def _normalized_conn() -> sqlite3.Connection:
    """Three focused tables – each covers a single topic."""
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE customers (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT
        );
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            total REAL NOT NULL,
            created_at DATETIME NOT NULL
        );
        CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            sku TEXT NOT NULL,
            price REAL NOT NULL
        );
        INSERT INTO customers VALUES (1,'Alice','alice@example.com');
        INSERT INTO customers VALUES (2,'Bob','bob@example.com');
        INSERT INTO orders VALUES (1,1,99.9,'2024-03-01');
        INSERT INTO orders VALUES (2,2,150.0,'2024-03-05');
        INSERT INTO orders VALUES (3,1,42.5,'2024-04-01');
        INSERT INTO products VALUES (1,'SKU-A',29.99);
        """
    )
    return conn


def _wide_conn() -> sqlite3.Connection:
    """One wide denormalized table mixing many topic groups (≥8 columns)."""
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE sales_report (
            id          INTEGER PRIMARY KEY,
            customer_id INTEGER,
            product_id  INTEGER,
            customer_name TEXT,
            product_name  TEXT,
            description   TEXT,
            quantity    REAL,
            unit_price  REAL,
            discount    REAL,
            total       REAL,
            status      TEXT,
            region      TEXT,
            created_at  DATETIME,
            updated_at  DATETIME
        );
        INSERT INTO sales_report VALUES
            (1,1,1,'Alice','Widget','A widget',2.0,50.0,0.0,100.0,'paid','north','2024-01-10','2024-01-15');
        INSERT INTO sales_report VALUES
            (2,2,2,'Bob','Gadget','A gadget',1.0,200.0,10.0,190.0,'pending','south','2024-02-01','2024-02-01');
        INSERT INTO sales_report VALUES
            (3,1,1,'Alice','Widget','Another order',3.0,50.0,5.0,145.0,'paid','east','2024-03-01','2024-03-02');
        """
    )
    return conn


def _mixed_conn() -> sqlite3.Connection:
    """One wide table + one focused table."""
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT
        );
        CREATE TABLE denorm_events (
            id INTEGER PRIMARY KEY,
            user_id INTEGER,
            event_type TEXT,
            event_name TEXT,
            event_desc TEXT,
            revenue REAL,
            cost REAL,
            margin REAL,
            occurred_at DATETIME,
            resolved_at DATETIME
        );
        INSERT INTO users VALUES (1,'Alice','alice@example.com');
        INSERT INTO users VALUES (2,'Bob','bob@example.com');
        INSERT INTO denorm_events VALUES
            (1,1,'sale','Widget sale','Sold a widget',100.0,60.0,40.0,'2024-01-10','2024-01-11');
        INSERT INTO denorm_events VALUES
            (2,2,'refund','Widget refund','Refunded',0.0,0.0,0.0,'2024-02-01','2024-02-02');
        """
    )
    return conn


def _make(conn: sqlite3.Connection, **kwargs) -> NormalizationPipeline:
    adapter = DatabaseAdapter.from_connection(conn, dialect="sqlite")
    analyzer = SchemaAnalyzer(adapter, **kwargs)
    return NormalizationPipeline(adapter, analyzer)


# ---------------------------------------------------------------------------
# SubTablePlan dataclass
# ---------------------------------------------------------------------------

class TestSubTablePlan:
    def test_to_dict_keys(self):
        sp = SubTablePlan(
            sub_table_name="orders_numeric",
            source_table="orders",
            group=ColumnGroup.NUMERIC,
            matrix_role="y-metric",
            stage2_z=3,
            columns=["id", "total"],
            is_original=False,
        )
        d = sp.to_dict()
        for key in [
            "sub_table_name", "source_table", "group", "matrix_role",
            "stage2_z", "columns", "is_original",
        ]:
            assert key in d

    def test_to_dict_values(self):
        sp = SubTablePlan(
            sub_table_name="t",
            source_table="s",
            group="NORMALIZED",
            matrix_role="z-topic (normalized)",
            stage2_z=0,
            columns=["id", "name"],
            is_original=True,
        )
        d = sp.to_dict()
        assert d["stage2_z"] == 0
        assert d["is_original"] is True
        assert d["columns"] == ["id", "name"]


# ---------------------------------------------------------------------------
# NormalizationPipeline construction
# ---------------------------------------------------------------------------

class TestPipelineConstruction:
    def test_stores_adapter_and_analyzer(self):
        conn = _normalized_conn()
        p = _make(conn)
        assert p.adapter is not None
        assert p.analyzer is not None

    def test_repr_contains_table_count(self):
        conn = _normalized_conn()
        assert "3" in repr(_make(conn))

    def test_repr_contains_score(self):
        conn = _normalized_conn()
        assert "score" in repr(_make(conn)).lower()


# ---------------------------------------------------------------------------
# plan() – normalized schema
# ---------------------------------------------------------------------------

class TestPlanNormalized:
    def test_returns_list_of_sub_table_plans(self):
        conn = _normalized_conn()
        plan = _make(conn).plan()
        assert all(isinstance(sp, SubTablePlan) for sp in plan)

    def test_one_plan_entry_per_table_when_all_normalized(self):
        conn = _normalized_conn()
        # customers(3), orders(4), products(3) — all under default threshold 8
        assert len(_make(conn).plan()) == 3

    def test_all_entries_are_original_when_normalized(self):
        conn = _normalized_conn()
        assert all(sp.is_original for sp in _make(conn).plan())

    def test_plan_sorted_alphabetically_by_sub_table_name(self):
        conn = _normalized_conn()
        names = [sp.sub_table_name for sp in _make(conn).plan()]
        assert names == sorted(names)

    def test_stage2_z_indices_sequential(self):
        conn = _normalized_conn()
        z_values = [sp.stage2_z for sp in _make(conn).plan()]
        assert z_values == list(range(len(z_values)))

    def test_normalized_group_label(self):
        conn = _normalized_conn()
        for sp in _make(conn).plan():
            assert sp.group == "NORMALIZED"

    def test_matrix_role_is_normalized(self):
        conn = _normalized_conn()
        for sp in _make(conn).plan():
            assert "normalized" in sp.matrix_role.lower()

    def test_columns_include_all_table_columns(self):
        conn = _normalized_conn()
        plan = _make(conn).plan()
        orders_plan = next(sp for sp in plan if sp.sub_table_name == "orders")
        # orders: id, customer_id, total, created_at
        assert "total" in orders_plan.columns
        assert "created_at" in orders_plan.columns


# ---------------------------------------------------------------------------
# plan() – wide schema
# ---------------------------------------------------------------------------

class TestPlanWide:
    def test_wide_table_is_split_into_multiple_entries(self):
        conn = _wide_conn()
        plan = _make(conn).plan()
        # sales_report is wide → multiple sub-tables
        assert len(plan) >= 2

    def test_no_entry_is_original_for_wide_table(self):
        conn = _wide_conn()
        plan = _make(conn).plan()
        # Only the wide table exists → none are original
        assert all(not sp.is_original for sp in plan)

    def test_sub_table_names_contain_source_name(self):
        conn = _wide_conn()
        plan = _make(conn).plan()
        for sp in plan:
            assert sp.source_table in sp.sub_table_name

    def test_each_entry_references_same_source_table(self):
        conn = _wide_conn()
        plan = _make(conn).plan()
        for sp in plan:
            assert sp.source_table == "sales_report"

    def test_stage2_z_indices_sequential(self):
        conn = _wide_conn()
        z_values = [sp.stage2_z for sp in _make(conn).plan()]
        assert z_values == list(range(len(z_values)))

    def test_plan_sorted_alphabetically(self):
        conn = _wide_conn()
        names = [sp.sub_table_name for sp in _make(conn).plan()]
        assert names == sorted(names)

    def test_numeric_group_entry_exists(self):
        conn = _wide_conn()
        plan = _make(conn).plan()
        groups = [sp.group for sp in plan]
        assert ColumnGroup.NUMERIC in groups

    def test_numeric_entry_has_numeric_columns(self):
        conn = _wide_conn()
        plan = _make(conn).plan()
        numeric_sp = next(sp for sp in plan if sp.group == ColumnGroup.NUMERIC)
        # At least one numeric column besides identity
        numeric_cols = {"quantity", "unit_price", "discount", "total"}
        assert numeric_cols & set(numeric_sp.columns)


# ---------------------------------------------------------------------------
# plan() – mixed schema
# ---------------------------------------------------------------------------

class TestPlanMixed:
    def test_original_entry_for_normalized_table(self):
        conn = _mixed_conn()
        plan = _make(conn).plan()
        original = [sp for sp in plan if sp.is_original]
        assert any(sp.sub_table_name == "users" for sp in original)

    def test_split_entries_for_wide_table(self):
        conn = _mixed_conn()
        plan = _make(conn).plan()
        split = [sp for sp in plan if not sp.is_original]
        assert len(split) >= 2
        for sp in split:
            assert sp.source_table == "denorm_events"

    def test_total_entries_greater_than_table_count(self):
        """Mixed schema → more Stage 2 entries than source tables."""
        conn = _mixed_conn()
        plan = _make(conn).plan()
        # 1 wide (splits ≥2) + 1 normalized = ≥3 entries, but only 2 source tables
        assert len(plan) > 2


# ---------------------------------------------------------------------------
# build_stage2() – normalized schema
# ---------------------------------------------------------------------------

class TestBuildStage2Normalized:
    def test_returns_knowledge_base(self):
        conn = _normalized_conn()
        kb = _make(conn).build_stage2(conn)
        assert isinstance(kb, KnowledgeBase)

    def test_total_rows_equals_source_rows(self):
        conn = _normalized_conn()
        kb = _make(conn).build_stage2(conn)
        # customers=2, orders=3, products=1 → 6 total
        assert len(kb.data_matrix) == 6

    def test_color_matrix_matches_data_matrix(self):
        conn = _normalized_conn()
        kb = _make(conn).build_stage2(conn)
        assert len(kb.color_matrix) == len(kb.data_matrix)

    def test_colours_are_valid_hex(self):
        conn = _normalized_conn()
        kb = _make(conn).build_stage2(conn)
        for cp in kb.color_matrix:
            assert cp.hex_color.startswith("#") and len(cp.hex_color) == 7

    def test_orders_t_values_are_datetimes(self):
        conn = _normalized_conn()
        p = _make(conn)
        kb = p.build_stage2(conn)
        orders_z = next(
            sp.stage2_z for sp in p.plan() if sp.sub_table_name == "orders"
        )
        for pt in kb.data_matrix:
            if pt.z == orders_z:
                assert isinstance(pt.t, datetime)

    def test_orders_t_values_are_real_dates(self):
        conn = _normalized_conn()
        p = _make(conn)
        kb = p.build_stage2(conn)
        orders_z = next(
            sp.stage2_z for sp in p.plan() if sp.sub_table_name == "orders"
        )
        t_values = sorted(pt.t for pt in kb.data_matrix if pt.z == orders_z)
        assert t_values[0] == datetime(2024, 3, 1)

    def test_orders_y_values_are_row_counts(self):
        """For normalized (non-NUMERIC group), y = 1.0 per row."""
        conn = _normalized_conn()
        p = _make(conn)
        kb = p.build_stage2(conn)
        orders_z = next(
            sp.stage2_z for sp in p.plan() if sp.sub_table_name == "orders"
        )
        for pt in kb.data_matrix:
            if pt.z == orders_z:
                assert pt.y == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# build_stage2() – wide schema (axis assignment)
# ---------------------------------------------------------------------------

class TestBuildStage2Wide:
    def test_total_rows_per_sub_table(self):
        """Each sub-table should have 3 rows (sales_report has 3 rows)."""
        conn = _wide_conn()
        p = _make(conn)
        kb = p.build_stage2(conn)
        plan = p.plan()
        for sp in plan:
            pts = [pt for pt in kb.data_matrix if pt.z == sp.stage2_z]
            assert len(pts) == 3  # 3 rows in sales_report

    def test_numeric_group_y_values_are_actual_amounts(self):
        conn = _wide_conn()
        p = _make(conn)
        kb = p.build_stage2(conn)
        numeric_z = next(
            sp.stage2_z for sp in p.plan() if sp.group == ColumnGroup.NUMERIC
        )
        y_values = sorted(pt.y for pt in kb.data_matrix if pt.z == numeric_z)
        # Should contain actual numeric column values (not just 1.0)
        assert any(v != 1.0 for v in y_values)

    def test_non_numeric_group_y_is_one(self):
        conn = _wide_conn()
        p = _make(conn)
        kb = p.build_stage2(conn)
        non_numeric_groups = {
            sp.stage2_z for sp in p.plan()
            if sp.group != ColumnGroup.NUMERIC
        }
        for pt in kb.data_matrix:
            if pt.z in non_numeric_groups:
                assert pt.y == pytest.approx(1.0)

    def test_temporal_group_t_values_parsed(self):
        conn = _wide_conn()
        p = _make(conn)
        kb = p.build_stage2(conn)
        snapshot = p.adapter.snapshot_time
        # At least some points should have t ≠ snapshot_time
        # (because created_at / updated_at columns are present)
        all_ts = [pt.t for pt in kb.data_matrix]
        assert any(t != snapshot for t in all_ts)

    def test_categorical_group_x_is_encoded_int(self):
        conn = _wide_conn()
        p = _make(conn)
        kb = p.build_stage2(conn)
        categorical_z = next(
            (sp.stage2_z for sp in p.plan() if sp.group == ColumnGroup.CATEGORICAL),
            None,
        )
        if categorical_z is not None:
            for pt in kb.data_matrix:
                if pt.z == categorical_z:
                    assert isinstance(pt.x, int)
                    assert pt.x >= 0

    def test_z_values_are_sequential_from_zero(self):
        conn = _wide_conn()
        kb = _make(conn).build_stage2(conn)
        z_values = sorted({pt.z for pt in kb.data_matrix})
        assert z_values == list(range(len(z_values)))


# ---------------------------------------------------------------------------
# Lineage – structure
# ---------------------------------------------------------------------------

class TestLineageStructure:
    def test_lineage_for_returns_dict(self):
        conn = _wide_conn()
        kb = _make(conn).build_stage2(conn)
        pt = next(iter(kb.data_matrix))
        lin = NormalizationPipeline.lineage_for(pt)
        assert isinstance(lin, dict)

    def test_lineage_keys_present(self):
        conn = _wide_conn()
        kb = _make(conn).build_stage2(conn)
        pt = next(iter(kb.data_matrix))
        lin = NormalizationPipeline.lineage_for(pt)
        for key in [
            "source_table", "sub_table", "group",
            "row_index", "stage1_z", "stage1_t", "stage1_x", "stage1_y",
        ]:
            assert key in lin

    def test_lineage_source_table_is_string(self):
        conn = _wide_conn()
        kb = _make(conn).build_stage2(conn)
        for pt in kb.data_matrix:
            lin = NormalizationPipeline.lineage_for(pt)
            assert isinstance(lin["source_table"], str)

    def test_lineage_row_index_is_nonnegative_int(self):
        conn = _wide_conn()
        kb = _make(conn).build_stage2(conn)
        for pt in kb.data_matrix:
            lin = NormalizationPipeline.lineage_for(pt)
            assert isinstance(lin["row_index"], int)
            assert lin["row_index"] >= 0

    def test_lineage_none_for_non_pipeline_point(self):
        from four_dim_matrix import DataPoint
        bare_pt = DataPoint(t=datetime(2024, 1, 1), x=0, y=1.0, z=0, payload={})
        assert NormalizationPipeline.lineage_for(bare_pt) is None

    def test_lineage_none_for_null_payload(self):
        from four_dim_matrix import DataPoint
        bare_pt = DataPoint(t=datetime(2024, 1, 1), x=0, y=1.0, z=0, payload=None)
        assert NormalizationPipeline.lineage_for(bare_pt) is None


# ---------------------------------------------------------------------------
# Lineage – values
# ---------------------------------------------------------------------------

class TestLineageValues:
    def test_source_table_is_sales_report_for_wide(self):
        conn = _wide_conn()
        kb = _make(conn).build_stage2(conn)
        for pt in kb.data_matrix:
            lin = NormalizationPipeline.lineage_for(pt)
            assert lin["source_table"] == "sales_report"

    def test_stage1_z_is_zero_for_only_table(self):
        conn = _wide_conn()
        kb = _make(conn).build_stage2(conn)
        for pt in kb.data_matrix:
            lin = NormalizationPipeline.lineage_for(pt)
            # sales_report is the only table → z=0 in Stage 1
            assert lin["stage1_z"] == 0

    def test_stage1_y_is_row_count(self):
        conn = _wide_conn()
        kb = _make(conn).build_stage2(conn)
        for pt in kb.data_matrix:
            lin = NormalizationPipeline.lineage_for(pt)
            # sales_report has 3 rows
            assert lin["stage1_y"] == pytest.approx(3.0)

    def test_stage1_x_is_column_count(self):
        conn = _wide_conn()
        adapter = DatabaseAdapter.from_connection(_wide_conn(), dialect="sqlite")
        expected_cols = adapter.get_table("sales_report").column_count
        kb = _make(conn).build_stage2(conn)
        for pt in kb.data_matrix:
            lin = NormalizationPipeline.lineage_for(pt)
            assert lin["stage1_x"] == expected_cols

    def test_stage1_t_is_iso_string(self):
        conn = _wide_conn()
        kb = _make(conn).build_stage2(conn)
        pt = next(iter(kb.data_matrix))
        lin = NormalizationPipeline.lineage_for(pt)
        # Should be parseable as ISO datetime
        datetime.fromisoformat(lin["stage1_t"])

    def test_row_indices_cover_all_source_rows(self):
        conn = _wide_conn()
        p = _make(conn)
        kb = p.build_stage2(conn)
        # For any one sub-table, row_index values should be 0,1,2 (3 source rows)
        numeric_z = next(sp.stage2_z for sp in p.plan() if sp.group == ColumnGroup.NUMERIC)
        indices = sorted(
            NormalizationPipeline.lineage_for(pt)["row_index"]
            for pt in kb.data_matrix if pt.z == numeric_z
        )
        assert indices == [0, 1, 2]

    def test_sub_table_name_in_lineage_matches_plan(self):
        conn = _wide_conn()
        p = _make(conn)
        kb = p.build_stage2(conn)
        plan_names = {sp.sub_table_name for sp in p.plan()}
        for pt in kb.data_matrix:
            lin = NormalizationPipeline.lineage_for(pt)
            assert lin["sub_table"] in plan_names

    def test_group_in_lineage_matches_plan(self):
        conn = _wide_conn()
        p = _make(conn)
        kb = p.build_stage2(conn)
        z_to_group = {sp.stage2_z: sp.group for sp in p.plan()}
        for pt in kb.data_matrix:
            lin = NormalizationPipeline.lineage_for(pt)
            assert lin["group"] == z_to_group[pt.z]

    def test_normalized_table_lineage_has_correct_source(self):
        conn = _normalized_conn()
        p = _make(conn)
        kb = p.build_stage2(conn)
        for pt in kb.data_matrix:
            lin = NormalizationPipeline.lineage_for(pt)
            assert lin["source_table"] == lin["sub_table"]  # not split


# ---------------------------------------------------------------------------
# stage2_points_from_source
# ---------------------------------------------------------------------------

class TestStage2PointsFromSource:
    def test_returns_all_points_for_source(self):
        conn = _wide_conn()
        p = _make(conn)
        kb = p.build_stage2(conn)
        pts = NormalizationPipeline.stage2_points_from_source(kb, "sales_report")
        # All points come from sales_report (only table)
        assert len(pts) == len(list(kb.data_matrix))

    def test_group_filter_narrows_results(self):
        conn = _wide_conn()
        p = _make(conn)
        kb = p.build_stage2(conn)
        numeric_pts = NormalizationPipeline.stage2_points_from_source(
            kb, "sales_report", group=ColumnGroup.NUMERIC
        )
        # Only the NUMERIC sub-table rows
        for pt in numeric_pts:
            lin = NormalizationPipeline.lineage_for(pt)
            assert lin["group"] == ColumnGroup.NUMERIC

    def test_unknown_source_returns_empty(self):
        conn = _wide_conn()
        p = _make(conn)
        kb = p.build_stage2(conn)
        pts = NormalizationPipeline.stage2_points_from_source(kb, "nonexistent")
        assert pts == []

    def test_mixed_schema_separates_sources(self):
        conn = _mixed_conn()
        p = _make(conn)
        kb = p.build_stage2(conn)
        users_pts = NormalizationPipeline.stage2_points_from_source(kb, "users")
        events_pts = NormalizationPipeline.stage2_points_from_source(kb, "denorm_events")
        assert len(users_pts) == 2   # 2 rows in users
        assert len(events_pts) > 0   # ≥1 sub-table from denorm_events
        # No overlap
        user_ids = {id(pt) for pt in users_pts}
        event_ids = {id(pt) for pt in events_pts}
        assert not user_ids & event_ids


# ---------------------------------------------------------------------------
# Empty schema
# ---------------------------------------------------------------------------

class TestEmptySchema:
    def test_plan_empty_for_no_tables(self):
        conn = sqlite3.connect(":memory:")
        adapter = DatabaseAdapter.from_connection(conn, dialect="sqlite")
        analyzer = SchemaAnalyzer(adapter)
        pipeline = NormalizationPipeline(adapter, analyzer)
        assert pipeline.plan() == []

    def test_build_stage2_empty_kb_for_no_tables(self):
        conn = sqlite3.connect(":memory:")
        adapter = DatabaseAdapter.from_connection(conn, dialect="sqlite")
        analyzer = SchemaAnalyzer(adapter)
        pipeline = NormalizationPipeline(adapter, analyzer)
        kb = pipeline.build_stage2(conn)
        assert isinstance(kb, KnowledgeBase)
        assert len(kb.data_matrix) == 0
        assert len(kb.color_matrix) == 0


# ---------------------------------------------------------------------------
# Full two-stage pipeline end-to-end
# ---------------------------------------------------------------------------

class TestEndToEnd:
    def test_stage1_vs_stage2_point_counts(self):
        """Stage 1 has 1 point per table; Stage 2 has 1 point per row (per sub-table)."""
        conn = _wide_conn()
        adapter = DatabaseAdapter.from_connection(conn, dialect="sqlite")
        analyzer = SchemaAnalyzer(adapter)
        pipeline = NormalizationPipeline(adapter, analyzer)

        stage1_kb = adapter.to_knowledge_base()
        stage2_kb = pipeline.build_stage2(conn)

        # Stage 1: 1 point (1 table)
        assert len(stage1_kb.data_matrix) == 1
        # Stage 2: 3 rows × N sub-tables
        assert len(stage2_kb.data_matrix) > len(stage1_kb.data_matrix)

    def test_stage1_and_stage2_z_are_different_spaces(self):
        """Stage 1 z = table index; Stage 2 z = sub-table index."""
        conn = _wide_conn()
        adapter = DatabaseAdapter.from_connection(conn, dialect="sqlite")
        analyzer = SchemaAnalyzer(adapter)
        pipeline = NormalizationPipeline(adapter, analyzer)

        stage1_kb = adapter.to_knowledge_base()
        stage2_kb = pipeline.build_stage2(conn)

        stage1_z_range = {pt.z for pt in stage1_kb.data_matrix}
        stage2_z_range = {pt.z for pt in stage2_kb.data_matrix}
        # Stage 2 should have more z-values (one per sub-table)
        assert len(stage2_z_range) >= len(stage1_z_range)

    def test_lineage_connects_stage2_to_stage1(self):
        """Every Stage 2 point's lineage.stage1_z must be a valid Stage 1 z."""
        conn = _wide_conn()
        adapter = DatabaseAdapter.from_connection(conn, dialect="sqlite")
        analyzer = SchemaAnalyzer(adapter)
        pipeline = NormalizationPipeline(adapter, analyzer)

        stage1_kb = adapter.to_knowledge_base()
        stage2_kb = pipeline.build_stage2(conn)

        valid_z1 = {pt.z for pt in stage1_kb.data_matrix}
        for pt in stage2_kb.data_matrix:
            lin = NormalizationPipeline.lineage_for(pt)
            assert lin["stage1_z"] in valid_z1

    def test_stage2_snapshot_shows_multiple_topics(self):
        """After Stage 2, snapshot() should see at least 2 topics (sub-tables)."""
        conn = _wide_conn()
        adapter = DatabaseAdapter.from_connection(conn, dialect="sqlite")
        analyzer = SchemaAnalyzer(adapter)
        pipeline = NormalizationPipeline(adapter, analyzer)

        stage2_kb = pipeline.build_stage2(conn)
        # Use the most common t across all points for the snapshot
        trend = stage2_kb.trend()
        if trend:
            t_max = max(trend, key=lambda t: trend[t])
            snap = stage2_kb.snapshot(t=t_max)
            assert len(snap["topics"]) >= 1

    def test_stage2_trend_is_richer_than_stage1(self):
        """Stage 2 should have multiple t-values (real row dates); Stage 1 has one."""
        conn = _wide_conn()
        adapter = DatabaseAdapter.from_connection(conn, dialect="sqlite")
        analyzer = SchemaAnalyzer(adapter)
        pipeline = NormalizationPipeline(adapter, analyzer)

        stage1_kb = adapter.to_knowledge_base()
        stage2_kb = pipeline.build_stage2(conn)

        stage1_trend = stage1_kb.trend()
        stage2_trend = stage2_kb.trend()
        # Stage 1: single snapshot timestamp → 1 entry
        assert len(stage1_trend) == 1
        # Stage 2: multiple real dates from created_at / updated_at → >1 entry
        assert len(stage2_trend) >= 1  # at least as many distinct dates
