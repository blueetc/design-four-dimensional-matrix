"""Tests for SchemaAnalyzer.

Uses in-memory SQLite databases representing three scenarios:
  1. A well-normalized schema (small focused tables)
  2. A denormalized schema (wide tables mixing many topic groups)
  3. An empty schema
"""

from __future__ import annotations

import sqlite3

import pytest

from four_dim_matrix import (
    ColumnGroup,
    DatabaseAdapter,
    SchemaAnalyzer,
    TableAnalysis,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _normalized_conn() -> sqlite3.Connection:
    """Three small, focused tables – each covers exactly one topic."""
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
            updated_at  DATETIME,
            closed_at   DATETIME
        );
        """
    )
    return conn


def _mixed_conn() -> sqlite3.Connection:
    """Mix: one wide table + one well-structured table."""
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE denormalized_events (
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
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT
        );
        """
    )
    return conn


def _make_analyzer(conn: sqlite3.Connection, **kwargs) -> SchemaAnalyzer:
    adapter = DatabaseAdapter.from_connection(conn, dialect="sqlite")
    return SchemaAnalyzer(adapter, **kwargs)


# ---------------------------------------------------------------------------
# ColumnGroup constants
# ---------------------------------------------------------------------------

class TestColumnGroup:
    def test_constants_are_strings(self):
        for g in [
            ColumnGroup.IDENTITY, ColumnGroup.RELATIONAL, ColumnGroup.TEMPORAL,
            ColumnGroup.NUMERIC, ColumnGroup.DESCRIPTIVE, ColumnGroup.CATEGORICAL,
            ColumnGroup.OTHER,
        ]:
            assert isinstance(g, str)

    def test_all_distinct(self):
        groups = [
            ColumnGroup.IDENTITY, ColumnGroup.RELATIONAL, ColumnGroup.TEMPORAL,
            ColumnGroup.NUMERIC, ColumnGroup.DESCRIPTIVE, ColumnGroup.CATEGORICAL,
            ColumnGroup.OTHER,
        ]
        assert len(set(groups)) == 7


# ---------------------------------------------------------------------------
# SchemaAnalyzer construction
# ---------------------------------------------------------------------------

class TestSchemaAnalyzerConstruction:
    def test_adapter_stored(self):
        conn = _normalized_conn()
        analyzer = _make_analyzer(conn)
        assert analyzer.adapter is not None
        assert len(analyzer.adapter.tables) == 3

    def test_default_threshold(self):
        conn = _normalized_conn()
        analyzer = _make_analyzer(conn)
        assert analyzer.wide_table_threshold == 8

    def test_custom_threshold(self):
        conn = _normalized_conn()
        analyzer = _make_analyzer(conn, wide_table_threshold=3)
        assert analyzer.wide_table_threshold == 3

    def test_missing_table_raises(self):
        conn = _normalized_conn()
        analyzer = _make_analyzer(conn)
        with pytest.raises(KeyError):
            analyzer.analyse_table("nonexistent_table")


# ---------------------------------------------------------------------------
# analyse_table – column group classification
# ---------------------------------------------------------------------------

class TestAnalyseTable:
    def test_returns_table_analysis(self):
        conn = _normalized_conn()
        analyzer = _make_analyzer(conn)
        result = analyzer.analyse_table("orders")
        assert isinstance(result, TableAnalysis)

    def test_table_name_preserved(self):
        conn = _normalized_conn()
        analyzer = _make_analyzer(conn)
        result = analyzer.analyse_table("orders")
        assert result.name == "orders"

    def test_column_count_correct(self):
        conn = _normalized_conn()
        analyzer = _make_analyzer(conn)
        # orders: id, customer_id, total, created_at → 4 columns
        assert analyzer.analyse_table("orders").column_count == 4

    def test_row_count_correct(self):
        conn = _normalized_conn()
        analyzer = _make_analyzer(conn)
        assert analyzer.analyse_table("orders").row_count == 0

    def test_primary_key_classified_as_identity(self):
        conn = _normalized_conn()
        analyzer = _make_analyzer(conn)
        groups = analyzer.analyse_table("customers").column_groups
        assert "id" in groups.get(ColumnGroup.IDENTITY, [])

    def test_fk_column_classified_as_relational(self):
        conn = _normalized_conn()
        analyzer = _make_analyzer(conn)
        groups = analyzer.analyse_table("orders").column_groups
        assert "customer_id" in groups.get(ColumnGroup.RELATIONAL, [])

    def test_datetime_column_classified_as_temporal(self):
        conn = _normalized_conn()
        analyzer = _make_analyzer(conn)
        groups = analyzer.analyse_table("orders").column_groups
        assert "created_at" in groups.get(ColumnGroup.TEMPORAL, [])

    def test_numeric_column_classified_correctly(self):
        conn = _normalized_conn()
        analyzer = _make_analyzer(conn)
        groups = analyzer.analyse_table("orders").column_groups
        assert "total" in groups.get(ColumnGroup.NUMERIC, [])

    def test_text_column_classified_as_descriptive(self):
        conn = _normalized_conn()
        analyzer = _make_analyzer(conn)
        groups = analyzer.analyse_table("customers").column_groups
        # "name" and "email" should be DESCRIPTIVE
        descriptive = groups.get(ColumnGroup.DESCRIPTIVE, [])
        assert "name" in descriptive

    def test_status_column_classified_as_categorical(self):
        conn = _wide_conn()
        analyzer = _make_analyzer(conn)
        groups = analyzer.analyse_table("sales_report").column_groups
        categorical = groups.get(ColumnGroup.CATEGORICAL, [])
        assert "status" in categorical

    def test_empty_groups_omitted(self):
        """Groups with no columns should not appear in column_groups."""
        conn = _normalized_conn()
        analyzer = _make_analyzer(conn)
        groups = analyzer.analyse_table("customers").column_groups
        for cols in groups.values():
            assert len(cols) > 0


# ---------------------------------------------------------------------------
# Wide-table detection
# ---------------------------------------------------------------------------

class TestWideTableDetection:
    def test_small_table_not_wide(self):
        conn = _normalized_conn()
        analyzer = _make_analyzer(conn)
        assert not analyzer.analyse_table("customers").is_wide_table
        assert not analyzer.analyse_table("orders").is_wide_table
        assert not analyzer.analyse_table("products").is_wide_table

    def test_large_mixed_table_is_wide(self):
        conn = _wide_conn()
        analyzer = _make_analyzer(conn)
        assert analyzer.analyse_table("sales_report").is_wide_table

    def test_custom_low_threshold_makes_small_table_wide(self):
        conn = _normalized_conn()
        analyzer = _make_analyzer(conn, wide_table_threshold=2)
        # "orders" has 4 columns and multiple groups → wide at threshold 2
        result = analyzer.analyse_table("orders")
        assert result.is_wide_table

    def test_wide_table_has_multiple_active_groups(self):
        conn = _wide_conn()
        analyzer = _make_analyzer(conn)
        result = analyzer.analyse_table("sales_report")
        assert len(result.active_groups) >= 2

    def test_normalization_hint_mentions_split(self):
        conn = _wide_conn()
        analyzer = _make_analyzer(conn)
        hint = analyzer.analyse_table("sales_report").normalization_hint
        assert "split" in hint.lower() or "mixing" in hint.lower()

    def test_non_wide_hint_says_single_topic_or_compact(self):
        conn = _normalized_conn()
        analyzer = _make_analyzer(conn)
        hint = analyzer.analyse_table("customers").normalization_hint
        assert "topic" in hint.lower() or "compact" in hint.lower() or "identity" in hint.lower()


# ---------------------------------------------------------------------------
# analyse_all
# ---------------------------------------------------------------------------

class TestAnalyseAll:
    def test_returns_one_per_table(self):
        conn = _normalized_conn()
        analyzer = _make_analyzer(conn)
        results = analyzer.analyse_all()
        assert len(results) == 3

    def test_sorted_alphabetically(self):
        conn = _normalized_conn()
        analyzer = _make_analyzer(conn)
        names = [a.name for a in analyzer.analyse_all()]
        assert names == sorted(names)

    def test_all_are_table_analysis_instances(self):
        conn = _wide_conn()
        analyzer = _make_analyzer(conn)
        for a in analyzer.analyse_all():
            assert isinstance(a, TableAnalysis)


# ---------------------------------------------------------------------------
# to_dict
# ---------------------------------------------------------------------------

class TestTableAnalysisToDict:
    def test_keys_present(self):
        conn = _normalized_conn()
        analyzer = _make_analyzer(conn)
        d = analyzer.analyse_table("orders").to_dict()
        assert "name" in d
        assert "column_count" in d
        assert "row_count" in d
        assert "column_groups" in d
        assert "is_wide_table" in d
        assert "active_groups" in d
        assert "normalization_hint" in d

    def test_column_groups_values_are_lists(self):
        conn = _normalized_conn()
        analyzer = _make_analyzer(conn)
        d = analyzer.analyse_table("orders").to_dict()
        for cols in d["column_groups"].values():
            assert isinstance(cols, list)


# ---------------------------------------------------------------------------
# suggest_normalization
# ---------------------------------------------------------------------------

class TestSuggestNormalization:
    def test_well_normalized_table_no_split_proposed(self):
        conn = _normalized_conn()
        analyzer = _make_analyzer(conn)
        suggestion = analyzer.suggest_normalization("customers")
        assert suggestion["suggested_tables"] == []

    def test_wide_table_produces_multiple_sub_tables(self):
        conn = _wide_conn()
        analyzer = _make_analyzer(conn)
        suggestion = analyzer.suggest_normalization("sales_report")
        assert len(suggestion["suggested_tables"]) >= 2

    def test_sub_table_names_contain_original(self):
        conn = _wide_conn()
        analyzer = _make_analyzer(conn)
        suggestion = analyzer.suggest_normalization("sales_report")
        for st in suggestion["suggested_tables"]:
            assert "sales_report" in st["name"]

    def test_sub_tables_include_identity_columns(self):
        conn = _wide_conn()
        analyzer = _make_analyzer(conn)
        suggestion = analyzer.suggest_normalization("sales_report")
        id_cols = analyzer.analyse_table("sales_report").column_groups.get(
            ColumnGroup.IDENTITY, []
        )
        for st in suggestion["suggested_tables"]:
            for id_col in id_cols:
                assert id_col in st["columns"]

    def test_sub_tables_have_matrix_role(self):
        conn = _wide_conn()
        analyzer = _make_analyzer(conn)
        suggestion = analyzer.suggest_normalization("sales_report")
        for st in suggestion["suggested_tables"]:
            assert "matrix_role" in st
            assert st["matrix_role"]

    def test_sub_tables_have_rationale(self):
        conn = _wide_conn()
        analyzer = _make_analyzer(conn)
        suggestion = analyzer.suggest_normalization("sales_report")
        for st in suggestion["suggested_tables"]:
            assert "rationale" in st
            assert st["rationale"]

    def test_sub_tables_have_columns_list(self):
        conn = _wide_conn()
        analyzer = _make_analyzer(conn)
        suggestion = analyzer.suggest_normalization("sales_report")
        for st in suggestion["suggested_tables"]:
            assert isinstance(st["columns"], list)
            assert len(st["columns"]) > 0

    def test_suggestion_keys(self):
        conn = _wide_conn()
        analyzer = _make_analyzer(conn)
        suggestion = analyzer.suggest_normalization("sales_report")
        assert "table" in suggestion
        assert "current_columns" in suggestion
        assert "is_wide_table" in suggestion
        assert "suggested_tables" in suggestion
        assert "verdict" in suggestion

    def test_all_wide_table_columns_covered(self):
        """Every non-identity column should appear in at least one sub-table."""
        conn = _wide_conn()
        analyzer = _make_analyzer(conn)
        suggestion = analyzer.suggest_normalization("sales_report")
        table = DatabaseAdapter.from_connection(_wide_conn(), dialect="sqlite").get_table("sales_report")
        all_col_names = {c.name for c in table.columns}

        covered: set = set()
        for st in suggestion["suggested_tables"]:
            covered.update(st["columns"])
        # Every column should appear in at least one sub-table
        assert all_col_names.issubset(covered)


# ---------------------------------------------------------------------------
# normalization_score
# ---------------------------------------------------------------------------

class TestNormalizationScore:
    def test_fully_normalized_score_is_one(self):
        conn = _normalized_conn()
        analyzer = _make_analyzer(conn)
        assert analyzer.normalization_score() == pytest.approx(1.0)

    def test_fully_denormalized_score_is_zero(self):
        conn = _wide_conn()
        analyzer = _make_analyzer(conn)
        assert analyzer.normalization_score() == pytest.approx(0.0)

    def test_mixed_schema_score_between_zero_and_one(self):
        conn = _mixed_conn()
        analyzer = _make_analyzer(conn)
        score = analyzer.normalization_score()
        assert 0.0 < score < 1.0

    def test_mixed_schema_score_is_half(self):
        conn = _mixed_conn()
        # 1 wide table out of 2 → score = 0.5
        analyzer = _make_analyzer(conn)
        assert analyzer.normalization_score() == pytest.approx(0.5)

    def test_empty_schema_score_is_one(self):
        conn = sqlite3.connect(":memory:")
        adapter = DatabaseAdapter.from_connection(conn, dialect="sqlite")
        analyzer = SchemaAnalyzer(adapter)
        assert analyzer.normalization_score() == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# report
# ---------------------------------------------------------------------------

class TestReport:
    def test_report_keys(self):
        conn = _normalized_conn()
        analyzer = _make_analyzer(conn)
        report = analyzer.report()
        for key in [
            "snapshot_time", "normalization_score", "table_count",
            "wide_table_count", "tables", "suggestions", "matrix_readiness",
        ]:
            assert key in report

    def test_report_table_count(self):
        conn = _normalized_conn()
        assert _make_analyzer(conn).report()["table_count"] == 3

    def test_report_wide_table_count_normalized(self):
        conn = _normalized_conn()
        assert _make_analyzer(conn).report()["wide_table_count"] == 0

    def test_report_wide_table_count_denormalized(self):
        conn = _wide_conn()
        assert _make_analyzer(conn).report()["wide_table_count"] == 1

    def test_report_suggestions_empty_for_normalized(self):
        conn = _normalized_conn()
        assert _make_analyzer(conn).report()["suggestions"] == {}

    def test_report_suggestions_populated_for_wide(self):
        conn = _wide_conn()
        report = _make_analyzer(conn).report()
        assert "sales_report" in report["suggestions"]
        assert len(report["suggestions"]["sales_report"]["suggested_tables"]) >= 2

    def test_report_tables_sorted_by_column_count_desc(self):
        conn = _normalized_conn()
        col_counts = [t["column_count"] for t in _make_analyzer(conn).report()["tables"]]
        assert col_counts == sorted(col_counts, reverse=True)

    def test_report_normalization_score_matches_method(self):
        conn = _mixed_conn()
        analyzer = _make_analyzer(conn)
        assert analyzer.report()["normalization_score"] == pytest.approx(
            analyzer.normalization_score(), abs=1e-4
        )

    def test_report_matrix_readiness_is_string(self):
        for conn in [_normalized_conn(), _wide_conn(), _mixed_conn()]:
            report = _make_analyzer(conn).report()
            assert isinstance(report["matrix_readiness"], str)
            assert len(report["matrix_readiness"]) > 0

    def test_report_matrix_readiness_fully_normalized(self):
        conn = _normalized_conn()
        readiness = _make_analyzer(conn).report()["matrix_readiness"]
        assert "normalized" in readiness.lower() or "fully" in readiness.lower()

    def test_report_matrix_readiness_denormalized_warns(self):
        conn = _wide_conn()
        readiness = _make_analyzer(conn).report()["matrix_readiness"]
        # Should mention denormalization or blurry / wide
        assert any(
            word in readiness.lower()
            for word in ["denormalized", "wide", "blurry", "mix", "split"]
        )
