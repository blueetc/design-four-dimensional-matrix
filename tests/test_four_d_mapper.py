"""Tests for FourDimensionalMapper (integration of Tracks A, B, C)."""

from __future__ import annotations

import sqlite3

import pytest

from four_dim_matrix import DatabaseAdapter, KnowledgeBase
from four_dim_matrix.four_d_mapper import (
    FourDimensionalMapper,
    MatrixConfig,
    _derive_t_from_strategy,
    _pick_y_column,
)
from four_dim_matrix.temporal_discovery import TMappingStrategy, TemporalType


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _erp_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE customers (
            id         INTEGER PRIMARY KEY,
            name       TEXT NOT NULL,
            signup_date DATETIME
        );
        CREATE TABLE orders (
            id          INTEGER PRIMARY KEY,
            customers_id INTEGER NOT NULL,
            total       REAL,
            created_at  DATETIME
        );
        CREATE TABLE order_items (
            id         INTEGER PRIMARY KEY,
            orders_id  INTEGER NOT NULL,
            products_id INTEGER NOT NULL,
            qty        INTEGER
        );
        CREATE TABLE products (
            id    INTEGER PRIMARY KEY,
            sku   TEXT NOT NULL,
            price REAL
        );
        INSERT INTO customers VALUES (1,'Alice','2024-01-10');
        INSERT INTO customers VALUES (2,'Bob','2024-02-15');
        INSERT INTO orders VALUES (1, 1, 99.9, '2024-03-01');
        INSERT INTO orders VALUES (2, 2, 49.5, '2024-03-15');
        INSERT INTO order_items VALUES (1, 1, 1, 2);
        INSERT INTO order_items VALUES (2, 2, 1, 1);
        INSERT INTO products VALUES (1, 'SKU-001', 49.95);
    """)
    return conn


def _dict_conn() -> sqlite3.Connection:
    """A pure dictionary/reference database with no time columns."""
    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE languages (
            id   INTEGER PRIMARY KEY,
            code TEXT NOT NULL,
            name TEXT NOT NULL
        );
        CREATE TABLE country_codes (
            id   INTEGER PRIMARY KEY,
            iso2 TEXT NOT NULL,
            name TEXT
        );
        INSERT INTO languages VALUES (1,'en','English');
        INSERT INTO languages VALUES (2,'zh','Chinese');
        INSERT INTO country_codes VALUES (1,'US','United States');
        INSERT INTO country_codes VALUES (2,'CN','China');
    """)
    return conn


# ---------------------------------------------------------------------------
# MatrixConfig
# ---------------------------------------------------------------------------

class TestMatrixConfig:
    def test_defaults(self):
        cfg = MatrixConfig()
        assert cfg.target_entity_count is None
        assert cfg.color_config is None
        assert cfg.include_lineage is True

    def test_custom_values(self):
        cfg = MatrixConfig(target_entity_count=5, include_lineage=False)
        assert cfg.target_entity_count == 5
        assert cfg.include_lineage is False


# ---------------------------------------------------------------------------
# FourDimensionalMapper.analyse()
# ---------------------------------------------------------------------------

class TestFourDimensionalMapperAnalyse:
    def _mapper(self, conn: sqlite3.Connection) -> FourDimensionalMapper:
        adapter = DatabaseAdapter.from_connection(conn, dialect="sqlite")
        return FourDimensionalMapper(adapter, MatrixConfig())

    def test_returns_plan_dict(self):
        mapper = self._mapper(_erp_conn())
        plan   = mapper.analyse()
        assert isinstance(plan, dict)
        for key in ("entity_count", "table_count", "entities", "z_allocation"):
            assert key in plan

    def test_table_count_matches_schema(self):
        mapper = self._mapper(_erp_conn())
        plan   = mapper.analyse()
        assert plan["table_count"] == 4  # 4 tables in the ERP schema

    def test_entity_count_positive(self):
        mapper = self._mapper(_erp_conn())
        plan   = mapper.analyse()
        assert plan["entity_count"] >= 1

    def test_each_entity_has_required_keys(self):
        mapper = self._mapper(_erp_conn())
        plan   = mapper.analyse()
        for entity in plan["entities"]:
            for key in ("name", "z0_index", "center_table", "member_tables",
                        "tables", "hue"):
                assert key in entity

    def test_tables_list_has_z_and_color(self):
        mapper = self._mapper(_erp_conn())
        plan   = mapper.analyse()
        for entity in plan["entities"]:
            for t in entity["tables"]:
                assert t["z_scalar"] is not None
                assert t["color"].startswith("#")

    def test_all_tables_allocated(self):
        mapper = self._mapper(_erp_conn())
        plan   = mapper.analyse()
        all_tables = {
            t["table"]
            for e in plan["entities"]
            for t in e["tables"]
        }
        assert all_tables == {"customers", "orders", "order_items", "products"}

    def test_dict_db_synthetic_strategy(self):
        mapper = self._mapper(_dict_conn())
        plan   = mapper.analyse()
        # All tables should get synthetic/topology t_source
        for entity in plan["entities"]:
            for t in entity["tables"]:
                strat = t["t_strategy"]
                assert strat.get("t_source_value") in ("synthetic", "topology", "version")


# ---------------------------------------------------------------------------
# FourDimensionalMapper.build() – schema mode
# ---------------------------------------------------------------------------

class TestFourDimensionalMapperBuildSchema:
    def test_returns_kb_and_plan(self):
        conn    = _erp_conn()
        adapter = DatabaseAdapter.from_connection(conn, dialect="sqlite")
        mapper  = FourDimensionalMapper(adapter)
        kb, plan = mapper.build(conn=None)
        assert isinstance(kb, KnowledgeBase)
        assert isinstance(plan, dict)

    def test_kb_has_one_point_per_table(self):
        conn    = _erp_conn()
        adapter = DatabaseAdapter.from_connection(conn, dialect="sqlite")
        kb, _   = FourDimensionalMapper(adapter).build()
        assert len(kb.data_matrix) == 4

    def test_lineage_in_payload_when_enabled(self):
        conn    = _erp_conn()
        adapter = DatabaseAdapter.from_connection(conn, dialect="sqlite")
        kb, _   = FourDimensionalMapper(adapter, MatrixConfig(include_lineage=True)).build()
        for point in kb.data_matrix:
            assert "_lineage" in point.payload

    def test_no_lineage_when_disabled(self):
        conn    = _erp_conn()
        adapter = DatabaseAdapter.from_connection(conn, dialect="sqlite")
        kb, _   = FourDimensionalMapper(adapter, MatrixConfig(include_lineage=False)).build()
        for point in kb.data_matrix:
            assert "_lineage" not in point.payload


# ---------------------------------------------------------------------------
# FourDimensionalMapper.build() – row mode
# ---------------------------------------------------------------------------

class TestFourDimensionalMapperBuildRows:
    def test_row_mode_loads_more_than_schema_mode(self):
        conn    = _erp_conn()
        adapter = DatabaseAdapter.from_connection(conn, dialect="sqlite")
        mapper  = FourDimensionalMapper(adapter)
        kb_schema, _ = mapper.build(conn=None)

        # Reset and rebuild in row mode
        mapper2 = FourDimensionalMapper(adapter)
        kb_rows, _ = mapper2.build(conn=conn)

        # Row mode should load more data points than schema mode
        assert len(kb_rows.data_matrix) >= len(kb_schema.data_matrix)

    def test_row_mode_kb_is_queryable(self):
        conn    = _erp_conn()
        adapter = DatabaseAdapter.from_connection(conn, dialect="sqlite")
        kb, _   = FourDimensionalMapper(adapter).build(conn=conn)
        trend   = kb.trend()
        assert isinstance(trend, dict)

    def test_x_equals_z1_relation_depth(self):
        conn    = _erp_conn()
        adapter = DatabaseAdapter.from_connection(conn, dialect="sqlite")
        kb, plan = FourDimensionalMapper(adapter).build(conn=conn)
        # x values should all be 0–9 (matching RelationType z1 values)
        for point in kb.data_matrix:
            assert 0 <= point.x <= 9


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

class TestPickYColumn:
    def _col(self, name, ctype, pk=False):
        from four_dim_matrix import ColumnInfo
        return ColumnInfo(name=name, type_str=ctype.name,
                          column_type=ctype, nullable=True, primary_key=pk)

    def test_picks_first_non_pk_numeric(self):
        from four_dim_matrix import ColumnType, TableInfo
        table = TableInfo(name="t", columns=[
            self._col("id", ColumnType.INTEGER, pk=True),
            self._col("total", ColumnType.FLOAT),
        ])
        assert _pick_y_column(table) == "total"

    def test_falls_back_to_first_column(self):
        from four_dim_matrix import ColumnType, TableInfo
        table = TableInfo(name="t", columns=[
            self._col("name", ColumnType.TEXT),
        ])
        assert _pick_y_column(table) == "name"

    def test_empty_table_returns_none(self):
        from four_dim_matrix import TableInfo
        table = TableInfo(name="t", columns=[])
        assert _pick_y_column(table) is None


class TestDeriveTFromStrategy:
    _EPOCH = __import__("datetime").datetime(2000, 1, 1)

    def test_column_strategy_parses_iso(self):
        strategy = TMappingStrategy(
            source_type=TemporalType.BUSINESS_TIME,
            column_name="ts",
            t_source_value="column",
        )
        row = {"ts": "2024-03-15"}
        from datetime import datetime
        t = _derive_t_from_strategy(row, 0, strategy, datetime(1999, 1, 1))
        assert t == datetime(2024, 3, 15)

    def test_synthetic_uses_row_index(self):
        strategy = TMappingStrategy(
            source_type=TemporalType.SYNTHETIC,
            column_name=None,
            t_source_value="synthetic",
        )
        from datetime import timedelta
        t = _derive_t_from_strategy({}, 5, strategy, self._EPOCH)
        assert t == self._EPOCH + timedelta(days=5)

    def test_version_converts_integer(self):
        strategy = TMappingStrategy(
            source_type=TemporalType.VERSION_SEQUENCE,
            column_name="version",
            t_source_value="version",
        )
        from datetime import timedelta
        t = _derive_t_from_strategy({"version": 10}, 0, strategy, self._EPOCH)
        assert t == self._EPOCH + timedelta(days=10)

    def test_none_strategy_uses_row_index(self):
        from datetime import timedelta
        t = _derive_t_from_strategy({}, 3, None, self._EPOCH)
        assert t == self._EPOCH + timedelta(days=3)

    def test_topology_behaves_like_synthetic(self):
        strategy = TMappingStrategy(
            source_type=TemporalType.LOGICAL_ORDER,
            column_name=None,
            t_source_value="topology",
        )
        from datetime import timedelta
        t = _derive_t_from_strategy({}, 7, strategy, self._EPOCH)
        assert t == self._EPOCH + timedelta(days=7)
