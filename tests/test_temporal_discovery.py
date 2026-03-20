"""Tests for Track B: TemporalDiscoveryEngine and TMappingStrategy."""

from __future__ import annotations

import pytest

from four_dim_matrix import ColumnInfo, ColumnType, TableInfo
from four_dim_matrix.temporal_discovery import (
    TemporalColumn,
    TemporalDiscoveryEngine,
    TemporalType,
    TMappingStrategy,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _col(name: str, col_type: ColumnType = ColumnType.TEXT,
         nullable: bool = True, pk: bool = False) -> ColumnInfo:
    return ColumnInfo(
        name=name,
        type_str=col_type.name,
        column_type=col_type,
        nullable=nullable,
        primary_key=pk,
    )


def _table(name: str, columns: list) -> TableInfo:
    return TableInfo(name=name, columns=columns, row_count=0)


# ---------------------------------------------------------------------------
# TemporalColumn classification
# ---------------------------------------------------------------------------

class TestTemporalDiscoveryEngine:
    def _engine(self) -> TemporalDiscoveryEngine:
        return TemporalDiscoveryEngine()

    # -- discover_temporal_columns ----------------------------------------

    def test_datetime_type_detected(self):
        engine = self._engine()
        table  = _table("orders", [_col("created_at", ColumnType.DATETIME)])
        cols   = engine.discover_temporal_columns(table)
        assert len(cols) == 1
        assert cols[0].column_name == "created_at"

    def test_non_temporal_column_ignored(self):
        engine = self._engine()
        table  = _table("products", [_col("sku", ColumnType.TEXT)])
        assert engine.discover_temporal_columns(table) == []

    def test_technical_time_detected_by_name(self):
        engine = self._engine()
        table  = _table("t", [_col("created_at", ColumnType.DATETIME)])
        cols   = engine.discover_temporal_columns(table)
        assert cols[0].temporal_type == TemporalType.TECHNICAL_TIME

    def test_business_time_detected_by_name(self):
        engine = self._engine()
        table  = _table("t", [_col("order_date", ColumnType.DATETIME)])
        cols   = engine.discover_temporal_columns(table)
        assert cols[0].temporal_type == TemporalType.BUSINESS_TIME

    def test_business_time_has_higher_confidence_than_technical(self):
        engine = self._engine()
        bt = _col("order_date", ColumnType.DATETIME)
        tt = _col("created_at", ColumnType.DATETIME)
        table = _table("t", [bt, tt])
        cols  = engine.discover_temporal_columns(table)
        bt_col = next(c for c in cols if c.column_name == "order_date")
        tt_col = next(c for c in cols if c.column_name == "created_at")
        assert bt_col.confidence >= tt_col.confidence

    def test_version_sequence_on_integer(self):
        engine = self._engine()
        table  = _table("t", [_col("version", ColumnType.INTEGER)])
        cols   = engine.discover_temporal_columns(table)
        assert any(c.temporal_type == TemporalType.VERSION_SEQUENCE for c in cols)

    def test_sorted_by_confidence_descending(self):
        engine = self._engine()
        table = _table("t", [
            _col("order_date", ColumnType.DATETIME),
            _col("updated_at", ColumnType.DATETIME),
            _col("version", ColumnType.INTEGER),
        ])
        cols = engine.discover_temporal_columns(table)
        confidences = [c.confidence for c in cols]
        assert confidences == sorted(confidences, reverse=True)

    def test_multiple_temporal_columns_all_detected(self):
        engine = self._engine()
        table = _table("t", [
            _col("created_at", ColumnType.DATETIME),
            _col("updated_at", ColumnType.DATETIME),
            _col("order_date", ColumnType.DATETIME),
        ])
        cols = engine.discover_temporal_columns(table)
        assert len(cols) == 3

    # -- generate_t_mapping ------------------------------------------------

    def test_business_time_is_priority_one(self):
        engine = self._engine()
        table = _table("orders", [
            _col("created_at", ColumnType.DATETIME),
            _col("order_date", ColumnType.DATETIME),
        ])
        strategy = engine.generate_t_mapping(table)
        assert strategy.source_type == TemporalType.BUSINESS_TIME
        assert strategy.t_source_value == "column"
        assert strategy.column_name == "order_date"

    def test_technical_time_fallback(self):
        engine = self._engine()
        table = _table("users", [_col("created_at", ColumnType.DATETIME)])
        strategy = engine.generate_t_mapping(table)
        assert strategy.source_type == TemporalType.TECHNICAL_TIME
        assert strategy.t_source_value == "column"
        assert strategy.column_name == "created_at"

    def test_version_sequence_when_no_time(self):
        engine = self._engine()
        table = _table("migrations", [
            _col("id", ColumnType.INTEGER, pk=True),
            _col("version", ColumnType.INTEGER),
            _col("name", ColumnType.TEXT),
        ])
        strategy = engine.generate_t_mapping(table)
        assert strategy.source_type == TemporalType.VERSION_SEQUENCE
        assert strategy.t_source_value == "version"

    def test_pk_logical_order_when_no_time_or_version(self):
        engine = self._engine()
        table = _table("countries", [
            _col("id", ColumnType.INTEGER, nullable=False, pk=True),
            _col("code", ColumnType.TEXT),
            _col("name", ColumnType.TEXT),
        ])
        strategy = engine.generate_t_mapping(table)
        assert strategy.source_type == TemporalType.LOGICAL_ORDER
        assert strategy.t_source_value == "synthetic"
        assert strategy.column_name == "id"

    def test_topology_when_center_table_provided(self):
        engine = self._engine()
        # Table with no time column, no integer PK, but has a center table
        table = _table("customer_notes", [
            _col("content", ColumnType.TEXT),
        ])
        strategy = engine.generate_t_mapping(table, center_table_name="customers")
        assert strategy.t_source_value == "topology"
        assert "customers" in strategy.note

    def test_synthetic_fallback_for_empty_table(self):
        engine = self._engine()
        table  = _table("empty_dict", [_col("word", ColumnType.TEXT)])
        strategy = engine.generate_t_mapping(table)
        assert strategy.t_source_value == "synthetic"
        assert strategy.source_type == TemporalType.SYNTHETIC

    def test_fallback_columns_populated(self):
        engine = self._engine()
        table = _table("orders", [
            _col("order_date", ColumnType.DATETIME),
            _col("created_at", ColumnType.DATETIME),
        ])
        strategy = engine.generate_t_mapping(table)
        # The primary is order_date (business); created_at should be a fallback
        assert "created_at" in strategy.fallback_columns

    def test_to_dict_round_trip(self):
        engine   = self._engine()
        table    = _table("t", [_col("order_date", ColumnType.DATETIME)])
        strategy = engine.generate_t_mapping(table)
        d = strategy.to_dict()
        assert d["t_source_value"] == strategy.t_source_value
        assert d["source_type"] == strategy.source_type.name
        assert d["column_name"] == strategy.column_name


# ---------------------------------------------------------------------------
# TMappingStrategy dataclass
# ---------------------------------------------------------------------------

class TestTMappingStrategy:
    def test_defaults(self):
        s = TMappingStrategy(
            source_type=TemporalType.BUSINESS_TIME,
            column_name="order_date",
        )
        assert s.granularity == "day"
        assert s.t_source_value == "column"
        assert s.fallback_columns == []
        assert s.note == ""

    def test_to_dict_has_required_keys(self):
        s = TMappingStrategy(
            source_type=TemporalType.SYNTHETIC,
            column_name=None,
            t_source_value="synthetic",
            note="test",
        )
        d = s.to_dict()
        for key in ("source_type", "column_name", "t_source_value", "note",
                    "granularity", "is_monotonic", "fallback_columns"):
            assert key in d
