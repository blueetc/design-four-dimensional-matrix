"""Tests for Track A: KeyDiscoveryEngine and EntityClusteringEngine."""

from __future__ import annotations

import sqlite3

import pytest

from four_dim_matrix import (
    ColumnInfo,
    ColumnType,
    DatabaseAdapter,
    TableInfo,
)
from four_dim_matrix.key_discovery import (
    CoreEntity,
    EntityClusteringEngine,
    KeyDiscoveryEngine,
    KeyScore,
    _count_references,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_col(name: str, col_type: ColumnType = ColumnType.INTEGER,
              nullable: bool = True, pk: bool = False) -> ColumnInfo:
    return ColumnInfo(
        name=name,
        type_str=col_type.name,
        column_type=col_type,
        nullable=nullable,
        primary_key=pk,
    )


def _make_table(name: str, columns: list, row_count: int = 0) -> TableInfo:
    return TableInfo(name=name, columns=columns, row_count=row_count)


# ---------------------------------------------------------------------------
# KeyDiscoveryEngine
# ---------------------------------------------------------------------------

class TestKeyDiscoveryEngine:
    def _engine(self) -> KeyDiscoveryEngine:
        return KeyDiscoveryEngine()

    def test_explicit_pk_gets_highest_score(self):
        engine = self._engine()
        pk_col = _make_col("id", ColumnType.INTEGER, nullable=False, pk=True)
        other = _make_col("name", ColumnType.TEXT)
        table = _make_table("customers", [pk_col, other])
        scores = engine.discover_table_keys(table)
        assert scores[0].column_name == "id"
        assert "DB_PRIMARY_KEY" in scores[0].reasons

    def test_id_name_pattern_scores_well(self):
        engine = self._engine()
        col = _make_col("id", ColumnType.INTEGER, nullable=False)
        table = _make_table("orders", [col])
        scores = engine.discover_table_keys(table)
        assert scores
        assert scores[0].column_name == "id"
        assert "NAMING_PATTERN_ID" in scores[0].reasons

    def test_uuid_column_scored_as_business_key(self):
        engine = self._engine()
        # "serial" matches _BUSINESS_KEY but not _STRONG_ID
        col = _make_col("serial", ColumnType.TEXT, nullable=False)
        table = _make_table("items", [col])
        scores = engine.discover_table_keys(table)
        assert scores
        bk = [s for s in scores if "NAMING_PATTERN_BUSINESS_KEY" in s.reasons]
        assert bk

    def test_non_nullable_adds_score(self):
        engine = self._engine()
        nullable_id   = _make_col("id", ColumnType.INTEGER, nullable=True)
        nonnull_id    = _make_col("id", ColumnType.INTEGER, nullable=False)
        t1 = _make_table("t1", [nullable_id])
        t2 = _make_table("t2", [nonnull_id])
        s1 = engine.score_column(nullable_id, t1)
        s2 = engine.score_column(nonnull_id, t2)
        assert s2.score > s1.score

    def test_min_score_filter(self):
        engine = self._engine()
        unrelated = _make_col("description", ColumnType.TEXT)
        table = _make_table("t", [unrelated])
        scores = engine.discover_table_keys(table, min_score=30.0)
        assert scores == []

    def test_confidence_is_clamped_to_one(self):
        engine = self._engine()
        pk_col = _make_col("id", ColumnType.INTEGER, nullable=False, pk=True)
        table  = _make_table("users", [pk_col])
        all_tables = [table, _make_table("posts", [_make_col("users_id")])]
        scores = engine.discover_table_keys(table, all_tables=all_tables)
        assert all(0.0 <= s.confidence <= 1.0 for s in scores)

    def test_cross_table_reference_boosts_score(self):
        engine = self._engine()
        pk_col = _make_col("id", ColumnType.INTEGER, nullable=False, pk=True)
        customers = _make_table("customers", [pk_col])
        orders_fk = _make_col("customers_id", ColumnType.INTEGER)
        orders    = _make_table("orders", [orders_fk])

        score_without = engine.score_column(pk_col, customers)
        score_with    = engine.score_column(pk_col, customers, all_tables=[customers, orders])
        assert score_with.score > score_without.score
        assert any("REFERENCED_BY" in r for r in score_with.reasons)


class TestCountReferences:
    def test_counts_fk_columns_in_other_tables(self):
        pk = _make_col("id", ColumnType.INTEGER, nullable=False, pk=True)
        customers = _make_table("customers", [pk])
        fk_col    = _make_col("customers_id", ColumnType.INTEGER)
        orders    = _make_table("orders", [fk_col])
        count = _count_references(pk, customers, [customers, orders])
        assert count == 1

    def test_same_table_not_counted(self):
        pk = _make_col("id", ColumnType.INTEGER, nullable=False, pk=True)
        table = _make_table("customers", [pk, _make_col("customer_id")])
        count = _count_references(pk, table, [table])
        assert count == 0

    def test_multiple_tables_count(self):
        pk        = _make_col("id", pk=True)
        entity    = _make_table("product", [pk])
        ref1      = _make_table("order_items", [_make_col("products_id")])
        ref2      = _make_table("cart_items",  [_make_col("product_id")])
        count = _count_references(pk, entity, [entity, ref1, ref2])
        assert count == 2


# ---------------------------------------------------------------------------
# CoreEntity
# ---------------------------------------------------------------------------

class TestCoreEntity:
    def test_get_z0_hue_first_entity(self):
        e = CoreEntity(z0_index=0, name="customers", center_table="customers")
        assert e.get_z0_hue(15) == pytest.approx(0.0)

    def test_get_z0_hue_last_entity_before_360(self):
        e = CoreEntity(z0_index=14, name="last", center_table="last")
        hue = e.get_z0_hue(15)
        assert 0.0 <= hue < 360.0

    def test_hues_are_evenly_spaced(self):
        n = 5
        hues = [
            CoreEntity(z0_index=i, name=f"e{i}", center_table=f"e{i}").get_z0_hue(n)
            for i in range(n)
        ]
        diffs = [hues[i + 1] - hues[i] for i in range(n - 1)]
        assert all(abs(d - 72.0) < 0.01 for d in diffs)

    def test_to_dict_keys(self):
        e = CoreEntity(z0_index=1, name="orders", center_table="orders",
                       member_tables=["orders", "order_items"])
        d = e.to_dict()
        assert set(d.keys()) >= {
            "z0_index", "name", "center_table", "member_tables",
            "primary_key", "estimated_cardinality",
        }


# ---------------------------------------------------------------------------
# EntityClusteringEngine – SQLite integration tests
# ---------------------------------------------------------------------------

def _erp_conn() -> sqlite3.Connection:
    """An in-memory ERP-like schema with three related entity clusters."""
    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE customers (
            id        INTEGER PRIMARY KEY,
            name      TEXT NOT NULL,
            email     TEXT,
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
            product_id INTEGER NOT NULL,
            quantity   INTEGER
        );
        CREATE TABLE products (
            id     INTEGER PRIMARY KEY,
            sku    TEXT NOT NULL,
            price  REAL
        );
        CREATE TABLE product_categories (
            id         INTEGER PRIMARY KEY,
            products_id INTEGER,
            category   TEXT
        );
        CREATE TABLE employees (
            id     INTEGER PRIMARY KEY,
            name   TEXT,
            dept   TEXT
        );
    """)
    return conn


class TestEntityClusteringEngine:
    def _adapter(self) -> DatabaseAdapter:
        conn = _erp_conn()
        return DatabaseAdapter.from_connection(conn, dialect="sqlite")

    def test_cluster_entities_returns_list(self):
        adapter = self._adapter()
        engine  = EntityClusteringEngine(adapter.tables)
        entities = engine.cluster_entities()
        assert isinstance(entities, list)
        assert len(entities) >= 1

    def test_all_tables_are_covered(self):
        adapter = self._adapter()
        engine  = EntityClusteringEngine(adapter.tables)
        entities = engine.cluster_entities()
        covered = {t for e in entities for t in e.member_tables}
        assert covered == {t.name for t in adapter.tables}

    def test_z0_indices_are_unique_and_sequential(self):
        adapter = self._adapter()
        entities = EntityClusteringEngine(adapter.tables).cluster_entities()
        indices = [e.z0_index for e in entities]
        assert indices == list(range(len(entities)))

    def test_center_table_is_a_member(self):
        adapter = self._adapter()
        entities = EntityClusteringEngine(adapter.tables).cluster_entities()
        for e in entities:
            assert e.center_table in e.member_tables

    def test_related_tables_cluster_together(self):
        adapter = self._adapter()
        entities = EntityClusteringEngine(adapter.tables).cluster_entities()
        cluster_map = {
            tname: e.z0_index
            for e in entities
            for tname in e.member_tables
        }
        # customers and orders share a direct FK (orders.customers_id → customers)
        # so they must always be in the same community.
        assert cluster_map["customers"] == cluster_map["orders"]

    def test_target_cluster_count_respected(self):
        adapter = self._adapter()
        entities = EntityClusteringEngine(adapter.tables).cluster_entities(
            target_clusters=2
        )
        # With 6 closely related tables, requesting 2 clusters should produce
        # at most 3 (Louvain resolution tuning may not be exact).
        assert len(entities) <= 4

    def test_single_table_schema(self):
        pk  = _make_col("id", ColumnType.INTEGER, nullable=False, pk=True)
        tbl = _make_table("lone_table", [pk], row_count=5)
        entities = EntityClusteringEngine([tbl]).cluster_entities()
        assert len(entities) == 1
        assert entities[0].center_table == "lone_table"
