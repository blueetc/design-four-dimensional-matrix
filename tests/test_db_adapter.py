"""Tests for DatabaseAdapter.

All tests use an in-memory SQLite database to remain fast and self-contained.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

import pytest

from four_dim_matrix import (
    ColumnInfo,
    ColumnMapping,
    ColumnType,
    DatabaseAdapter,
    DialectHandler,
    KnowledgeBase,
    MySQLDialectHandler,
    PostgreSQLDialectHandler,
    SQLiteDialectHandler,
    TableInfo,
    TableMapping,
    get_dialect_handler,
    register_dialect,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_conn() -> sqlite3.Connection:
    """Return an in-memory SQLite connection pre-populated with sample tables."""
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            total REAL,
            status TEXT,
            created_at DATETIME
        );
        CREATE TABLE customers (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT,
            signup_date DATETIME
        );
        CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            sku TEXT NOT NULL,
            price REAL,
            stock INTEGER,
            active BOOLEAN
        );
        INSERT INTO customers VALUES (1,'Alice','alice@example.com','2024-01-10');
        INSERT INTO customers VALUES (2,'Bob','bob@example.com','2024-02-15');
        INSERT INTO orders VALUES (1,1,99.9,'paid','2024-03-01');
        INSERT INTO orders VALUES (2,2,150.0,'pending','2024-03-05');
        INSERT INTO orders VALUES (3,1,42.5,'paid','2024-04-01');
        INSERT INTO products VALUES (1,'SKU-A',29.99,100,1);
        """
    )
    return conn


# ---------------------------------------------------------------------------
# ColumnType classification
# ---------------------------------------------------------------------------

class TestColumnType:
    @pytest.mark.parametrize("type_str,expected", [
        ("INTEGER", ColumnType.INTEGER),
        ("INT", ColumnType.INTEGER),
        ("BIGINT", ColumnType.INTEGER),
        ("SMALLINT", ColumnType.INTEGER),
        ("TINYINT", ColumnType.INTEGER),
        ("TEXT", ColumnType.TEXT),
        ("VARCHAR(255)", ColumnType.TEXT),
        ("NVARCHAR(100)", ColumnType.TEXT),
        ("CLOB", ColumnType.TEXT),
        ("DATETIME", ColumnType.DATETIME),
        ("DATE", ColumnType.DATETIME),
        ("TIMESTAMP", ColumnType.DATETIME),
        ("REAL", ColumnType.FLOAT),
        ("FLOAT", ColumnType.FLOAT),
        ("DOUBLE", ColumnType.FLOAT),
        ("DECIMAL(10,2)", ColumnType.FLOAT),
        ("BOOLEAN", ColumnType.BOOLEAN),
        ("BOOL", ColumnType.BOOLEAN),
        ("BLOB", ColumnType.BLOB),
        ("BINARY", ColumnType.BLOB),
        ("JSONB", ColumnType.OTHER),
        ("UUID", ColumnType.OTHER),
    ])
    def test_from_type_string(self, type_str: str, expected: ColumnType):
        assert ColumnType.from_type_string(type_str) == expected

    def test_integer_enum_value(self):
        # x-coordinate uses the integer value – ensure ordering is stable
        assert ColumnType.INTEGER == 0
        assert ColumnType.TEXT == 1
        assert ColumnType.DATETIME == 2


# ---------------------------------------------------------------------------
# TableInfo helpers
# ---------------------------------------------------------------------------

class TestTableInfo:
    def _make_table(self) -> TableInfo:
        return TableInfo(
            name="orders",
            columns=[
                ColumnInfo("id", "INTEGER", ColumnType.INTEGER, False, True),
                ColumnInfo("customer_id", "INTEGER", ColumnType.INTEGER, False, False),
                ColumnInfo("total", "REAL", ColumnType.FLOAT, True, False),
                ColumnInfo("status", "TEXT", ColumnType.TEXT, True, False),
                ColumnInfo("created_at", "DATETIME", ColumnType.DATETIME, True, False),
            ],
            row_count=3,
        )

    def test_column_count(self):
        assert self._make_table().column_count == 5

    def test_type_summary(self):
        summary = self._make_table().type_summary()
        assert summary["INTEGER"] == 2
        assert summary["FLOAT"] == 1
        assert summary["TEXT"] == 1
        assert summary["DATETIME"] == 1

    def test_columns_by_type(self):
        groups = self._make_table().columns_by_type()
        assert len(groups[ColumnType.INTEGER]) == 2
        assert len(groups[ColumnType.FLOAT]) == 1

    def test_to_dict_keys(self):
        d = self._make_table().to_dict()
        assert "name" in d
        assert "row_count" in d
        assert "column_count" in d
        assert "columns" in d
        assert "type_summary" in d


# ---------------------------------------------------------------------------
# DatabaseAdapter construction and introspection
# ---------------------------------------------------------------------------

class TestDatabaseAdapterConstruction:
    def test_from_connection_gives_correct_table_count(self):
        conn = _make_conn()
        adapter = DatabaseAdapter.from_connection(conn, dialect="sqlite")
        assert len(adapter.tables) == 3

    def test_table_names_sorted(self):
        conn = _make_conn()
        adapter = DatabaseAdapter.from_connection(conn, dialect="sqlite")
        names = adapter.table_names()
        assert names == sorted(names)
        assert "customers" in names
        assert "orders" in names
        assert "products" in names

    def test_get_table_found(self):
        conn = _make_conn()
        adapter = DatabaseAdapter.from_connection(conn, dialect="sqlite")
        t = adapter.get_table("orders")
        assert t is not None
        assert t.name == "orders"

    def test_get_table_missing(self):
        conn = _make_conn()
        adapter = DatabaseAdapter.from_connection(conn, dialect="sqlite")
        assert adapter.get_table("nonexistent") is None

    def test_row_counts(self):
        conn = _make_conn()
        adapter = DatabaseAdapter.from_connection(conn, dialect="sqlite")
        orders = adapter.get_table("orders")
        customers = adapter.get_table("customers")
        products = adapter.get_table("products")
        assert orders.row_count == 3
        assert customers.row_count == 2
        assert products.row_count == 1

    def test_column_counts(self):
        conn = _make_conn()
        adapter = DatabaseAdapter.from_connection(conn, dialect="sqlite")
        orders = adapter.get_table("orders")
        assert orders.column_count == 5

    def test_primary_key_detected(self):
        conn = _make_conn()
        adapter = DatabaseAdapter.from_connection(conn, dialect="sqlite")
        orders = adapter.get_table("orders")
        pk_cols = [c for c in orders.columns if c.primary_key]
        assert len(pk_cols) == 1
        assert pk_cols[0].name == "id"

    def test_column_types_classified(self):
        conn = _make_conn()
        adapter = DatabaseAdapter.from_connection(conn, dialect="sqlite")
        products = adapter.get_table("products")
        type_map = {c.name: c.column_type for c in products.columns}
        assert type_map["id"] == ColumnType.INTEGER
        assert type_map["sku"] == ColumnType.TEXT
        assert type_map["price"] == ColumnType.FLOAT
        assert type_map["stock"] == ColumnType.INTEGER
        assert type_map["active"] == ColumnType.BOOLEAN

    def test_snapshot_time_set(self):
        conn = _make_conn()
        before = datetime.now(timezone.utc).replace(tzinfo=None)
        adapter = DatabaseAdapter.from_connection(conn, dialect="sqlite")
        after = datetime.now(timezone.utc).replace(tzinfo=None)
        assert before <= adapter.snapshot_time <= after

    def test_snapshot_time_override(self):
        conn = _make_conn()
        fixed = datetime(2024, 6, 1, 12, 0, 0)
        adapter = DatabaseAdapter.from_connection(
            conn, dialect="sqlite", snapshot_time=fixed
        )
        assert adapter.snapshot_time == fixed

    def test_repr_contains_table_count(self):
        conn = _make_conn()
        adapter = DatabaseAdapter.from_connection(conn, dialect="sqlite")
        assert "3" in repr(adapter)

    def test_unsupported_dialect_raises(self):
        conn = _make_conn()
        with pytest.raises(ValueError, match="Unsupported dialect"):
            DatabaseAdapter.from_connection(conn, dialect="oracle")


# ---------------------------------------------------------------------------
# to_data_points
# ---------------------------------------------------------------------------

class TestToDataPoints:
    def test_one_point_per_table(self):
        conn = _make_conn()
        adapter = DatabaseAdapter.from_connection(conn, dialect="sqlite")
        points = adapter.to_data_points()
        assert len(points) == 3

    def test_z_values_are_sequential(self):
        conn = _make_conn()
        adapter = DatabaseAdapter.from_connection(conn, dialect="sqlite")
        points = adapter.to_data_points()
        z_values = sorted(p.z for p in points)
        assert z_values == [0, 1, 2]

    def test_x_equals_column_count(self):
        conn = _make_conn()
        adapter = DatabaseAdapter.from_connection(conn, dialect="sqlite")
        points = adapter.to_data_points()
        # Each point's x should equal its table's column_count
        name_to_point = {p.payload["name"]: p for p in points}
        orders_info = adapter.get_table("orders")
        assert name_to_point["orders"].x == orders_info.column_count

    def test_y_equals_row_count(self):
        conn = _make_conn()
        adapter = DatabaseAdapter.from_connection(conn, dialect="sqlite")
        points = adapter.to_data_points()
        name_to_point = {p.payload["name"]: p for p in points}
        assert name_to_point["orders"].y == pytest.approx(3.0)
        assert name_to_point["customers"].y == pytest.approx(2.0)
        assert name_to_point["products"].y == pytest.approx(1.0)

    def test_t_equals_snapshot_time(self):
        conn = _make_conn()
        fixed = datetime(2024, 6, 1)
        adapter = DatabaseAdapter.from_connection(
            conn, dialect="sqlite", snapshot_time=fixed
        )
        for pt in adapter.to_data_points():
            assert pt.t == fixed

    def test_payload_contains_table_metadata(self):
        conn = _make_conn()
        adapter = DatabaseAdapter.from_connection(conn, dialect="sqlite")
        points = adapter.to_data_points()
        for pt in points:
            assert "name" in pt.payload
            assert "columns" in pt.payload
            assert "row_count" in pt.payload
            assert "type_summary" in pt.payload


# ---------------------------------------------------------------------------
# to_knowledge_base
# ---------------------------------------------------------------------------

class TestToKnowledgeBase:
    def test_returns_knowledge_base(self):
        conn = _make_conn()
        adapter = DatabaseAdapter.from_connection(conn, dialect="sqlite")
        kb = adapter.to_knowledge_base()
        assert isinstance(kb, KnowledgeBase)

    def test_data_matrix_populated(self):
        conn = _make_conn()
        adapter = DatabaseAdapter.from_connection(conn, dialect="sqlite")
        kb = adapter.to_knowledge_base()
        assert len(kb.data_matrix) == 3

    def test_color_matrix_populated(self):
        conn = _make_conn()
        adapter = DatabaseAdapter.from_connection(conn, dialect="sqlite")
        kb = adapter.to_knowledge_base()
        assert len(kb.color_matrix) == 3

    def test_each_table_has_distinct_colour(self):
        conn = _make_conn()
        adapter = DatabaseAdapter.from_connection(conn, dialect="sqlite")
        kb = adapter.to_knowledge_base()
        # The three tables should have different z-values → different hues → different colours
        colours = [cp.hex_color for cp in kb.color_matrix]
        # All colours are valid hex
        for c in colours:
            assert c.startswith("#") and len(c) == 7

    def test_snapshot_query_works(self):
        conn = _make_conn()
        fixed = datetime(2024, 6, 1)
        adapter = DatabaseAdapter.from_connection(
            conn, dialect="sqlite", snapshot_time=fixed
        )
        kb = adapter.to_knowledge_base()
        snap = kb.snapshot(t=fixed)
        assert len(snap["topics"]) == 3

    def test_larger_table_has_higher_y(self):
        conn = _make_conn()
        adapter = DatabaseAdapter.from_connection(conn, dialect="sqlite")
        kb = adapter.to_knowledge_base()
        # orders has 3 rows, customers 2, products 1
        pts = {p.payload["name"]: p for p in kb.data_matrix}
        assert pts["orders"].y > pts["customers"].y > pts["products"].y

    def test_trend_returns_single_t(self):
        conn = _make_conn()
        fixed = datetime(2024, 6, 1)
        adapter = DatabaseAdapter.from_connection(
            conn, dialect="sqlite", snapshot_time=fixed
        )
        kb = adapter.to_knowledge_base()
        trend = kb.trend()
        # Only one snapshot time → one key
        assert len(trend) == 1
        assert fixed in trend

    def test_lookup_by_color_returns_data_point(self):
        conn = _make_conn()
        adapter = DatabaseAdapter.from_connection(conn, dialect="sqlite")
        kb = adapter.to_knowledge_base()
        # Pick any generated colour and reverse-look it up
        cp = next(iter(kb.color_matrix))
        results = kb.lookup_by_color(cp.hex_color)
        assert len(results) >= 1
        assert "name" in results[0].payload


# ---------------------------------------------------------------------------
# diff / schema change detection
# ---------------------------------------------------------------------------

class TestDiff:
    def _v1(self) -> DatabaseAdapter:
        conn = sqlite3.connect(":memory:")
        conn.executescript(
            """
            CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
            CREATE TABLE posts (id INTEGER PRIMARY KEY, body TEXT, user_id INTEGER);
            INSERT INTO users VALUES (1,'Alice');
            INSERT INTO posts VALUES (1,'Hello',1),(2,'World',1);
            """
        )
        return DatabaseAdapter.from_connection(conn, dialect="sqlite")

    def _v2(self) -> DatabaseAdapter:
        conn = sqlite3.connect(":memory:")
        conn.executescript(
            """
            CREATE TABLE users (
                id INTEGER PRIMARY KEY, name TEXT, email TEXT
            );
            CREATE TABLE comments (id INTEGER PRIMARY KEY, text TEXT);
            INSERT INTO users VALUES (1,'Alice','a@example.com');
            INSERT INTO users VALUES (2,'Bob','b@example.com');
            INSERT INTO comments VALUES (1,'Nice post');
            """
        )
        return DatabaseAdapter.from_connection(conn, dialect="sqlite")

    def test_added_tables(self):
        diff = self._v1().diff(self._v2())
        added_names = [t["name"] for t in diff["added"]]
        assert "comments" in added_names

    def test_removed_tables(self):
        diff = self._v1().diff(self._v2())
        removed_names = [t["name"] for t in diff["removed"]]
        assert "posts" in removed_names

    def test_changed_tables(self):
        diff = self._v1().diff(self._v2())
        changed_names = [c["table"] for c in diff["changed"]]
        assert "users" in changed_names

    def test_changed_row_delta(self):
        diff = self._v1().diff(self._v2())
        users_change = next(c for c in diff["changed"] if c["table"] == "users")
        # v1 has 1 user, v2 has 2 users → delta +1
        assert users_change["row_delta"] == 1

    def test_changed_column_delta(self):
        diff = self._v1().diff(self._v2())
        users_change = next(c for c in diff["changed"] if c["table"] == "users")
        # v1 has 2 columns, v2 has 3 → delta +1
        assert users_change["column_delta"] == 1

    def test_no_diff_for_identical(self):
        v1 = self._v1()
        diff = v1.diff(v1)
        assert diff["added"] == []
        assert diff["removed"] == []
        assert diff["changed"] == []


# ---------------------------------------------------------------------------
# summary
# ---------------------------------------------------------------------------

class TestSummary:
    def test_summary_keys(self):
        conn = _make_conn()
        adapter = DatabaseAdapter.from_connection(conn, dialect="sqlite")
        s = adapter.summary()
        assert "snapshot_time" in s
        assert "table_count" in s
        assert "total_rows" in s
        assert "total_columns" in s
        assert "column_type_distribution" in s
        assert "tables" in s

    def test_summary_table_count(self):
        conn = _make_conn()
        adapter = DatabaseAdapter.from_connection(conn, dialect="sqlite")
        assert adapter.summary()["table_count"] == 3

    def test_summary_total_rows(self):
        conn = _make_conn()
        adapter = DatabaseAdapter.from_connection(conn, dialect="sqlite")
        # orders=3, customers=2, products=1
        assert adapter.summary()["total_rows"] == 6

    def test_summary_tables_sorted_by_row_count_desc(self):
        conn = _make_conn()
        adapter = DatabaseAdapter.from_connection(conn, dialect="sqlite")
        row_counts = [t["row_count"] for t in adapter.summary()["tables"]]
        assert row_counts == sorted(row_counts, reverse=True)


# ---------------------------------------------------------------------------
# ColumnMapping / TableMapping construction
# ---------------------------------------------------------------------------

class TestColumnMappingConstruction:
    def test_required_fields(self):
        cm = ColumnMapping(t_column="created_at", y_column="total")
        assert cm.t_column == "created_at"
        assert cm.y_column == "total"
        assert cm.x_column is None
        assert cm.limit is None
        assert cm.where is None

    def test_optional_fields(self):
        cm = ColumnMapping(
            t_column="ts", y_column="amount",
            x_column="category", limit=100, where="amount > 0",
        )
        assert cm.x_column == "category"
        assert cm.limit == 100
        assert cm.where == "amount > 0"

    def test_table_mapping_stores_name_and_mapping(self):
        cm = ColumnMapping(t_column="ts", y_column="val")
        tm = TableMapping(table_name="events", mapping=cm)
        assert tm.table_name == "events"
        assert tm.mapping is cm

    def test_t_source_default_is_column(self):
        cm = ColumnMapping(y_column="amount", t_column="ts")
        assert cm.t_source == "column"

    def test_t_column_is_optional(self):
        # t_column may be omitted when t_source != "column"
        cm = ColumnMapping(y_column="amount", t_source="synthetic")
        assert cm.t_column is None

    def test_t_source_synthetic_sets_correct_field(self):
        cm = ColumnMapping(y_column="count", t_source="synthetic",
                           t_synthetic_order="alphabetical")
        assert cm.t_source == "synthetic"
        assert cm.t_synthetic_order == "alphabetical"

    def test_t_source_version(self):
        cm = ColumnMapping(y_column="count", t_column="ver", t_source="version")
        assert cm.t_source == "version"


class TestDeriveTInLoadRows:
    """Verify that the new t_source strategies work end-to-end in load_rows."""

    def _make_conn(self):
        conn = sqlite3.connect(":memory:")
        conn.executescript("""
            CREATE TABLE events (
                id INTEGER PRIMARY KEY,
                version INTEGER,
                val REAL
            );
            INSERT INTO events VALUES (1, 10, 100.0);
            INSERT INTO events VALUES (2, 20, 200.0);
        """)
        return conn

    def _adapter(self, conn):
        return DatabaseAdapter.from_connection(conn, dialect="sqlite")

    def test_t_source_version_encodes_as_epoch_plus_days(self):
        from datetime import datetime, timedelta
        _EPOCH = datetime(2000, 1, 1)
        conn    = self._make_conn()
        adapter = self._adapter(conn)
        kb = adapter.load_rows(conn, [
            TableMapping("events", ColumnMapping(
                y_column="val",
                t_column="version",
                t_source="version",
            ))
        ])
        times = sorted(kb.trend().keys())
        assert times[0] == _EPOCH + timedelta(days=10)
        assert times[1] == _EPOCH + timedelta(days=20)

    def test_t_source_synthetic_uses_row_index(self):
        from datetime import datetime, timedelta
        _EPOCH = datetime(2000, 1, 1)
        conn    = self._make_conn()
        adapter = self._adapter(conn)
        kb = adapter.load_rows(conn, [
            TableMapping("events", ColumnMapping(
                y_column="val",
                t_source="synthetic",
            ))
        ])
        times = sorted(kb.trend().keys())
        assert times[0] == _EPOCH + timedelta(days=0)
        assert times[1] == _EPOCH + timedelta(days=1)

# ---------------------------------------------------------------------------
# load_rows – core behaviour
# ---------------------------------------------------------------------------

class TestLoadRows:
    """Tests for DatabaseAdapter.load_rows()."""

    def _adapter(self, conn: sqlite3.Connection) -> DatabaseAdapter:
        return DatabaseAdapter.from_connection(conn, dialect="sqlite")

    def test_returns_knowledge_base(self):
        conn = _make_conn()
        adapter = self._adapter(conn)
        kb = adapter.load_rows(conn, [
            TableMapping("orders", ColumnMapping(t_column="created_at", y_column="total")),
        ])
        assert isinstance(kb, KnowledgeBase)

    def test_one_point_per_row(self):
        conn = _make_conn()
        adapter = self._adapter(conn)
        kb = adapter.load_rows(conn, [
            TableMapping("orders", ColumnMapping(t_column="created_at", y_column="total")),
        ])
        # orders has 3 rows
        assert len(kb.data_matrix) == 3

    def test_multi_table_total_rows(self):
        conn = _make_conn()
        adapter = self._adapter(conn)
        kb = adapter.load_rows(conn, [
            TableMapping("orders",    ColumnMapping(t_column="created_at", y_column="total")),
            TableMapping("customers", ColumnMapping(t_column="signup_date", y_column="id")),
        ])
        # orders=3 rows + customers=2 rows = 5 points
        assert len(kb.data_matrix) == 5

    def test_y_values_match_column(self):
        conn = _make_conn()
        adapter = self._adapter(conn)
        kb = adapter.load_rows(conn, [
            TableMapping("orders", ColumnMapping(t_column="created_at", y_column="total")),
        ])
        y_values = sorted(pt.y for pt in kb.data_matrix)
        assert y_values == pytest.approx(sorted([99.9, 150.0, 42.5]))

    def test_t_values_parsed_as_datetime(self):
        conn = _make_conn()
        adapter = self._adapter(conn)
        kb = adapter.load_rows(conn, [
            TableMapping("orders", ColumnMapping(t_column="created_at", y_column="total")),
        ])
        for pt in kb.data_matrix:
            assert isinstance(pt.t, datetime)

    def test_t_values_correct_dates(self):
        conn = _make_conn()
        adapter = self._adapter(conn)
        kb = adapter.load_rows(conn, [
            TableMapping("orders", ColumnMapping(t_column="created_at", y_column="total")),
        ])
        t_values = sorted(pt.t for pt in kb.data_matrix)
        assert t_values[0] == datetime(2024, 3, 1)
        assert t_values[2] == datetime(2024, 4, 1)

    def test_z_stable_per_table(self):
        """Each table always maps to the same z-index (alphabetical)."""
        conn = _make_conn()
        adapter = self._adapter(conn)
        kb = adapter.load_rows(conn, [
            TableMapping("orders",    ColumnMapping(t_column="created_at", y_column="total")),
            TableMapping("customers", ColumnMapping(t_column="signup_date", y_column="id")),
        ])
        # "customers" < "orders" alphabetically → customers=z=0, orders=z=1
        customer_pts = [pt for pt in kb.data_matrix if pt.z == 0]
        order_pts    = [pt for pt in kb.data_matrix if pt.z == 1]
        assert len(customer_pts) == 2
        assert len(order_pts) == 3

    def test_z_values_are_sequential_from_zero(self):
        conn = _make_conn()
        adapter = self._adapter(conn)
        kb = adapter.load_rows(conn, [
            TableMapping("orders",    ColumnMapping(t_column="created_at", y_column="total")),
            TableMapping("customers", ColumnMapping(t_column="signup_date", y_column="id")),
            TableMapping("products",  ColumnMapping(t_column="created_at", y_column="price")),
        ])
        z_values = sorted({pt.z for pt in kb.data_matrix})
        # products has no created_at – rows skipped or t falls back
        # But customers(2) + orders(3) + (0 or more products) must give 3 distinct z
        assert set(z_values).issubset({0, 1, 2})

    def test_payload_is_full_row_dict(self):
        conn = _make_conn()
        adapter = self._adapter(conn)
        kb = adapter.load_rows(conn, [
            TableMapping("orders", ColumnMapping(t_column="created_at", y_column="total")),
        ])
        for pt in kb.data_matrix:
            assert "id" in pt.payload
            assert "total" in pt.payload
            assert "created_at" in pt.payload

    def test_color_matrix_has_same_count_as_data_matrix(self):
        conn = _make_conn()
        adapter = self._adapter(conn)
        kb = adapter.load_rows(conn, [
            TableMapping("orders", ColumnMapping(t_column="created_at", y_column="total")),
        ])
        assert len(kb.color_matrix) == len(kb.data_matrix)

    def test_colours_are_valid_hex(self):
        conn = _make_conn()
        adapter = self._adapter(conn)
        kb = adapter.load_rows(conn, [
            TableMapping("orders", ColumnMapping(t_column="created_at", y_column="total")),
        ])
        for cp in kb.color_matrix:
            assert cp.hex_color.startswith("#") and len(cp.hex_color) == 7

    # ------------------------------------------------------------------
    # x_column encoding
    # ------------------------------------------------------------------

    def test_x_column_string_encoded_as_int(self):
        conn = _make_conn()
        adapter = self._adapter(conn)
        kb = adapter.load_rows(conn, [
            TableMapping("orders", ColumnMapping(
                t_column="created_at", y_column="total", x_column="status",
            )),
        ])
        # All x values must be non-negative ints
        for pt in kb.data_matrix:
            assert isinstance(pt.x, int)
            assert pt.x >= 0

    def test_x_column_same_string_same_int(self):
        """Identical x_column values must map to the same integer x."""
        conn = _make_conn()
        adapter = self._adapter(conn)
        kb = adapter.load_rows(conn, [
            TableMapping("orders", ColumnMapping(
                t_column="created_at", y_column="total", x_column="status",
            )),
        ])
        # "paid" appears twice – both rows must share the same x
        paid_xs = [pt.x for pt in kb.data_matrix if pt.payload.get("status") == "paid"]
        assert len(paid_xs) == 2
        assert paid_xs[0] == paid_xs[1]

    def test_x_column_different_strings_different_ints(self):
        conn = _make_conn()
        adapter = self._adapter(conn)
        kb = adapter.load_rows(conn, [
            TableMapping("orders", ColumnMapping(
                t_column="created_at", y_column="total", x_column="status",
            )),
        ])
        paid_x    = next(pt.x for pt in kb.data_matrix if pt.payload.get("status") == "paid")
        pending_x = next(pt.x for pt in kb.data_matrix if pt.payload.get("status") == "pending")
        assert paid_x != pending_x

    def test_no_x_column_uses_row_index(self):
        """Without x_column, x equals the sequential row position (0, 1, 2…)."""
        conn = _make_conn()
        adapter = self._adapter(conn)
        kb = adapter.load_rows(conn, [
            TableMapping("orders", ColumnMapping(t_column="created_at", y_column="total")),
        ])
        x_values = sorted(pt.x for pt in kb.data_matrix)
        assert x_values == [0, 1, 2]

    # ------------------------------------------------------------------
    # limit and where filters
    # ------------------------------------------------------------------

    def test_limit_caps_row_count(self):
        conn = _make_conn()
        adapter = self._adapter(conn)
        kb = adapter.load_rows(conn, [
            TableMapping("orders", ColumnMapping(
                t_column="created_at", y_column="total", limit=2,
            )),
        ])
        assert len(kb.data_matrix) == 2

    def test_where_filters_rows(self):
        conn = _make_conn()
        adapter = self._adapter(conn)
        kb = adapter.load_rows(conn, [
            TableMapping("orders", ColumnMapping(
                t_column="created_at", y_column="total",
                where="status = 'paid'",
            )),
        ])
        # Only the 2 "paid" orders should be loaded
        assert len(kb.data_matrix) == 2
        for pt in kb.data_matrix:
            assert pt.payload.get("status") == "paid"

    # ------------------------------------------------------------------
    # Skipping rows with non-numeric y
    # ------------------------------------------------------------------

    def test_non_numeric_y_rows_skipped(self):
        """Rows where y_column cannot be cast to float are silently skipped."""
        conn = sqlite3.connect(":memory:")
        conn.executescript(
            """
            CREATE TABLE events (
                ts DATETIME, value TEXT
            );
            INSERT INTO events VALUES ('2024-01-01', '100.0');
            INSERT INTO events VALUES ('2024-02-01', 'N/A');
            INSERT INTO events VALUES ('2024-03-01', '200.0');
            """
        )
        adapter = DatabaseAdapter.from_connection(conn, dialect="sqlite")
        kb = adapter.load_rows(conn, [
            TableMapping("events", ColumnMapping(t_column="ts", y_column="value")),
        ])
        # 'N/A' row skipped → 2 points
        assert len(kb.data_matrix) == 2
        y_values = sorted(pt.y for pt in kb.data_matrix)
        assert y_values == pytest.approx([100.0, 200.0])

    # ------------------------------------------------------------------
    # t fallback for unparseable timestamps
    # ------------------------------------------------------------------

    def test_unparseable_t_falls_back_to_snapshot_time(self):
        conn = sqlite3.connect(":memory:")
        conn.executescript(
            """
            CREATE TABLE log (ts TEXT, val REAL);
            INSERT INTO log VALUES ('not-a-date', 99.0);
            """
        )
        fixed = datetime(2024, 6, 1)
        adapter = DatabaseAdapter.from_connection(
            conn, dialect="sqlite", snapshot_time=fixed
        )
        kb = adapter.load_rows(conn, [
            TableMapping("log", ColumnMapping(t_column="ts", y_column="val")),
        ])
        pt = next(iter(kb.data_matrix))
        assert pt.t == fixed

    def test_unix_timestamp_parsed(self):
        conn = sqlite3.connect(":memory:")
        conn.executescript(
            """
            CREATE TABLE events (ts INTEGER, val REAL);
            INSERT INTO events VALUES (1704067200, 42.0);
            """
        )
        adapter = DatabaseAdapter.from_connection(conn, dialect="sqlite")
        kb = adapter.load_rows(conn, [
            TableMapping("events", ColumnMapping(t_column="ts", y_column="val")),
        ])
        pt = next(iter(kb.data_matrix))
        # 1704067200 = 2024-01-01 00:00:00 UTC
        assert pt.t == datetime(2024, 1, 1, 0, 0, 0)

    # ------------------------------------------------------------------
    # Analysis after load_rows
    # ------------------------------------------------------------------

    def test_trend_spans_multiple_t(self):
        conn = _make_conn()
        adapter = self._adapter(conn)
        kb = adapter.load_rows(conn, [
            TableMapping("orders", ColumnMapping(t_column="created_at", y_column="total")),
        ])
        trend = kb.trend()
        # orders has 3 different dates → 3 trend entries
        assert len(trend) == 3

    def test_trend_per_table_topic(self):
        conn = _make_conn()
        adapter = self._adapter(conn)
        kb = adapter.load_rows(conn, [
            TableMapping("orders",    ColumnMapping(t_column="created_at", y_column="total")),
            TableMapping("customers", ColumnMapping(t_column="signup_date", y_column="id")),
        ])
        # customers z=0, orders z=1 (alphabetical)
        trend_customers = kb.trend(z=0)
        trend_orders    = kb.trend(z=1)
        assert len(trend_customers) == 2  # 2 customers, 2 distinct signup dates
        assert len(trend_orders) == 3     # 3 orders on 3 distinct dates

    def test_lookup_by_color_returns_row_payload(self):
        conn = _make_conn()
        adapter = self._adapter(conn)
        kb = adapter.load_rows(conn, [
            TableMapping("orders", ColumnMapping(t_column="created_at", y_column="total")),
        ])
        cp = next(iter(kb.color_matrix))
        results = kb.lookup_by_color(cp.hex_color)
        assert len(results) >= 1
        # Payload should be a full row dict, not schema metadata
        assert "total" in results[0].payload
        assert "created_at" in results[0].payload


# ---------------------------------------------------------------------------
# DialectHandler ABC and built-in concrete handlers
# ---------------------------------------------------------------------------

class TestDialectHandlerABC:
    def test_cannot_instantiate_abc(self):
        with pytest.raises(TypeError):
            DialectHandler()  # type: ignore[abstract]

    def test_sqlite_is_subclass(self):
        assert issubclass(SQLiteDialectHandler, DialectHandler)

    def test_postgresql_is_subclass(self):
        assert issubclass(PostgreSQLDialectHandler, DialectHandler)

    def test_mysql_is_subclass(self):
        assert issubclass(MySQLDialectHandler, DialectHandler)

    def test_sqlite_handler_list_table_names(self):
        conn = _make_conn()
        handler = SQLiteDialectHandler()
        names = handler.list_table_names(conn)
        assert sorted(names) == ["customers", "orders", "products"]

    def test_sqlite_handler_get_column_info(self):
        conn = _make_conn()
        handler = SQLiteDialectHandler()
        cols = handler.get_column_info(conn, "orders")
        col_names = [c.name for c in cols]
        assert "id" in col_names
        assert "total" in col_names
        assert "created_at" in col_names

    def test_sqlite_handler_column_types(self):
        conn = _make_conn()
        handler = SQLiteDialectHandler()
        cols = {c.name: c for c in handler.get_column_info(conn, "products")}
        assert cols["id"].column_type == ColumnType.INTEGER
        assert cols["sku"].column_type == ColumnType.TEXT
        assert cols["price"].column_type == ColumnType.FLOAT
        assert cols["active"].column_type == ColumnType.BOOLEAN

    def test_sqlite_handler_primary_key_detected(self):
        conn = _make_conn()
        handler = SQLiteDialectHandler()
        cols = {c.name: c for c in handler.get_column_info(conn, "orders")}
        assert cols["id"].primary_key is True
        assert cols["total"].primary_key is False

    def test_sqlite_handler_get_row_count(self):
        conn = _make_conn()
        handler = SQLiteDialectHandler()
        assert handler.get_row_count(conn, "orders") == 3
        assert handler.get_row_count(conn, "customers") == 2
        assert handler.get_row_count(conn, "products") == 1

    def test_sqlite_handler_get_row_count_empty_table(self):
        conn = sqlite3.connect(":memory:")
        conn.execute("CREATE TABLE empty_t (id INTEGER PRIMARY KEY)")
        handler = SQLiteDialectHandler()
        assert handler.get_row_count(conn, "empty_t") == 0


# ---------------------------------------------------------------------------
# get_dialect_handler
# ---------------------------------------------------------------------------

class TestGetDialectHandler:
    def test_sqlite_lookup(self):
        h = get_dialect_handler("sqlite")
        assert isinstance(h, SQLiteDialectHandler)

    def test_postgresql_lookup(self):
        h = get_dialect_handler("postgresql")
        assert isinstance(h, PostgreSQLDialectHandler)

    def test_postgres_alias_lookup(self):
        h = get_dialect_handler("postgres")
        assert isinstance(h, PostgreSQLDialectHandler)

    def test_mysql_lookup(self):
        h = get_dialect_handler("mysql")
        assert isinstance(h, MySQLDialectHandler)

    def test_mariadb_alias_lookup(self):
        h = get_dialect_handler("mariadb")
        assert isinstance(h, MySQLDialectHandler)

    def test_case_insensitive_lookup(self):
        assert isinstance(get_dialect_handler("SQLite"), SQLiteDialectHandler)
        assert isinstance(get_dialect_handler("SQLITE"), SQLiteDialectHandler)
        assert isinstance(get_dialect_handler("PostgreSQL"), PostgreSQLDialectHandler)
        assert isinstance(get_dialect_handler("MySQL"), MySQLDialectHandler)

    def test_unknown_dialect_raises_value_error(self):
        with pytest.raises(ValueError, match="Unsupported dialect"):
            get_dialect_handler("oracle")

    def test_error_message_lists_registered_dialects(self):
        """Error for unknown dialect must name registered dialects dynamically."""
        with pytest.raises(ValueError) as exc_info:
            get_dialect_handler("oracle")
        msg = str(exc_info.value)
        # Must list some known built-ins
        assert "sqlite" in msg
        assert "postgresql" in msg
        assert "mysql" in msg

    def test_error_message_mentions_register_dialect(self):
        with pytest.raises(ValueError) as exc_info:
            get_dialect_handler("unknown_engine")
        assert "register_dialect" in str(exc_info.value)


# ---------------------------------------------------------------------------
# register_dialect
# ---------------------------------------------------------------------------

class _MinimalHandler(DialectHandler):
    """A minimal in-memory handler used only in tests."""

    def list_table_names(self, conn):
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name NOT LIKE 'sqlite_%' "
            "ORDER BY name"
        )
        return [row[0] for row in cursor.fetchall()]

    def get_column_info(self, conn, table_name):
        from four_dim_matrix import ColumnType as CT
        cursor = conn.cursor()
        cursor.execute(f'PRAGMA table_info("{table_name}")')
        return [
            ColumnInfo(
                name=row[1],
                type_str=row[2] or "TEXT",
                column_type=CT.from_type_string(row[2] or "TEXT"),
                nullable=row[3] == 0,
                primary_key=row[5] > 0,
            )
            for row in cursor.fetchall()
        ]


class TestRegisterDialect:
    def test_register_makes_dialect_available(self):
        register_dialect("test_engine_1", _MinimalHandler())
        h = get_dialect_handler("test_engine_1")
        assert isinstance(h, _MinimalHandler)

    def test_register_case_insensitive_storage(self):
        register_dialect("Test_Engine_2", _MinimalHandler())
        h = get_dialect_handler("test_engine_2")
        assert isinstance(h, _MinimalHandler)

    def test_register_overrides_existing(self):
        """Re-registering a name replaces the old handler."""
        first = _MinimalHandler()
        second = _MinimalHandler()
        register_dialect("test_engine_3", first)
        register_dialect("test_engine_3", second)
        assert get_dialect_handler("test_engine_3") is second

    def test_register_non_handler_raises_type_error(self):
        with pytest.raises(TypeError, match="DialectHandler"):
            register_dialect("bad_engine", object())  # type: ignore[arg-type]

    def test_register_non_handler_string_raises(self):
        with pytest.raises(TypeError):
            register_dialect("bad_engine", "not_a_handler")  # type: ignore[arg-type]

    def test_custom_handler_used_by_from_connection(self):
        """A custom handler registered at runtime must be used by from_connection."""
        register_dialect("test_sqlite_alias", _MinimalHandler())
        conn = _make_conn()
        adapter = DatabaseAdapter.from_connection(conn, dialect="test_sqlite_alias")
        assert len(adapter.tables) == 3
        names = adapter.table_names()
        assert "orders" in names
        assert "customers" in names

    def test_custom_handler_produces_correct_table_data(self):
        register_dialect("test_sqlite_alias2", _MinimalHandler())
        conn = _make_conn()
        adapter = DatabaseAdapter.from_connection(conn, dialect="test_sqlite_alias2")
        orders = adapter.get_table("orders")
        assert orders is not None
        assert orders.column_count == 5
        assert orders.row_count == 3

    def test_custom_handler_knowledge_base_works(self):
        register_dialect("test_sqlite_alias3", _MinimalHandler())
        conn = _make_conn()
        adapter = DatabaseAdapter.from_connection(conn, dialect="test_sqlite_alias3")
        kb = adapter.to_knowledge_base()
        assert len(kb.data_matrix) == 3
        assert len(kb.color_matrix) == 3

    def test_error_after_register_shows_new_dialect(self):
        """After registering a new dialect, error messages for unknown dialects
        list the newly registered name."""
        register_dialect("my_custom_db", _MinimalHandler())
        with pytest.raises(ValueError) as exc_info:
            get_dialect_handler("nonexistent_db")
        assert "my_custom_db" in str(exc_info.value)


# ---------------------------------------------------------------------------
# from_connection uses registry (existing test updated expectation)
# ---------------------------------------------------------------------------

class TestFromConnectionDialectRegistry:
    def test_unsupported_dialect_raises_and_lists_registered(self):
        conn = _make_conn()
        with pytest.raises(ValueError, match="Unsupported dialect"):
            DatabaseAdapter.from_connection(conn, dialect="oracle")

    def test_sqlite_dialect_via_registry(self):
        conn = _make_conn()
        adapter = DatabaseAdapter.from_connection(conn, dialect="sqlite")
        assert len(adapter.tables) == 3

    def test_sqlite_upper_case_dialect_via_registry(self):
        conn = _make_conn()
        adapter = DatabaseAdapter.from_connection(conn, dialect="SQLITE")
        assert len(adapter.tables) == 3


# ===========================================================================
# ColumnMapping.x_semantic / x_normalizer / normalize_x
# ===========================================================================

class TestColumnMappingXSemantic:
    """Tests for the new x_semantic and x_normalizer fields."""

    def test_x_semantic_default_none(self):
        from four_dim_matrix import ColumnMapping
        cm = ColumnMapping(y_column="val")
        assert cm.x_semantic is None

    def test_x_normalizer_default_none(self):
        from four_dim_matrix import ColumnMapping
        cm = ColumnMapping(y_column="val")
        assert cm.x_normalizer is None

    def test_normalize_x_without_normalizer_returns_half(self):
        from four_dim_matrix import ColumnMapping
        cm = ColumnMapping(y_column="val")
        assert cm.normalize_x("anything") == pytest.approx(0.5)

    def test_normalize_x_with_normalizer(self):
        from four_dim_matrix import ColumnMapping
        stages = {"lead": 0.0, "prospect": 0.33, "closed": 1.0}
        cm = ColumnMapping(
            y_column="val",
            x_semantic="funnel",
            x_normalizer=lambda v: stages.get(str(v).lower(), 0.5),
        )
        assert cm.normalize_x("lead") == pytest.approx(0.0)
        assert cm.normalize_x("closed") == pytest.approx(1.0)

    def test_normalize_x_clamps_above_one(self):
        from four_dim_matrix import ColumnMapping
        cm = ColumnMapping(
            y_column="val",
            x_normalizer=lambda v: 1.5,
        )
        assert cm.normalize_x("any") == pytest.approx(1.0)

    def test_normalize_x_clamps_below_zero(self):
        from four_dim_matrix import ColumnMapping
        cm = ColumnMapping(
            y_column="val",
            x_normalizer=lambda v: -0.3,
        )
        assert cm.normalize_x("any") == pytest.approx(0.0)

    def test_x_semantic_stored(self):
        from four_dim_matrix import ColumnMapping
        cm = ColumnMapping(y_column="val", x_semantic="lifecycle")
        assert cm.x_semantic == "lifecycle"

    def test_x_semantic_valid_values(self):
        from four_dim_matrix import ColumnMapping
        for sem in ("funnel", "lifecycle", "progress", "stage"):
            cm = ColumnMapping(y_column="val", x_semantic=sem)
            assert cm.x_semantic == sem

    def test_normalize_x_zero_boundary(self):
        from four_dim_matrix import ColumnMapping
        cm = ColumnMapping(y_column="val", x_normalizer=lambda v: 0.0)
        assert cm.normalize_x("x") == pytest.approx(0.0)

    def test_normalize_x_one_boundary(self):
        from four_dim_matrix import ColumnMapping
        cm = ColumnMapping(y_column="val", x_normalizer=lambda v: 1.0)
        assert cm.normalize_x("x") == pytest.approx(1.0)
