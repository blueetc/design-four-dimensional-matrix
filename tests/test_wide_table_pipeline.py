"""Tests for the wide-table pipeline: field analysis, wide table design, ETL, and 3-D visualisation."""

from __future__ import annotations

import json
import sqlite3

import pytest

from toolserver.field_analyzer import (
    FieldProfile,
    TableProfile,
    _is_datetime_value,
    _numeric_value,
    analyze_database,
    analyze_table,
)
from toolserver.visualizer import (
    _build_hover_text,
    _esc,
    _query_wide_data,
    generate_3d_html,
    save_3d_html,
)
from toolserver.wide_table import (
    WIDE_TABLE_NAME,
    _row_hash,
    _safe_col_name,
    create_wide_table,
    design_wide_table,
    incremental_etl,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def db() -> sqlite3.Connection:
    """In-memory SQLite database with sample business data."""
    conn = sqlite3.connect(":memory:")
    conn.execute("""
        CREATE TABLE orders (
            order_id   INTEGER PRIMARY KEY,
            created_at TEXT,
            customer   TEXT,
            region     TEXT,
            amount     REAL,
            status     TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE products (
            product_id   INTEGER PRIMARY KEY,
            product_name TEXT,
            category     TEXT,
            price        REAL,
            launch_date  TEXT
        )
    """)
    # Insert sample data
    orders = [
        (1, "2024-01-15 08:30:00", "Alice", "East", 150.0, "completed"),
        (2, "2024-01-16 09:00:00", "Bob", "West", 250.0, "completed"),
        (3, "2024-02-01 10:15:00", "Alice", "East", 75.5, "pending"),
        (4, "2024-02-14 14:20:00", "Charlie", "North", 320.0, "completed"),
        (5, "2024-03-01 11:00:00", "Bob", "West", 180.0, "cancelled"),
        (6, "2024-03-15 16:45:00", "Diana", "South", 420.0, "completed"),
        (7, "2024-04-01 09:30:00", "Eve", "East", 95.0, "pending"),
        (8, "2024-04-10 13:00:00", "Frank", "North", 510.0, "completed"),
    ]
    conn.executemany(
        "INSERT INTO orders (order_id, created_at, customer, region, amount, status) VALUES (?,?,?,?,?,?)",
        orders,
    )
    products = [
        (1, "Widget A", "Hardware", 29.99, "2023-06-01"),
        (2, "Widget B", "Hardware", 49.99, "2023-07-15"),
        (3, "Service X", "Software", 199.0, "2024-01-01"),
        (4, "Service Y", "Software", 399.0, "2024-03-01"),
    ]
    conn.executemany(
        "INSERT INTO products (product_id, product_name, category, price, launch_date) VALUES (?,?,?,?,?)",
        products,
    )
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# field_analyzer tests
# ---------------------------------------------------------------------------


class TestDatetimeDetection:
    def test_iso_date(self) -> None:
        assert _is_datetime_value("2024-03-24") is True

    def test_datetime_with_time(self) -> None:
        assert _is_datetime_value("2024-03-24 12:30:00") is True

    def test_slash_date(self) -> None:
        assert _is_datetime_value("2024/3/24") is True

    def test_compact_date(self) -> None:
        assert _is_datetime_value("20240324") is True

    def test_unix_timestamp(self) -> None:
        assert _is_datetime_value("1711234567") is True

    def test_plain_text(self) -> None:
        assert _is_datetime_value("hello world") is False

    def test_short_number(self) -> None:
        assert _is_datetime_value("42") is False


class TestNumericValue:
    def test_int(self) -> None:
        assert _numeric_value(42) is True

    def test_float(self) -> None:
        assert _numeric_value(3.14) is True

    def test_string_number(self) -> None:
        assert _numeric_value("123.45") is True

    def test_comma_number(self) -> None:
        assert _numeric_value("1,234.56") is True

    def test_text(self) -> None:
        assert _numeric_value("hello") is False

    def test_none(self) -> None:
        assert _numeric_value(None) is False


class TestAnalyzeTable:
    def test_orders_table(self, db: sqlite3.Connection) -> None:
        profile = analyze_table(db, "orders")
        assert isinstance(profile, TableProfile)
        assert profile.name == "orders"
        assert profile.row_count == 8
        assert len(profile.fields) == 6

        # Check that created_at is detected as time
        created_field = next(f for f in profile.fields if f.column == "created_at")
        assert created_field.inferred_role == "time"

        # Check that amount is detected as measure
        amount_field = next(f for f in profile.fields if f.column == "amount")
        assert amount_field.inferred_role == "measure"

    def test_products_table(self, db: sqlite3.Connection) -> None:
        profile = analyze_table(db, "products")
        assert profile.row_count == 4
        # launch_date should be time
        date_field = next(f for f in profile.fields if f.column == "launch_date")
        assert date_field.inferred_role == "time"

        # price should be measure
        price_field = next(f for f in profile.fields if f.column == "price")
        assert price_field.inferred_role == "measure"


class TestAnalyzeDatabase:
    def test_returns_all_tables(self, db: sqlite3.Connection) -> None:
        result = analyze_database(db)
        assert isinstance(result, list)
        assert len(result) == 2
        table_names = {t["name"] for t in result}
        assert table_names == {"orders", "products"}


# ---------------------------------------------------------------------------
# wide_table tests
# ---------------------------------------------------------------------------


class TestSafeColName:
    def test_basic(self) -> None:
        assert _safe_col_name("orders", "amount") == "orders__amount"

    def test_spaces(self) -> None:
        assert _safe_col_name("my table", "my col") == "my_table__my_col"


class TestRowHash:
    def test_deterministic(self) -> None:
        vals = ["a", 1, 2.0]
        assert _row_hash(vals) == _row_hash(vals)

    def test_different_values(self) -> None:
        assert _row_hash(["a"]) != _row_hash(["b"])


class TestDesignWideTable:
    def test_design_from_analysis(self, db: sqlite3.Connection) -> None:
        analysis = analyze_database(db)
        design = design_wide_table(analysis)

        assert "columns" in design
        assert "time_column" in design
        assert "measure_columns" in design
        assert "dimension_columns" in design

        # Synthetic columns present
        col_names = [c["name"] for c in design["columns"]]
        assert "_row_hash" in col_names
        assert "_source_table" in col_names
        assert "_source_rowid" in col_names

        # Time column should be detected
        assert design["time_column"] != ""

        # Measure columns should include amount
        assert any("amount" in m for m in design["measure_columns"])


class TestCreateWideTable:
    def test_creates_table(self, db: sqlite3.Connection) -> None:
        analysis = analyze_database(db)
        design = design_wide_table(analysis)
        ddl = create_wide_table(db, design)

        assert "CREATE TABLE" in ddl
        assert WIDE_TABLE_NAME in ddl

        # Verify table exists
        cur = db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (WIDE_TABLE_NAME,),
        )
        assert cur.fetchone() is not None


class TestIncrementalEtl:
    def test_first_load(self, db: sqlite3.Connection) -> None:
        analysis = analyze_database(db)
        design = design_wide_table(analysis)
        create_wide_table(db, design)

        result = incremental_etl(db, design)
        assert result["total_new"] > 0
        assert "orders" in result["inserted"]
        assert "products" in result["inserted"]

        # Verify data in wide table
        cur = db.execute(f"SELECT COUNT(*) FROM [{WIDE_TABLE_NAME}]")
        count = cur.fetchone()[0]
        assert count == 12  # 8 orders + 4 products

    def test_incremental_no_new_data(self, db: sqlite3.Connection) -> None:
        analysis = analyze_database(db)
        design = design_wide_table(analysis)
        create_wide_table(db, design)

        # First load
        incremental_etl(db, design)
        # Second load — no new data
        result = incremental_etl(db, design)
        assert result["total_new"] == 0

    def test_incremental_with_new_data(self, db: sqlite3.Connection) -> None:
        analysis = analyze_database(db)
        design = design_wide_table(analysis)
        create_wide_table(db, design)

        # First load
        incremental_etl(db, design)

        # Insert new order
        db.execute(
            "INSERT INTO orders (order_id, created_at, customer, region, amount, status) "
            "VALUES (9, '2024-05-01 10:00:00', 'Grace', 'East', 600.0, 'completed')"
        )
        db.commit()

        # Second load should pick up the new row
        result = incremental_etl(db, design)
        assert result["inserted"]["orders"] == 1
        assert result["inserted"]["products"] == 0


# ---------------------------------------------------------------------------
# visualizer tests
# ---------------------------------------------------------------------------


class TestBuildHoverText:
    def test_excludes_internal_cols(self) -> None:
        row = {"_row_hash": "abc", "customer": "Alice", "amount": 100}
        text = _build_hover_text(row)
        assert "_row_hash" not in text
        assert "customer: Alice" in text
        assert "amount: 100" in text

    def test_excludes_none(self) -> None:
        row = {"customer": None, "region": "East"}
        text = _build_hover_text(row)
        assert "customer" not in text
        assert "region: East" in text


class TestEscape:
    def test_html_entities(self) -> None:
        assert _esc("<b>hi</b>") == "&lt;b&gt;hi&lt;/b&gt;"
        assert _esc('"test"') == "&quot;test&quot;"


class TestGenerate3dHtml:
    def test_generates_html(self, db: sqlite3.Connection) -> None:
        analysis = analyze_database(db)
        design = design_wide_table(analysis)
        create_wide_table(db, design)
        incremental_etl(db, design)

        time_col = design["time_column"]
        measure_col = design["measure_columns"][0] if design["measure_columns"] else ""
        theme_col = design["dimension_columns"][0] if design["dimension_columns"] else ""

        html = generate_3d_html(db, WIDE_TABLE_NAME, time_col, measure_col, theme_col)
        assert "<!DOCTYPE html>" in html
        assert "Plotly.newPlot" in html
        assert "scatter3d" in html

    def test_empty_table(self, db: sqlite3.Connection) -> None:
        db.execute(f"CREATE TABLE [{WIDE_TABLE_NAME}] (x TEXT, y REAL, z TEXT)")
        html = generate_3d_html(db, WIDE_TABLE_NAME, "x", "y", "z")
        assert "No data available" in html


class TestSave3dHtml:
    def test_saves_file(self, db: sqlite3.Connection, tmp_path) -> None:
        analysis = analyze_database(db)
        design = design_wide_table(analysis)
        create_wide_table(db, design)
        incremental_etl(db, design)

        time_col = design["time_column"]
        measure_col = design["measure_columns"][0]
        theme_col = design["dimension_columns"][0]
        out = str(tmp_path / "test_3d.html")

        saved = save_3d_html(db, WIDE_TABLE_NAME, time_col, measure_col, theme_col, out)
        assert saved.endswith("test_3d.html")
        with open(saved) as f:
            content = f.read()
        assert "Plotly.newPlot" in content


# ---------------------------------------------------------------------------
# Integration: full pipeline
# ---------------------------------------------------------------------------


class TestFullPipeline:
    def test_end_to_end(self, db: sqlite3.Connection, tmp_path) -> None:
        """Run the complete pipeline: analyze → design → create → etl → visualize."""
        # Step 1: Analyze
        analysis = analyze_database(db)
        assert len(analysis) == 2

        # Step 2: Design
        design = design_wide_table(analysis)
        assert design["time_column"]
        assert len(design["measure_columns"]) > 0

        # Step 3: Create
        ddl = create_wide_table(db, design)
        assert "CREATE TABLE" in ddl

        # Step 4: ETL
        result = incremental_etl(db, design)
        assert result["total_new"] == 12

        # Step 5: Visualize
        out = str(tmp_path / "pipeline.html")
        saved = save_3d_html(
            db,
            WIDE_TABLE_NAME,
            design["time_column"],
            design["measure_columns"][0],
            design["dimension_columns"][0],
            out,
        )
        with open(saved) as f:
            html = f.read()
        assert "scatter3d" in html
        assert "Plotly.newPlot" in html
