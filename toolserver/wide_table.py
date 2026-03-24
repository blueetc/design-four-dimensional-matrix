"""Design a wide (denormalized) table from field analysis and perform incremental ETL.

The wide table flattens multiple source tables into a single analytical view,
making it easy to drive the 3-D visualization (x = time, y = measure, z = theme).
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import asdict, dataclass, field
from typing import Any

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

WIDE_TABLE_NAME = "_wide_table"
META_TABLE_NAME = "_wide_meta"

# The meta table tracks incremental ETL state.
_META_DDL = f"""
CREATE TABLE IF NOT EXISTS [{META_TABLE_NAME}] (
    source_table TEXT NOT NULL,
    last_rowid   INTEGER NOT NULL DEFAULT 0,
    last_etl_ts  TEXT    NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (source_table)
);
"""


@dataclass
class WideColumn:
    """Definition of a column in the wide table."""

    name: str
    source_table: str
    source_column: str
    role: str  # time | dimension | measure | identifier | text | unknown
    dtype: str  # SQLite type


@dataclass
class WideTableDesign:
    """The designed schema for the wide table."""

    columns: list[WideColumn] = field(default_factory=list)
    time_column: str = ""      # chosen x-axis
    measure_columns: list[str] = field(default_factory=list)  # y-axis candidates
    dimension_columns: list[str] = field(default_factory=list)  # z-axis (theme) candidates


# ---------------------------------------------------------------------------
# Design logic
# ---------------------------------------------------------------------------

def _safe_col_name(table: str, column: str) -> str:
    """Create a unique, safe column name like ``orders__amount``."""
    return f"{table}__{column}".replace(" ", "_").replace("-", "_")


def design_wide_table(analysis: list[dict]) -> dict:
    """Given field-analysis output, design the wide table.

    Returns the design as a plain dict (JSON-serialisable).
    """
    columns: list[WideColumn] = []
    time_candidates: list[tuple[str, str, int]] = []  # (safe_name, source, row_count)
    measure_cols: list[str] = []
    dimension_cols: list[str] = []

    # Add a synthetic row-hash column for deduplication.
    columns.append(WideColumn(
        name="_row_hash",
        source_table="__synthetic",
        source_column="__hash",
        role="identifier",
        dtype="TEXT",
    ))
    # Source tracking column.
    columns.append(WideColumn(
        name="_source_table",
        source_table="__synthetic",
        source_column="__source",
        role="dimension",
        dtype="TEXT",
    ))
    # Original rowid for incremental tracking.
    columns.append(WideColumn(
        name="_source_rowid",
        source_table="__synthetic",
        source_column="__rowid",
        role="identifier",
        dtype="INTEGER",
    ))

    for tbl in analysis:
        tbl_name = tbl["name"]
        row_count = tbl.get("row_count", 0)
        for f in tbl.get("fields", []):
            safe = _safe_col_name(tbl_name, f["column"])
            wc = WideColumn(
                name=safe,
                source_table=tbl_name,
                source_column=f["column"],
                role=f["inferred_role"],
                dtype=_sqlite_type(f["dtype"]),
            )
            columns.append(wc)

            if f["inferred_role"] == "time":
                time_candidates.append((safe, tbl_name, row_count))
            elif f["inferred_role"] == "measure":
                measure_cols.append(safe)
            elif f["inferred_role"] == "dimension":
                dimension_cols.append(safe)

    # Pick the best time column: prefer the one from the table with the most rows.
    time_col = ""
    if time_candidates:
        time_candidates.sort(key=lambda x: x[2], reverse=True)
        time_col = time_candidates[0][0]

    design = WideTableDesign(
        columns=columns,
        time_column=time_col,
        measure_columns=measure_cols,
        dimension_columns=dimension_cols,
    )
    return asdict(design)


def _sqlite_type(declared: str) -> str:
    """Map a declared type to a simple SQLite affinity."""
    up = (declared or "").upper()
    if any(k in up for k in ("INT",)):
        return "INTEGER"
    if any(k in up for k in ("REAL", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC")):
        return "REAL"
    return "TEXT"


# ---------------------------------------------------------------------------
# DDL generation & table creation
# ---------------------------------------------------------------------------

def create_wide_table(conn: sqlite3.Connection, design: dict) -> str:
    """Create the wide table (if not exists) and the meta table.  Returns DDL."""
    cols = design.get("columns", [])
    parts: list[str] = []
    for c in cols:
        parts.append(f'  [{c["name"]}] {c.get("dtype", "TEXT")}')

    ddl = (
        f"CREATE TABLE IF NOT EXISTS [{WIDE_TABLE_NAME}] (\n"
        + ",\n".join(parts)
        + "\n);"
    )
    conn.execute(ddl)
    conn.execute(_META_DDL)
    conn.commit()
    return ddl


# ---------------------------------------------------------------------------
# Incremental ETL
# ---------------------------------------------------------------------------

def _row_hash(values: list[Any]) -> str:
    raw = json.dumps(values, ensure_ascii=False, default=str)
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def incremental_etl(
    conn: sqlite3.Connection,
    design: dict,
    batch_size: int = 500,
) -> dict:
    """Load new rows from every source table into the wide table.

    Uses ``rowid`` watermarking stored in ``_wide_meta`` to only read new rows.

    Returns a summary dict with per-table insert counts.
    """
    columns = design.get("columns", [])
    # Build a mapping: source_table -> list of WideColumn dicts
    source_map: dict[str, list[dict]] = {}
    for c in columns:
        st = c.get("source_table", "")
        if st.startswith("__"):
            continue
        source_map.setdefault(st, []).append(c)

    # Column names in wide table (in order)
    wide_col_names = [c["name"] for c in columns]
    placeholders = ", ".join(["?"] * len(wide_col_names))
    insert_sql = (
        f"INSERT INTO [{WIDE_TABLE_NAME}] ({', '.join(f'[{n}]' for n in wide_col_names)}) "
        f"VALUES ({placeholders})"
    )

    summary: dict[str, int] = {}

    for src_table, src_cols in source_map.items():
        # Get watermark
        cur = conn.execute(
            f"SELECT last_rowid FROM [{META_TABLE_NAME}] WHERE source_table = ?",
            (src_table,),
        )
        row = cur.fetchone()
        last_rowid = row[0] if row else 0

        # Fetch new rows
        src_col_select = ", ".join(f"[{c['source_column']}]" for c in src_cols)
        cur = conn.execute(  # noqa: S608
            f"SELECT rowid, {src_col_select} FROM [{src_table}] "
            f"WHERE rowid > ? ORDER BY rowid LIMIT ?",
            (last_rowid, batch_size),
        )
        rows = cur.fetchall()
        if not rows:
            summary[src_table] = 0
            continue

        inserted = 0
        max_rowid = last_rowid
        for r in rows:
            rowid_val = r[0]
            values_from_src = list(r[1:])
            max_rowid = max(max_rowid, rowid_val)

            # Build wide row: fill in values for this source table's columns,
            # set NULL for columns from other source tables.
            wide_values: list[Any] = []
            for c in columns:
                cname = c["name"]
                if cname == "_row_hash":
                    wide_values.append(_row_hash(values_from_src))
                elif cname == "_source_table":
                    wide_values.append(src_table)
                elif cname == "_source_rowid":
                    wide_values.append(rowid_val)
                elif c.get("source_table") == src_table:
                    idx = [sc["source_column"] for sc in src_cols].index(c["source_column"])
                    wide_values.append(values_from_src[idx])
                else:
                    wide_values.append(None)

            conn.execute(insert_sql, wide_values)
            inserted += 1

        # Update watermark
        conn.execute(
            f"INSERT INTO [{META_TABLE_NAME}] (source_table, last_rowid, last_etl_ts) "
            f"VALUES (?, ?, datetime('now')) "
            f"ON CONFLICT(source_table) DO UPDATE SET last_rowid=excluded.last_rowid, "
            f"last_etl_ts=excluded.last_etl_ts",
            (src_table, max_rowid),
        )
        conn.commit()
        summary[src_table] = inserted

    return {"table": WIDE_TABLE_NAME, "inserted": summary, "total_new": sum(summary.values())}
