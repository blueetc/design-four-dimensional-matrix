"""Analyze database fields by sampling actual data values.

When column comments / descriptions are missing, this module infers each
field's semantic role (time, dimension, measure, identifier, text, unknown)
by inspecting a random sample of values.
"""

from __future__ import annotations

import re
import sqlite3
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Common datetime patterns used to identify time-like strings.
_DATETIME_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"^\d{4}[-/]\d{1,2}[-/]\d{1,2}"),          # 2024-03-24 or 2024/3/24
    re.compile(r"^\d{1,2}[-/]\d{1,2}[-/]\d{4}"),          # 03-24-2024
    re.compile(r"^\d{14}$"),                                # 20240324120000
    re.compile(r"^\d{4}\d{2}\d{2}$"),                       # 20240324
    re.compile(r"^\d{10,13}$"),                             # Unix timestamp (s/ms)
]

# Heuristic name patterns (Chinese & English) for field role detection.
_TIME_NAME_HINTS = re.compile(
    r"(time|date|_at$|_ts$|timestamp|created|updated|modified|日期|时间|年|月|日)",
    re.IGNORECASE,
)
_ID_NAME_HINTS = re.compile(
    r"(^id$|_id$|_no$|_code$|编号|编码|代码|号码|主键|pk)",
    re.IGNORECASE,
)
_MEASURE_NAME_HINTS = re.compile(
    r"(amount|count|total|sum|qty|quantity|price|fee|cost|rate|num|"
    r"金额|数量|总计|合计|费用|价格|利率|笔数|余额|balance)",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class FieldProfile:
    """Profiling result for a single database column."""

    table: str
    column: str
    dtype: str  # SQLite declared type or "UNKNOWN"
    inferred_role: str  # time | dimension | measure | identifier | text | unknown
    sample_values: list[Any] = field(default_factory=list)
    null_ratio: float = 0.0
    distinct_ratio: float = 0.0
    numeric_ratio: float = 0.0
    reasoning: str = ""


@dataclass
class TableProfile:
    """Profiling result for a single table."""

    name: str
    row_count: int
    fields: list[FieldProfile] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Analysis helpers
# ---------------------------------------------------------------------------

def _is_datetime_value(value: Any) -> bool:
    """Return *True* if *value* looks like a datetime."""
    if isinstance(value, datetime):
        return True
    s = str(value).strip()
    return any(p.search(s) for p in _DATETIME_PATTERNS)


def _numeric_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return True
    try:
        float(str(value).replace(",", ""))
        return True
    except (ValueError, TypeError):
        return False


def _infer_role(
    col_name: str,
    dtype: str,
    values: list[Any],
    null_ratio: float,
    distinct_ratio: float,
    numeric_ratio: float,
) -> tuple[str, str]:
    """Return ``(role, reasoning)``."""
    reasons: list[str] = []

    # 1. Name heuristic
    if _TIME_NAME_HINTS.search(col_name):
        reasons.append(f"column name '{col_name}' matches time pattern")
        # Verify with values
        if values and sum(_is_datetime_value(v) for v in values if v is not None) / max(len(values), 1) > 0.5:
            reasons.append("majority of sampled values look like datetime")
            return "time", "; ".join(reasons)
        # Name alone is a strong hint
        return "time", "; ".join(reasons) + "; values not conclusive but name suggests time"

    if _ID_NAME_HINTS.search(col_name):
        reasons.append(f"column name '{col_name}' matches identifier pattern")
        if distinct_ratio > 0.9:
            reasons.append(f"high distinct ratio ({distinct_ratio:.2f})")
        return "identifier", "; ".join(reasons)

    if _MEASURE_NAME_HINTS.search(col_name):
        reasons.append(f"column name '{col_name}' matches measure pattern")
        return "measure", "; ".join(reasons)

    # 2. Value-based heuristic
    if values:
        dt_ratio = sum(_is_datetime_value(v) for v in values if v is not None) / max(len(values), 1)
        if dt_ratio > 0.7:
            reasons.append(f"~{dt_ratio:.0%} of values look like datetime")
            return "time", "; ".join(reasons)

    if numeric_ratio > 0.9 and distinct_ratio < 0.3:
        reasons.append(f"highly numeric ({numeric_ratio:.0%}) with low cardinality ({distinct_ratio:.2f})")
        return "dimension", "; ".join(reasons)

    if numeric_ratio > 0.8:
        reasons.append(f"numeric ratio {numeric_ratio:.0%}")
        if distinct_ratio > 0.5:
            reasons.append(f"distinct ratio {distinct_ratio:.2f} — likely measure")
            return "measure", "; ".join(reasons)
        reasons.append(f"distinct ratio {distinct_ratio:.2f} — could be dimension")
        return "dimension", "; ".join(reasons)

    if distinct_ratio < 0.5 and null_ratio < 0.5 and numeric_ratio < 0.1:
        reasons.append(f"low cardinality ({distinct_ratio:.2f}) non-numeric, likely categorical dimension")
        return "dimension", "; ".join(reasons)

    if dtype.upper() in ("TEXT", "VARCHAR", "CHAR", "NVARCHAR", "CLOB", ""):
        avg_len = sum(len(str(v)) for v in values if v is not None) / max(len(values), 1)
        if avg_len > 50:
            reasons.append(f"long text avg length {avg_len:.0f}")
            return "text", "; ".join(reasons)
        if distinct_ratio < 0.8 and numeric_ratio < 0.1:
            reasons.append(f"short text ({avg_len:.0f} avg chars) with moderate cardinality ({distinct_ratio:.2f})")
            return "dimension", "; ".join(reasons)

    reasons.append("no strong signal detected")
    return "unknown", "; ".join(reasons)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def analyze_table(conn: sqlite3.Connection, table: str, sample_size: int = 200) -> TableProfile:
    """Profile every column in *table* by sampling *sample_size* rows."""
    cur = conn.execute(f"SELECT COUNT(*) FROM [{table}]")  # noqa: S608
    row_count: int = cur.fetchone()[0]

    cur = conn.execute(f"PRAGMA table_info([{table}])")
    columns = [(row[1], row[2] or "UNKNOWN") for row in cur.fetchall()]  # (name, type)

    # Sample rows
    cur = conn.execute(  # noqa: S608
        f"SELECT * FROM [{table}] ORDER BY RANDOM() LIMIT ?", (sample_size,)
    )
    rows = cur.fetchall()
    col_names_ordered = [desc[0] for desc in cur.description]

    fields: list[FieldProfile] = []
    for col_name, col_type in columns:
        idx = col_names_ordered.index(col_name) if col_name in col_names_ordered else -1
        if idx < 0:
            continue
        values = [row[idx] for row in rows]
        non_null = [v for v in values if v is not None]
        total = len(values) or 1
        null_ratio = (total - len(non_null)) / total
        distinct_ratio = len(set(non_null)) / max(len(non_null), 1)
        numeric_ratio = sum(_numeric_value(v) for v in non_null) / max(len(non_null), 1)

        role, reasoning = _infer_role(col_name, col_type, non_null[:30], null_ratio, distinct_ratio, numeric_ratio)

        fields.append(FieldProfile(
            table=table,
            column=col_name,
            dtype=col_type,
            inferred_role=role,
            sample_values=[str(v) for v in non_null[:5]],
            null_ratio=round(null_ratio, 4),
            distinct_ratio=round(distinct_ratio, 4),
            numeric_ratio=round(numeric_ratio, 4),
            reasoning=reasoning,
        ))

    return TableProfile(name=table, row_count=row_count, fields=fields)


def analyze_database(conn: sqlite3.Connection, sample_size: int = 200) -> list[dict]:
    """Analyze all user tables in the database.  Returns a list of dicts."""
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    )
    tables = [row[0] for row in cur.fetchall()]
    profiles: list[dict] = []
    for t in tables:
        tp = analyze_table(conn, t, sample_size)
        profiles.append(asdict(tp))
    return profiles
