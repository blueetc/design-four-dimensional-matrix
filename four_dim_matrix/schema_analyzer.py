"""SchemaAnalyzer – normalization analysis for the dual-matrix knowledge system.

The 4D matrix transformation reveals a fundamental truth about any database:
its tables are (often imperfect) attempts to encode multi-dimensional knowledge
as flat rows.  After loading a raw database into the two matrices via
:class:`~four_dim_matrix.DatabaseAdapter`, this analyzer examines the schema
and answers three questions:

1. **Which tables are "wide"?**  A wide table mixes multiple topics in one
   structure (entity attributes + metrics + status + relations), making it
   hard to place on a single z-axis slice.

2. **How should each wide table be split?**  Columns are classified into
   topic-oriented groups (identity, relational, numeric, descriptive,
   temporal, categorical) and concrete sub-table proposals are generated.

3. **How normalized is the schema overall?**  A score from ``0.0``
   (completely denormalized – one giant wide table) to ``1.0``
   (every table is a single-topic, well-structured entity) summarises the
   current state and guides the normalization roadmap.

The end result is a **standardized knowledge base blueprint**: a set of
single-topic tables that map cleanly onto the z-axis of the 4D matrices,
where each table/topic has a consistent hue, its rows are the data points,
and trends are readable without ambiguity.

Example::

    from four_dim_matrix import DatabaseAdapter, SchemaAnalyzer

    adapter = DatabaseAdapter.from_sqlite("sales.db")
    analyzer = SchemaAnalyzer(adapter)

    report = analyzer.report()
    print(f"Normalization score: {report['normalization_score']:.0%}")
    for t in report["tables"]:
        if t["is_wide_table"]:
            sugg = analyzer.suggest_normalization(t["name"])
            for st in sugg["suggested_tables"]:
                print(f"  → {st['name']}: {st['columns']}")
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from .db_adapter import ColumnInfo, ColumnType, DatabaseAdapter, TableInfo


# ---------------------------------------------------------------------------
# Column group classification
# ---------------------------------------------------------------------------

class ColumnGroup(str):
    """String constants that name the six semantic column groups.

    Used as dictionary keys in :attr:`TableAnalysis.column_groups` and in the
    split proposals returned by :meth:`SchemaAnalyzer.suggest_normalization`.

    Constants
    ---------
    IDENTITY
        The table's own primary-key and surrogate-key columns (``id``,
        ``uuid``, ``pk``, …).  Every normalized table should have exactly
        one.
    RELATIONAL
        Foreign-key columns that point to other entities (names ending in
        ``_id``, ``_fk``, ``_key``).  Grouping these separately highlights
        cross-entity dependencies.
    TEMPORAL
        Datetime / date / timestamp columns.  When a wide table mixes
        multiple temporal columns (``created_at``, ``updated_at``,
        ``closed_at``, …) they may deserve their own event-log table.
    NUMERIC
        Non-key numeric columns (``REAL``, ``FLOAT``, non-identity
        ``INTEGER``).  These are *metrics* – the y-axis of the 4D matrix.
    DESCRIPTIVE
        Free-text columns (names, descriptions, labels, codes).  These are
        the human-readable attributes of an entity.
    CATEGORICAL
        Boolean and short-text columns that represent state or category
        (``status``, ``type``, ``flag``).  The x-axis candidate.
    OTHER
        Anything that doesn't fit the above (BLOBs, JSON, custom types).
    """

    IDENTITY    = "IDENTITY"
    RELATIONAL  = "RELATIONAL"
    TEMPORAL    = "TEMPORAL"
    NUMERIC     = "NUMERIC"
    DESCRIPTIVE = "DESCRIPTIVE"
    CATEGORICAL = "CATEGORICAL"
    OTHER       = "OTHER"


# ---------------------------------------------------------------------------
# Per-table analysis result
# ---------------------------------------------------------------------------

@dataclass
class TableAnalysis:
    """Normalization analysis result for a single table.

    Attributes:
        name: Table name.
        column_count: Total number of columns.
        row_count: Total number of rows.
        column_groups: Mapping of :class:`ColumnGroup` → list of column names
            assigned to that group.
        is_wide_table: ``True`` when the table exceeds the wide-table
            threshold *and* covers more than one non-identity topic group.
        active_groups: Sorted list of non-empty, non-identity group names
            present in this table.
        normalization_hint: Human-readable one-line verdict.
    """

    name: str
    column_count: int
    row_count: int
    column_groups: Dict[str, List[str]] = field(default_factory=dict)
    is_wide_table: bool = False
    active_groups: List[str] = field(default_factory=list)
    normalization_hint: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "column_count": self.column_count,
            "row_count": self.row_count,
            "column_groups": self.column_groups,
            "is_wide_table": self.is_wide_table,
            "active_groups": self.active_groups,
            "normalization_hint": self.normalization_hint,
        }


# ---------------------------------------------------------------------------
# Main analyzer
# ---------------------------------------------------------------------------

class SchemaAnalyzer:
    """Analyze a :class:`~four_dim_matrix.DatabaseAdapter`'s schema and
    suggest how to normalize it into a clean topic-based knowledge structure.

    Parameters:
        adapter: A fully-introspected :class:`~four_dim_matrix.DatabaseAdapter`.
        wide_table_threshold: Minimum column count for a table to be
            considered a wide-table candidate (default ``8``).

    The normalization insight
    ------------------------
    The 4D matrix maps *tables → z-axis topics*.  For this mapping to be
    meaningful, each table should represent exactly **one** topic.  Wide
    tables that mix entity identity, metrics, status, relations and temporal
    information violate this principle and produce "blurry" colour blocks
    that are hard to interpret.

    :meth:`suggest_normalization` proposes a concrete split into 2–4
    focused sub-tables, each of which maps cleanly to one z-axis colour.
    :meth:`report` ties everything together into an actionable report.
    """

    def __init__(
        self,
        adapter: DatabaseAdapter,
        wide_table_threshold: int = 8,
    ) -> None:
        self.adapter = adapter
        self.wide_table_threshold = wide_table_threshold

    # ------------------------------------------------------------------
    # Per-table analysis
    # ------------------------------------------------------------------

    def analyse_table(self, table_name: str) -> TableAnalysis:
        """Return a :class:`TableAnalysis` for *table_name*.

        Raises:
            KeyError: If *table_name* is not found in the adapter's tables.
        """
        table = self.adapter.get_table(table_name)
        if table is None:
            raise KeyError(f"Table {table_name!r} not found in adapter.")
        return _analyse(table, self.wide_table_threshold)

    def analyse_all(self) -> List[TableAnalysis]:
        """Return a :class:`TableAnalysis` for every table, sorted by name."""
        return [
            _analyse(t, self.wide_table_threshold)
            for t in sorted(self.adapter.tables, key=lambda t: t.name)
        ]

    # ------------------------------------------------------------------
    # Normalization suggestions
    # ------------------------------------------------------------------

    def suggest_normalization(self, table_name: str) -> Dict[str, Any]:
        """Propose a normalized split for *table_name*.

        Returns a dictionary with:

        * ``"table"`` – original table name.
        * ``"current_columns"`` – current column count.
        * ``"is_wide_table"`` – whether the table exceeded the threshold.
        * ``"suggested_tables"`` – list of proposed sub-tables, each with:
          - ``"name"`` – suggested name (``{original}_{group_suffix}``).
          - ``"rationale"`` – why these columns belong together.
          - ``"columns"`` – list of column names for this sub-table.
          - ``"matrix_role"`` – which 4D-matrix axis this sub-table
            primarily feeds (``"z-topic"``, ``"y-metric"``, ``"x-phase"``,
            ``"t-timeline"``).

        For tables that are already well-normalized the suggestion confirms
        the current structure without proposing a split.
        """
        analysis = self.analyse_table(table_name)
        return _build_suggestion(analysis)

    # ------------------------------------------------------------------
    # Schema-level scoring and reporting
    # ------------------------------------------------------------------

    def normalization_score(self) -> float:
        """Return an overall normalization score in ``[0.0, 1.0]``.

        The score is the fraction of tables that are **not** wide tables.
        A score of ``1.0`` means every table is already single-topic; a score
        of ``0.0`` means every table is a wide denormalized blob.

        Returns ``1.0`` for an empty schema (no tables to criticise).
        """
        if not self.adapter.tables:
            return 1.0
        analyses = self.analyse_all()
        well_formed = sum(1 for a in analyses if not a.is_wide_table)
        return well_formed / len(analyses)

    def report(self) -> Dict[str, Any]:
        """Return the full normalization report for the entire schema.

        The report contains:

        * ``"snapshot_time"`` – when the adapter introspected the database.
        * ``"normalization_score"`` – float in ``[0.0, 1.0]``.
        * ``"table_count"`` – total number of tables.
        * ``"wide_table_count"`` – tables flagged as wide/denormalized.
        * ``"tables"`` – list of :meth:`TableAnalysis.to_dict` results,
          ordered by descending column count so the most problematic
          tables appear first.
        * ``"suggestions"`` – normalization suggestions for every wide table,
          keyed by table name.
        * ``"matrix_readiness"`` – human-readable summary of how cleanly the
          current schema maps onto the 4D matrix axes.
        """
        analyses = self.analyse_all()
        wide_tables = [a for a in analyses if a.is_wide_table]

        suggestions: Dict[str, Any] = {}
        for a in wide_tables:
            suggestions[a.name] = _build_suggestion(a)

        score = self.normalization_score()

        return {
            "snapshot_time": self.adapter.snapshot_time.isoformat(),
            "normalization_score": round(score, 4),
            "table_count": len(analyses),
            "wide_table_count": len(wide_tables),
            "tables": sorted(
                [a.to_dict() for a in analyses],
                key=lambda d: d["column_count"],
                reverse=True,
            ),
            "suggestions": suggestions,
            "matrix_readiness": _matrix_readiness_summary(score, wide_tables),
        }


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

# Patterns used to classify column names heuristically.
_RE_IDENTITY   = re.compile(r"^(id|pk|uuid|guid|key|oid|row_?id)$", re.I)
_RE_RELATIONAL = re.compile(r"(_id|_fk|_key|_ref|_code)$", re.I)
_RE_TEMPORAL   = re.compile(
    r"(date|time|_at|_on|_ts|stamp|created|updated|modified|deleted|expires)", re.I
)
_RE_CATEGORICAL = re.compile(
    r"(status|state|type|kind|flag|mode|category|class|tier|level|phase|stage)", re.I
)
_RE_DESCRIPTIVE = re.compile(
    r"(name|title|label|description|desc|summary|note|comment|remark|text|body|content|code|sku|slug|email|url|address|phone)", re.I
)


def _classify_column(col: ColumnInfo) -> str:
    """Return the :class:`ColumnGroup` constant for *col*."""
    name = col.name.lower()

    # Primary-key identity column
    if col.primary_key or _RE_IDENTITY.match(name):
        return ColumnGroup.IDENTITY

    # Foreign key / relational link (must check before numeric to catch integer FKs)
    if _RE_RELATIONAL.search(name):
        return ColumnGroup.RELATIONAL

    # Temporal (datetime columns OR column names that suggest a timestamp)
    if col.column_type == ColumnType.DATETIME or _RE_TEMPORAL.search(name):
        return ColumnGroup.TEMPORAL

    # Categorical state / boolean
    if col.column_type == ColumnType.BOOLEAN or _RE_CATEGORICAL.search(name):
        return ColumnGroup.CATEGORICAL

    # Numeric metric
    if col.column_type in (ColumnType.FLOAT, ColumnType.INTEGER):
        return ColumnGroup.NUMERIC

    # Descriptive text
    if col.column_type == ColumnType.TEXT or _RE_DESCRIPTIVE.search(name):
        return ColumnGroup.DESCRIPTIVE

    return ColumnGroup.OTHER


def _analyse(table: TableInfo, threshold: int) -> TableAnalysis:
    """Build a :class:`TableAnalysis` for *table*."""
    groups: Dict[str, List[str]] = {
        ColumnGroup.IDENTITY:    [],
        ColumnGroup.RELATIONAL:  [],
        ColumnGroup.TEMPORAL:    [],
        ColumnGroup.NUMERIC:     [],
        ColumnGroup.DESCRIPTIVE: [],
        ColumnGroup.CATEGORICAL: [],
        ColumnGroup.OTHER:       [],
    }
    for col in table.columns:
        groups[_classify_column(col)].append(col.name)

    # Remove empty groups for cleaner output
    non_empty = {g: cols for g, cols in groups.items() if cols}

    # Active groups = non-identity groups that are present
    active = sorted(
        g for g, cols in non_empty.items()
        if g != ColumnGroup.IDENTITY and cols
    )

    # Wide-table flag: exceeds column threshold AND covers multiple distinct
    # topic groups (at least 2 non-identity groups with ≥1 column each).
    is_wide = (
        table.column_count >= threshold
        and len(active) >= 2
    )

    hint = _normalization_hint(table, is_wide, active)

    return TableAnalysis(
        name=table.name,
        column_count=table.column_count,
        row_count=table.row_count,
        column_groups=non_empty,
        is_wide_table=is_wide,
        active_groups=active,
        normalization_hint=hint,
    )


def _normalization_hint(
    table: TableInfo, is_wide: bool, active_groups: List[str]
) -> str:
    """Return a one-line normalization verdict for *table*."""
    if not is_wide:
        if len(active_groups) == 0:
            return "Identity-only table – consider adding descriptive columns."
        if len(active_groups) == 1:
            return f"Single-topic table ({active_groups[0].lower()}) – maps cleanly to one z-axis slice."
        return "Compact multi-aspect table – acceptable structure for the 4D matrix."
    # Wide table
    topics = " + ".join(g.lower() for g in active_groups)
    return (
        f"Wide table mixing {len(active_groups)} topic groups ({topics}). "
        f"Consider splitting into {len(active_groups)} focused sub-tables to "
        f"align each with a single z-axis colour."
    )


# Descriptions for each group used in split-suggestion rationales.
_GROUP_RATIONALE: Dict[str, str] = {
    ColumnGroup.IDENTITY:    "Entity identity and surrogate key",
    ColumnGroup.RELATIONAL:  "Cross-entity relationships (foreign keys)",
    ColumnGroup.TEMPORAL:    "Temporal tracking (timestamps / lifecycle dates)",
    ColumnGroup.NUMERIC:     "Quantitative metrics – y-axis of the 4D matrix",
    ColumnGroup.DESCRIPTIVE: "Human-readable descriptive attributes",
    ColumnGroup.CATEGORICAL: "State / phase / category – x-axis of the 4D matrix",
    ColumnGroup.OTHER:       "Unclassified / binary / blob columns",
}

# How each group feeds the 4D matrix axes.
_GROUP_MATRIX_ROLE: Dict[str, str] = {
    ColumnGroup.IDENTITY:    "z-topic (entity anchor)",
    ColumnGroup.RELATIONAL:  "z-topic (relationship anchor)",
    ColumnGroup.TEMPORAL:    "t-timeline",
    ColumnGroup.NUMERIC:     "y-metric",
    ColumnGroup.DESCRIPTIVE: "z-topic (descriptive)",
    ColumnGroup.CATEGORICAL: "x-phase",
    ColumnGroup.OTHER:       "payload",
}


def _build_suggestion(analysis: TableAnalysis) -> Dict[str, Any]:
    """Build the normalization suggestion dict for one :class:`TableAnalysis`."""
    groups = analysis.column_groups
    identity_cols = groups.get(ColumnGroup.IDENTITY, [])

    if not analysis.is_wide_table:
        return {
            "table": analysis.name,
            "current_columns": analysis.column_count,
            "is_wide_table": False,
            "suggested_tables": [],
            "verdict": analysis.normalization_hint,
        }

    # Build one sub-table per non-empty, non-identity group.
    # Each sub-table inherits the identity column(s) as its anchor key.
    suggested: List[Dict[str, Any]] = []

    # Decide the suffix and group ordering: put the most "entity-defining"
    # groups first.
    priority_order = [
        ColumnGroup.DESCRIPTIVE,
        ColumnGroup.CATEGORICAL,
        ColumnGroup.NUMERIC,
        ColumnGroup.TEMPORAL,
        ColumnGroup.RELATIONAL,
        ColumnGroup.OTHER,
    ]

    for group in priority_order:
        cols = groups.get(group, [])
        if not cols:
            continue
        suffix = group.lower()
        suggested.append({
            "name": f"{analysis.name}_{suffix}",
            "rationale": _GROUP_RATIONALE[group],
            "columns": identity_cols + cols,
            "matrix_role": _GROUP_MATRIX_ROLE[group],
        })

    # If there is only one non-identity group after filtering, the table is
    # already effectively single-topic (the wide flag came from raw column
    # count); re-mark it as acceptable.
    if len(suggested) <= 1:
        return {
            "table": analysis.name,
            "current_columns": analysis.column_count,
            "is_wide_table": True,
            "suggested_tables": [],
            "verdict": (
                "Column count exceeds threshold but only one topic group found. "
                "Consider whether columns can be pruned or the threshold adjusted."
            ),
        }

    return {
        "table": analysis.name,
        "current_columns": analysis.column_count,
        "is_wide_table": True,
        "suggested_tables": suggested,
        "verdict": analysis.normalization_hint,
    }


def _matrix_readiness_summary(
    score: float, wide_tables: List[TableAnalysis]
) -> str:
    """Return a human-readable summary of matrix readiness."""
    if score == 1.0:
        return (
            "Schema is fully normalized. Every table maps to exactly one "
            "z-axis topic – the 4D matrix colour cloud will be clean and "
            "unambiguous."
        )
    if score >= 0.75:
        wide_names = ", ".join(a.name for a in wide_tables)
        return (
            f"Schema is mostly normalized (score {score:.0%}). "
            f"Wide tables detected: {wide_names}. "
            "Splitting these will sharpen the matrix colour separation."
        )
    if score >= 0.5:
        return (
            f"Schema is partially normalized (score {score:.0%}). "
            f"{len(wide_tables)} wide table(s) mix multiple topics. "
            "Normalization will significantly improve matrix readability."
        )
    return (
        f"Schema is largely denormalized (score {score:.0%}). "
        "Most tables mix multiple topics, producing blurry colour blocks. "
        "Follow the normalization suggestions to build a clean topic-based "
        "knowledge structure before relying on the 4D matrix for insights."
    )
