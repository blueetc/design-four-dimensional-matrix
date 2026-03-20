"""NormalizationPipeline – two-stage 4D matrix builder with lineage tracking.

This module connects the full knowledge-extraction pipeline:

Stage 1 (raw)
    Load the database schema as-is into the dual matrices.  One DataPoint per
    table; axes encode *structural* properties (column count → x, row count → y,
    snapshot timestamp → t, alphabetical table index → z).  This gives an
    *imperfect* but *immediate* overview of the database topology — like a rough
    sketch before the detailed painting.

Stage 2 (normalized — the target)
    Apply the :class:`~four_dim_matrix.SchemaAnalyzer` normalization plan to
    produce a clean, topic-oriented pair of matrices where every z-axis slice
    is a single-purpose domain.  Wide denormalized tables are split into
    focused sub-tables; already-normalized tables pass through unchanged.

    For each sub-table the axes carry *semantic* meaning:

    * **t** – first temporal column in the row, giving true business time.
    * **y** – first numeric metric column (for the NUMERIC group); ``1.0``
      for all other groups so that row counts remain visible.
    * **x** – first categorical column, integer-encoded per sub-table; falls
      back to the sequential row index if no categorical column is present.
    * **z** – alphabetical index across **all** Stage 2 sub-table names,
      ensuring a stable, consistent hue for each domain across calls.

Lineage
    Every Stage 2 :class:`~four_dim_matrix.DataPoint` carries a ``_lineage``
    key in its payload.  It records:

    * ``source_table``  – original table name in the raw database.
    * ``sub_table``     – Stage 2 sub-table name (same as source_table when
      the table was not split).
    * ``group``         – :class:`~four_dim_matrix.ColumnGroup` constant
      (or ``"NORMALIZED"`` for non-split tables).
    * ``row_index``     – 0-based row position within the source table.
    * ``stage1_z``      – z-coordinate of the table in the Stage 1 matrix.
    * ``stage1_t``      – t-coordinate (ISO-8601) in Stage 1.
    * ``stage1_x``      – x-coordinate (column count) in Stage 1.
    * ``stage1_y``      – y-coordinate (row count) in Stage 1.

    This lets you drill from any colour block in the Stage 2 matrix all the
    way back to the raw database row and to the Stage 1 block it came from.

Example::

    import sqlite3
    from four_dim_matrix import DatabaseAdapter, SchemaAnalyzer, NormalizationPipeline

    conn = sqlite3.connect("business.db")
    adapter = DatabaseAdapter.from_connection(conn, dialect="sqlite")
    analyzer = SchemaAnalyzer(adapter)
    pipeline = NormalizationPipeline(adapter, analyzer)

    # Inspect Stage 2 structure without loading any data
    for sp in pipeline.plan():
        print(sp.stage2_z, sp.sub_table_name, "←", sp.source_table, sp.group)

    # Build Stage 2 knowledge base
    stage2_kb = pipeline.build_stage2(conn)

    # Trace any Stage 2 point back to its Stage 1 origin
    pt = next(iter(stage2_kb.data_matrix))
    lin = NormalizationPipeline.lineage_for(pt)
    print(lin["source_table"], lin["stage1_z"], lin["stage1_y"])  # rows in source

    # Find all Stage 2 points that came from a specific source table + group
    numeric_pts = NormalizationPipeline.stage2_points_from_source(
        stage2_kb, "sales_report", group="NUMERIC"
    )
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

from .data_matrix import DataPoint
from .db_adapter import DatabaseAdapter, _parse_t_value, _quote_identifier
from .knowledge_base import KnowledgeBase
from .schema_analyzer import ColumnGroup, SchemaAnalyzer, TableAnalysis


# ---------------------------------------------------------------------------
# Plan dataclass
# ---------------------------------------------------------------------------

@dataclass
class SubTablePlan:
    """Describes one z-axis slot in the Stage 2 matrix.

    Attributes:
        sub_table_name: The name assigned to this slot, e.g.
            ``"sales_report_numeric"`` for a split or ``"customers"`` for an
            already-normalized table.
        source_table: The original table name in the raw database.
        group: The :class:`~four_dim_matrix.ColumnGroup` constant that this
            slot represents, or ``"NORMALIZED"`` for un-split tables.
        matrix_role: How this slot feeds the 4D matrix axes
            (``"y-metric"``, ``"x-phase"``, ``"t-timeline"``,
            ``"z-topic (normalized)"``).
        stage2_z: The z-coordinate assigned to this slot (alphabetical order
            across all Stage 2 sub-table names).
        columns: The column names included in this slot.
        is_original: ``True`` when the source table was already
            well-normalized and was not split.
    """

    sub_table_name: str
    source_table: str
    group: str
    matrix_role: str
    stage2_z: int
    columns: List[str]
    is_original: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "sub_table_name": self.sub_table_name,
            "source_table": self.source_table,
            "group": self.group,
            "matrix_role": self.matrix_role,
            "stage2_z": self.stage2_z,
            "columns": self.columns,
            "is_original": self.is_original,
        }


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

class NormalizationPipeline:
    """Builds a Stage 2 (normalized) dual-matrix KnowledgeBase from a raw DB.

    Parameters:
        adapter: A fully-introspected :class:`~four_dim_matrix.DatabaseAdapter`.
        analyzer: A :class:`~four_dim_matrix.SchemaAnalyzer` built on the same
            *adapter*.

    The two-stage pipeline
    ----------------------
    **Stage 1** is already implemented by
    :meth:`~four_dim_matrix.DatabaseAdapter.to_knowledge_base`: one point per
    table, axes encode structural properties.  It is fast and automatic but
    *imperfect* — wide tables produce blurry, multi-topic colour blocks.

    **Stage 2** is what this class produces: one point per *row* in each
    focused sub-table.  Wide tables are split by
    :meth:`~four_dim_matrix.SchemaAnalyzer.suggest_normalization`; the
    resulting sub-tables each map to one z-axis colour.  The colour cloud is
    now clean, unambiguous, and directly interpretable as a topic-time-quantity
    space.

    Lineage connection
    ------------------
    Every Stage 2 DataPoint records in its ``payload["_lineage"]`` exactly
    which Stage 1 block it originated from (``stage1_z/t/x/y``) and which
    source row (``row_index``).  This preserves the chain:

    ``raw DB row → Stage 2 DataPoint → Stage 1 block → database table``
    """

    def __init__(
        self,
        adapter: DatabaseAdapter,
        analyzer: SchemaAnalyzer,
    ) -> None:
        self.adapter = adapter
        self.analyzer = analyzer

    # ------------------------------------------------------------------
    # Plan inspection
    # ------------------------------------------------------------------

    def plan(self) -> List[SubTablePlan]:
        """Return the normalization plan as a sorted list of :class:`SubTablePlan`.

        This is a read-only preview — no data is fetched from the database.
        Use it to inspect how the Stage 2 z-axis is structured before calling
        :meth:`build_stage2`.

        The list is sorted by ``stage2_z`` (alphabetical by sub-table name).
        """
        return _build_plan_entries(self.analyzer)

    # ------------------------------------------------------------------
    # Stage 2 builder
    # ------------------------------------------------------------------

    def build_stage2(self, conn: Any) -> KnowledgeBase:
        """Execute the normalization plan and return the Stage 2 KnowledgeBase.

        For every :class:`SubTablePlan` entry, rows are fetched from the
        source table in *conn*.  Each row produces one
        :class:`~four_dim_matrix.DataPoint` whose coordinates are assigned as
        follows:

        * **z** – ``plan.stage2_z`` (stable, alphabetical sub-table index).
        * **t** – first TEMPORAL column found anywhere in the parent table's
          analysis that is also present in this row; else
          :attr:`~four_dim_matrix.DatabaseAdapter.snapshot_time`.
        * **y** – for ``NUMERIC`` groups: first numeric column in the
          sub-table's column list; for all other groups: ``1.0`` (unit count).
        * **x** – first CATEGORICAL column in the sub-table's column list,
          integer-encoded in first-seen order per sub-table; falls back to the
          sequential row index when none is present.

        Every DataPoint's ``payload`` is the full row dict **plus** a
        ``"_lineage"`` key (see :meth:`lineage_for`).

        Parameters:
            conn: An open DBAPI-2 connection to the same database that the
                *adapter* was built from.

        Returns:
            A :class:`~four_dim_matrix.KnowledgeBase` with one DataPoint /
            ColorPoint per successfully loaded row.
        """
        plan_entries = _build_plan_entries(self.analyzer)

        # Stage 1 index: table_name → DataPoint (for lineage coordinates)
        stage1_by_table = _build_stage1_index(self.adapter)

        points: List[DataPoint] = []
        for entry in plan_entries:
            rows = _fetch_plan_rows(conn, entry.source_table, entry.columns)
            parent_analysis = self.analyzer.analyse_table(entry.source_table)
            stage1_pt = stage1_by_table.get(entry.source_table)

            x_encoder: Dict[Any, int] = {}
            for row_idx, row_dict in enumerate(rows):
                t = _t_for_row(row_dict, parent_analysis, self.adapter.snapshot_time)
                y = _y_for_row(row_dict, entry, parent_analysis)
                x = _x_for_row(row_dict, entry, parent_analysis, row_idx, x_encoder)

                lineage: Dict[str, Any] = {
                    "source_table": entry.source_table,
                    "sub_table": entry.sub_table_name,
                    "group": entry.group,
                    "row_index": row_idx,
                    "stage1_z": stage1_pt.z if stage1_pt else None,
                    "stage1_t": stage1_pt.t.isoformat() if stage1_pt else None,
                    "stage1_x": stage1_pt.x if stage1_pt else None,
                    "stage1_y": stage1_pt.y if stage1_pt else None,
                }
                payload = {**row_dict, "_lineage": lineage}
                points.append(
                    DataPoint(t=t, x=x, y=y, z=entry.stage2_z, payload=payload)
                )

        kb = KnowledgeBase()
        if points:
            kb.insert_many(points)
        return kb

    # ------------------------------------------------------------------
    # Lineage helpers
    # ------------------------------------------------------------------

    @staticmethod
    def lineage_for(stage2_point: DataPoint) -> Optional[Dict[str, Any]]:
        """Return the lineage dict embedded in a Stage 2 DataPoint's payload.

        Returns ``None`` if *stage2_point* has no ``_lineage`` key (i.e. was
        not produced by :meth:`build_stage2`).
        """
        if stage2_point.payload is None:
            return None
        return stage2_point.payload.get("_lineage")

    @staticmethod
    def stage2_points_from_source(
        stage2_kb: KnowledgeBase,
        source_table: str,
        group: Optional[str] = None,
    ) -> List[DataPoint]:
        """Return all Stage 2 DataPoints originating from *source_table*.

        Parameters:
            stage2_kb: The Stage 2 :class:`~four_dim_matrix.KnowledgeBase`
                returned by :meth:`build_stage2`.
            source_table: Filter by this source table name.
            group: Optional :class:`~four_dim_matrix.ColumnGroup` constant to
                further filter by column group (e.g. ``ColumnGroup.NUMERIC``).

        Returns:
            List of matching :class:`~four_dim_matrix.DataPoint` objects.
        """
        results: List[DataPoint] = []
        for pt in stage2_kb.data_matrix:
            lin = NormalizationPipeline.lineage_for(pt)
            if lin is None:
                continue
            if lin["source_table"] != source_table:
                continue
            if group is not None and lin["group"] != group:
                continue
            results.append(pt)
        return results

    def __repr__(self) -> str:
        return (
            f"NormalizationPipeline("
            f"tables={len(self.adapter.tables)}, "
            f"score={self.analyzer.normalization_score():.2f})"
        )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _build_plan_entries(analyzer: SchemaAnalyzer) -> List[SubTablePlan]:
    """Build the sorted, z-indexed list of SubTablePlan entries."""
    raw_entries: List[Dict[str, Any]] = []

    for analysis in analyzer.analyse_all():
        if analysis.is_wide_table:
            suggestion = analyzer.suggest_normalization(analysis.name)
            for st in suggestion["suggested_tables"]:
                group = _group_from_sub_name(st["name"], analysis.name)
                raw_entries.append({
                    "sub_table_name": st["name"],
                    "source_table": analysis.name,
                    "group": group,
                    "matrix_role": st["matrix_role"],
                    "columns": st["columns"],
                    "is_original": False,
                })
        else:
            # Already normalized – keep all columns; group = NORMALIZED
            table_obj = next(
                t for t in analyzer.adapter.tables if t.name == analysis.name
            )
            all_cols = [c.name for c in table_obj.columns]
            raw_entries.append({
                "sub_table_name": analysis.name,
                "source_table": analysis.name,
                "group": "NORMALIZED",
                "matrix_role": "z-topic (normalized)",
                "columns": all_cols,
                "is_original": True,
            })

    # Alphabetical sort → stable z-indices
    raw_entries.sort(key=lambda e: e["sub_table_name"])
    return [
        SubTablePlan(
            sub_table_name=e["sub_table_name"],
            source_table=e["source_table"],
            group=e["group"],
            matrix_role=e["matrix_role"],
            stage2_z=idx,
            columns=e["columns"],
            is_original=e["is_original"],
        )
        for idx, e in enumerate(raw_entries)
    ]


def _group_from_sub_name(sub_name: str, source_name: str) -> str:
    """Infer the ColumnGroup from a sub-table name like ``'{source}_numeric'``."""
    suffix = sub_name[len(source_name):].lstrip("_").upper()
    valid = {
        ColumnGroup.IDENTITY,
        ColumnGroup.RELATIONAL,
        ColumnGroup.TEMPORAL,
        ColumnGroup.NUMERIC,
        ColumnGroup.DESCRIPTIVE,
        ColumnGroup.CATEGORICAL,
        ColumnGroup.OTHER,
    }
    return suffix if suffix in valid else ColumnGroup.OTHER


def _build_stage1_index(adapter: DatabaseAdapter) -> Dict[str, DataPoint]:
    """Return a map of ``table_name → Stage 1 DataPoint``.

    Stage 1 is the schema-level snapshot produced by
    :meth:`~four_dim_matrix.DatabaseAdapter.to_data_points`.
    """
    return {pt.payload["name"]: pt for pt in adapter.to_data_points()}


def _fetch_plan_rows(
    conn: Any,
    table_name: str,
    columns: List[str],
) -> List[Dict[str, Any]]:
    """Fetch all rows from *table_name*, selecting only *columns*.

    Both the table name and each column name are properly quoted to prevent
    SQL injection.
    """
    quoted_table = _quote_identifier(table_name)
    quoted_cols = ", ".join(_quote_identifier(c) for c in columns)
    cursor = conn.cursor()
    cursor.execute(f"SELECT {quoted_cols} FROM {quoted_table}")
    col_names = [desc[0] for desc in cursor.description]
    return [dict(zip(col_names, row)) for row in cursor.fetchall()]


def _t_for_row(
    row_dict: Dict[str, Any],
    analysis: TableAnalysis,
    fallback: datetime,
) -> datetime:
    """Return the t-coordinate for this row.

    Tries each TEMPORAL column from the parent table analysis in declaration
    order; returns the first one that parses successfully.  Falls back to
    *fallback* (the adapter's ``snapshot_time``) if none can be parsed.
    """
    for col_name in analysis.column_groups.get(ColumnGroup.TEMPORAL, []):
        if col_name not in row_dict:
            continue
        t = _parse_t_value(row_dict[col_name], fallback)
        if t is not fallback:
            return t
    return fallback


def _y_for_row(
    row_dict: Dict[str, Any],
    entry: SubTablePlan,
    analysis: TableAnalysis,
) -> float:
    """Return the y-coordinate for this row.

    NUMERIC group: first numeric column in the sub-table's column list that
    can be cast to ``float``.
    All other groups: ``1.0`` (each row counts as one unit of presence).
    """
    if entry.group != ColumnGroup.NUMERIC:
        return 1.0
    numeric_in_sub = [
        c for c in entry.columns
        if c in analysis.column_groups.get(ColumnGroup.NUMERIC, [])
    ]
    for col_name in numeric_in_sub:
        raw = row_dict.get(col_name)
        try:
            return float(raw)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            continue
    return 1.0


def _x_for_row(
    row_dict: Dict[str, Any],
    entry: SubTablePlan,
    analysis: TableAnalysis,
    row_idx: int,
    encoder: Dict[Any, int],
) -> int:
    """Return the x-coordinate for this row.

    CATEGORICAL group (or any sub-table that contains categorical columns):
    integer-encodes the first categorical column value in first-seen order,
    keeping the encoder state across calls within the same sub-table.

    Falls back to the sequential *row_idx* when no categorical column is
    present.
    """
    categorical_in_sub = [
        c for c in entry.columns
        if c in analysis.column_groups.get(ColumnGroup.CATEGORICAL, [])
    ]
    if categorical_in_sub:
        raw_x = row_dict.get(categorical_in_sub[0])
        if raw_x not in encoder:
            encoder[raw_x] = len(encoder)
        return encoder[raw_x]
    return row_idx
