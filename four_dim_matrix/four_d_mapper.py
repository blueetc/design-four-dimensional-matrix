"""Integration layer: FourDimensionalMapper.

Orchestrates Tracks A, B, and C into a single entry point that converts a
:class:`~four_dim_matrix.DatabaseAdapter` into a fully populated
:class:`~four_dim_matrix.KnowledgeBase`.

Pipeline
--------
1. **Track A** – :class:`~four_dim_matrix.EntityClusteringEngine` clusters
   tables into business entities and scores primary-key candidates.
2. **Track B** – :class:`~four_dim_matrix.TemporalDiscoveryEngine` selects
   the best t-axis strategy for each table (business time, technical time,
   version number, or synthetic row order).
3. **Track C** – :class:`~four_dim_matrix.ZAxisAllocator` assigns
   hierarchical :class:`~four_dim_matrix.ZCoordinate` values so every
   table's rows land on a semantically meaningful z-slot.

Two loading modes
-----------------
* **Schema mode** (default / ``conn=None``) – one DataPoint per table,
  coordinates ``(snapshot_time, column_count, row_count, z_scalar)``.
  Fast: no SQL queries beyond what the adapter already ran.
* **Row mode** (``conn`` provided) – one DataPoint per row, using the
  t-axis strategy selected by Track B.

Example::

    from four_dim_matrix import DatabaseAdapter
    from four_dim_matrix.four_d_mapper import FourDimensionalMapper, MatrixConfig

    adapter = DatabaseAdapter.from_sqlite("erp.db")
    mapper  = FourDimensionalMapper(adapter, MatrixConfig(target_entity_count=15))

    # Preview mapping without loading data
    plan = mapper.analyse()
    for entity in plan["entities"]:
        print(entity["name"], "→", entity["z0_index"],
              f"({len(entity['member_tables'])} tables)")

    # Full load (row mode)
    import sqlite3
    conn = sqlite3.connect("erp.db")
    kb, plan = mapper.build(conn=conn)
    conn.close()
    print(f"Loaded {len(kb.data_matrix)} data points")
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from .color_mapping import ColorConfig
from .data_matrix import DataPoint
from .db_adapter import (
    ColumnMapping,
    DatabaseAdapter,
    TableInfo,
    TableMapping,
    _fetch_rows,
    _parse_t_value,
)
from .key_discovery import CoreEntity, EntityClusteringEngine
from .knowledge_base import KnowledgeBase
from .temporal_discovery import TemporalDiscoveryEngine, TemporalType, TMappingStrategy
from .z_axis_encoding import RelationType, ZAxisAllocator, ZCoordinate


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class MatrixConfig:
    """Configuration for :class:`FourDimensionalMapper`.

    Attributes:
        target_entity_count: Desired number of business-entity clusters.
            ``None`` lets the Louvain algorithm decide naturally.
        color_config: Optional :class:`~four_dim_matrix.ColorConfig`.  When
            ``None`` a default configuration is auto-calibrated after loading.
        include_lineage: When ``True`` (default) each DataPoint payload
            carries a ``_lineage`` sub-dictionary with z-component details
            and the source table name.
    """

    target_entity_count: Optional[int] = None
    color_config: Optional[ColorConfig] = None
    include_lineage: bool = True


# ---------------------------------------------------------------------------
# Mapper
# ---------------------------------------------------------------------------

class FourDimensionalMapper:
    """End-to-end pipeline: :class:`~four_dim_matrix.DatabaseAdapter` → KnowledgeBase.

    Parameters:
        adapter: A pre-populated :class:`~four_dim_matrix.DatabaseAdapter`
            (call :meth:`~four_dim_matrix.DatabaseAdapter.from_sqlite` or
            :meth:`~four_dim_matrix.DatabaseAdapter.from_connection` first).
        config: Optional :class:`MatrixConfig`.  Defaults are suitable for
            a first exploration run.

    Notes
    -----
    The mapper is stateful: :meth:`analyse` and :meth:`build` both populate
    ``self.entities``, ``self.t_strategies``, and ``self.z_allocator``.
    Calling :meth:`build` after :meth:`analyse` reuses the analysis results
    and does not re-run the clustering.
    """

    def __init__(
        self,
        adapter: DatabaseAdapter,
        config: Optional[MatrixConfig] = None,
    ) -> None:
        self.adapter = adapter
        self.config = config or MatrixConfig()
        self.entities: List[CoreEntity] = []
        self.t_strategies: Dict[str, TMappingStrategy] = {}
        self.z_allocator = ZAxisAllocator()

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    def analyse(self) -> Dict[str, Any]:
        """Run the analysis phase only (no data loading) and return the plan.

        Populates :attr:`entities`, :attr:`t_strategies`, and
        :attr:`z_allocator` so that a subsequent :meth:`build` call can
        reuse the results.

        Returns:
            A dictionary with keys ``entity_count``, ``table_count``,
            ``entities`` (list of entity summaries), and ``z_allocation``
            (utilisation report).
        """
        self._run_analysis()
        return self._build_plan()

    def build(self, conn: Optional[Any] = None) -> tuple:
        """Run the full pipeline and return ``(KnowledgeBase, plan)``.

        Parameters:
            conn: An open DBAPI-2 connection to use for row-level loading.
                When ``None``, schema-snapshot mode is used (one DataPoint
                per table).

        Returns:
            ``(kb, plan)`` where *kb* is the populated
            :class:`~four_dim_matrix.KnowledgeBase` and *plan* is the
            same analysis dictionary returned by :meth:`analyse`.
        """
        if not self.entities:
            self._run_analysis()

        if conn is not None:
            kb = self._load_rows(conn)
        else:
            kb = self._load_schema_snapshot()

        return kb, self._build_plan()

    # ------------------------------------------------------------------
    # Internal: analysis phase
    # ------------------------------------------------------------------

    def _run_analysis(self) -> None:
        """Execute Tracks A, B, and C without loading any row data."""
        tables = self.adapter.tables

        # Track A: entity clustering
        clustering = EntityClusteringEngine(tables)
        self.entities = clustering.cluster_entities(
            target_clusters=self.config.target_entity_count
        )

        # Track B: temporal mapping
        temporal_engine = TemporalDiscoveryEngine()
        entity_by_table: Dict[str, CoreEntity] = {
            tname: entity
            for entity in self.entities
            for tname in entity.member_tables
        }
        for table in tables:
            entity = entity_by_table.get(table.name)
            center = entity.center_table if entity else None
            self.t_strategies[table.name] = temporal_engine.generate_t_mapping(
                table, center_table_name=center
            )

        # Track C: z-axis allocation
        self.z_allocator = ZAxisAllocator()
        for entity in self.entities:
            self.z_allocator.allocate_cluster(entity, tables)

    # ------------------------------------------------------------------
    # Internal: loading
    # ------------------------------------------------------------------

    def _load_schema_snapshot(self) -> KnowledgeBase:
        """Schema mode: one DataPoint per table."""
        kb = KnowledgeBase(config=self.config.color_config)
        allocated = self.z_allocator.allocated
        points: List[DataPoint] = []

        for table in sorted(self.adapter.tables, key=lambda t: t.name):
            coord = allocated.get(table.name)
            z = coord.to_scalar() if coord else 0
            payload: Dict[str, Any] = table.to_dict()
            if self.config.include_lineage:
                strategy = self.t_strategies.get(table.name)
                payload["_lineage"] = {
                    "source_table": table.name,
                    "z_components": coord.to_dict() if coord else {},
                    "t_strategy": strategy.to_dict() if strategy else {},
                }
            points.append(DataPoint(
                t=self.adapter.snapshot_time,
                x=table.column_count,
                y=float(table.row_count),
                z=z,
                payload=payload,
            ))

        kb.insert_many(points)
        return kb

    def _load_rows(self, conn: Any) -> KnowledgeBase:
        """Row mode: one DataPoint per row in each table."""
        kb = KnowledgeBase(config=self.config.color_config)
        allocated = self.z_allocator.allocated
        table_map: Dict[str, TableInfo] = {t.name: t for t in self.adapter.tables}
        points: List[DataPoint] = []

        for table in sorted(self.adapter.tables, key=lambda t: t.name):
            coord = allocated.get(table.name)
            if coord is None:
                continue
            z = coord.to_scalar()
            strategy = self.t_strategies.get(table.name)
            table_info = table_map.get(table.name)
            if table_info is None:
                continue

            y_col = _pick_y_column(table_info)
            if y_col is None:
                continue

            t_col = strategy.column_name if strategy else None
            t_src = strategy.t_source_value if strategy else "synthetic"
            mapping = ColumnMapping(
                y_column=y_col,
                t_column=t_col,
                t_source=t_src,
            )
            tm = TableMapping(table_name=table.name, mapping=mapping)

            try:
                rows = _fetch_rows(conn, tm)
            except Exception:
                continue

            x_encoder: Dict[Any, int] = {}
            for row_idx, (col_names, row_values) in enumerate(rows):
                row_dict = dict(zip(col_names, row_values))

                raw_y = row_dict.get(y_col)
                try:
                    y = float(raw_y)  # type: ignore[arg-type]
                except (TypeError, ValueError):
                    y = float(row_idx + 1)

                t = _derive_t_from_strategy(
                    row_dict, row_idx, strategy, self.adapter.snapshot_time
                )

                # x = z₁ (relation depth); 0 for core, 1 for direct FK, …
                x = coord.z1

                payload: Dict[str, Any] = dict(row_dict)
                if self.config.include_lineage:
                    payload["_lineage"] = {
                        "source_table": table.name,
                        "row_index": row_idx,
                        "z_components": coord.to_dict(),
                    }
                points.append(DataPoint(t=t, x=x, y=y, z=z, payload=payload))

        kb.insert_many(points)
        return kb

    # ------------------------------------------------------------------
    # Internal: plan builder
    # ------------------------------------------------------------------

    def _build_plan(self) -> Dict[str, Any]:
        allocated = self.z_allocator.allocated
        total_entities = len(self.entities)

        entity_summaries = []
        for entity in self.entities:
            tables_info = []
            for tname in entity.member_tables:
                coord = allocated.get(tname)
                strategy = self.t_strategies.get(tname)
                tables_info.append({
                    "table": tname,
                    "z_scalar": coord.to_scalar() if coord else None,
                    "z_components": coord.to_dict() if coord else {},
                    "color": coord.to_hex_color(total_entities) if coord else "#808080",
                    "relation_type": (
                        RelationType(coord.z1).name if coord else "UNKNOWN"
                    ),
                    "t_strategy": strategy.to_dict() if strategy else {},
                })
            entity_summaries.append({
                **entity.to_dict(),
                "hue": entity.get_z0_hue(total_entities),
                "tables": tables_info,
            })

        return {
            "entity_count": total_entities,
            "table_count": sum(len(e.member_tables) for e in self.entities),
            "entities": entity_summaries,
            "z_allocation": self.z_allocator.allocation_report(total_entities),
        }


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------

def _pick_y_column(table: TableInfo) -> Optional[str]:
    """Return the best non-PK numeric column to use as y."""
    from .db_adapter import ColumnType

    for col in table.columns:
        if col.column_type in (ColumnType.FLOAT, ColumnType.INTEGER) and not col.primary_key:
            return col.name
    return table.columns[0].name if table.columns else None


def _derive_t_from_strategy(
    row_dict: Dict[str, Any],
    row_idx: int,
    strategy: Optional[TMappingStrategy],
    fallback: datetime,
) -> datetime:
    """Derive the t-coordinate for a single row using *strategy*."""
    _EPOCH = datetime(2000, 1, 1)

    if strategy is None:
        return _EPOCH + timedelta(days=row_idx)

    src = strategy.t_source_value

    if src == "column" and strategy.column_name:
        raw = row_dict.get(strategy.column_name)
        return _parse_t_value(raw, fallback)

    if src == "version" and strategy.column_name:
        raw = row_dict.get(strategy.column_name)
        try:
            return _EPOCH + timedelta(days=int(float(str(raw))))
        except (TypeError, ValueError):
            return fallback

    if src in ("synthetic", "topology"):
        return _EPOCH + timedelta(days=row_idx)

    if src == "access_log" and strategy.column_name:
        raw = row_dict.get(strategy.column_name)
        try:
            return _EPOCH + timedelta(days=int(float(str(raw))))
        except (TypeError, ValueError):
            return fallback

    return fallback
