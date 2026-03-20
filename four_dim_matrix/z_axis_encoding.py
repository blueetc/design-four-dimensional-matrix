"""Track C: Hierarchical z-axis coordinate system and space allocator.

Encoding
--------
``z_scalar = z0 * 100 + z1 * 10 + z2``

* ``z0`` (0–99)  – core business entity ID (one per :class:`~four_dim_matrix.CoreEntity`).
* ``z1`` (0–9)   – relationship type; see :class:`RelationType`.
* ``z2`` (0–9)   – sub-index within the (z0, z1) bucket.

Capacity
--------
15 entities × 10 relation types × 10 sub-indices = **1 500 z-slots**.
A typical database with 40 tables and 15 core entities uses fewer than 60
slots, leaving plenty of headroom.

Colour mapping
--------------
* ``z0`` drives the **base hue** (evenly spaced 360° / N entities).
* ``z1`` applies a small ±15° shift so relation-type variants of the same
  entity family share a recognisable hue while remaining distinguishable.
* ``z2`` applies a tiny ±4.5° shift to distinguish sub-tables.

Example::

    from four_dim_matrix.z_axis_encoding import ZCoordinate, RelationType

    coord = ZCoordinate(z0=1, z1=RelationType.ONE_TO_MANY, z2=0)
    print(coord.to_scalar())       # 120
    print(coord.to_hex_color())    # e.g. '#e06644'
    print(ZCoordinate.from_scalar(120))  # ZCoordinate(z0=1, z1=2, z2=0)
"""

from __future__ import annotations

import colorsys
import re
from dataclasses import dataclass
from enum import IntEnum
from typing import Any, Dict, List, Optional, Tuple

from .db_adapter import TableInfo
from .temporal_discovery import TemporalDiscoveryEngine


# ---------------------------------------------------------------------------
# Relation type enum
# ---------------------------------------------------------------------------

class RelationType(IntEnum):
    """Standard relationship types mapped to the z₁ layer.

    The integer value **is** z₁ – do not change existing values once
    they have been assigned to a running knowledge base.
    """

    PRIMARY      = 0  # The core entity table itself
    EXTENSION    = 1  # One-to-one extension (vertically partitioned attributes)
    ONE_TO_MANY  = 2  # One-to-many child tables (order_items, addresses …)
    MANY_TO_MANY = 3  # Junction / bridge tables
    HIERARCHY    = 4  # Self-referential or parent-child (categories, org chart)
    TEMPORAL     = 5  # Time-series / event-log tables for this entity
    AGGREGATION  = 6  # Pre-aggregated / statistical summary tables
    REFERENCE    = 7  # Reference / code / dictionary tables (static)
    MISC         = 8  # Anything that does not fit the above
    RESERVED     = 9  # Reserved for future use / domain-specific extensions


# ---------------------------------------------------------------------------
# Z-coordinate dataclass
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ZCoordinate:
    """Immutable three-level z-axis coordinate.

    Encoding: ``z_scalar = z0 * 100 + z1 * 10 + z2``

    Parameters:
        z0: Core business entity ID (0–99).
        z1: Relationship type (0–9); see :class:`RelationType`.
        z2: Sub-index within the (z0, z1) bucket (0–9).
    """

    z0: int
    z1: int = 0
    z2: int = 0

    def __post_init__(self) -> None:
        if not (0 <= self.z1 <= 9):
            raise ValueError(f"z1 must be in 0–9, got {self.z1}")
        if not (0 <= self.z2 <= 9):
            raise ValueError(f"z2 must be in 0–9, got {self.z2}")

    # ------------------------------------------------------------------
    # Scalar conversion
    # ------------------------------------------------------------------

    def to_scalar(self) -> int:
        """Return the flat integer z-value used for matrix indexing."""
        return self.z0 * 100 + self.z1 * 10 + self.z2

    @classmethod
    def from_scalar(cls, z: int) -> "ZCoordinate":
        """Decode a flat z-value back to its three-level representation."""
        z0, rem = divmod(z, 100)
        z1, z2  = divmod(rem, 10)
        return cls(z0=z0, z1=z1, z2=z2)

    # ------------------------------------------------------------------
    # Colour helpers
    # ------------------------------------------------------------------

    def get_hue(self, total_entities: int = 15) -> float:
        """Return the HSL hue (degrees) for this coordinate.

        The base hue is evenly distributed across the colour wheel for
        *total_entities* entities.  z₁ and z₂ apply small offsets so
        tables within the same entity share a recognisable hue while
        remaining visually distinguishable.
        """
        base_hue = (self.z0 * 360.0 / max(total_entities, 1)) % 360.0
        # ±13.5° centred on z1=4.5
        z1_shift = (self.z1 - 4.5) * 3.0
        # ±4.5° centred on z2=4.5
        z2_shift = (self.z2 - 4.5) * 1.0
        return (base_hue + z1_shift + z2_shift) % 360.0

    def to_hex_color(
        self,
        total_entities: int = 15,
        saturation: float = 0.65,
        lightness: float = 0.52,
    ) -> str:
        """Return a ``#rrggbb`` colour string for this coordinate.

        Primary tables (z₁ = 0) retain full saturation; deeper relation
        types and sub-tables are slightly desaturated / darkened to give a
        visual hierarchy within each entity family.
        """
        hue = self.get_hue(total_entities)
        sat = saturation if self.z1 == RelationType.PRIMARY else saturation * 0.85
        lit = lightness if self.z2 == 0 else lightness * 0.90
        r, g, b = colorsys.hls_to_rgb(hue / 360.0, lit, sat)
        return f"#{round(r * 255):02x}{round(g * 255):02x}{round(b * 255):02x}"

    def color_family(self, total_entities: int = 15) -> str:
        """Return a human-readable colour-family name (for UI legends)."""
        families = [
            "Red", "Orange-Red", "Orange", "Yellow-Orange", "Yellow",
            "Yellow-Green", "Green", "Teal", "Cyan", "Sky-Blue",
            "Blue", "Indigo", "Violet", "Purple", "Magenta",
        ]
        return families[self.z0 % len(families)]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "z0": self.z0,
            "z1": self.z1,
            "z2": self.z2,
            "scalar": self.to_scalar(),
        }


# ---------------------------------------------------------------------------
# Z-axis allocator
# ---------------------------------------------------------------------------

class ZAxisAllocator:
    """Allocate :class:`ZCoordinate` objects to database tables.

    For each table the allocator decides:

    * Which entity cluster it belongs to (z₀ from
      :class:`~four_dim_matrix.CoreEntity`).
    * What relationship it has to the cluster's core table (z₁ from
      :class:`RelationType`).
    * Its sub-index within that relationship bucket (z₂,
      auto-incremented per (z₀, z₁) pair).

    Auto-detection rules (applied by :meth:`allocate_cluster`)
    -----------------------------------------------------------
    * Core table → ``PRIMARY`` (0), z₂ = 0.
    * Table name contains ``stat``, ``summary``, ``count``, ``agg``,
      ``report``, ``metric`` → ``AGGREGATION``.
    * Table name contains ``type``, ``code``, ``ref``, ``dict``,
      ``lookup``, ``enum``, ``const``, ``config`` → ``REFERENCE``.
    * Table has ≥ 2 columns ending in ``_id`` → ``MANY_TO_MANY``.
    * Table has high-confidence temporal columns → ``TEMPORAL``.
    * Table has one FK to the core and ≤ 5 columns → ``EXTENSION``.
    * Table has one FK to the core and > 5 columns → ``ONE_TO_MANY``.
    * Anything else → ``MISC``.
    """

    def __init__(self) -> None:
        # table_name → ZCoordinate
        self._allocated: Dict[str, ZCoordinate] = {}
        # (z0, z1) → next available z2
        self._z1_counters: Dict[Tuple[int, int], int] = {}

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    def allocate(
        self,
        table_name: str,
        z0: int,
        relation_type: RelationType,
        is_core: bool = False,
    ) -> ZCoordinate:
        """Assign and record a :class:`ZCoordinate` for *table_name*.

        If *is_core* is ``True`` the table receives z₁ = PRIMARY, z₂ = 0
        regardless of *relation_type*.
        """
        if is_core:
            coord = ZCoordinate(z0=z0, z1=int(RelationType.PRIMARY), z2=0)
        else:
            z1 = int(relation_type)
            z2 = self._next_z2(z0, z1)
            coord = ZCoordinate(z0=z0, z1=z1, z2=z2)
        self._allocated[table_name] = coord
        return coord

    def allocate_cluster(
        self,
        entity: Any,           # CoreEntity – avoid circular import
        all_tables: List[TableInfo],
    ) -> Dict[str, ZCoordinate]:
        """Allocate z-coordinates for every table in *entity*'s cluster.

        Returns a mapping ``{table_name: ZCoordinate}``.
        """
        z0 = entity.z0_index
        table_map: Dict[str, TableInfo] = {t.name: t for t in all_tables}
        result: Dict[str, ZCoordinate] = {}

        for table_name in entity.member_tables:
            is_core = table_name == entity.center_table
            table_info = table_map.get(table_name)
            if table_info is None:
                continue
            rel = _infer_relation_type(table_info, entity.center_table, table_map)
            coord = self.allocate(table_name, z0, rel, is_core=is_core)
            result[table_name] = coord

        return result

    @property
    def allocated(self) -> Dict[str, ZCoordinate]:
        """Read-only snapshot of the current allocation map."""
        return dict(self._allocated)

    def allocation_report(self, total_entities: int = 15) -> Dict[str, Any]:
        """Return a summary of z-axis space utilisation."""
        by_entity: Dict[int, List[str]] = {}
        for table_name, coord in self._allocated.items():
            by_entity.setdefault(coord.z0, []).append(table_name)

        return {
            "total_tables_allocated": len(self._allocated),
            "entities": {
                z0: {
                    "table_count": len(tables),
                    "tables": sorted(tables),
                    "z1_distribution": self._z1_dist(z0),
                }
                for z0, tables in sorted(by_entity.items())
            },
        }

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _next_z2(self, z0: int, z1: int) -> int:
        """Return and increment the next available z₂ for bucket (z₀, z₁)."""
        key = (z0, z1)
        z2 = self._z1_counters.get(key, 0)
        if z2 > 9:
            raise ValueError(
                f"Bucket z0={z0}, z1={z1} is full (max 10 sub-indices). "
                "Use RelationType.MISC or a different z₁ bucket."
            )
        self._z1_counters[key] = z2 + 1
        return z2

    def _z1_dist(self, z0: int) -> Dict[str, int]:
        dist: Dict[str, int] = {}
        for coord in self._allocated.values():
            if coord.z0 != z0:
                continue
            try:
                name = RelationType(coord.z1).name
            except ValueError:
                name = str(coord.z1)
            dist[name] = dist.get(name, 0) + 1
        return dist


# ---------------------------------------------------------------------------
# Internal: relation-type inference
# ---------------------------------------------------------------------------

_AGGREGATION_TOKENS = frozenset([
    "stat", "stats", "summary", "count", "counts", "agg",
    "report", "metric", "metrics",
])
_REFERENCE_TOKENS = frozenset([
    "type", "types", "code", "codes", "ref", "refs",
    "dict", "lookup", "enum", "const", "config",
])


def _name_has_token(table_name: str, token_set: frozenset) -> bool:
    """Return True when any underscore-delimited token of *table_name* is in *token_set*."""
    return any(tok in token_set for tok in table_name.lower().split("_"))
_TEMPORAL_ENGINE = TemporalDiscoveryEngine()


def _infer_relation_type(
    table: TableInfo,
    core_table_name: str,
    table_map: Dict[str, TableInfo],
) -> RelationType:
    """Heuristically assign a :class:`RelationType` for *table*."""
    name = table.name.lower()

    if _name_has_token(table.name, _AGGREGATION_TOKENS):
        return RelationType.AGGREGATION

    if _name_has_token(table.name, _REFERENCE_TOKENS):
        return RelationType.REFERENCE

    fk_cols = [c for c in table.columns if c.name.lower().endswith("_id")]

    if len(fk_cols) >= 2:
        return RelationType.MANY_TO_MANY

    # High-confidence temporal columns → temporal event log
    temporal = _TEMPORAL_ENGINE.discover_temporal_columns(table)
    if temporal and any(tc.confidence > 0.7 for tc in temporal):
        return RelationType.TEMPORAL

    # Extension or one-to-many based on FK to core
    core_lower = core_table_name.lower()
    for col in fk_cols:
        prefix = col.name.lower()[:-3]  # strip "_id"
        if (
            prefix == core_lower
            or prefix == core_lower.rstrip("s")
            or core_lower.startswith(prefix)
        ):
            return RelationType.EXTENSION if len(table.columns) <= 5 else RelationType.ONE_TO_MANY

    return RelationType.MISC
