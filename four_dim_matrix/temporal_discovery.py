"""Track B: Temporal column discovery and t-axis mapping strategy.

Classifies every column in a table into one of five temporal roles:

* ``BUSINESS_TIME``    – domain-event timestamp (``order_date``, ``delivery_at``…)
* ``TECHNICAL_TIME``   – record-lifecycle timestamp (``created_at``, ``updated_at``…)
* ``VERSION_SEQUENCE`` – integer version / ETL batch number
* ``LOGICAL_ORDER``    – auto-increment PK used as proxy time
* ``SYNTHETIC``        – pure row-position order (no time signal at all)

:meth:`TemporalDiscoveryEngine.generate_t_mapping` converts the best
candidate into a :class:`TMappingStrategy` that maps directly to the
``t_source`` field of :class:`~four_dim_matrix.ColumnMapping`.

Example::

    from four_dim_matrix import DatabaseAdapter
    from four_dim_matrix.temporal_discovery import TemporalDiscoveryEngine

    adapter = DatabaseAdapter.from_sqlite("dictionary.db")
    engine  = TemporalDiscoveryEngine()

    for table in adapter.tables:
        strategy = engine.generate_t_mapping(table)
        print(table.name, "→", strategy.t_source_value,
              f"({strategy.column_name or 'none'})")
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Dict, List, Optional

from .db_adapter import ColumnInfo, ColumnType, TableInfo


# ---------------------------------------------------------------------------
# Temporal role enumeration
# ---------------------------------------------------------------------------

class TemporalType(Enum):
    """Classification of a column's temporal or ordering semantics."""

    BUSINESS_TIME    = auto()  # Domain-event time (order_date, delivery_at …)
    TECHNICAL_TIME   = auto()  # Record lifecycle (created_at, updated_at …)
    VERSION_SEQUENCE = auto()  # Integer version / ETL batch number
    LOGICAL_ORDER    = auto()  # Ordered by key or alphabet (static dicts)
    SYNTHETIC        = auto()  # Row-position-based synthetic order


# ---------------------------------------------------------------------------
# Result dataclasses
# ---------------------------------------------------------------------------

@dataclass
class TemporalColumn:
    """A column classified as having temporal or ordering semantics.

    Attributes:
        column_name: The column name.
        temporal_type: The discovered :class:`TemporalType`.
        confidence: Score in ``[0, 1]`` – higher means more certain.
        format_hint: Informal format hint (``"iso8601"``, ``"unix_timestamp"``,
            ``"integer_version"``, ``"auto"``, …).
        granularity: Time granularity (``"second"``, ``"day"``, ``"month"``,
            ``"version"``, ``"record"``).
    """

    column_name: str
    temporal_type: TemporalType
    confidence: float
    format_hint: str = "auto"
    granularity: str = "day"


@dataclass
class TMappingStrategy:
    """How a table's rows should be mapped to the t-axis.

    Attributes:
        source_type: The :class:`TemporalType` driving this strategy.
        column_name: Source column (``None`` for synthetic strategies).
        granularity: Time-granularity hint for axis labels.
        is_monotonic: Whether t values are expected to be non-decreasing.
        t_source_value: The ``t_source`` string to pass to
            :class:`~four_dim_matrix.ColumnMapping` (``"column"``,
            ``"version"``, ``"synthetic"``, ``"topology"``,
            ``"access_log"``).
        note: Human-readable explanation of the strategy choice.
        fallback_columns: Other temporal columns to fall back to if the
            primary column is missing.
    """

    source_type: TemporalType
    column_name: Optional[str]
    granularity: str = "day"
    is_monotonic: bool = True
    t_source_value: str = "column"
    note: str = ""
    fallback_columns: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source_type": self.source_type.name,
            "column_name": self.column_name,
            "granularity": self.granularity,
            "is_monotonic": self.is_monotonic,
            "t_source_value": self.t_source_value,
            "note": self.note,
            "fallback_columns": self.fallback_columns,
        }


# ---------------------------------------------------------------------------
# Discovery engine
# ---------------------------------------------------------------------------

class TemporalDiscoveryEngine:
    """Discover temporal columns and recommend a t-axis mapping strategy.

    Discovery algorithm
    -------------------
    For each column of a table:

    1. Check the SQL type: ``DATETIME`` / ``TIMESTAMP`` / ``DATE`` → strong
       temporal signal (+0.70 base confidence).
    2. Check the column name against:

       * ``_TECHNICAL_PATTERNS`` → :attr:`TemporalType.TECHNICAL_TIME`
       * ``_BUSINESS_PATTERNS``  → :attr:`TemporalType.BUSINESS_TIME`
       * ``_TEMPORAL_NAME``      → ambiguous; classified as TECHNICAL_TIME
         (conservative default).
       * ``_VERSION_PATTERNS``   on INTEGER columns → VERSION_SEQUENCE.

    The best candidate is selected by :meth:`generate_t_mapping` following
    the priority order: BUSINESS_TIME > TECHNICAL_TIME > VERSION_SEQUENCE >
    auto-increment PK (LOGICAL_ORDER) > inherited topology > SYNTHETIC.
    """

    _BUSINESS_PATTERNS = re.compile(
        r"(order|trade|payment|delivery|ship|bill|invoice|event|action|"
        r"login|logout|visit|purchase|transfer|request|response|"
        r"issued|occurred|happened|effective|due|expire|start|end|"
        r"transaction|signup|register|enroll|begin|finish|publish)",
        re.I,
    )
    _TECHNICAL_PATTERNS = re.compile(
        r"^(created|updated|modified|deleted|inserted|changed)(_at|_on|_ts)?$"
        r"|_(created|updated|modified|deleted|inserted)(_at|_on|_ts)?$",
        re.I,
    )
    _TEMPORAL_NAME = re.compile(
        r"(date|time|_at|_on|_ts|stamp|timestamp)",
        re.I,
    )
    _VERSION_PATTERNS = re.compile(
        r"(version|ver|revision|batch|etl|sync|import|seq|sequence|epoch)",
        re.I,
    )
    _TEMPORAL_TYPES = frozenset({ColumnType.DATETIME})

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    def discover_temporal_columns(self, table: TableInfo) -> List[TemporalColumn]:
        """Return all temporal/ordering columns in *table*, sorted by confidence."""
        results: List[TemporalColumn] = []
        for col in table.columns:
            tc = self._classify(col)
            if tc is not None:
                results.append(tc)
        results.sort(key=lambda c: c.confidence, reverse=True)
        return results

    def generate_t_mapping(
        self,
        table: TableInfo,
        center_table_name: Optional[str] = None,
    ) -> TMappingStrategy:
        """Return the recommended :class:`TMappingStrategy` for *table*.

        Priority order:

        1. Best **business-time** column → ``t_source="column"``
        2. Best **technical-time** column → ``t_source="column"``
        3. Best **version/sequence** column → ``t_source="version"``
        4. Auto-increment PK → ``t_source="synthetic"`` (logical order)
        5. Inherited from center table → ``t_source="topology"``
        6. Pure synthetic row-index order → ``t_source="synthetic"``
        """
        candidates = self.discover_temporal_columns(table)
        business  = [c for c in candidates if c.temporal_type == TemporalType.BUSINESS_TIME]
        technical = [c for c in candidates if c.temporal_type == TemporalType.TECHNICAL_TIME]
        versions  = [c for c in candidates if c.temporal_type == TemporalType.VERSION_SEQUENCE]

        # Priority 1: best business-time column
        if business:
            primary = business[0]
            return TMappingStrategy(
                source_type=TemporalType.BUSINESS_TIME,
                column_name=primary.column_name,
                granularity=primary.granularity,
                is_monotonic=False,
                t_source_value="column",
                note=(
                    f"Business-time column '{primary.column_name}' "
                    f"(confidence {primary.confidence:.2f})"
                ),
                fallback_columns=[c.column_name for c in technical + versions],
            )

        # Priority 2: best technical-time column
        if technical:
            primary = technical[0]
            return TMappingStrategy(
                source_type=TemporalType.TECHNICAL_TIME,
                column_name=primary.column_name,
                granularity=primary.granularity,
                is_monotonic=True,
                t_source_value="column",
                note=(
                    f"Technical-time column '{primary.column_name}' "
                    f"– no business-time column found."
                ),
                fallback_columns=[c.column_name for c in versions],
            )

        # Priority 3: version/sequence column
        if versions:
            primary = versions[0]
            return TMappingStrategy(
                source_type=TemporalType.VERSION_SEQUENCE,
                column_name=primary.column_name,
                granularity=primary.granularity,
                is_monotonic=True,
                t_source_value="version",
                note=(
                    f"No time column; using version/sequence "
                    f"column '{primary.column_name}'."
                ),
                fallback_columns=[],
            )

        # Priority 4: auto-increment primary key as logical order
        pk_col = next(
            (c for c in table.columns if c.primary_key and c.column_type == ColumnType.INTEGER),
            None,
        )
        if pk_col:
            return TMappingStrategy(
                source_type=TemporalType.LOGICAL_ORDER,
                column_name=pk_col.name,
                granularity="record",
                is_monotonic=True,
                t_source_value="synthetic",
                note=(
                    f"No time column; auto-increment PK "
                    f"'{pk_col.name}' used as logical order."
                ),
                fallback_columns=[],
            )

        # Priority 5: inherit from center table (topology)
        if center_table_name and center_table_name != table.name:
            return TMappingStrategy(
                source_type=TemporalType.LOGICAL_ORDER,
                column_name=None,
                granularity="inherited",
                is_monotonic=False,
                t_source_value="topology",
                note=(
                    f"No time column; t inherited from "
                    f"center entity table '{center_table_name}'."
                ),
                fallback_columns=[],
            )

        # Priority 6: pure synthetic row-index order
        return TMappingStrategy(
            source_type=TemporalType.SYNTHETIC,
            column_name=None,
            granularity="record",
            is_monotonic=True,
            t_source_value="synthetic",
            note="No time or ordering column found; using synthetic row-index order.",
            fallback_columns=[],
        )

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _classify(self, col: ColumnInfo) -> Optional[TemporalColumn]:
        """Classify *col* and return a :class:`TemporalColumn`, or ``None``."""
        name = col.name.lower()
        is_time_type = col.column_type in self._TEMPORAL_TYPES
        has_time_name = bool(self._TEMPORAL_NAME.search(name))

        if not is_time_type and not has_time_name:
            # Check for integer version/sequence columns
            if (
                col.column_type == ColumnType.INTEGER
                and self._VERSION_PATTERNS.search(name)
            ):
                return TemporalColumn(
                    column_name=col.name,
                    temporal_type=TemporalType.VERSION_SEQUENCE,
                    confidence=0.55,
                    format_hint="integer_version",
                    granularity="version",
                )
            return None

        base_confidence = 0.70 if is_time_type else 0.50
        if is_time_type and has_time_name:
            base_confidence = 0.85

        fmt = "iso8601" if is_time_type else "auto"
        gran = "second" if is_time_type else "day"

        if self._TECHNICAL_PATTERNS.search(name):
            return TemporalColumn(
                column_name=col.name,
                temporal_type=TemporalType.TECHNICAL_TIME,
                confidence=base_confidence,
                format_hint=fmt,
                granularity=gran,
            )

        if self._BUSINESS_PATTERNS.search(name):
            return TemporalColumn(
                column_name=col.name,
                temporal_type=TemporalType.BUSINESS_TIME,
                confidence=base_confidence + 0.10,
                format_hint=fmt,
                granularity=gran,
            )

        # Ambiguous: default to technical (more conservative)
        return TemporalColumn(
            column_name=col.name,
            temporal_type=TemporalType.TECHNICAL_TIME,
            confidence=base_confidence - 0.10,
            format_hint=fmt,
            granularity=gran,
        )
