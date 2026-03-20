"""DataMatrix – the first of the two four-dimensional matrices.

Each cell is addressed by ``(t, x, y, z)`` and stores an arbitrary JSON-
serialisable dictionary (``payload``).  All four coordinates are stored
alongside every record so that the matrix can be queried efficiently without
a dense multi-dimensional array.
"""

from __future__ import annotations

import json
import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, Iterator, List, Optional


@dataclass
class DataPoint:
    """A single record inside the DataMatrix.

    Attributes:
        t: Global time coordinate (ISO-8601 string *or* datetime).
        x: Business-cycle / phase coordinate (non-negative integer).
        y: Total-quantity / value coordinate (float).
        z: Topic / category coordinate (non-negative integer).
        payload: Arbitrary JSON-serialisable dictionary with the full
            business record.
    """

    t: datetime
    x: int
    y: float
    z: int
    payload: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if isinstance(self.t, str):
            self.t = datetime.fromisoformat(self.t)
        if self.x < 0:
            raise ValueError(f"x must be >= 0, got {self.x}")
        if self.z < 0:
            raise ValueError(f"z must be >= 0, got {self.z}")

    @property
    def coordinates(self) -> tuple:
        """Return the four-dimensional address ``(t, x, y, z)``."""
        return (self.t, self.x, self.y, self.z)

    def to_dict(self) -> Dict[str, Any]:
        """Serialise to a plain dictionary."""
        return {
            "t": self.t.isoformat(),
            "x": self.x,
            "y": self.y,
            "z": self.z,
            "payload": self.payload,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DataPoint":
        """Deserialise from a plain dictionary."""
        return cls(
            t=data["t"],
            x=data["x"],
            y=data["y"],
            z=data["z"],
            payload=data.get("payload", {}),
        )

    def to_json(self) -> str:
        """Serialise to a JSON string."""
        return json.dumps(self.to_dict(), default=str)


class DataMatrix:
    """In-memory four-dimensional data store.

    The matrix is implemented as a flat list of :class:`DataPoint` objects.
    This keeps the storage sparse (useful when many cells are empty) and
    lets us query along any combination of dimensions.

    Example::

        from datetime import datetime
        from four_dim_matrix import DataMatrix, DataPoint

        dm = DataMatrix()
        dm.insert(DataPoint(
            t=datetime(2024, 1, 1),
            x=1,
            y=15000.0,
            z=0,
            payload={"product": "A", "revenue": 15000},
        ))
        results = dm.query(z=0)
    """

    def __init__(self) -> None:
        self._records: List[DataPoint] = []

    # ------------------------------------------------------------------
    # Mutation
    # ------------------------------------------------------------------

    def insert(self, point: DataPoint) -> None:
        """Append *point* to the matrix."""
        self._records.append(point)

    def insert_many(self, points: List[DataPoint]) -> None:
        """Append multiple points at once."""
        self._records.extend(points)

    # ------------------------------------------------------------------
    # Querying
    # ------------------------------------------------------------------

    def query(
        self,
        t: Optional[datetime] = None,
        x: Optional[int] = None,
        y: Optional[float] = None,
        z: Optional[int] = None,
        t_from: Optional[datetime] = None,
        t_to: Optional[datetime] = None,
        x_min: Optional[int] = None,
        x_max: Optional[int] = None,
        y_min: Optional[float] = None,
        y_max: Optional[float] = None,
    ) -> List[DataPoint]:
        """Return all DataPoints matching the given filter criteria.

        Exact-match filters (``t``, ``x``, ``y``, ``z``) are ANDed together
        with range filters (``t_from``/``t_to``, ``x_min``/``x_max``,
        ``y_min``/``y_max``).
        """
        results: List[DataPoint] = []
        for pt in self._records:
            if t is not None and pt.t != t:
                continue
            if x is not None and pt.x != x:
                continue
            if y is not None and pt.y != y:
                continue
            if z is not None and pt.z != z:
                continue
            if t_from is not None and pt.t < t_from:
                continue
            if t_to is not None and pt.t > t_to:
                continue
            if x_min is not None and pt.x < x_min:
                continue
            if x_max is not None and pt.x > x_max:
                continue
            if y_min is not None and pt.y < y_min:
                continue
            if y_max is not None and pt.y > y_max:
                continue
            results.append(pt)
        return results

    # ------------------------------------------------------------------
    # Aggregation helpers
    # ------------------------------------------------------------------

    def distinct_z(self) -> List[int]:
        """Return the sorted list of distinct ``z`` values (topic IDs)."""
        return sorted({pt.z for pt in self._records})

    def distinct_t(self) -> List[datetime]:
        """Return the sorted list of distinct ``t`` values."""
        return sorted({pt.t for pt in self._records})

    def y_range(self) -> tuple:
        """Return ``(min_y, max_y)`` across all records, or ``(0, 1)``."""
        if not self._records:
            return (0.0, 1.0)
        ys = [pt.y for pt in self._records]
        return (min(ys), max(ys))

    def aggregate_y_by_z(self, t: Optional[datetime] = None) -> Dict[int, float]:
        """Return total ``y`` grouped by ``z`` (optionally filtered by ``t``)."""
        totals: Dict[int, float] = {}
        points = self.query(t=t) if t is not None else self._records
        for pt in points:
            totals[pt.z] = totals.get(pt.z, 0.0) + pt.y
        return totals

    def trend_by_t(self, z: Optional[int] = None) -> Dict[datetime, float]:
        """Return total ``y`` per time-step ``t``, optionally for a single ``z``."""
        trend: Dict[datetime, float] = {}
        points = self.query(z=z) if z is not None else self._records
        for pt in points:
            trend[pt.t] = trend.get(pt.t, 0.0) + pt.y
        return dict(sorted(trend.items()))

    # ------------------------------------------------------------------
    # Level-of-detail (LOD) / performance helpers
    # ------------------------------------------------------------------

    def aggregate_by_time(
        self,
        resolution: str = "day",
        z: Optional[int] = None,
    ) -> "DataMatrix":
        """Return a new :class:`DataMatrix` with ``t`` bucketed to *resolution*.

        Points with the same bucketed ``(t, x, z)`` are merged by summing
        their ``y`` values.  This is the primary pre-aggregation step for
        rendering large datasets at reduced fidelity.

        Parameters:
            resolution: Bucketing granularity – one of ``"hour"``,
                ``"day"`` (default), ``"month"``, or ``"year"``.
            z: When provided only records for topic *z* are included.

        Returns:
            A new DataMatrix with one record per unique bucketed
            ``(t_bucket, x, z)`` coordinate.
        """
        def _bucket(t: datetime) -> datetime:
            if resolution == "hour":
                return t.replace(minute=0, second=0, microsecond=0)
            if resolution == "month":
                return t.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            if resolution == "year":
                return t.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
            # default: "day"
            return t.replace(hour=0, minute=0, second=0, microsecond=0)

        source = self.query(z=z) if z is not None else list(self._records)
        buckets: Dict[tuple, DataPoint] = {}
        for pt in source:
            key = (_bucket(pt.t), pt.x, pt.z)
            if key in buckets:
                buckets[key] = DataPoint(
                    t=key[0], x=pt.x, y=buckets[key].y + pt.y, z=pt.z,
                    payload=buckets[key].payload,
                )
            else:
                buckets[key] = DataPoint(
                    t=key[0], x=pt.x, y=pt.y, z=pt.z, payload=pt.payload
                )

        result = DataMatrix()
        result.insert_many(sorted(buckets.values(), key=lambda p: p.t))
        return result

    def downsample(self, max_points: int, seed: int = 42) -> "DataMatrix":
        """Return a random subsample of at most *max_points* records.

        The sample is stratified by ``z`` so that each topic is represented
        proportionally.  Uses *seed* for reproducibility.

        Parameters:
            max_points: Maximum number of records in the returned matrix.
            seed: Random seed for reproducibility.

        Returns:
            A new DataMatrix with at most *max_points* records.
        """
        if len(self._records) <= max_points:
            return self  # nothing to do

        # Stratify by z
        by_z: Dict[int, List[DataPoint]] = {}
        for pt in self._records:
            by_z.setdefault(pt.z, []).append(pt)

        rng = random.Random(seed)
        sampled: List[DataPoint] = []

        for z_val, pts in sorted(by_z.items()):
            # Proportional quota per topic, floor to avoid over-sampling
            quota = max(1, int(max_points * len(pts) / len(self._records)))
            quota = min(quota, len(pts))
            sampled.extend(rng.sample(pts, quota))

        # Hard cap: if rounding pushed us over, trim randomly
        if len(sampled) > max_points:
            rng.shuffle(sampled)
            sampled = sampled[:max_points]

        result = DataMatrix()
        result.insert_many(sampled)
        return result

    # ------------------------------------------------------------------
    # Persistence helpers
    # ------------------------------------------------------------------

    def to_list(self) -> List[Dict[str, Any]]:
        """Export all records as a list of plain dictionaries."""
        return [pt.to_dict() for pt in self._records]

    @classmethod
    def from_list(cls, records: List[Dict[str, Any]]) -> "DataMatrix":
        """Import from a list of plain dictionaries."""
        dm = cls()
        dm.insert_many([DataPoint.from_dict(r) for r in records])
        return dm

    # ------------------------------------------------------------------
    # Dunder helpers
    # ------------------------------------------------------------------

    def __len__(self) -> int:
        return len(self._records)

    def __iter__(self) -> Iterator[DataPoint]:
        return iter(self._records)

    def __repr__(self) -> str:
        return f"DataMatrix(records={len(self._records)})"
