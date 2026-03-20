"""KnowledgeBase – high-level API for the dual-matrix architecture.

The :class:`KnowledgeBase` keeps a :class:`~four_dim_matrix.DataMatrix` and
a :class:`~four_dim_matrix.ColorMatrix` in sync.  Inserting a
:class:`~four_dim_matrix.DataPoint` automatically generates the matching
:class:`~four_dim_matrix.ColorPoint` via the configured
:class:`~four_dim_matrix.ColorMapper`.

It also provides higher-level analysis methods:

* **trend** – total quantity along the time axis (optionally per topic).
* **snapshot** – cross-topic view of a single time slice.
* **lookup_by_color** – hover-style reverse lookup from a colour to the
  underlying data record.
* **topic_distribution** – how much of the total quantity is attributed to
  each topic at a given time.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from .color_mapping import ColorConfig, ColorMapper
from .color_matrix import ColorMatrix, ColorPoint
from .data_matrix import DataMatrix, DataPoint


class KnowledgeBase:
    """Dual-matrix knowledge base.

    Parameters:
        config: Optional :class:`~four_dim_matrix.ColorConfig` for colour
            mapping.  If omitted a default config is used and will be
            recalibrated automatically after the first batch insert.

    Example::

        from datetime import datetime
        from four_dim_matrix import KnowledgeBase, DataPoint, ColorConfig

        kb = KnowledgeBase()
        kb.insert(DataPoint(
            t=datetime(2024, 1, 1), x=1, y=50_000.0, z=0,
            payload={"product": "A", "revenue": 50000},
        ))
        trend = kb.trend(z=0)
        print(trend)
    """

    def __init__(self, config: Optional[ColorConfig] = None) -> None:
        self.data_matrix = DataMatrix()
        self.color_matrix = ColorMatrix()
        self._config = config or ColorConfig()
        self._mapper = ColorMapper(self._config)

    # ------------------------------------------------------------------
    # Insert
    # ------------------------------------------------------------------

    def insert(self, point: DataPoint) -> ColorPoint:
        """Insert *point* into the DataMatrix and generate its ColorPoint.

        Returns:
            The newly created :class:`~four_dim_matrix.ColorPoint`.
        """
        self.data_matrix.insert(point)
        color_point = self._make_color_point(point)
        self.color_matrix.insert(color_point)
        return color_point

    def insert_many(self, points: List[DataPoint]) -> List[ColorPoint]:
        """Insert multiple DataPoints, recalibrating the mapper afterwards.

        After inserting the entire batch the :class:`ColorMapper` is
        recalibrated so that colours cover the full value range.  The
        ColorMatrix is then regenerated for all points.

        Returns:
            List of generated :class:`~four_dim_matrix.ColorPoint` objects.
        """
        self.data_matrix.insert_many(points)
        self._recalibrate()
        color_points = [self._make_color_point(pt) for pt in points]
        self.color_matrix.insert_many(color_points)
        return color_points

    # ------------------------------------------------------------------
    # Colour ↔ Data lookups
    # ------------------------------------------------------------------

    def lookup_by_color(self, hex_color: str) -> List[DataPoint]:
        """Return all DataPoints whose colour matches *hex_color* exactly.

        This is the "hover on a colour block → reveal data" operation.
        """
        matching_coords = [
            cp.coordinates
            for cp in self.color_matrix
            if cp.hex_color == hex_color
        ]
        results: List[DataPoint] = []
        for coords in matching_coords:
            t, x, y, z = coords
            results.extend(self.data_matrix.query(t=t, x=x, y=y, z=z))
        return results

    def color_for_point(self, t: datetime, x: int, y: float, z: int) -> Optional[str]:
        """Return the hex colour for a specific DataMatrix address."""
        cp = self.color_matrix.get(t, x, y, z)
        return cp.hex_color if cp else None

    # ------------------------------------------------------------------
    # Analysis
    # ------------------------------------------------------------------

    def trend(self, z: Optional[int] = None) -> Dict[datetime, float]:
        """Return total quantity ``y`` per time step, optionally for topic *z*.

        This is the primary trend-calculation method – it aggregates along the
        t-axis to show how a topic (or the whole dataset) grows/shrinks over
        time.
        """
        return self.data_matrix.trend_by_t(z=z)

    def snapshot(self, t: datetime) -> Dict[str, Any]:
        """Return a cross-topic summary for time slice *t*.

        Returns a dictionary with:

        * ``"t"`` – the queried timestamp.
        * ``"topics"`` – list of per-topic records containing ``z``,
          ``total_y``, ``hex_color``, and ``record_count``.
        * ``"total_y"`` – sum of ``y`` across all topics.
        """
        data_points = self.data_matrix.query(t=t)
        color_points = self.color_matrix.query(t=t)
        color_by_z: Dict[int, str] = {}
        for cp in color_points:
            color_by_z[cp.z] = cp.hex_color

        by_z: Dict[int, Dict[str, Any]] = {}
        for dp in data_points:
            entry = by_z.setdefault(
                dp.z,
                {"z": dp.z, "total_y": 0.0, "record_count": 0},
            )
            entry["total_y"] += dp.y
            entry["record_count"] += 1
            entry["hex_color"] = color_by_z.get(dp.z, "#808080")

        topics = sorted(by_z.values(), key=lambda e: e["z"])
        return {
            "t": t.isoformat(),
            "topics": topics,
            "total_y": sum(e["total_y"] for e in topics),
        }

    def topic_distribution(self, t: Optional[datetime] = None) -> Dict[int, float]:
        """Return the fractional distribution of ``y`` across topics.

        Returns a mapping ``{z: fraction}`` where ``fraction`` is the share
        of each topic's total ``y`` relative to the global total.
        """
        totals = self.data_matrix.aggregate_y_by_z(t=t)
        grand_total = sum(totals.values())
        if grand_total == 0:
            return {z: 0.0 for z in totals}
        return {z: v / grand_total for z, v in totals.items()}

    def colour_timeline(self, z: int) -> List[Tuple[datetime, str]]:
        """Return the colour trail for topic *z* along the t-axis."""
        return self.color_matrix.colour_timeline(z)

    # ------------------------------------------------------------------
    # Persistence helpers
    # ------------------------------------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        """Serialise the entire knowledge base to a plain dictionary."""
        return {
            "data_matrix": self.data_matrix.to_list(),
            "color_matrix": self.color_matrix.to_list(),
        }

    def to_json(self) -> str:
        """Serialise the entire knowledge base to a JSON string."""
        return json.dumps(self.to_dict(), default=str)

    @classmethod
    def from_dict(
        cls, data: Dict[str, Any], config: Optional[ColorConfig] = None
    ) -> "KnowledgeBase":
        """Deserialise a knowledge base from a plain dictionary."""
        kb = cls(config=config)
        kb.data_matrix = DataMatrix.from_list(data.get("data_matrix", []))
        kb.color_matrix = ColorMatrix.from_list(data.get("color_matrix", []))
        kb._recalibrate()
        return kb

    @classmethod
    def from_json(
        cls, json_str: str, config: Optional[ColorConfig] = None
    ) -> "KnowledgeBase":
        """Deserialise a knowledge base from a JSON string."""
        return cls.from_dict(json.loads(json_str), config=config)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _recalibrate(self) -> None:
        """Update the ColorMapper to cover the full range of the DataMatrix."""
        if len(self.data_matrix) == 0:
            return
        self._mapper = ColorMapper.from_data_matrix(self.data_matrix)
        self._config = self._mapper.config

    def _make_color_point(self, dp: DataPoint) -> ColorPoint:
        """Generate a ColorPoint for the given DataPoint."""
        hex_color = self._mapper.map(t=dp.t, x=dp.x, y=dp.y, z=dp.z)
        return ColorPoint(
            t=dp.t,
            x=dp.x,
            y=dp.y,
            z=dp.z,
            hex_color=hex_color,
            opacity=1.0,
            computed_from={
                "mapper": "ColorMapper",
                "hue_for_z": self._mapper.config.hue_for_z(dp.z),
                "y_normalised": self._mapper.config.normalise_y(dp.y),
                "saturation_for_x": self._mapper.config.saturation_for_x(dp.x),
                "time_hue_offset": self._mapper.config.time_hue_offset(dp.t),
            },
        )

    def __repr__(self) -> str:
        return (
            f"KnowledgeBase("
            f"data_records={len(self.data_matrix)}, "
            f"color_points={len(self.color_matrix)})"
        )
