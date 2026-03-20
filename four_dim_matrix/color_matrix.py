"""ColorMatrix – the second of the two four-dimensional matrices.

Each cell at address ``(t, x, y, z)`` stores a :class:`ColorPoint` – a
colour value plus optional metadata about how the colour was derived.

The ColorMatrix mirrors the DataMatrix coordinate space but carries only
visual/semantic information.  Users can inspect the colour cloud and hover
over any cell to retrieve the matching :class:`~four_dim_matrix.DataPoint`
from the DataMatrix.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional, Tuple


@dataclass
class ColorPoint:
    """A single colour entry in the ColorMatrix.

    Attributes:
        t: Global time coordinate (must match the DataMatrix value).
        x: Business-cycle / phase coordinate.
        y: Total-quantity / value coordinate.
        z: Topic / category coordinate.
        hex_color: Colour encoded as a ``#rrggbb`` hex string.
        opacity: Transparency value in ``[0, 1]`` (can encode data quality or
            confidence – ``1.0`` = fully opaque).
        computed_from: Optional dictionary describing the mapping rules that
            produced this colour (useful for auditability).
    """

    t: datetime
    x: int
    y: float
    z: int
    hex_color: str = "#808080"
    opacity: float = 1.0
    computed_from: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if isinstance(self.t, str):
            self.t = datetime.fromisoformat(self.t)
        if not self.hex_color.startswith("#") or len(self.hex_color) != 7:
            raise ValueError(
                f"hex_color must be a '#rrggbb' string, got {self.hex_color!r}"
            )
        if not (0.0 <= self.opacity <= 1.0):
            raise ValueError(f"opacity must be in [0, 1], got {self.opacity}")

    @property
    def coordinates(self) -> tuple:
        """Return the four-dimensional address ``(t, x, y, z)``."""
        return (self.t, self.x, self.y, self.z)

    @property
    def rgb(self) -> Tuple[int, int, int]:
        """Return the ``(r, g, b)`` integer tuple for this colour."""
        h = self.hex_color.lstrip("#")
        return (int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16))

    @property
    def rgba(self) -> Tuple[int, int, int, float]:
        """Return the ``(r, g, b, a)`` tuple for this colour."""
        r, g, b = self.rgb
        return (r, g, b, self.opacity)

    def to_dict(self) -> Dict[str, Any]:
        """Serialise to a plain dictionary."""
        return {
            "t": self.t.isoformat(),
            "x": self.x,
            "y": self.y,
            "z": self.z,
            "hex_color": self.hex_color,
            "opacity": self.opacity,
            "computed_from": self.computed_from,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ColorPoint":
        """Deserialise from a plain dictionary."""
        return cls(
            t=data["t"],
            x=data["x"],
            y=data["y"],
            z=data["z"],
            hex_color=data.get("hex_color", "#808080"),
            opacity=data.get("opacity", 1.0),
            computed_from=data.get("computed_from", {}),
        )

    def to_json(self) -> str:
        """Serialise to a JSON string."""
        return json.dumps(self.to_dict(), default=str)


class ColorMatrix:
    """In-memory four-dimensional colour store.

    Stores one :class:`ColorPoint` per ``(t, x, y, z)`` address.  Like the
    :class:`~four_dim_matrix.DataMatrix`, the implementation is sparse – it
    holds a flat list of points rather than a dense array.

    Example::

        from four_dim_matrix import ColorMatrix, ColorPoint
        from datetime import datetime

        cm = ColorMatrix()
        cm.insert(ColorPoint(
            t=datetime(2024, 1, 1), x=1, y=15000.0, z=0,
            hex_color="#3498db",
        ))
        matches = cm.query(z=0)
    """

    def __init__(self) -> None:
        self._points: List[ColorPoint] = []

    # ------------------------------------------------------------------
    # Mutation
    # ------------------------------------------------------------------

    def insert(self, point: ColorPoint) -> None:
        """Append *point* to the matrix."""
        self._points.append(point)

    def insert_many(self, points: List[ColorPoint]) -> None:
        """Append multiple points at once."""
        self._points.extend(points)

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
    ) -> List[ColorPoint]:
        """Return all ColorPoints matching the given filter criteria."""
        results: List[ColorPoint] = []
        for pt in self._points:
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
            results.append(pt)
        return results

    def get(
        self, t: datetime, x: int, y: float, z: int
    ) -> Optional[ColorPoint]:
        """Exact-match lookup; return ``None`` if not found."""
        for pt in self._points:
            if pt.t == t and pt.x == x and pt.y == y and pt.z == z:
                return pt
        return None

    # ------------------------------------------------------------------
    # Colour distance and similarity
    # ------------------------------------------------------------------

    @staticmethod
    def color_distance(hex1: str, hex2: str) -> float:
        """Return the perceptual colour distance between *hex1* and *hex2*.

        Uses the *redmean* approximation, which weights the RGB channels
        according to human visual sensitivity without requiring a full
        CIEDE2000 implementation.

        The result is on a scale of roughly ``[0, 765]``:

        * **< 30**   – highly similar; colours appear nearly identical.
        * **30–100** – noticeably different but in the same family.
        * **> 150**  – clearly distinct colours.

        This threshold (< 30 → "highly related") matches the design
        specification for automatic business-entity association discovery.
        """
        def _parse(h: str) -> Tuple[int, int, int]:
            h = h.lstrip("#")
            return (int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16))

        r1, g1, b1 = _parse(hex1)
        r2, g2, b2 = _parse(hex2)
        dr = r1 - r2
        dg = g1 - g2
        db = b1 - b2
        mean_r = (r1 + r2) / 2.0
        return math.sqrt(
            (2.0 + mean_r / 256.0) * dr * dr
            + 4.0 * dg * dg
            + (2.0 + (255.0 - mean_r) / 256.0) * db * db
        )

    def query_by_color_distance(
        self,
        hex_color: str,
        max_distance: float,
    ) -> List[Tuple["ColorPoint", float]]:
        """Return all :class:`ColorPoint` objects within *max_distance* of *hex_color*.

        Returns a list of ``(point, distance)`` pairs sorted by distance
        ascending (most similar first).

        Example::

            near = cm.query_by_color_distance("#3498db", max_distance=30)
            for point, dist in near:
                print(f"z={point.z} dist={dist:.1f} color={point.hex_color}")
        """
        results: List[Tuple[ColorPoint, float]] = []
        for pt in self._points:
            d = self.color_distance(hex_color, pt.hex_color)
            if d <= max_distance:
                results.append((pt, d))
        results.sort(key=lambda pair: pair[1])
        return results

    def find_related_topics(
        self,
        hex_color: str,
        max_distance: float = 30.0,
    ) -> List[int]:
        """Return distinct z-values whose colour is within *max_distance* of *hex_color*.

        Two topics with colour distance < 30 occupy a similar position in
        the HSL space, meaning they share the same business-entity family,
        quantity range, *and* lifecycle phase.  This is the "visual
        similarity → business association" signal described in the design.

        Returns z-values in order of ascending distance (most similar first).
        """
        matches = self.query_by_color_distance(hex_color, max_distance)
        seen: set = set()
        result: List[int] = []
        for pt, _ in matches:
            if pt.z not in seen:
                seen.add(pt.z)
                result.append(pt.z)
        return result

    # ------------------------------------------------------------------

    def colour_timeline(self, z: int) -> List[Tuple[datetime, str]]:
        """Return ``[(t, hex_color), ...]`` for topic *z*, sorted by time.

        This is the "colour trail" for a single topic along the t-axis –
        the list of colours shows how the business evolved over time.
        """
        points = sorted(self.query(z=z), key=lambda p: p.t)
        return [(p.t, p.hex_color) for p in points]

    def snapshot(self, t: datetime) -> List[ColorPoint]:
        """Return all colour points at a single time slice *t*.

        This gives the "global health dashboard" for a particular moment.
        """
        return self.query(t=t)

    # ------------------------------------------------------------------
    # Persistence helpers
    # ------------------------------------------------------------------

    def to_list(self) -> List[Dict[str, Any]]:
        """Export all points as a list of plain dictionaries."""
        return [pt.to_dict() for pt in self._points]

    @classmethod
    def from_list(cls, records: List[Dict[str, Any]]) -> "ColorMatrix":
        """Import from a list of plain dictionaries."""
        cm = cls()
        cm.insert_many([ColorPoint.from_dict(r) for r in records])
        return cm

    # ------------------------------------------------------------------
    # Dunder helpers
    # ------------------------------------------------------------------

    def __len__(self) -> int:
        return len(self._points)

    def __iter__(self) -> Iterator[ColorPoint]:
        return iter(self._points)

    def __repr__(self) -> str:
        return f"ColorMatrix(points={len(self._points)})"
