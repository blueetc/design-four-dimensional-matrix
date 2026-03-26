"""HierarchicalAggregator – multi-resolution LOD pre-computation for the
four-dimensional matrix.

The core problem with naïve full-resolution rendering is scale: a matrix with
``t=1 000 × x=100 × y=100 × z=50`` produces up to 500 million grid points,
which is impractical to render in real time.

The :class:`HierarchicalAggregator` solves this by pre-computing three
progressively coarser *resolution levels* from the underlying
:class:`~four_dim_matrix.KnowledgeBase`.  The
:meth:`~HierarchicalAggregator.materialize_for_viewport` method then selects
the cheapest level that still fits inside a caller-supplied viewport volume
threshold.

Resolution levels
-----------------
+-----------+-------------------+-------------------+----------------------------+
| Level     | Time bucket       | Typical use-case  | Approximate point budget   |
+===========+===================+===================+============================+
| overview  | Monthly           | Full-timeline pan | 10 000 – 100 000 pts       |
+-----------+-------------------+-------------------+----------------------------+
| standard  | Daily             | Week/month drill  | 100 000 – 1 000 000 pts    |
+-----------+-------------------+-------------------+----------------------------+
| detail    | Hourly            | Day/hour zoom     | 1 000 000+ pts (raw-ish)   |
+-----------+-------------------+-------------------+----------------------------+

Usage::

    from four_dim_matrix import KnowledgeBase
    from four_dim_matrix.aggregation_layer import HierarchicalAggregator
    from datetime import datetime

    kb = KnowledgeBase.from_json(open("my_kb.json").read())
    agg = HierarchicalAggregator(kb)
    agg.precompute_all()

    # When the user's viewport covers 2 months × all x × z topics 0-4 …
    dm = agg.materialize_for_viewport(
        t_range=(datetime(2024, 1, 1), datetime(2024, 3, 1)),
        x_range=(0, 10),
        z_filter=[0, 1, 2, 3, 4],
    )
    print(f"Rendering {len(dm)} points at appropriate resolution.")
"""

from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional, Tuple

from .data_matrix import DataMatrix


# ---------------------------------------------------------------------------
# Resolution-level constants
# ---------------------------------------------------------------------------

#: Threshold in "volume units" above which the *overview* resolution is used.
_OVERVIEW_THRESHOLD = 1_000_000
#: Threshold below which the *detail* resolution is used.
_DETAIL_THRESHOLD = 10_000


class HierarchicalAggregator:
    """Pre-compute and serve multi-resolution views of a
    :class:`~four_dim_matrix.KnowledgeBase`.

    Parameters:
        kb: The source knowledge base.

    Attributes:
        RESOLUTION_LEVELS: Mapping from level name to time-bucket granularity
            string as accepted by
            :meth:`~four_dim_matrix.DataMatrix.aggregate_by_time`.
    """

    RESOLUTION_LEVELS: Dict[str, str] = {
        "overview": "month",
        "standard": "day",
        "detail": "hour",
    }

    def __init__(self, kb: "KnowledgeBase") -> None:  # noqa: F821
        from .knowledge_base import KnowledgeBase  # local import avoids circularity

        if not isinstance(kb, KnowledgeBase):
            raise TypeError(f"Expected KnowledgeBase, got {type(kb).__name__}")
        self._kb = kb
        # Cache for pre-computed DataMatrices keyed by resolution level
        self._cache: Dict[str, DataMatrix] = {}

    # ------------------------------------------------------------------
    # Pre-computation
    # ------------------------------------------------------------------

    def precompute_all(self) -> None:
        """Pre-compute aggregated :class:`~four_dim_matrix.DataMatrix` objects
        for all three resolution levels and store them in the internal cache.

        This is an **optional** optimisation step.  If
        :meth:`materialize_for_viewport` is called before
        :meth:`precompute_all`, each level is computed on demand and cached
        transparently.
        """
        for level, granularity in self.RESOLUTION_LEVELS.items():
            self._cache[level] = self._compute_level(granularity)

    def precompute_level(self, level: str) -> DataMatrix:
        """Pre-compute a single resolution level and return it.

        Parameters:
            level: One of ``"overview"``, ``"standard"``, or ``"detail"``.

        Returns:
            The aggregated :class:`~four_dim_matrix.DataMatrix`.

        Raises:
            ValueError: If *level* is not recognised.
        """
        if level not in self.RESOLUTION_LEVELS:
            raise ValueError(
                f"Unknown level {level!r}. "
                f"Must be one of {list(self.RESOLUTION_LEVELS.keys())!r}."
            )
        dm = self._compute_level(self.RESOLUTION_LEVELS[level])
        self._cache[level] = dm
        return dm

    # ------------------------------------------------------------------
    # Viewport-aware materialisation
    # ------------------------------------------------------------------

    def materialize_for_viewport(
        self,
        t_range: Tuple[datetime, datetime],
        x_range: Tuple[int, int],
        z_filter: Optional[List[int]] = None,
    ) -> DataMatrix:
        """Return a :class:`~four_dim_matrix.DataMatrix` at an appropriate
        resolution for the given viewport.

        The method estimates the *viewport volume* as::

            volume = Δt_days × Δx × len(z_filter)

        and selects:

        * **overview** (monthly)  when ``volume > 1 000 000``
        * **standard** (daily)    when ``10 000 < volume ≤ 1 000 000``
        * **detail**   (hourly)   when ``volume ≤ 10 000``

        The chosen level is then queried with the supplied ranges so that only
        the relevant slice is returned.

        Parameters:
            t_range: ``(t_start, t_end)`` inclusive date range.
            x_range: ``(x_min, x_max)`` inclusive phase range.
            z_filter: Optional list of topic IDs to include.  When ``None``
                all topics are included.

        Returns:
            A filtered :class:`~four_dim_matrix.DataMatrix` at the selected
            resolution.
        """
        t_start, t_end = t_range
        x_min, x_max = x_range

        # Estimate viewport volume
        delta_days = max(1.0, (t_end - t_start).total_seconds() / 86_400.0)
        delta_x = max(1, x_max - x_min + 1)
        n_topics = len(z_filter) if z_filter is not None else 1
        volume = delta_days * delta_x * n_topics

        if volume > _OVERVIEW_THRESHOLD:
            level = "overview"
        elif volume > _DETAIL_THRESHOLD:
            level = "standard"
        else:
            level = "detail"

        dm = self._get_or_compute(level)

        # Apply viewport filtering
        filtered = DataMatrix()
        for pt in dm:
            if pt.t < t_start or pt.t > t_end:
                continue
            if pt.x < x_min or pt.x > x_max:
                continue
            if z_filter is not None and pt.z not in z_filter:
                continue
            filtered.insert(pt)

        return filtered

    def selected_level(
        self,
        t_range: Tuple[datetime, datetime],
        x_range: Tuple[int, int],
        z_filter: Optional[List[int]] = None,
    ) -> str:
        """Return the resolution-level name that would be chosen for the given
        viewport without actually materialising any data.

        Useful for UI debugging or logging.
        """
        t_start, t_end = t_range
        x_min, x_max = x_range
        delta_days = max(1.0, (t_end - t_start).total_seconds() / 86_400.0)
        delta_x = max(1, x_max - x_min + 1)
        n_topics = len(z_filter) if z_filter is not None else 1
        volume = delta_days * delta_x * n_topics
        if volume > _OVERVIEW_THRESHOLD:
            return "overview"
        if volume > _DETAIL_THRESHOLD:
            return "standard"
        return "detail"

    def get_level(self, level: str) -> DataMatrix:
        """Return the pre-computed DataMatrix for *level*, computing it first
        if not already in the cache.

        Parameters:
            level: One of ``"overview"``, ``"standard"``, or ``"detail"``.
        """
        if level not in self.RESOLUTION_LEVELS:
            raise ValueError(
                f"Unknown level {level!r}. "
                f"Must be one of {list(self.RESOLUTION_LEVELS.keys())!r}."
            )
        return self._get_or_compute(level)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_or_compute(self, level: str) -> DataMatrix:
        if level not in self._cache:
            self._cache[level] = self._compute_level(self.RESOLUTION_LEVELS[level])
        return self._cache[level]

    def _compute_level(self, granularity: str) -> DataMatrix:
        """Aggregate the source DataMatrix at *granularity* using the
        existing :meth:`~four_dim_matrix.DataMatrix.aggregate_by_time` helper.
        """
        return self._kb.data_matrix.aggregate_by_time(resolution=granularity)

    # ------------------------------------------------------------------
    # Cache management
    # ------------------------------------------------------------------

    def clear_cache(self) -> None:
        """Evict all pre-computed levels from the cache."""
        self._cache.clear()

    def cache_keys(self) -> List[str]:
        """Return the list of currently cached resolution levels."""
        return list(self._cache.keys())

    # ------------------------------------------------------------------
    # Summary / reporting
    # ------------------------------------------------------------------

    def summary(self) -> Dict[str, int]:
        """Return point counts for each cached resolution level.

        Levels that have not yet been computed are shown as ``-1``.
        """
        return {
            level: len(self._cache[level]) if level in self._cache else -1
            for level in self.RESOLUTION_LEVELS
        }

    def __repr__(self) -> str:
        cached = list(self._cache.keys())
        return (
            f"HierarchicalAggregator("
            f"source_records={len(self._kb.data_matrix)}, "
            f"cached_levels={cached!r})"
        )
