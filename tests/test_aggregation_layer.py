"""Tests for HierarchicalAggregator (aggregation_layer.py)."""
from __future__ import annotations

import pytest
from datetime import datetime, timedelta

from four_dim_matrix import KnowledgeBase, DataPoint, ColorConfig
from four_dim_matrix.aggregation_layer import HierarchicalAggregator


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_kb(n_days: int = 30, n_topics: int = 3) -> KnowledgeBase:
    """Return a KB with n_days × n_topics rows."""
    base = datetime(2024, 1, 1)
    pts = [
        DataPoint(
            t=base + timedelta(hours=h),
            x=h % 5,
            y=float((h + 1) * 10),
            z=h % n_topics,
            payload={"h": h},
        )
        for h in range(n_days * 24)  # hourly points for 30 days
    ]
    config = ColorConfig(y_min=10.0, y_max=float(n_days * 24 * 10))
    kb = KnowledgeBase(config=config)
    kb.insert_many(pts)
    return kb


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------

class TestConstruction:
    def test_requires_knowledge_base(self):
        with pytest.raises(TypeError):
            HierarchicalAggregator("not_a_kb")  # type: ignore

    def test_repr_shows_source_record_count(self):
        kb = _make_kb(n_days=2)
        agg = HierarchicalAggregator(kb)
        assert str(len(kb.data_matrix)) in repr(agg)

    def test_cache_empty_before_precompute(self):
        kb = _make_kb(n_days=2)
        agg = HierarchicalAggregator(kb)
        assert agg.cache_keys() == []

    def test_summary_shows_minus_one_before_compute(self):
        kb = _make_kb(n_days=2)
        agg = HierarchicalAggregator(kb)
        s = agg.summary()
        assert all(v == -1 for v in s.values())


# ---------------------------------------------------------------------------
# Pre-computation
# ---------------------------------------------------------------------------

class TestPrecomputation:
    def test_precompute_all_fills_cache(self):
        kb = _make_kb(n_days=5)
        agg = HierarchicalAggregator(kb)
        agg.precompute_all()
        assert set(agg.cache_keys()) == {"overview", "standard", "detail"}

    def test_precompute_level_individual(self):
        kb = _make_kb(n_days=5)
        agg = HierarchicalAggregator(kb)
        dm = agg.precompute_level("standard")
        assert len(dm) > 0
        assert "standard" in agg.cache_keys()

    def test_precompute_level_invalid(self):
        kb = _make_kb(n_days=2)
        agg = HierarchicalAggregator(kb)
        with pytest.raises(ValueError, match="Unknown level"):
            agg.precompute_level("nanosecond")

    def test_overview_fewer_points_than_detail(self):
        kb = _make_kb(n_days=30)
        agg = HierarchicalAggregator(kb)
        agg.precompute_all()
        assert len(agg.get_level("overview")) <= len(agg.get_level("detail"))

    def test_standard_between_overview_and_detail(self):
        kb = _make_kb(n_days=30)
        agg = HierarchicalAggregator(kb)
        agg.precompute_all()
        overview_n = len(agg.get_level("overview"))
        standard_n = len(agg.get_level("standard"))
        detail_n = len(agg.get_level("detail"))
        # overview ≤ standard ≤ detail (monotone)
        assert overview_n <= standard_n <= detail_n

    def test_clear_cache(self):
        kb = _make_kb(n_days=2)
        agg = HierarchicalAggregator(kb)
        agg.precompute_all()
        agg.clear_cache()
        assert agg.cache_keys() == []

    def test_summary_after_partial_precompute(self):
        kb = _make_kb(n_days=3)
        agg = HierarchicalAggregator(kb)
        agg.precompute_level("overview")
        s = agg.summary()
        assert s["overview"] >= 0
        assert s["standard"] == -1
        assert s["detail"] == -1


# ---------------------------------------------------------------------------
# get_level / lazy computation
# ---------------------------------------------------------------------------

class TestGetLevel:
    def test_get_level_computes_on_demand(self):
        kb = _make_kb(n_days=3)
        agg = HierarchicalAggregator(kb)
        dm = agg.get_level("detail")
        assert len(dm) > 0
        assert "detail" in agg.cache_keys()

    def test_get_level_invalid_raises(self):
        kb = _make_kb(n_days=2)
        agg = HierarchicalAggregator(kb)
        with pytest.raises(ValueError):
            agg.get_level("weekly")


# ---------------------------------------------------------------------------
# Viewport materialisation
# ---------------------------------------------------------------------------

class TestMaterializeForViewport:
    def test_returns_data_matrix(self):
        from four_dim_matrix import DataMatrix
        kb = _make_kb(n_days=5)
        agg = HierarchicalAggregator(kb)
        base = datetime(2024, 1, 1)
        dm = agg.materialize_for_viewport(
            t_range=(base, base + timedelta(days=5)),
            x_range=(0, 4),
        )
        assert isinstance(dm, DataMatrix)

    def test_t_filter_applied(self):
        kb = _make_kb(n_days=10)
        agg = HierarchicalAggregator(kb)
        base = datetime(2024, 1, 3)
        end = datetime(2024, 1, 5)
        dm = agg.materialize_for_viewport(
            t_range=(base, end),
            x_range=(0, 100),
        )
        for pt in dm:
            assert base <= pt.t <= end

    def test_z_filter_applied(self):
        kb = _make_kb(n_days=5, n_topics=3)
        agg = HierarchicalAggregator(kb)
        base = datetime(2024, 1, 1)
        dm = agg.materialize_for_viewport(
            t_range=(base, base + timedelta(days=5)),
            x_range=(0, 100),
            z_filter=[0, 1],
        )
        for pt in dm:
            assert pt.z in (0, 1)

    def test_large_viewport_chooses_overview(self):
        kb = _make_kb(n_days=30)
        agg = HierarchicalAggregator(kb)
        base = datetime(2024, 1, 1)
        level = agg.selected_level(
            t_range=(base, base + timedelta(days=400)),
            x_range=(0, 100),
            z_filter=list(range(30)),
        )
        assert level == "overview"

    def test_small_viewport_chooses_detail(self):
        kb = _make_kb(n_days=3)
        agg = HierarchicalAggregator(kb)
        base = datetime(2024, 1, 1)
        level = agg.selected_level(
            t_range=(base, base + timedelta(hours=6)),
            x_range=(0, 2),
            z_filter=[0],
        )
        assert level == "detail"

    def test_medium_viewport_chooses_standard(self):
        kb = _make_kb(n_days=10)
        agg = HierarchicalAggregator(kb)
        base = datetime(2024, 1, 1)
        # volume = 100 days × 15 x × 10 topics = 15 000 → standard
        level = agg.selected_level(
            t_range=(base, base + timedelta(days=100)),
            x_range=(0, 14),
            z_filter=list(range(10)),
        )
        assert level == "standard"

    def test_no_z_filter_includes_all_topics(self):
        kb = _make_kb(n_days=3, n_topics=3)
        agg = HierarchicalAggregator(kb)
        base = datetime(2024, 1, 1)
        dm = agg.materialize_for_viewport(
            t_range=(base, base + timedelta(days=3)),
            x_range=(0, 10),
            z_filter=None,
        )
        z_seen = {pt.z for pt in dm}
        assert len(z_seen) > 1
