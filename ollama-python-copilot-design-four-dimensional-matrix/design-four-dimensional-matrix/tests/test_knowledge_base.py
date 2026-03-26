"""Tests for KnowledgeBase."""

import json
from datetime import datetime

import pytest

from four_dim_matrix import ColorConfig, DataPoint, KnowledgeBase


def _sample_kb():
    """Return a KnowledgeBase populated with representative data."""
    config = ColorConfig(
        z_palette={0: 210.0, 1: 120.0, 2: 270.0},
        y_min=0.0,
        y_max=100_000.0,
        t_start=datetime(2024, 1, 1),
        t_end=datetime(2024, 12, 31),
    )
    kb = KnowledgeBase(config=config)
    kb.insert_many([
        # Topic 0 – revenue
        DataPoint(t=datetime(2024, 1, 1), x=1, y=20_000.0, z=0, payload={"rev": 20000}),
        DataPoint(t=datetime(2024, 2, 1), x=1, y=35_000.0, z=0, payload={"rev": 35000}),
        DataPoint(t=datetime(2024, 3, 1), x=2, y=50_000.0, z=0, payload={"rev": 50000}),
        # Topic 1 – users
        DataPoint(t=datetime(2024, 1, 1), x=1, y=1_000.0, z=1, payload={"users": 1000}),
        DataPoint(t=datetime(2024, 2, 1), x=1, y=1_500.0, z=1, payload={"users": 1500}),
        DataPoint(t=datetime(2024, 3, 1), x=2, y=2_000.0, z=1, payload={"users": 2000}),
        # Topic 2 – errors (small values)
        DataPoint(t=datetime(2024, 1, 1), x=1, y=10.0, z=2, payload={"errors": 10}),
    ])
    return kb


class TestKnowledgeBaseInsert:
    def test_insert_single(self):
        kb = KnowledgeBase()
        cp = kb.insert(DataPoint(t=datetime(2024, 1, 1), x=0, y=1.0, z=0))
        assert cp.hex_color.startswith("#")
        assert len(kb.data_matrix) == 1
        assert len(kb.color_matrix) == 1

    def test_insert_many_sizes_match(self):
        kb = _sample_kb()
        assert len(kb.data_matrix) == 7
        assert len(kb.color_matrix) == 7

    def test_colors_generated_after_insert_many(self):
        kb = _sample_kb()
        for cp in kb.color_matrix:
            assert cp.hex_color.startswith("#")
            assert len(cp.hex_color) == 7


class TestKnowledgeBaseLookup:
    def test_color_for_point(self):
        kb = _sample_kb()
        colour = kb.color_for_point(t=datetime(2024, 1, 1), x=1, y=20_000.0, z=0)
        assert colour is not None
        assert colour.startswith("#")

    def test_color_for_point_missing(self):
        kb = _sample_kb()
        colour = kb.color_for_point(t=datetime(2099, 1, 1), x=99, y=0.0, z=99)
        assert colour is None

    def test_lookup_by_color_finds_record(self):
        kb = _sample_kb()
        # Pick an existing colour and reverse-look it up
        cp = next(iter(kb.color_matrix))
        results = kb.lookup_by_color(cp.hex_color)
        assert len(results) >= 1


class TestKnowledgeBaseAnalysis:
    def test_trend_all(self):
        kb = _sample_kb()
        trend = kb.trend()
        assert datetime(2024, 1, 1) in trend
        assert datetime(2024, 3, 1) in trend
        # Totals should be positive
        assert all(v > 0 for v in trend.values())

    def test_trend_by_z(self):
        kb = _sample_kb()
        trend_revenue = kb.trend(z=0)
        assert trend_revenue[datetime(2024, 1, 1)] == pytest.approx(20_000.0)
        assert trend_revenue[datetime(2024, 2, 1)] == pytest.approx(35_000.0)
        assert trend_revenue[datetime(2024, 3, 1)] == pytest.approx(50_000.0)

    def test_trend_by_z_excludes_other_topics(self):
        kb = _sample_kb()
        trend_users = kb.trend(z=1)
        # The user topic values are much smaller than revenue
        assert max(trend_users.values()) < 10_000

    def test_snapshot_structure(self):
        kb = _sample_kb()
        snap = kb.snapshot(t=datetime(2024, 1, 1))
        assert snap["t"] == datetime(2024, 1, 1).isoformat()
        assert len(snap["topics"]) == 3  # z = 0, 1, 2
        assert snap["total_y"] == pytest.approx(21_010.0)  # 20000 + 1000 + 10

    def test_snapshot_includes_hex_color(self):
        kb = _sample_kb()
        snap = kb.snapshot(t=datetime(2024, 1, 1))
        for topic in snap["topics"]:
            assert "hex_color" in topic
            assert topic["hex_color"].startswith("#")

    def test_topic_distribution_sums_to_one(self):
        kb = _sample_kb()
        dist = kb.topic_distribution()
        total = sum(dist.values())
        assert total == pytest.approx(1.0)

    def test_topic_distribution_at_t(self):
        kb = _sample_kb()
        dist = kb.topic_distribution(t=datetime(2024, 1, 1))
        assert sum(dist.values()) == pytest.approx(1.0)

    def test_colour_timeline_sorted(self):
        kb = _sample_kb()
        timeline = kb.colour_timeline(z=0)
        assert len(timeline) == 3
        times = [t for t, _ in timeline]
        assert times == sorted(times)

    def test_colour_timeline_colours_are_strings(self):
        kb = _sample_kb()
        for _, colour in kb.colour_timeline(z=1):
            assert colour.startswith("#")
            assert len(colour) == 7


class TestKnowledgeBasePersistence:
    def test_to_dict_and_back(self):
        kb = _sample_kb()
        data = kb.to_dict()
        assert "data_matrix" in data
        assert "color_matrix" in data

        restored = KnowledgeBase.from_dict(data)
        assert len(restored.data_matrix) == len(kb.data_matrix)
        assert len(restored.color_matrix) == len(kb.color_matrix)

    def test_to_json_and_back(self):
        kb = _sample_kb()
        json_str = kb.to_json()
        assert isinstance(json_str, str)
        restored = KnowledgeBase.from_json(json_str)
        assert len(restored.data_matrix) == len(kb.data_matrix)

    def test_json_is_valid_json(self):
        kb = _sample_kb()
        json.loads(kb.to_json())  # should not raise

    def test_repr(self):
        kb = _sample_kb()
        r = repr(kb)
        assert "7" in r  # 7 records
