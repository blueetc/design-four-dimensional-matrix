"""Tests for DataMatrix and DataPoint."""

import json
from datetime import datetime

import pytest

from four_dim_matrix import DataMatrix, DataPoint


# ---------------------------------------------------------------------------
# DataPoint construction
# ---------------------------------------------------------------------------


class TestDataPoint:
    def test_basic_construction(self):
        dp = DataPoint(t=datetime(2024, 1, 1), x=1, y=15000.0, z=0)
        assert dp.t == datetime(2024, 1, 1)
        assert dp.x == 1
        assert dp.y == 15000.0
        assert dp.z == 0
        assert dp.payload == {}

    def test_string_t_is_parsed(self):
        dp = DataPoint(t="2024-06-15", x=0, y=0.0, z=0)
        assert dp.t == datetime(2024, 6, 15)

    def test_negative_x_raises(self):
        with pytest.raises(ValueError):
            DataPoint(t=datetime(2024, 1, 1), x=-1, y=0.0, z=0)

    def test_negative_z_raises(self):
        with pytest.raises(ValueError):
            DataPoint(t=datetime(2024, 1, 1), x=0, y=0.0, z=-1)

    def test_coordinates_property(self):
        dp = DataPoint(t=datetime(2024, 3, 1), x=2, y=500.0, z=1)
        assert dp.coordinates == (datetime(2024, 3, 1), 2, 500.0, 1)

    def test_serialisation_round_trip(self):
        original = DataPoint(
            t=datetime(2024, 7, 4, 12, 0, 0),
            x=3,
            y=99.9,
            z=2,
            payload={"foo": "bar"},
        )
        restored = DataPoint.from_dict(original.to_dict())
        assert restored.t == original.t
        assert restored.x == original.x
        assert restored.y == original.y
        assert restored.z == original.z
        assert restored.payload == original.payload

    def test_to_json(self):
        dp = DataPoint(t=datetime(2024, 1, 1), x=0, y=1.0, z=0, payload={"k": "v"})
        doc = json.loads(dp.to_json())
        assert doc["x"] == 0
        assert doc["payload"] == {"k": "v"}


# ---------------------------------------------------------------------------
# DataMatrix operations
# ---------------------------------------------------------------------------


class TestDataMatrix:
    def _sample(self):
        dm = DataMatrix()
        dm.insert_many([
            DataPoint(t=datetime(2024, 1, 1), x=1, y=100.0, z=0),
            DataPoint(t=datetime(2024, 1, 1), x=1, y=200.0, z=1),
            DataPoint(t=datetime(2024, 2, 1), x=2, y=150.0, z=0),
            DataPoint(t=datetime(2024, 3, 1), x=3, y=300.0, z=1),
        ])
        return dm

    def test_len(self):
        dm = self._sample()
        assert len(dm) == 4

    def test_query_by_z(self):
        dm = self._sample()
        results = dm.query(z=0)
        assert len(results) == 2
        assert all(r.z == 0 for r in results)

    def test_query_by_t(self):
        dm = self._sample()
        results = dm.query(t=datetime(2024, 1, 1))
        assert len(results) == 2

    def test_query_range_t(self):
        dm = self._sample()
        results = dm.query(t_from=datetime(2024, 2, 1), t_to=datetime(2024, 3, 1))
        assert len(results) == 2

    def test_query_range_y(self):
        dm = self._sample()
        results = dm.query(y_min=150.0)
        assert all(r.y >= 150.0 for r in results)

    def test_distinct_z(self):
        dm = self._sample()
        assert dm.distinct_z() == [0, 1]

    def test_distinct_t(self):
        dm = self._sample()
        assert len(dm.distinct_t()) == 3

    def test_y_range(self):
        dm = self._sample()
        lo, hi = dm.y_range()
        assert lo == 100.0
        assert hi == 300.0

    def test_aggregate_y_by_z(self):
        dm = self._sample()
        totals = dm.aggregate_y_by_z()
        assert totals[0] == pytest.approx(250.0)
        assert totals[1] == pytest.approx(500.0)

    def test_trend_by_t(self):
        dm = self._sample()
        trend = dm.trend_by_t()
        assert trend[datetime(2024, 1, 1)] == pytest.approx(300.0)
        assert trend[datetime(2024, 2, 1)] == pytest.approx(150.0)

    def test_trend_by_t_filtered_by_z(self):
        dm = self._sample()
        trend = dm.trend_by_t(z=1)
        assert datetime(2024, 2, 1) not in trend  # z=1 has no Feb record
        assert trend[datetime(2024, 3, 1)] == pytest.approx(300.0)

    def test_serialisation_round_trip(self):
        dm = self._sample()
        restored = DataMatrix.from_list(dm.to_list())
        assert len(restored) == len(dm)
        for orig, rest in zip(dm, restored):
            assert orig.to_dict() == rest.to_dict()

    def test_iter(self):
        dm = self._sample()
        count = sum(1 for _ in dm)
        assert count == 4

    def test_repr(self):
        dm = self._sample()
        assert "4" in repr(dm)


class TestAggregateByTime:
    def _dm(self):
        from four_dim_matrix import DataMatrix, DataPoint
        dm = DataMatrix()
        dm.insert_many([
            DataPoint(t=datetime(2024, 1, 1,  9,  0), x=0, y=10.0, z=0),
            DataPoint(t=datetime(2024, 1, 1, 15,  0), x=0, y=20.0, z=0),
            DataPoint(t=datetime(2024, 1, 2, 10,  0), x=0, y=30.0, z=0),
            DataPoint(t=datetime(2024, 2, 5,  0,  0), x=1, y=40.0, z=1),
            DataPoint(t=datetime(2024, 2, 6,  0,  0), x=1, y=50.0, z=1),
        ])
        return dm

    def test_day_resolution_merges_same_day(self):
        dm = self._dm()
        agg = dm.aggregate_by_time(resolution="day")
        # (z=0, x=0, day=2024-01-01) should merge two points → y=30
        t_key = datetime(2024, 1, 1)
        pts = agg.query(t=t_key, z=0)
        assert len(pts) == 1
        assert pts[0].y == pytest.approx(30.0)

    def test_month_resolution(self):
        dm = self._dm()
        agg = dm.aggregate_by_time(resolution="month")
        t_jan = datetime(2024, 1, 1)
        t_feb = datetime(2024, 2, 1)
        pts_jan = agg.query(t=t_jan, z=0)
        pts_feb = agg.query(t=t_feb, z=1)
        assert len(pts_jan) == 1
        assert pts_jan[0].y == pytest.approx(60.0)   # 10+20+30
        assert pts_feb[0].y == pytest.approx(90.0)   # 40+50

    def test_z_filter(self):
        dm = self._dm()
        agg = dm.aggregate_by_time(resolution="day", z=0)
        for pt in agg:
            assert pt.z == 0

    def test_result_is_new_data_matrix(self):
        from four_dim_matrix import DataMatrix
        dm = self._dm()
        agg = dm.aggregate_by_time()
        assert isinstance(agg, DataMatrix)
        assert agg is not dm


class TestDownsample:
    def _big_dm(self):
        from datetime import timedelta
        from four_dim_matrix import DataMatrix, DataPoint
        dm = DataMatrix()
        base = datetime(2024, 1, 1)
        points = [
            DataPoint(t=base + timedelta(days=i), x=0, y=float(i), z=i % 3)
            for i in range(100)
        ]
        dm.insert_many(points)
        return dm

    def test_reduces_record_count(self):
        dm = self._big_dm()
        sample = dm.downsample(max_points=20)
        assert len(sample) <= 20

    def test_no_op_when_within_limit(self):
        dm = self._big_dm()
        sample = dm.downsample(max_points=200)
        assert sample is dm  # returns self unchanged

    def test_reproducible_with_seed(self):
        dm = self._big_dm()
        s1 = dm.downsample(max_points=30, seed=7)
        s2 = dm.downsample(max_points=30, seed=7)
        pts1 = sorted((p.t, p.y) for p in s1)
        pts2 = sorted((p.t, p.y) for p in s2)
        assert pts1 == pts2

    def test_all_topics_represented(self):
        dm = self._big_dm()
        sample = dm.downsample(max_points=9)
        topics = {pt.z for pt in sample}
        assert len(topics) == 3
