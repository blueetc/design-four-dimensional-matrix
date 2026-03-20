"""Tests for ColorMatrix and ColorPoint."""

from datetime import datetime

import pytest

from four_dim_matrix import ColorMatrix, ColorPoint


class TestColorPoint:
    def test_basic_construction(self):
        cp = ColorPoint(
            t=datetime(2024, 1, 1), x=1, y=100.0, z=0, hex_color="#3498db"
        )
        assert cp.hex_color == "#3498db"
        assert cp.opacity == 1.0

    def test_string_t_is_parsed(self):
        cp = ColorPoint(t="2024-06-15", x=0, y=0.0, z=0, hex_color="#ffffff")
        assert cp.t == datetime(2024, 6, 15)

    def test_invalid_hex_color_raises(self):
        with pytest.raises(ValueError):
            ColorPoint(t=datetime(2024, 1, 1), x=0, y=0.0, z=0, hex_color="red")

    def test_invalid_opacity_raises(self):
        with pytest.raises(ValueError):
            ColorPoint(
                t=datetime(2024, 1, 1), x=0, y=0.0, z=0,
                hex_color="#000000", opacity=1.5
            )

    def test_rgb_property(self):
        cp = ColorPoint(t=datetime(2024, 1, 1), x=0, y=0.0, z=0, hex_color="#3498db")
        r, g, b = cp.rgb
        assert r == 0x34
        assert g == 0x98
        assert b == 0xDB

    def test_rgba_property(self):
        cp = ColorPoint(
            t=datetime(2024, 1, 1), x=0, y=0.0, z=0,
            hex_color="#ff0000", opacity=0.5
        )
        r, g, b, a = cp.rgba
        assert r == 255
        assert g == 0
        assert b == 0
        assert a == pytest.approx(0.5)

    def test_coordinates_property(self):
        t = datetime(2024, 4, 1)
        cp = ColorPoint(t=t, x=2, y=999.0, z=3, hex_color="#aabbcc")
        assert cp.coordinates == (t, 2, 999.0, 3)

    def test_serialisation_round_trip(self):
        original = ColorPoint(
            t=datetime(2024, 7, 4, 12, 0, 0),
            x=3, y=99.9, z=2,
            hex_color="#abcdef",
            opacity=0.7,
            computed_from={"rule": "hsl"},
        )
        restored = ColorPoint.from_dict(original.to_dict())
        assert restored.t == original.t
        assert restored.hex_color == original.hex_color
        assert restored.opacity == pytest.approx(original.opacity)
        assert restored.computed_from == original.computed_from


class TestColorMatrix:
    def _sample(self):
        cm = ColorMatrix()
        cm.insert_many([
            ColorPoint(t=datetime(2024, 1, 1), x=1, y=100.0, z=0, hex_color="#0000ff"),
            ColorPoint(t=datetime(2024, 1, 1), x=1, y=200.0, z=1, hex_color="#00ff00"),
            ColorPoint(t=datetime(2024, 2, 1), x=2, y=150.0, z=0, hex_color="#ff0000"),
            ColorPoint(t=datetime(2024, 3, 1), x=3, y=300.0, z=1, hex_color="#ff00ff"),
        ])
        return cm

    def test_len(self):
        cm = self._sample()
        assert len(cm) == 4

    def test_query_by_z(self):
        cm = self._sample()
        results = cm.query(z=0)
        assert len(results) == 2
        assert all(r.z == 0 for r in results)

    def test_query_by_t(self):
        cm = self._sample()
        results = cm.query(t=datetime(2024, 1, 1))
        assert len(results) == 2

    def test_get_exact_match(self):
        cm = self._sample()
        pt = cm.get(t=datetime(2024, 1, 1), x=1, y=100.0, z=0)
        assert pt is not None
        assert pt.hex_color == "#0000ff"

    def test_get_no_match(self):
        cm = self._sample()
        pt = cm.get(t=datetime(2024, 1, 1), x=99, y=0.0, z=9)
        assert pt is None

    def test_colour_timeline(self):
        cm = self._sample()
        timeline = cm.colour_timeline(z=1)
        assert len(timeline) == 2
        # Should be sorted by time
        assert timeline[0][0] < timeline[1][0]
        assert timeline[0][1] == "#00ff00"
        assert timeline[1][1] == "#ff00ff"

    def test_snapshot(self):
        cm = self._sample()
        results = cm.snapshot(t=datetime(2024, 1, 1))
        assert len(results) == 2

    def test_serialisation_round_trip(self):
        cm = self._sample()
        restored = ColorMatrix.from_list(cm.to_list())
        assert len(restored) == len(cm)
        for orig, rest in zip(cm, restored):
            assert orig.to_dict() == rest.to_dict()

    def test_repr(self):
        cm = self._sample()
        assert "4" in repr(cm)


class TestColorDistance:
    def test_identical_colors_distance_zero(self):
        assert ColorMatrix.color_distance("#ff0000", "#ff0000") == pytest.approx(0.0)

    def test_black_white_distance_maximum(self):
        d = ColorMatrix.color_distance("#000000", "#ffffff")
        assert d > 400  # maximum is ~764

    def test_distance_is_symmetric(self):
        d1 = ColorMatrix.color_distance("#3498db", "#e74c3c")
        d2 = ColorMatrix.color_distance("#e74c3c", "#3498db")
        assert d1 == pytest.approx(d2)

    def test_similar_colors_low_distance(self):
        # Two very similar blues
        d = ColorMatrix.color_distance("#3498db", "#3590d0")
        assert d < 30

    def test_complementary_colors_high_distance(self):
        d = ColorMatrix.color_distance("#ff0000", "#00ffff")
        assert d > 100

    def test_distance_strips_hash(self):
        # Ensure the method handles the # prefix correctly
        d = ColorMatrix.color_distance("#aabbcc", "#aabbcc")
        assert d == pytest.approx(0.0)


class TestQueryByColorDistance:
    def _sample_cm(self):
        cm = ColorMatrix()
        cm.insert_many([
            ColorPoint(t=datetime(2024, 1, 1), x=0, y=10.0, z=0, hex_color="#3498db"),
            ColorPoint(t=datetime(2024, 1, 1), x=0, y=20.0, z=1, hex_color="#3490d0"),
            ColorPoint(t=datetime(2024, 1, 1), x=0, y=30.0, z=2, hex_color="#ff0000"),
        ])
        return cm

    def test_returns_nearby_points(self):
        cm = self._sample_cm()
        results = cm.query_by_color_distance("#3498db", max_distance=30)
        colors  = [cp.hex_color for cp, _ in results]
        assert "#3498db" in colors
        assert "#3490d0" in colors

    def test_excludes_distant_colors(self):
        cm = self._sample_cm()
        results = cm.query_by_color_distance("#3498db", max_distance=30)
        colors  = [cp.hex_color for cp, _ in results]
        assert "#ff0000" not in colors

    def test_sorted_by_distance_ascending(self):
        cm = self._sample_cm()
        results = cm.query_by_color_distance("#3498db", max_distance=500)
        distances = [d for _, d in results]
        assert distances == sorted(distances)

    def test_exact_match_has_zero_distance(self):
        cm = self._sample_cm()
        results = cm.query_by_color_distance("#3498db", max_distance=0)
        assert len(results) == 1
        assert results[0][1] == pytest.approx(0.0)

    def test_empty_matrix(self):
        cm = ColorMatrix()
        assert cm.query_by_color_distance("#000000", max_distance=100) == []


class TestFindRelatedTopics:
    def _cm(self):
        cm = ColorMatrix()
        cm.insert_many([
            ColorPoint(t=datetime(2024, 1, 1), x=0, y=1.0, z=0, hex_color="#3498db"),
            ColorPoint(t=datetime(2024, 1, 1), x=0, y=2.0, z=1, hex_color="#3490d0"),
            ColorPoint(t=datetime(2024, 1, 1), x=0, y=3.0, z=2, hex_color="#e74c3c"),
        ])
        return cm

    def test_returns_related_z_values(self):
        cm = self._cm()
        related = cm.find_related_topics("#3498db", max_distance=30)
        assert 0 in related
        assert 1 in related
        assert 2 not in related

    def test_no_duplicates(self):
        cm = ColorMatrix()
        # Two points with same z and similar color
        cm.insert_many([
            ColorPoint(t=datetime(2024, 1, 1), x=0, y=1.0, z=5, hex_color="#aaaaaa"),
            ColorPoint(t=datetime(2024, 2, 1), x=0, y=2.0, z=5, hex_color="#aaaaab"),
        ])
        related = cm.find_related_topics("#aaaaaa", max_distance=30)
        assert related.count(5) == 1

    def test_most_similar_first(self):
        cm = self._cm()
        related = cm.find_related_topics("#3498db", max_distance=500)
        # z=0 (exact match) should come before z=1 (close), which before z=2 (far)
        assert related[0] == 0

    def test_empty_result(self):
        cm = self._cm()
        assert cm.find_related_topics("#000000", max_distance=0) == []
