"""Tests for ColorMapping (ColorConfig and ColorMapper)."""

from datetime import datetime

import pytest

from four_dim_matrix import ColorConfig, ColorMapper, DataMatrix, DataPoint


class TestColorConfig:
    def test_hue_for_z_explicit_palette(self):
        config = ColorConfig(z_palette={0: 210.0, 1: 120.0})
        assert config.hue_for_z(0) == 210.0
        assert config.hue_for_z(1) == 120.0

    def test_hue_for_z_auto_assign(self):
        config = ColorConfig()
        h0 = config.hue_for_z(0)
        h1 = config.hue_for_z(1)
        # Auto-assigned hues should differ and stay in [0, 360)
        assert 0.0 <= h0 < 360.0
        assert 0.0 <= h1 < 360.0
        assert h0 != h1

    def test_normalise_y_full_range(self):
        config = ColorConfig(y_min=0.0, y_max=100.0)
        assert config.normalise_y(0.0) == pytest.approx(0.0)
        assert config.normalise_y(100.0) == pytest.approx(1.0)
        assert config.normalise_y(50.0) == pytest.approx(0.5)

    def test_normalise_y_clamps(self):
        config = ColorConfig(y_min=10.0, y_max=20.0)
        assert config.normalise_y(0.0) == pytest.approx(0.0)
        assert config.normalise_y(30.0) == pytest.approx(1.0)

    def test_normalise_y_zero_span(self):
        config = ColorConfig(y_min=5.0, y_max=5.0)
        assert config.normalise_y(5.0) == pytest.approx(0.5)

    def test_saturation_for_x_explicit(self):
        config = ColorConfig(x_saturation={0: 0.3, 2: 0.9})
        assert config.saturation_for_x(0) == pytest.approx(0.3)
        assert config.saturation_for_x(2) == pytest.approx(0.9)

    def test_saturation_for_x_default(self):
        config = ColorConfig()
        assert config.saturation_for_x(99) == pytest.approx(0.6)

    def test_time_hue_offset_boundaries(self):
        t_start = datetime(2024, 1, 1)
        t_end = datetime(2024, 12, 31)
        config = ColorConfig(t_start=t_start, t_end=t_end, t_hue_shift=30.0)
        assert config.time_hue_offset(t_start) == pytest.approx(0.0)
        assert config.time_hue_offset(t_end) == pytest.approx(30.0, abs=0.1)

    def test_time_hue_offset_no_range(self):
        config = ColorConfig()
        assert config.time_hue_offset(datetime(2024, 6, 1)) == pytest.approx(0.0)


class TestColorMapper:
    def _mapper(self):
        config = ColorConfig(
            z_palette={0: 210.0, 1: 120.0},
            y_min=0.0,
            y_max=100_000.0,
            t_start=datetime(2024, 1, 1),
            t_end=datetime(2024, 12, 31),
        )
        return ColorMapper(config)

    def test_map_returns_hex_string(self):
        mapper = self._mapper()
        colour = mapper.map(t=datetime(2024, 6, 1), x=1, y=50_000.0, z=0)
        assert colour.startswith("#")
        assert len(colour) == 7

    def test_map_different_z_different_colour(self):
        mapper = self._mapper()
        c0 = mapper.map(t=datetime(2024, 6, 1), x=1, y=50_000.0, z=0)
        c1 = mapper.map(t=datetime(2024, 6, 1), x=1, y=50_000.0, z=1)
        assert c0 != c1

    def test_map_rgba_returns_tuple(self):
        mapper = self._mapper()
        rgba = mapper.map_rgba(t=datetime(2024, 3, 1), x=2, y=20_000.0, z=0)
        r, g, b, a = rgba
        assert 0 <= r <= 255
        assert 0 <= g <= 255
        assert 0 <= b <= 255
        assert a == pytest.approx(1.0)

    def test_map_rgba_custom_opacity(self):
        mapper = self._mapper()
        _, _, _, a = mapper.map_rgba(
            t=datetime(2024, 3, 1), x=2, y=20_000.0, z=0, opacity=0.5
        )
        assert a == pytest.approx(0.5)

    def test_from_data_matrix(self):
        dm = DataMatrix()
        dm.insert_many([
            DataPoint(t=datetime(2024, 1, 1), x=1, y=10.0, z=0),
            DataPoint(t=datetime(2024, 6, 1), x=2, y=90.0, z=1),
        ])
        mapper = ColorMapper.from_data_matrix(dm)
        assert mapper.config.y_min == pytest.approx(10.0)
        assert mapper.config.y_max == pytest.approx(90.0)

    def test_hex_values_in_range(self):
        mapper = self._mapper()
        for y in [0, 25_000, 50_000, 75_000, 100_000]:
            colour = mapper.map(
                t=datetime(2024, 1, 1), x=0, y=float(y), z=0
            )
            r = int(colour[1:3], 16)
            g = int(colour[3:5], 16)
            b = int(colour[5:7], 16)
            assert 0 <= r <= 255
            assert 0 <= g <= 255
            assert 0 <= b <= 255


class TestYScale:
    def _config(self, scale: str) -> ColorConfig:
        return ColorConfig(y_min=0.0, y_max=1_000_000.0, y_scale=scale)

    # -- Linear (existing behaviour) ---------------------------------------

    def test_linear_midpoint(self):
        cfg = self._config("linear")
        assert cfg.normalise_y(500_000.0) == pytest.approx(0.5)

    def test_linear_minimum(self):
        cfg = self._config("linear")
        assert cfg.normalise_y(0.0) == pytest.approx(0.0)

    def test_linear_maximum(self):
        cfg = self._config("linear")
        assert cfg.normalise_y(1_000_000.0) == pytest.approx(1.0)

    # -- Log scale ---------------------------------------------------------

    def test_log_maximum_is_one(self):
        cfg = self._config("log")
        assert cfg.normalise_y(1_000_000.0) == pytest.approx(1.0)

    def test_log_minimum_is_zero(self):
        cfg = self._config("log")
        assert cfg.normalise_y(0.0) == pytest.approx(0.0)

    def test_log_expands_low_end(self):
        """Log-scale should give a higher normalised value than linear for
        very small y (relative to y_max), compressing the upper range."""
        cfg_lin = self._config("linear")
        cfg_log = self._config("log")
        small_y = 1_000.0  # 0.1 % of y_max
        assert cfg_log.normalise_y(small_y) > cfg_lin.normalise_y(small_y)

    def test_log_clamps_below_zero(self):
        cfg = self._config("log")
        assert cfg.normalise_y(-100.0) == pytest.approx(0.0)

    def test_log_scale_monotonic(self):
        cfg = self._config("log")
        vals = [0, 1, 10, 100, 1_000, 10_000, 100_000, 1_000_000]
        normed = [cfg.normalise_y(float(v)) for v in vals]
        assert normed == sorted(normed)

    # -- Sqrt scale --------------------------------------------------------

    def test_sqrt_maximum_is_one(self):
        cfg = self._config("sqrt")
        assert cfg.normalise_y(1_000_000.0) == pytest.approx(1.0)

    def test_sqrt_minimum_is_zero(self):
        cfg = self._config("sqrt")
        assert cfg.normalise_y(0.0) == pytest.approx(0.0)

    def test_sqrt_between_linear_and_log(self):
        """Sqrt compression should be weaker than log but stronger than linear."""
        cfg_lin  = self._config("linear")
        cfg_sqrt = self._config("sqrt")
        cfg_log  = self._config("log")
        y = 1_000.0
        assert cfg_log.normalise_y(y) > cfg_sqrt.normalise_y(y) > cfg_lin.normalise_y(y)

    def test_sqrt_scale_monotonic(self):
        cfg = self._config("sqrt")
        vals = [0, 100, 10_000, 1_000_000]
        normed = [cfg.normalise_y(float(v)) for v in vals]
        assert normed == sorted(normed)

    # -- Zero-span edge case -----------------------------------------------

    def test_log_zero_span_returns_half(self):
        cfg = ColorConfig(y_min=5.0, y_max=5.0, y_scale="log")
        assert cfg.normalise_y(5.0) == pytest.approx(0.5)

    def test_sqrt_zero_span_returns_half(self):
        cfg = ColorConfig(y_min=5.0, y_max=5.0, y_scale="sqrt")
        assert cfg.normalise_y(5.0) == pytest.approx(0.5)


class TestPaletteMode:
    """Tests for the color-blind accessibility palette modes."""

    def _mapper(self, mode: str) -> ColorMapper:
        config = ColorConfig(
            y_min=0.0, y_max=100.0,
            t_start=datetime(2024, 1, 1),
            t_end=datetime(2024, 12, 31),
            palette_mode=mode,
        )
        return ColorMapper(config)

    # -- normal (existing behaviour) ---------------------------------------

    def test_normal_different_z_differ(self):
        mapper = self._mapper("normal")
        c0 = mapper.map(t=datetime(2024, 6, 1), x=0, y=50.0, z=0)
        c1 = mapper.map(t=datetime(2024, 6, 1), x=0, y=50.0, z=1)
        assert c0 != c1

    # -- accessible (Okabe-Ito) -------------------------------------------

    def test_accessible_returns_valid_hex(self):
        mapper = self._mapper("accessible")
        color = mapper.map(t=datetime(2024, 6, 1), x=0, y=50.0, z=0)
        assert color.startswith("#") and len(color) == 7

    def test_accessible_different_z_differ(self):
        mapper = self._mapper("accessible")
        colors = {mapper.map(t=datetime(2024, 6, 1), x=0, y=50.0, z=i) for i in range(8)}
        assert len(colors) == 8

    def test_accessible_wraps_at_8(self):
        """z=0 and z=8 use the same palette slot, so same hue → same color."""
        cfg = ColorConfig(y_min=0.0, y_max=100.0, palette_mode="accessible")
        c0 = cfg.hue_for_z(0)
        c8 = cfg.hue_for_z(8)
        assert c0 == pytest.approx(c8)

    # -- monochrome --------------------------------------------------------

    def test_monochrome_zero_saturation(self):
        """In monochrome mode all colors should be grey (r == g == b)."""
        mapper = self._mapper("monochrome")
        for z in range(5):
            color = mapper.map(t=datetime(2024, 6, 1), x=0, y=50.0, z=z)
            r = int(color[1:3], 16)
            g = int(color[3:5], 16)
            b = int(color[5:7], 16)
            assert r == g == b, f"Expected grey for z={z}, got {color}"

    def test_monochrome_varies_with_y(self):
        """Lightness (and therefore greyscale value) must vary with y."""
        mapper = self._mapper("monochrome")
        c_low  = mapper.map(t=datetime(2024, 6, 1), x=0, y=0.0,   z=0)
        c_high = mapper.map(t=datetime(2024, 6, 1), x=0, y=100.0,  z=0)
        # Extract grey level (r=g=b)
        lv_low  = int(c_low[1:3], 16)
        lv_high = int(c_high[1:3], 16)
        assert lv_high > lv_low  # brighter for higher y

    def test_monochrome_hue_always_zero(self):
        cfg = ColorConfig(palette_mode="monochrome")
        assert cfg.hue_for_z(0) == pytest.approx(0.0)
        assert cfg.hue_for_z(5) == pytest.approx(0.0)


# ===========================================================================
# ColorPreset and apply_preset
# ===========================================================================

class TestColorPreset:
    """Tests for the ColorPreset enum and ColorMapper.apply_preset()."""

    def _base_mapper(self) -> "ColorMapper":
        from four_dim_matrix import ColorMapper, ColorConfig
        cfg = ColorConfig(y_min=0.0, y_max=1000.0)
        return ColorMapper(cfg)

    # -- enum values ---------------------------------------------------------

    def test_preset_enum_members_exist(self):
        from four_dim_matrix import ColorPreset
        assert ColorPreset.ANALYTICAL.value == "analytical"
        assert ColorPreset.INTUITIVE.value == "intuitive"
        assert ColorPreset.COLORBLIND_SAFE.value == "colorblind_safe"

    # -- INTUITIVE preset ----------------------------------------------------

    def test_intuitive_disables_t_shift(self):
        from four_dim_matrix import ColorPreset
        m = self._base_mapper()
        m.config.t_hue_shift = 30.0
        m.apply_preset(ColorPreset.INTUITIVE)
        assert m.config.t_hue_shift == 0.0

    def test_intuitive_enables_y_to_opacity(self):
        from four_dim_matrix import ColorPreset
        m = self._base_mapper()
        m.apply_preset(ColorPreset.INTUITIVE)
        assert m.config.y_to_opacity is True

    def test_intuitive_fixed_lightness(self):
        from four_dim_matrix import ColorPreset
        m = self._base_mapper()
        m.apply_preset(ColorPreset.INTUITIVE)
        assert m.config.lightness_min == m.config.lightness_max == 0.55

    def test_intuitive_map_rgba_encodes_y_as_opacity(self):
        from datetime import datetime
        from four_dim_matrix import ColorPreset
        m = self._base_mapper()
        m.apply_preset(ColorPreset.INTUITIVE)
        _, _, _, a_low  = m.map_rgba(t=datetime(2024, 1, 1), x=0, y=0.0,    z=0)
        _, _, _, a_high = m.map_rgba(t=datetime(2024, 1, 1), x=0, y=1000.0, z=0)
        assert a_low < a_high

    def test_intuitive_map_rgba_zero_y_near_transparent(self):
        from datetime import datetime
        from four_dim_matrix import ColorPreset
        m = self._base_mapper()
        m.apply_preset(ColorPreset.INTUITIVE)
        _, _, _, alpha = m.map_rgba(t=datetime(2024, 1, 1), x=0, y=0.0, z=0)
        assert alpha == pytest.approx(0.0, abs=1e-9)

    def test_intuitive_map_rgba_max_y_opaque(self):
        from datetime import datetime
        from four_dim_matrix import ColorPreset
        m = self._base_mapper()
        m.apply_preset(ColorPreset.INTUITIVE)
        _, _, _, alpha = m.map_rgba(t=datetime(2024, 1, 1), x=0, y=1000.0, z=0)
        assert alpha == pytest.approx(1.0, abs=1e-9)

    # -- COLORBLIND_SAFE preset ----------------------------------------------

    def test_colorblind_safe_sets_accessible_palette(self):
        from four_dim_matrix import ColorPreset
        m = self._base_mapper()
        m.apply_preset(ColorPreset.COLORBLIND_SAFE)
        assert m.config.palette_mode == "accessible"

    def test_colorblind_safe_does_not_enable_y_to_opacity(self):
        from four_dim_matrix import ColorPreset
        m = self._base_mapper()
        m.apply_preset(ColorPreset.COLORBLIND_SAFE)
        assert m.config.y_to_opacity is False

    # -- ANALYTICAL preset ---------------------------------------------------

    def test_analytical_restores_defaults_after_intuitive(self):
        from four_dim_matrix import ColorPreset
        m = self._base_mapper()
        m.apply_preset(ColorPreset.INTUITIVE)
        m.apply_preset(ColorPreset.ANALYTICAL)
        assert m.config.y_to_opacity is False
        assert m.config.palette_mode == "normal"
        assert m.config.t_hue_shift == 30.0

    # -- y_to_opacity field default ------------------------------------------

    def test_y_to_opacity_defaults_false(self):
        from four_dim_matrix import ColorConfig
        assert ColorConfig().y_to_opacity is False

    def test_map_rgba_ignores_explicit_opacity_when_y_to_opacity(self):
        from datetime import datetime
        from four_dim_matrix import ColorPreset
        m = self._base_mapper()
        m.apply_preset(ColorPreset.INTUITIVE)
        # Passing opacity=0.99 should be overridden by y
        _, _, _, a = m.map_rgba(t=datetime(2024, 1, 1), x=0, y=0.0, z=0, opacity=0.99)
        assert a != pytest.approx(0.99, abs=0.01)
