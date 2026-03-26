"""ColorMapper – translates four-dimensional coordinates into HSL colours.

The mapping rules follow the design described in the architecture:

* ``z`` (topic)        → hue   (which colour family identifies the topic)
* ``y`` (quantity)     → lightness  (higher value = brighter)
* ``x`` (phase)        → saturation (later phases = more vivid)
* ``t`` (time)         → hue offset (time-flow colour temperature shift)
"""

from __future__ import annotations

import colorsys
import math
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Tuple


class ColorPreset(Enum):
    """Named presets that reconfigure :class:`ColorMapper` for common use-cases.

    Attributes:
        ANALYTICAL: Full HSL encoding – the default mode where all four
            dimensions are expressed through hue, saturation, lightness, and
            hue-temperature offset.  Optimal for expert / analytical users who
            understand the mapping rules.
        INTUITIVE: Simplified mode designed for first-time users.

            * ``z`` → colour-family (hue), as usual.
            * ``y`` → **opacity** instead of lightness.  Transparent = low
              quantity; opaque = high quantity.  Opacity is a more instinctive
              visual encoding than lightness for "how much is there?".
            * ``x`` saturation is fixed at a comfortable mid-level (0.65)
              so the chart is not distracting.
            * ``t`` hue-temperature shift is disabled; time is meant to be
              communicated through the animation frame instead.

        COLORBLIND_SAFE: Wraps the **Okabe-Ito 8-colour palette** and enables
            ``palette_mode="accessible"`` so the chart is legible for the ~8 %
            of males with deuteranopia.  No other dimensions are changed.
    """

    ANALYTICAL = "analytical"
    INTUITIVE = "intuitive"
    COLORBLIND_SAFE = "colorblind_safe"


@dataclass
class ColorConfig:
    """Configuration for the colour-mapping function.

    Attributes:
        z_palette: Mapping from topic ID (``z``) to a base hue in degrees
            ``[0, 360)``.  Missing topics are assigned hues evenly spaced
            around the colour wheel.
        y_min: Minimum expected ``y`` value (used for normalisation).
        y_max: Maximum expected ``y`` value (used for normalisation).
        x_saturation: Mapping from phase index (``x``) to saturation in
            ``[0, 1]``.  Missing phases default to ``0.6``.
        t_start: Earliest expected timestamp (used for the time-shift).
        t_end: Latest expected timestamp (used for the time-shift).
        t_hue_shift: Total hue degrees shifted from ``t_start`` to ``t_end``.
            Positive values shift toward warmer tones over time.
        lightness_min: Minimum lightness value (default ``0.20``).
        lightness_max: Maximum lightness value (default ``0.80``).
    """

    z_palette: Dict[int, float] = field(default_factory=dict)
    y_min: float = 0.0
    y_max: float = 1.0
    x_saturation: Dict[int, float] = field(default_factory=dict)
    t_start: Optional[datetime] = None
    t_end: Optional[datetime] = None
    t_hue_shift: float = 30.0
    lightness_min: float = 0.20
    lightness_max: float = 0.80
    y_to_opacity: bool = False
    """When ``True`` the ``y`` axis is mapped to **opacity** (0–1) rather than
    lightness.  Lightness is then fixed at a comfortable mid-point (0.55) so
    that colours remain vivid at all quantity levels.

    This is the encoding used by the :attr:`ColorPreset.INTUITIVE` preset –
    opacity is a more instinctive "how much?" signal than HSL lightness for
    users who are unfamiliar with the colour-space encoding.
    """
    y_scale: str = "linear"
    """Normalisation scale for the y-axis.

    * ``"linear"`` (default) – proportional mapping; preserves ratio
      differences but compresses large dynamic ranges.
    * ``"log"`` – logarithmic mapping; expands the low end of the range,
      making small values visually distinguishable even when the maximum is
      orders of magnitude larger.  Suitable for row-count distributions
      that span several decades (e.g. 100 vs 100 000 000 rows).
    * ``"sqrt"`` – square-root mapping; a middle ground between linear and
      log, softer compression than log.
    """
    palette_mode: str = "normal"
    """Colour-palette mode for accessibility.

    * ``"normal"`` (default) – full HSL rainbow; optimal colour separation
      for typical vision.
    * ``"accessible"`` – Okabe-Ito 8-colour palette, designed to be
      distinguishable by people with deuteranopia (red-green colour
      blindness, ~8 % of males).  When ``z`` exceeds 7 the palette wraps.
    * ``"monochrome"`` – grey-scale only; lightness encodes z as well as y,
      and saturation is forced to 0.  Suitable for print or extremely
      high-contrast environments.
    """

    # Okabe-Ito hues (degrees), colour-blind safe palette
    _OKABE_ITO_HUES: List[float] = field(
        default_factory=lambda: [202.0, 27.0, 0.0, 270.0, 55.0, 180.0, 320.0, 36.0],
        repr=False,
    )

    def hue_for_z(self, z: int) -> float:
        """Return the base hue (degrees) for topic *z*.

        Respects :attr:`palette_mode`:

        * ``"normal"``     – golden-angle distribution (default).
        * ``"accessible"`` – wraps the Okabe-Ito 8-hue colour-blind-safe
          palette.
        * ``"monochrome"`` – always returns 0° (saturation is zeroed in
          :meth:`~ColorMapper._to_hsl`).
        """
        if self.palette_mode == "accessible":
            return self._OKABE_ITO_HUES[z % len(self._OKABE_ITO_HUES)]
        if self.palette_mode == "monochrome":
            return 0.0
        # "normal" or any unrecognised value
        if z in self.z_palette:
            return self.z_palette[z]
        # Auto-assign: spread evenly using a golden-angle step so topics
        # remain visually distinguishable even when many are added.
        golden_angle = 137.508
        return (z * golden_angle) % 360.0

    def saturation_for_x(self, x: int) -> float:
        """Return the saturation (0–1) for phase *x*."""
        return self.x_saturation.get(x, 0.6)

    def normalise_y(self, y: float) -> float:
        """Map *y* to ``[0, 1]`` using the configured y-range and scale.

        * ``y_scale="linear"`` – proportional (default).
        * ``y_scale="log"``    – log₂(1 + shifted) / log₂(1 + max_shifted).
        * ``y_scale="sqrt"``   – sqrt(shifted) / sqrt(max_shifted).
        """
        y_shifted = max(0.0, y - self.y_min)
        max_shifted = max(0.0, self.y_max - self.y_min)
        if max_shifted == 0:
            return 0.5

        if self.y_scale == "log":
            normalised = math.log1p(y_shifted) / math.log1p(max_shifted)
        elif self.y_scale == "sqrt":
            normalised = math.sqrt(y_shifted) / math.sqrt(max_shifted)
        else:  # "linear" or any unknown value
            normalised = y_shifted / max_shifted

        return max(0.0, min(1.0, normalised))

    def time_hue_offset(self, t: datetime) -> float:
        """Return the hue offset (degrees) that represents *t* on the time axis."""
        if self.t_start is None or self.t_end is None:
            return 0.0
        total_seconds = (self.t_end - self.t_start).total_seconds()
        if total_seconds == 0:
            return 0.0
        elapsed = (t - self.t_start).total_seconds()
        ratio = max(0.0, min(1.0, elapsed / total_seconds))
        return ratio * self.t_hue_shift


class ColorMapper:
    """Maps ``(t, x, y, z)`` coordinates to a hex colour string.

    Example::

        from datetime import datetime
        from four_dim_matrix import ColorMapper, ColorConfig

        config = ColorConfig(
            z_palette={0: 210.0, 1: 120.0, 2: 270.0},
            y_min=0.0,
            y_max=100_000.0,
            t_start=datetime(2024, 1, 1),
            t_end=datetime(2024, 12, 31),
        )
        mapper = ColorMapper(config)
        colour = mapper.map(t=datetime(2024, 6, 1), x=2, y=50000, z=1)
        print(colour)  # e.g. '#4caf6e'
    """

    def __init__(self, config: ColorConfig) -> None:
        self.config = config

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def map(self, t: datetime, x: int, y: float, z: int) -> str:
        """Return the hex colour string for the given four-dimensional point."""
        hue, saturation, lightness = self._to_hsl(t, x, y, z)
        return self._hsl_to_hex(hue, saturation, lightness)

    def map_rgba(
        self, t: datetime, x: int, y: float, z: int, opacity: float = 1.0
    ) -> Tuple[int, int, int, float]:
        """Return an ``(r, g, b, a)`` tuple for the given point.

        When :attr:`~ColorConfig.y_to_opacity` is ``True`` (i.e. the
        :attr:`~ColorPreset.INTUITIVE` preset is active) the *opacity*
        argument is ignored and ``y`` is instead used to compute the
        alpha channel so that quantity is encoded as transparency.
        """
        hue, saturation, lightness = self._to_hsl(t, x, y, z)
        r, g, b = self._hsl_to_rgb_int(hue, saturation, lightness)
        if self.config.y_to_opacity:
            opacity = self.config.normalise_y(y)
        return (r, g, b, opacity)

    def apply_preset(self, preset: "ColorPreset") -> None:
        """Reconfigure this mapper in-place to match *preset*.

        Parameters:
            preset: One of :class:`ColorPreset`.ANALYTICAL,
                :class:`ColorPreset`.INTUITIVE, or
                :class:`ColorPreset`.COLORBLIND_SAFE.

        Example::

            mapper = ColorMapper(ColorConfig(y_min=0, y_max=1000))
            mapper.apply_preset(ColorPreset.INTUITIVE)
            # y now drives opacity, t shift disabled, fixed saturation
        """
        cfg = self.config
        if preset == ColorPreset.ANALYTICAL:
            cfg.y_to_opacity = False
            cfg.t_hue_shift = 30.0
            cfg.palette_mode = "normal"
        elif preset == ColorPreset.INTUITIVE:
            cfg.y_to_opacity = True
            cfg.t_hue_shift = 0.0          # time shown via animation, not colour temp
            cfg.palette_mode = "normal"
            # Fix saturation to a comfortable mid-level (overrides per-x mapping)
            cfg.x_saturation = {}          # cleared so default 0.6 applies everywhere
            # Lightness is now a fixed mid-point so the chart stays vivid
            cfg.lightness_min = 0.55
            cfg.lightness_max = 0.55
        elif preset == ColorPreset.COLORBLIND_SAFE:
            cfg.y_to_opacity = False
            cfg.palette_mode = "accessible"

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _to_hsl(
        self, t: datetime, x: int, y: float, z: int
    ) -> Tuple[float, float, float]:
        """Compute ``(hue°, saturation, lightness)`` for the given coordinates."""
        cfg = self.config

        base_hue = cfg.hue_for_z(z)
        time_offset = cfg.time_hue_offset(t)
        hue = (base_hue + time_offset) % 360.0

        normalised_y = cfg.normalise_y(y)
        lightness = (
            cfg.lightness_min
            + normalised_y * (cfg.lightness_max - cfg.lightness_min)
        )

        if cfg.palette_mode == "monochrome":
            saturation = 0.0
        else:
            saturation = cfg.saturation_for_x(x)

        return (hue, saturation, lightness)

    @staticmethod
    def _hsl_to_rgb_int(
        hue: float, saturation: float, lightness: float
    ) -> Tuple[int, int, int]:
        """Convert HSL (hue in degrees, sat and lit in [0,1]) to RGB ints."""
        r, g, b = colorsys.hls_to_rgb(hue / 360.0, lightness, saturation)
        return (round(r * 255), round(g * 255), round(b * 255))

    @classmethod
    def _hsl_to_hex(cls, hue: float, saturation: float, lightness: float) -> str:
        """Convert HSL to a ``#rrggbb`` string."""
        r, g, b = cls._hsl_to_rgb_int(hue, saturation, lightness)
        return f"#{r:02x}{g:02x}{b:02x}"

    # ------------------------------------------------------------------
    # Auto-configuration helper
    # ------------------------------------------------------------------

    @classmethod
    def from_data_matrix(cls, dm: "DataMatrix") -> "ColorMapper":  # noqa: F821
        """Build a :class:`ColorMapper` calibrated to the given DataMatrix.

        This convenience method inspects the matrix and creates a
        :class:`ColorConfig` that covers the full value ranges found in *dm*.
        """
        from .data_matrix import DataMatrix

        assert isinstance(dm, DataMatrix)
        y_min, y_max = dm.y_range()
        ts = dm.distinct_t()
        t_start = ts[0] if ts else None
        t_end = ts[-1] if ts else None
        config = ColorConfig(y_min=y_min, y_max=y_max, t_start=t_start, t_end=t_end)
        return cls(config)
