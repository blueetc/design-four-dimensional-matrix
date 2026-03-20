"""MVP Demo – Single-topic 2-D animated heatmap for user-perception validation.

This script creates a minimal, self-contained demonstration of the
four-dimensional colour matrix system.  It follows the **"Option A – continue"**
recommendation from the feasibility analysis:

* Select **one topic** (``z = 0``, e.g. "customer" entity).
* Fix a **7-day window** of synthetic data.
* Render a 2-D heatmap ``x`` vs ``y_bucket``, animated by day (``t``), with
  each cell coloured by the mapped hex colour.
* Write the result to ``mvp_demo.html`` so it can be opened in any browser.
* Print a structured **feedback questionnaire** to validate that users can read
  business meaning from the colours.

Usage::

    python -m four_dim_matrix.mvp_demo          # write mvp_demo.html
    python -m four_dim_matrix.mvp_demo --help   # show options

Dependencies:
    plotly  (``pip install plotly``) — optional, gracefully skipped if absent.
"""

from __future__ import annotations

import argparse
import math
import random
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from .color_mapping import ColorConfig, ColorMapper, ColorPreset
from .data_matrix import DataMatrix, DataPoint
from .knowledge_base import KnowledgeBase


# ---------------------------------------------------------------------------
# Synthetic data builder
# ---------------------------------------------------------------------------

def _build_synthetic_kb(
    n_days: int = 7,
    n_phases: int = 5,
    target_z: int = 0,
    seed: int = 42,
    inject_anomaly: bool = True,
) -> KnowledgeBase:
    """Build a synthetic :class:`~four_dim_matrix.KnowledgeBase` for the MVP.

    Parameters:
        n_days: Number of time steps (days) to generate.
        n_phases: Number of ``x`` (phase) values (0 … n_phases-1).
        target_z: The single topic ID to use.
        seed: Random seed for reproducibility.
        inject_anomaly: When ``True`` a deliberate spike is inserted on day 4
            at phase 2 so that users can be asked "can you spot the anomaly?".

    Returns:
        A populated :class:`~four_dim_matrix.KnowledgeBase`.
    """
    rng = random.Random(seed)
    base_date = datetime(2024, 1, 1)
    points: List[DataPoint] = []

    for day_idx in range(n_days):
        t = base_date + timedelta(days=day_idx)
        for phase in range(n_phases):
            # Simulate a funnel: earlier phases have more records
            base_y = (n_phases - phase) * 1_000 + rng.gauss(0, 200)
            y = max(100.0, base_y)

            # Inject a visible anomaly on day 4, phase 2
            if inject_anomaly and day_idx == 4 and phase == 2:
                y *= 5  # 5× spike – should be visually obvious

            points.append(
                DataPoint(
                    t=t,
                    x=phase,
                    y=round(y, 2),
                    z=target_z,
                    payload={
                        "day": t.strftime("%Y-%m-%d"),
                        "phase": phase,
                        "topic": target_z,
                        "anomaly": inject_anomaly and day_idx == 4 and phase == 2,
                    },
                )
            )

    config = ColorConfig(
        y_min=100.0,
        y_max=max(p.y for p in points),
        t_start=base_date,
        t_end=base_date + timedelta(days=n_days - 1),
    )
    kb = KnowledgeBase(config=config)
    kb.insert_many(points)
    return kb


# ---------------------------------------------------------------------------
# Plotly rendering
# ---------------------------------------------------------------------------

def _build_plotly_animation(
    kb: KnowledgeBase,
    target_z: int = 0,
    preset: ColorPreset = ColorPreset.INTUITIVE,
    output_path: str = "mvp_demo.html",
    title: str = "MVP Demo – Single-topic 4D Colour Matrix (7-Day Window)",
) -> None:
    """Render the KnowledgeBase as an animated Plotly heatmap and save to HTML.

    Each **animation frame** represents one day (``t``).  The x-axis is the
    business phase; the y-axis buckets records by quantity level; cell colour
    is the mapped hex colour from the :class:`~four_dim_matrix.ColorMapper`.

    Parameters:
        kb: The knowledge base to visualise.
        target_z: Topic ID to render (ignored if the KB only contains one topic).
        preset: Colour preset – defaults to :attr:`~ColorPreset.INTUITIVE` for
            maximum first-impression accessibility.
        output_path: Where to write the HTML file.
        title: Chart title.
    """
    try:
        import plotly.graph_objects as go
    except ImportError as exc:
        raise ImportError(
            "plotly is required for mvp_demo.  Install it with: pip install plotly"
        ) from exc

    # Apply preset to the mapper
    mapper = ColorMapper(kb._config)
    mapper.apply_preset(preset)

    # Collect all data points for the target topic
    points = kb.data_matrix.query(z=target_z)
    if not points:
        points = list(kb.data_matrix)  # fallback: render everything

    # Unique sorted time steps
    time_steps = sorted({p.t for p in points})

    # Build Y buckets (log-scale bins for better visual separation)
    y_values = [p.y for p in points]
    y_min_all = min(y_values) if y_values else 0
    y_max_all = max(y_values) if y_values else 1
    N_BINS = 8
    y_edges = [
        y_min_all + (y_max_all - y_min_all) * i / N_BINS for i in range(N_BINS + 1)
    ]

    def _y_bin(y: float) -> int:
        for i in range(N_BINS):
            if y <= y_edges[i + 1]:
                return i
        return N_BINS - 1

    x_values = sorted({p.x for p in points})
    x_labels = [f"Phase {x}" for x in x_values]
    y_labels = [
        f"{y_edges[i]:.0f}–{y_edges[i+1]:.0f}" for i in range(N_BINS)
    ]

    # ---- Build frames ----
    frames = []
    for t in time_steps:
        # Build a colour grid: rows = y-bins, cols = x-phases
        colour_grid = [["#d0d0d0"] * len(x_values) for _ in range(N_BINS)]
        hover_grid = [[""] * len(x_values) for _ in range(N_BINS)]

        day_points = [p for p in points if p.t == t]
        for pt in day_points:
            xi = x_values.index(pt.x) if pt.x in x_values else -1
            yi = _y_bin(pt.y)
            if xi < 0:
                continue
            # Use the KB-generated colour (already computed during insert)
            cp = kb.color_matrix.get(pt.t, pt.x, pt.y, pt.z)
            colour_grid[yi][xi] = cp.hex_color if cp else mapper.map(
                t=pt.t, x=pt.x, y=pt.y, z=pt.z
            )
            opacity = cp.opacity if cp else 1.0
            hover_grid[yi][xi] = (
                f"Day: {t.strftime('%Y-%m-%d')}<br>"
                f"Phase: {pt.x}<br>"
                f"Qty: {pt.y:,.0f}<br>"
                f"Colour: {colour_grid[yi][xi]}<br>"
                f"Anomaly: {pt.payload.get('anomaly', False)}"
            )

        cell_colours = []
        cell_hover = []
        for row in colour_grid:
            cell_colours.extend(row)
        for row in hover_grid:
            cell_hover.extend(row)

        frame_data = go.Heatmap(
            z=[[1] * len(x_values) for _ in range(N_BINS)],
            colorscale=[[i / (len(cell_colours) - 1), c] for i, c in enumerate(cell_colours)]
            if len(cell_colours) > 1
            else [[0, cell_colours[0]], [1, cell_colours[0]]],
            showscale=False,
            customdata=[[hover_grid[r][c] for c in range(len(x_values))] for r in range(N_BINS)],
            hovertemplate="%{customdata}<extra></extra>",
            x=x_labels,
            y=y_labels,
        )
        frames.append(go.Frame(data=[frame_data], name=t.strftime("%Y-%m-%d")))

    # ---- Build initial figure ----
    fig = go.Figure(
        data=[frames[0].data[0]] if frames else [],
        frames=frames,
        layout=go.Layout(
            title=title,
            xaxis=dict(title="Business Phase (x)"),
            yaxis=dict(title="Quantity Band (y)"),
            updatemenus=[
                dict(
                    type="buttons",
                    showactive=False,
                    buttons=[
                        dict(label="▶ Play", method="animate",
                             args=[None, {"frame": {"duration": 800, "redraw": True},
                                          "fromcurrent": True}]),
                        dict(label="⏸ Pause", method="animate",
                             args=[[None], {"frame": {"duration": 0, "redraw": False},
                                            "mode": "immediate"}]),
                    ],
                    x=0.1, y=0.02,
                )
            ],
            sliders=[
                dict(
                    steps=[
                        dict(args=[[f.name], {"frame": {"duration": 300, "redraw": True},
                                              "mode": "immediate"}],
                             label=f.name, method="animate")
                        for f in frames
                    ],
                    transition={"duration": 300},
                    x=0.1, y=0.0,
                    currentvalue={"prefix": "Day: "},
                    len=0.85,
                )
            ],
        ),
    )

    fig.write_html(output_path)
    print(f"[mvp_demo] Saved interactive animation → {output_path}")


# ---------------------------------------------------------------------------
# Feedback questionnaire
# ---------------------------------------------------------------------------

_FEEDBACK_QUESTIONNAIRE = """
╔══════════════════════════════════════════════════════════════════════════════╗
║             MVP Validation – User Feedback Questionnaire                    ║
║   Please open  mvp_demo.html  in your browser, watch the animation, and     ║
║   try to answer the following questions without any prior explanation.      ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  Perception test (colour meaning)                                            ║
║  ─────────────────────────────────                                           ║
║  Q1. Do brighter / more opaque cells represent higher or lower quantities?   ║
║  Q2. As the animation plays, can you tell that time is moving forward?       ║
║  Q3. What colour family dominates the chart – and what does it mean?         ║
║                                                                              ║
║  Anomaly detection test                                                      ║
║  ─────────────────────                                                       ║
║  Q4. On which day does a colour spike appear?                                ║
║      (Answer: Day 5, i.e. 2024-01-05; day_idx=4 is the 5th day, 1-indexed) ║
║  Q5. In which phase (column) does the spike appear?  (Answer: Phase 2)      ║
║  Q6. How long did it take you to notice the anomalous cell? (in seconds)    ║
║                                                                              ║
║  Story-telling test                                                          ║
║  ──────────────────                                                          ║
║  Q7. In your own words, describe the business trend shown by the animation.  ║
║  Q8. Which phase consistently has the highest quantity across all days?      ║
║                                                                              ║
║  Success criteria (from feasibility analysis):                               ║
║  ● Q1 correct rate  > 80 %                                                   ║
║  ● Q4+Q5 combined < 10 seconds to identify anomaly                           ║
║  ● Q7 captures "funnel shape" or "phase 0 dominates"                         ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------

def main(argv: Optional[list] = None) -> None:
    """Run the MVP demo end-to-end."""
    parser = argparse.ArgumentParser(
        description="Generate a single-topic Plotly MVP demo for the 4D colour matrix."
    )
    parser.add_argument(
        "--output", default="mvp_demo.html",
        help="Path for the output HTML file (default: mvp_demo.html)"
    )
    parser.add_argument(
        "--days", type=int, default=7,
        help="Number of time-steps (days) to generate (default: 7)"
    )
    parser.add_argument(
        "--phases", type=int, default=5,
        help="Number of x (phase) values to generate (default: 5)"
    )
    parser.add_argument(
        "--no-anomaly", action="store_true",
        help="Disable the injected anomaly spike (for comparison runs)"
    )
    parser.add_argument(
        "--preset", choices=["intuitive", "analytical", "colorblind_safe"],
        default="intuitive",
        help="Colour preset (default: intuitive)"
    )
    args = parser.parse_args(argv)

    preset_map = {
        "intuitive": ColorPreset.INTUITIVE,
        "analytical": ColorPreset.ANALYTICAL,
        "colorblind_safe": ColorPreset.COLORBLIND_SAFE,
    }

    print("[mvp_demo] Building synthetic knowledge base …")
    kb = _build_synthetic_kb(
        n_days=args.days,
        n_phases=args.phases,
        inject_anomaly=not args.no_anomaly,
    )
    print(f"[mvp_demo] Generated {len(kb.data_matrix)} data points.")

    print("[mvp_demo] Rendering Plotly animation …")
    _build_plotly_animation(
        kb=kb,
        preset=preset_map[args.preset],
        output_path=args.output,
    )

    print(_FEEDBACK_QUESTIONNAIRE)


if __name__ == "__main__":
    main()
