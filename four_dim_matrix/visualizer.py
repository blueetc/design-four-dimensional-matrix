"""Visualization utilities for the dual-matrix knowledge system.

Provides static snapshots and interactive plots via **Matplotlib** (always
available after ``pip install four-dim-matrix[viz]``) and **Plotly** (optional
but strongly recommended for interactive exploration).

Quick start::

    from four_dim_matrix import KnowledgeBase, DataPoint
    from four_dim_matrix.visualizer import MatrixVisualizer
    from datetime import datetime

    kb = KnowledgeBase()
    # ... populate kb ...

    viz = MatrixVisualizer(kb)
    fig = viz.plot_snapshot()          # 2D topic × quantity view at latest t
    fig.savefig("snapshot.png")

    fig2 = viz.plot_timeline(z=0)      # colour trail for topic 0
    fig3 = viz.plot_heatmap()          # z × t heat map

    # Interactive Plotly version
    pfig = viz.to_plotly_snapshot()
    pfig.show()

    # Animated Plotly across all time steps
    afig = viz.to_plotly_animation()
    afig.show()

Performance note
----------------
For matrices with >10 000 points call :meth:`~four_dim_matrix.DataMatrix.downsample`
or :meth:`~four_dim_matrix.DataMatrix.aggregate_by_time` before passing the
:class:`~four_dim_matrix.KnowledgeBase` to the visualizer.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from .color_matrix import ColorPoint
from .data_matrix import DataPoint
from .knowledge_base import KnowledgeBase


# ---------------------------------------------------------------------------
# Main visualizer class
# ---------------------------------------------------------------------------

class MatrixVisualizer:
    """Create static (Matplotlib) and interactive (Plotly) visualizations of
    a :class:`~four_dim_matrix.KnowledgeBase`.

    Parameters:
        kb: The knowledge base to visualize.
        title_prefix: Optional string prepended to every figure title.
    """

    def __init__(self, kb: KnowledgeBase, title_prefix: str = "") -> None:
        self.kb = kb
        self.title_prefix = title_prefix

    # ------------------------------------------------------------------
    # Matplotlib plots
    # ------------------------------------------------------------------

    def plot_snapshot(
        self,
        t: Optional[datetime] = None,
        figsize: Tuple[int, int] = (10, 6),
    ) -> "matplotlib.figure.Figure":  # type: ignore[name-defined]
        """2-D scatter: z (topic index) on x-axis, y (quantity) on y-axis.

        Each point is coloured using the :class:`~four_dim_matrix.ColorMatrix`
        value at its ``(t, x, y, z)`` coordinate, so the plot literally
        *shows* the dual-matrix colour encoding.

        Parameters:
            t: The time slice to visualize.  When ``None`` the latest
               ``t`` in the matrix is used.
            figsize: Matplotlib figure size ``(width, height)`` in inches.

        Returns:
            A ``matplotlib.figure.Figure``.
        """
        import matplotlib.pyplot as plt  # type: ignore[import]

        if t is None:
            ts = self.kb.data_matrix.distinct_t()
            if not ts:
                fig, ax = plt.subplots(figsize=figsize)
                ax.set_title(f"{self.title_prefix}Snapshot (empty)")
                return fig
            t = ts[-1]

        data_pts  = self.kb.data_matrix.query(t=t)
        color_pts = {(cp.x, cp.y, cp.z): cp.hex_color
                     for cp in self.kb.color_matrix.query(t=t)}

        xs   = [pt.z for pt in data_pts]
        ys   = [pt.y for pt in data_pts]
        cols = [
            color_pts.get((pt.x, pt.y, pt.z), "#808080")
            for pt in data_pts
        ]

        fig, ax = plt.subplots(figsize=figsize)
        sc = ax.scatter(xs, ys, c=cols, s=80, edgecolors="none", alpha=0.85)
        ax.set_xlabel("z  (topic index)")
        ax.set_ylabel("y  (quantity)")
        ax.set_title(f"{self.title_prefix}Snapshot at t={t.date()}")
        fig.tight_layout()
        return fig

    def plot_timeline(
        self,
        z: int,
        figsize: Tuple[int, int] = (12, 4),
    ) -> "matplotlib.figure.Figure":  # type: ignore[name-defined]
        """Line chart of total ``y`` over time for topic *z*.

        Each data point on the line is coloured with the corresponding
        :class:`~four_dim_matrix.ColorPoint` hex value, making the
        "colour trail" of the topic visible as a sequence of coloured
        markers.

        Parameters:
            z: The topic index to plot.
            figsize: Figure size in inches.
        """
        import matplotlib.pyplot as plt  # type: ignore[import]
        import matplotlib.patches as mpatches  # type: ignore[import]

        trend   = self.kb.trend(z=z)
        ctimeline = self.kb.colour_timeline(z=z)
        color_by_t: Dict[datetime, str] = dict(ctimeline)

        times   = sorted(trend.keys())
        totals  = [trend[t] for t in times]
        colors  = [color_by_t.get(t, "#808080") for t in times]

        fig, ax = plt.subplots(figsize=figsize)
        ax.plot(times, totals, color="#cccccc", linewidth=1, zorder=1)
        ax.scatter(times, totals, c=colors, s=60, edgecolors="none",
                   zorder=2, alpha=0.9)
        ax.set_xlabel("t  (time)")
        ax.set_ylabel("y  (total quantity)")
        ax.set_title(f"{self.title_prefix}Colour trail – topic z={z}")
        fig.autofmt_xdate()
        fig.tight_layout()
        return fig

    def plot_topic_distribution(
        self,
        t: Optional[datetime] = None,
        figsize: Tuple[int, int] = (8, 8),
    ) -> "matplotlib.figure.Figure":  # type: ignore[name-defined]
        """Bar chart showing the fractional ``y`` share per topic at time *t*.

        Each bar is filled with the topic's colour from the ColorMatrix.

        Parameters:
            t: Time slice.  ``None`` → aggregate over all time.
            figsize: Figure size in inches.
        """
        import matplotlib.pyplot as plt  # type: ignore[import]

        dist  = self.kb.topic_distribution(t=t)
        if not dist:
            fig, ax = plt.subplots(figsize=figsize)
            ax.set_title(f"{self.title_prefix}Topic distribution (empty)")
            return fig

        # Fetch representative color for each topic
        color_by_z: Dict[int, str] = {}
        for cp in self.kb.color_matrix:
            if cp.z not in color_by_z:
                color_by_z[cp.z] = cp.hex_color

        zs      = sorted(dist.keys())
        fracs   = [dist[z] for z in zs]
        colors  = [color_by_z.get(z, "#808080") for z in zs]

        fig, ax = plt.subplots(figsize=figsize)
        ax.bar([str(z) for z in zs], fracs, color=colors, edgecolor="none")
        ax.set_xlabel("z  (topic index)")
        ax.set_ylabel("fraction of total y")
        label = f"at t={t.date()}" if t else "(all time)"
        ax.set_title(f"{self.title_prefix}Topic distribution {label}")
        ax.set_ylim(0, 1)
        fig.tight_layout()
        return fig

    def plot_heatmap(
        self,
        z_labels: Optional[Dict[int, str]] = None,
        figsize: Tuple[int, int] = (12, 6),
    ) -> "matplotlib.figure.Figure":  # type: ignore[name-defined]
        """Heat map with ``t`` on the x-axis and ``z`` (topic) on the y-axis.

        Cell colour is taken directly from the ColorMatrix so the heat map
        is the canonical 2-D projection of the colour cloud (collapsed over
        the x-axis by taking the first matching ColorPoint per (t, z) cell).

        Parameters:
            z_labels: Optional mapping ``{z: label_str}`` for the y-axis.
            figsize: Figure size in inches.
        """
        import matplotlib.pyplot as plt  # type: ignore[import]
        import matplotlib.colors as mcolors  # type: ignore[import]
        import numpy as np  # type: ignore[import]

        all_t = sorted(self.kb.data_matrix.distinct_t())
        all_z = sorted(self.kb.data_matrix.distinct_z())

        if not all_t or not all_z:
            fig, ax = plt.subplots(figsize=figsize)
            ax.set_title(f"{self.title_prefix}Heat map (empty)")
            return fig

        t_idx = {t: i for i, t in enumerate(all_t)}
        z_idx = {z: i for i, z in enumerate(all_z)}
        rgb_grid = np.full((len(all_z), len(all_t), 3), 0.5)

        for cp in self.kb.color_matrix:
            if cp.t in t_idx and cp.z in z_idx:
                r, g, b = cp.rgb
                ti, zi = t_idx[cp.t], z_idx[cp.z]
                rgb_grid[zi, ti] = (r / 255.0, g / 255.0, b / 255.0)

        fig, ax = plt.subplots(figsize=figsize)
        ax.imshow(rgb_grid, aspect="auto", origin="lower",
                  extent=[-0.5, len(all_t) - 0.5, -0.5, len(all_z) - 0.5])

        # x-axis: time labels
        step = max(1, len(all_t) // 10)
        ax.set_xticks(range(0, len(all_t), step))
        ax.set_xticklabels(
            [all_t[i].strftime("%Y-%m-%d") for i in range(0, len(all_t), step)],
            rotation=45, ha="right",
        )

        # y-axis: topic labels
        ax.set_yticks(range(len(all_z)))
        if z_labels:
            ax.set_yticklabels([z_labels.get(z, str(z)) for z in all_z])
        else:
            ax.set_yticklabels([str(z) for z in all_z])

        ax.set_xlabel("t  (time)")
        ax.set_ylabel("z  (topic)")
        ax.set_title(f"{self.title_prefix}Colour heat map")
        fig.tight_layout()
        return fig

    # ------------------------------------------------------------------
    # Plotly interactive plots
    # ------------------------------------------------------------------

    def to_plotly_snapshot(
        self,
        t: Optional[datetime] = None,
    ) -> "plotly.graph_objects.Figure":  # type: ignore[name-defined]
        """Interactive Plotly scatter: z vs y at time *t*.

        Hover over any point to see its full payload dictionary.

        Parameters:
            t: Time slice.  ``None`` → latest ``t``.
        """
        import plotly.graph_objects as go  # type: ignore[import]

        if t is None:
            ts = self.kb.data_matrix.distinct_t()
            if not ts:
                return go.Figure()
            t = ts[-1]

        data_pts  = self.kb.data_matrix.query(t=t)
        color_map = {(cp.x, cp.y, cp.z): cp.hex_color
                     for cp in self.kb.color_matrix.query(t=t)}

        xs, ys, colors, hover = [], [], [], []
        for pt in data_pts:
            xs.append(pt.z)
            ys.append(pt.y)
            colors.append(color_map.get((pt.x, pt.y, pt.z), "#808080"))
            hover.append(
                f"z={pt.z}  x={pt.x}  y={pt.y:.2f}<br>"
                + "<br>".join(
                    f"{k}: {v}" for k, v in (pt.payload or {}).items()
                    if not k.startswith("_")
                )
            )

        fig = go.Figure(go.Scatter(
            x=xs, y=ys,
            mode="markers",
            marker=dict(color=colors, size=10, opacity=0.85),
            text=hover,
            hoverinfo="text",
        ))
        fig.update_layout(
            title=f"{self.title_prefix}Snapshot at t={t.date()}",
            xaxis_title="z (topic index)",
            yaxis_title="y (quantity)",
            template="plotly_white",
        )
        return fig

    def to_plotly_animation(
        self,
        frame_duration_ms: int = 500,
    ) -> "plotly.graph_objects.Figure":  # type: ignore[name-defined]
        """Animated Plotly figure that steps through all ``t`` values.

        Each frame is a snapshot at one time step.  Use the play/pause
        button in the browser to navigate the t-axis.

        Parameters:
            frame_duration_ms: Duration of each animation frame in
                milliseconds.
        """
        import plotly.graph_objects as go  # type: ignore[import]

        all_t = sorted(self.kb.data_matrix.distinct_t())
        if not all_t:
            return go.Figure()

        # Pre-compute per-frame data
        frames = []
        for t_val in all_t:
            data_pts  = self.kb.data_matrix.query(t=t_val)
            color_map = {(cp.x, cp.y, cp.z): cp.hex_color
                         for cp in self.kb.color_matrix.query(t=t_val)}
            xs = [pt.z for pt in data_pts]
            ys = [pt.y for pt in data_pts]
            cols = [color_map.get((pt.x, pt.y, pt.z), "#808080") for pt in data_pts]
            frames.append(go.Frame(
                data=[go.Scatter(
                    x=xs, y=ys,
                    mode="markers",
                    marker=dict(color=cols, size=10, opacity=0.85),
                )],
                name=t_val.strftime("%Y-%m-%d"),
            ))

        # Initial frame (latest t for display)
        first = frames[0]
        fig = go.Figure(
            data=first.data,
            frames=frames,
            layout=go.Layout(
                title=f"{self.title_prefix}Animated colour cloud",
                xaxis_title="z (topic index)",
                yaxis_title="y (quantity)",
                template="plotly_white",
                updatemenus=[{
                    "type": "buttons",
                    "buttons": [
                        {"label": "▶ Play",
                         "method": "animate",
                         "args": [None, {"frame": {"duration": frame_duration_ms},
                                         "fromcurrent": True}]},
                        {"label": "⏸ Pause",
                         "method": "animate",
                         "args": [[None], {"frame": {"duration": 0},
                                           "mode": "immediate"}]},
                    ],
                }],
                sliders=[{
                    "steps": [
                        {"args": [[f.name],
                                  {"frame": {"duration": frame_duration_ms},
                                   "mode": "immediate"}],
                         "label": f.name,
                         "method": "animate"}
                        for f in frames
                    ],
                    "x": 0.1, "len": 0.9,
                }],
            ),
        )
        return fig

    def to_plotly_heatmap(
        self,
        z_labels: Optional[Dict[int, str]] = None,
    ) -> "plotly.graph_objects.Figure":  # type: ignore[name-defined]
        """Interactive Plotly heat map: z on y-axis, t on x-axis.

        Cell colour comes from the ColorMatrix; hover shows z, t, and y.
        """
        import plotly.graph_objects as go  # type: ignore[import]

        all_t = sorted(self.kb.data_matrix.distinct_t())
        all_z = sorted(self.kb.data_matrix.distinct_z())

        if not all_t or not all_z:
            return go.Figure()

        t_idx = {t: i for i, t in enumerate(all_t)}
        z_idx = {z: i for i, z in enumerate(all_z)}

        # Build per-cell color and hover text
        color_grid: List[List[str]] = [
            ["#808080"] * len(all_t) for _ in all_z
        ]
        hover_grid: List[List[str]] = [
            [""] * len(all_t) for _ in all_z
        ]
        y_grid: List[List[float]] = [
            [0.0] * len(all_t) for _ in all_z
        ]

        # Aggregate y per (t, z) from data matrix
        for pt in self.kb.data_matrix:
            if pt.t in t_idx and pt.z in z_idx:
                y_grid[z_idx[pt.z]][t_idx[pt.t]] += pt.y

        for cp in self.kb.color_matrix:
            if cp.t in t_idx and cp.z in z_idx:
                zi, ti = z_idx[cp.z], t_idx[cp.t]
                color_grid[zi][ti] = cp.hex_color
                y_lab = z_labels.get(cp.z, str(cp.z)) if z_labels else str(cp.z)
                hover_grid[zi][ti] = (
                    f"z={cp.z} ({y_lab})<br>"
                    f"t={cp.t.date()}<br>"
                    f"y={y_grid[zi][ti]:.2f}"
                )

        t_labels = [t.strftime("%Y-%m-%d") for t in all_t]
        z_label_list = [
            (z_labels.get(z, str(z)) if z_labels else str(z))
            for z in all_z
        ]

        # Use a scatter approach to render per-cell custom colors
        xs_flat, ys_flat, cs_flat, ht_flat = [], [], [], []
        for zi, z_val in enumerate(all_z):
            for ti, t_val in enumerate(all_t):
                xs_flat.append(t_labels[ti])
                ys_flat.append(z_label_list[zi])
                cs_flat.append(color_grid[zi][ti])
                ht_flat.append(hover_grid[zi][ti])

        fig = go.Figure(go.Scatter(
            x=xs_flat, y=ys_flat,
            mode="markers",
            marker=dict(color=cs_flat, size=20, symbol="square", opacity=0.9),
            text=ht_flat, hoverinfo="text",
        ))
        fig.update_layout(
            title=f"{self.title_prefix}Colour heat map",
            xaxis_title="t (time)",
            yaxis_title="z (topic)",
            template="plotly_white",
        )
        return fig


# ---------------------------------------------------------------------------
# Convenience export function
# ---------------------------------------------------------------------------

def render_snapshot(
    kb: KnowledgeBase,
    t: Optional[datetime] = None,
    backend: str = "matplotlib",
    save_path: Optional[str] = None,
) -> Any:
    """One-call helper to render and optionally save a 2-D snapshot.

    Parameters:
        kb: Knowledge base to visualize.
        t: Time slice.  ``None`` → latest ``t``.
        backend: ``"matplotlib"`` (default) or ``"plotly"``.
        save_path: File path to save the figure.  For Matplotlib, pass a
            ``.png`` / ``.svg`` path; for Plotly pass an ``.html`` path.
            ``None`` → return the figure object without saving.

    Returns:
        A Matplotlib ``Figure`` or a Plotly ``Figure``.
    """
    viz = MatrixVisualizer(kb)
    if backend == "plotly":
        fig = viz.to_plotly_snapshot(t=t)
        if save_path:
            fig.write_html(save_path)
    else:
        fig = viz.plot_snapshot(t=t)
        if save_path:
            fig.savefig(save_path, dpi=150, bbox_inches="tight")
    return fig
