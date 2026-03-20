"""Tests for MatrixVisualizer and render_snapshot.

Matplotlib and Plotly are exercised in "headless" mode (no display).
All tests verify structural correctness (axes labels, data counts, figure
types) rather than pixel output, so they run in CI without a display server.
"""

from __future__ import annotations

import os
import tempfile
from datetime import datetime

import pytest

from four_dim_matrix import (
    ColorConfig,
    DataPoint,
    KnowledgeBase,
    MatrixVisualizer,
    render_snapshot,
)

# Force Matplotlib into non-interactive backend before any import of pyplot
os.environ.setdefault("MPLBACKEND", "Agg")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_kb() -> KnowledgeBase:
    """Small but representative knowledge base with 3 topics and 3 time steps."""
    kb = KnowledgeBase()
    points = [
        DataPoint(t=datetime(2024, 1, 1), x=0, y=100.0, z=0, payload={"name": "alpha"}),
        DataPoint(t=datetime(2024, 1, 1), x=1, y=200.0, z=1, payload={"name": "beta"}),
        DataPoint(t=datetime(2024, 1, 1), x=2, y=50.0,  z=2, payload={"name": "gamma"}),
        DataPoint(t=datetime(2024, 2, 1), x=0, y=150.0, z=0, payload={"name": "alpha"}),
        DataPoint(t=datetime(2024, 2, 1), x=1, y=180.0, z=1, payload={"name": "beta"}),
        DataPoint(t=datetime(2024, 2, 1), x=2, y=90.0,  z=2, payload={"name": "gamma"}),
        DataPoint(t=datetime(2024, 3, 1), x=0, y=200.0, z=0, payload={"name": "alpha"}),
        DataPoint(t=datetime(2024, 3, 1), x=1, y=160.0, z=1, payload={"name": "beta"}),
        DataPoint(t=datetime(2024, 3, 1), x=2, y=120.0, z=2, payload={"name": "gamma"}),
    ]
    kb.insert_many(points)
    return kb


# ---------------------------------------------------------------------------
# MatrixVisualizer – Matplotlib
# ---------------------------------------------------------------------------

class TestMatplotlibPlots:
    @pytest.fixture(autouse=True)
    def close_figs(self):
        import matplotlib.pyplot as plt
        yield
        plt.close("all")

    def test_plot_snapshot_returns_figure(self):
        import matplotlib.figure
        kb  = _make_kb()
        viz = MatrixVisualizer(kb)
        fig = viz.plot_snapshot()
        assert isinstance(fig, matplotlib.figure.Figure)

    def test_plot_snapshot_uses_latest_t_by_default(self):
        import matplotlib.figure
        kb  = _make_kb()
        viz = MatrixVisualizer(kb)
        fig = viz.plot_snapshot()
        assert "2024-03-01" in fig.axes[0].get_title()

    def test_plot_snapshot_explicit_t(self):
        import matplotlib.figure
        kb  = _make_kb()
        viz = MatrixVisualizer(kb)
        fig = viz.plot_snapshot(t=datetime(2024, 1, 1))
        assert "2024-01-01" in fig.axes[0].get_title()

    def test_plot_snapshot_empty_kb(self):
        import matplotlib.figure
        viz = MatrixVisualizer(KnowledgeBase())
        fig = viz.plot_snapshot()
        assert isinstance(fig, matplotlib.figure.Figure)

    def test_plot_timeline_returns_figure(self):
        import matplotlib.figure
        kb  = _make_kb()
        viz = MatrixVisualizer(kb)
        fig = viz.plot_timeline(z=0)
        assert isinstance(fig, matplotlib.figure.Figure)

    def test_plot_timeline_title_includes_z(self):
        kb  = _make_kb()
        viz = MatrixVisualizer(kb)
        fig = viz.plot_timeline(z=1)
        assert "z=1" in fig.axes[0].get_title()

    def test_plot_topic_distribution_returns_figure(self):
        import matplotlib.figure
        kb  = _make_kb()
        viz = MatrixVisualizer(kb)
        fig = viz.plot_topic_distribution()
        assert isinstance(fig, matplotlib.figure.Figure)

    def test_plot_topic_distribution_bar_count(self):
        kb  = _make_kb()
        viz = MatrixVisualizer(kb)
        fig = viz.plot_topic_distribution()
        ax  = fig.axes[0]
        # Should have one bar per topic (3 topics)
        assert len(ax.patches) == 3

    def test_plot_heatmap_returns_figure(self):
        import matplotlib.figure
        kb  = _make_kb()
        viz = MatrixVisualizer(kb)
        fig = viz.plot_heatmap()
        assert isinstance(fig, matplotlib.figure.Figure)

    def test_plot_heatmap_with_z_labels(self):
        import matplotlib.figure
        kb   = _make_kb()
        viz  = MatrixVisualizer(kb)
        fig  = viz.plot_heatmap(z_labels={0: "Alpha", 1: "Beta", 2: "Gamma"})
        ax   = fig.axes[0]
        tick_labels = [t.get_text() for t in ax.get_yticklabels()]
        assert "Alpha" in tick_labels

    def test_title_prefix_applied(self):
        kb  = _make_kb()
        viz = MatrixVisualizer(kb, title_prefix="[TEST] ")
        fig = viz.plot_snapshot()
        assert fig.axes[0].get_title().startswith("[TEST]")

    def test_plot_snapshot_save_to_file(self):
        kb  = _make_kb()
        viz = MatrixVisualizer(kb)
        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as f:
            path = f.name
        try:
            fig = viz.plot_snapshot()
            fig.savefig(path)
            assert os.path.getsize(path) > 0
        finally:
            os.unlink(path)


# ---------------------------------------------------------------------------
# MatrixVisualizer – Plotly
# ---------------------------------------------------------------------------

class TestPlotlyPlots:
    def test_to_plotly_snapshot_returns_figure(self):
        import plotly.graph_objects as go
        kb  = _make_kb()
        viz = MatrixVisualizer(kb)
        fig = viz.to_plotly_snapshot()
        assert isinstance(fig, go.Figure)

    def test_to_plotly_snapshot_trace_count(self):
        kb  = _make_kb()
        viz = MatrixVisualizer(kb)
        fig = viz.to_plotly_snapshot(t=datetime(2024, 1, 1))
        # One scatter trace
        assert len(fig.data) == 1

    def test_to_plotly_snapshot_hover_text(self):
        kb  = _make_kb()
        viz = MatrixVisualizer(kb)
        fig = viz.to_plotly_snapshot(t=datetime(2024, 1, 1))
        # Hover text should include topic info
        hover_texts = fig.data[0].text
        assert any("z=" in t for t in hover_texts)

    def test_to_plotly_snapshot_empty_kb(self):
        import plotly.graph_objects as go
        viz = MatrixVisualizer(KnowledgeBase())
        fig = viz.to_plotly_snapshot()
        assert isinstance(fig, go.Figure)

    def test_to_plotly_animation_returns_figure(self):
        import plotly.graph_objects as go
        kb  = _make_kb()
        viz = MatrixVisualizer(kb)
        fig = viz.to_plotly_animation()
        assert isinstance(fig, go.Figure)

    def test_to_plotly_animation_frame_count(self):
        kb  = _make_kb()
        viz = MatrixVisualizer(kb)
        fig = viz.to_plotly_animation()
        # Should have one frame per distinct t (3 time steps)
        assert len(fig.frames) == 3

    def test_to_plotly_heatmap_returns_figure(self):
        import plotly.graph_objects as go
        kb  = _make_kb()
        viz = MatrixVisualizer(kb)
        fig = viz.to_plotly_heatmap()
        assert isinstance(fig, go.Figure)

    def test_to_plotly_heatmap_with_z_labels(self):
        kb  = _make_kb()
        viz = MatrixVisualizer(kb)
        fig = viz.to_plotly_heatmap(z_labels={0: "Alpha", 1: "Beta", 2: "Gamma"})
        hover = fig.data[0].text
        assert any("Alpha" in t for t in hover)

    def test_to_plotly_snapshot_save_html(self):
        kb  = _make_kb()
        viz = MatrixVisualizer(kb)
        fig = viz.to_plotly_snapshot()
        with tempfile.NamedTemporaryFile(suffix=".html", delete=False) as f:
            path = f.name
        try:
            fig.write_html(path)
            assert os.path.getsize(path) > 0
        finally:
            os.unlink(path)


# ---------------------------------------------------------------------------
# render_snapshot convenience function
# ---------------------------------------------------------------------------

class TestRenderSnapshot:
    def test_matplotlib_backend_returns_figure(self):
        import matplotlib.figure
        kb  = _make_kb()
        fig = render_snapshot(kb, backend="matplotlib")
        assert isinstance(fig, matplotlib.figure.Figure)
        import matplotlib.pyplot as plt
        plt.close("all")

    def test_plotly_backend_returns_figure(self):
        import plotly.graph_objects as go
        kb  = _make_kb()
        fig = render_snapshot(kb, backend="plotly")
        assert isinstance(fig, go.Figure)

    def test_save_to_png(self):
        kb = _make_kb()
        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as f:
            path = f.name
        try:
            render_snapshot(kb, backend="matplotlib", save_path=path)
            assert os.path.getsize(path) > 0
        finally:
            os.unlink(path)
            import matplotlib.pyplot as plt
            plt.close("all")

    def test_save_html(self):
        kb = _make_kb()
        with tempfile.NamedTemporaryFile(suffix=".html", delete=False) as f:
            path = f.name
        try:
            render_snapshot(kb, backend="plotly", save_path=path)
            assert os.path.getsize(path) > 0
        finally:
            os.unlink(path)
