"""Generate an interactive 3-D visualization from the wide table.

Axes:
- **X** – time (the selected time column)
- **Y** – business volume (a chosen measure column)
- **Z** – theme / dimension (a categorical column, each category gets its own
  position along the Z-axis)

The output is a self-contained HTML file using **Plotly.js** that renders a 3-D
scatter plot with hover tooltips showing the full wide-table record.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

PLOTLY_CDN = "https://cdn.plot.ly/plotly-2.35.0.min.js"

# ---------------------------------------------------------------------------
# Data query
# ---------------------------------------------------------------------------


def _query_wide_data(
    conn: sqlite3.Connection,
    wide_table: str,
    time_col: str,
    measure_col: str,
    theme_col: str,
    limit: int = 5000,
) -> list[dict[str, Any]]:
    """Fetch rows from the wide table for visualization."""
    cur = conn.execute(  # noqa: S608
        f"SELECT * FROM [{wide_table}] "
        f"WHERE [{time_col}] IS NOT NULL AND [{measure_col}] IS NOT NULL "
        f"ORDER BY [{time_col}] LIMIT ?",
        (limit,),
    )
    col_names = [d[0] for d in cur.description]
    rows: list[dict[str, Any]] = []
    for r in cur.fetchall():
        rows.append(dict(zip(col_names, r)))
    return rows


# ---------------------------------------------------------------------------
# HTML generation
# ---------------------------------------------------------------------------


def _build_hover_text(row: dict[str, Any]) -> str:
    """Build a multi-line hover text showing all non-null fields."""
    parts: list[str] = []
    for k, v in row.items():
        if v is not None and not k.startswith("_"):
            parts.append(f"{k}: {v}")
    return "<br>".join(parts)


def generate_3d_html(
    conn: sqlite3.Connection,
    wide_table: str,
    time_col: str,
    measure_col: str,
    theme_col: str,
    *,
    title: str = "Wide Table – 3D Business Space",
    limit: int = 5000,
) -> str:
    """Return a self-contained HTML string with an interactive 3-D scatter plot.

    Parameters
    ----------
    conn : sqlite3.Connection
    wide_table : str – name of the wide table
    time_col : str – column to use for x-axis (time)
    measure_col : str – column for y-axis (business volume)
    theme_col : str – column for z-axis grouping (theme / dimension)
    title : str – page title
    limit : int – max rows to visualize
    """
    rows = _query_wide_data(conn, wide_table, time_col, measure_col, theme_col, limit)

    if not rows:
        return _empty_html(title)

    # Assign numeric Z positions by theme category.
    themes = sorted({str(r.get(theme_col, "unknown")) for r in rows})
    theme_index: dict[str, int] = {t: i for i, t in enumerate(themes)}

    # Build trace data grouped by theme for coloring.
    traces_data: dict[str, dict[str, list[Any]]] = {}
    for r in rows:
        theme_val = str(r.get(theme_col, "unknown"))
        if theme_val not in traces_data:
            traces_data[theme_val] = {"x": [], "y": [], "z": [], "text": []}

        td = traces_data[theme_val]
        td["x"].append(str(r.get(time_col, "")))
        y_val = r.get(measure_col, 0)
        try:
            y_val = float(str(y_val).replace(",", "")) if y_val is not None else 0
        except (ValueError, TypeError):
            y_val = 0
        td["y"].append(y_val)
        td["z"].append(theme_index.get(theme_val, 0))
        td["text"].append(_build_hover_text(r))

    traces_json = json.dumps(
        [
            {
                "type": "scatter3d",
                "mode": "markers",
                "name": theme,
                "x": data["x"],
                "y": data["y"],
                "z": data["z"],
                "text": data["text"],
                "hoverinfo": "text",
                "marker": {"size": 4, "opacity": 0.8},
            }
            for theme, data in traces_data.items()
        ],
        ensure_ascii=False,
    )

    layout_json = json.dumps(
        {
            "title": {"text": title},
            "scene": {
                "xaxis": {"title": f"Time ({time_col})"},
                "yaxis": {"title": f"Volume ({measure_col})"},
                "zaxis": {
                    "title": f"Theme ({theme_col})",
                    "tickvals": list(theme_index.values()),
                    "ticktext": list(theme_index.keys()),
                },
            },
            "margin": {"l": 0, "r": 0, "b": 0, "t": 40},
            "hovermode": "closest",
        },
        ensure_ascii=False,
    )

    html = f"""\
<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="utf-8">
<title>{_esc(title)}</title>
<script src="{PLOTLY_CDN}"></script>
<style>
  body {{ margin: 0; font-family: sans-serif; }}
  #chart {{ width: 100vw; height: 100vh; }}
</style>
</head>
<body>
<div id="chart"></div>
<script>
  var traces = {traces_json};
  var layout = {layout_json};
  Plotly.newPlot('chart', traces, layout, {{responsive: true}});
</script>
</body>
</html>"""
    return html


def save_3d_html(
    conn: sqlite3.Connection,
    wide_table: str,
    time_col: str,
    measure_col: str,
    theme_col: str,
    out_path: str,
    **kwargs: Any,
) -> str:
    """Generate and save the 3-D HTML file.  Returns the absolute path."""
    html = generate_3d_html(conn, wide_table, time_col, measure_col, theme_col, **kwargs)
    p = Path(out_path).expanduser().resolve()
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(html, encoding="utf-8")
    return str(p)


def _empty_html(title: str) -> str:
    return f"""\
<!DOCTYPE html>
<html lang="zh">
<head><meta charset="utf-8"><title>{_esc(title)}</title></head>
<body><h2>No data available for visualization.</h2></body>
</html>"""


def _esc(s: str) -> str:
    """Minimal HTML-entity escaping for title text."""
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")
