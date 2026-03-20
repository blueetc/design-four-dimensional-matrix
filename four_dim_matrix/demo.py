"""四维矩阵数据库演示 – 通过数据库访问生成2个四维矩阵与可视化分析界面

本模块演示了通过访问2个 SQLite 示例数据库，如何生成 2 个四维矩阵（每个数据库
各生成一个 **数据矩阵 DataMatrix** 和一个 **颜色矩阵 ColorMatrix**），并输出
交互式可视化分析 HTML 页面。

架构说明
--------
四维矩阵使用 ``(t, x, y, z)`` 四个坐标轴描述数据库表信息：

* ``t`` – 快照时间（本次扫描的时刻）
* ``x`` – 生命周期阶段（表的列数映射到 new/growth/mature/legacy）
* ``y`` – 量级（行数经对数压缩后的归一化值）
* ``z`` – 业务域（按表名关键词自动分类）

每次扫描一个数据库都会同时产生 2 个矩阵：

1. **DataMatrix（数据矩阵）** – 存储每张表的坐标和元数据
2. **ColorMatrix（颜色矩阵）** – 与数据矩阵一一对应的 HSL 颜色编码矩阵

用法::

    python -m four_dim_matrix.demo               # 生成 dual_matrix_demo.html
    python -m four_dim_matrix.demo --help        # 查看全部选项

依赖::

    plotly  (pip install plotly) – 可选，缺失时跳过 HTML 输出
"""

from __future__ import annotations

import argparse
import math
import os
import sqlite3
import tempfile
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from .data_matrix import DataCell
from .db_adapter import DatabaseAdapter
from .hypercube import HyperCube


# ---------------------------------------------------------------------------
# Domain / lifecycle classification helpers
# ---------------------------------------------------------------------------

_DOMAIN_KEYWORDS: Dict[str, List[str]] = {
    "user":       ["user", "customer", "client", "member", "account", "profile"],
    "revenue":    ["order", "invoice", "payment", "sale", "transaction", "cart"],
    "product":    ["product", "item", "catalog", "inventory", "sku", "category"],
    "marketing":  ["campaign", "lead", "contact", "promo", "coupon", "banner"],
    "operations": ["log", "event", "audit", "config", "setting", "session", "task"],
}

_STAGE_TO_X: Dict[str, int] = {
    "new": 20, "growth": 50, "mature": 80, "legacy": 110,
}


def _classify_domain(table_name: str) -> Tuple[int, str]:
    """Return ``(z_id, domain_name)`` by matching *table_name* against keyword lists."""
    name_lower = table_name.lower()
    for z_id, (domain, keywords) in enumerate(_DOMAIN_KEYWORDS.items()):
        if any(kw in name_lower for kw in keywords):
            return z_id, domain
    return len(_DOMAIN_KEYWORDS), "operations"


def _classify_lifecycle(column_count: int) -> Tuple[str, int]:
    """Return ``(stage, x_value)`` from the number of columns."""
    if column_count <= 4:
        return "legacy", _STAGE_TO_X["legacy"]
    if column_count <= 8:
        return "mature", _STAGE_TO_X["mature"]
    if column_count <= 14:
        return "growth", _STAGE_TO_X["growth"]
    return "new", _STAGE_TO_X["new"]


def _compress_rows(row_count: int, max_rows: int) -> float:
    """Log-compress *row_count* into the range ``[1.0, 255.0]``."""
    if max_rows == 0 or row_count == 0:
        return 1.0
    ratio = math.log10(row_count + 1) / math.log10(max_rows + 1)
    return max(1.0, min(255.0, ratio * 255.0))


# ---------------------------------------------------------------------------
# DatabaseAdapter → HyperCube bridge
# ---------------------------------------------------------------------------

def build_hypercube_from_adapter(
    adapter: DatabaseAdapter,
    db_label: str = "database",
) -> HyperCube:
    """Convert a :class:`~four_dim_matrix.DatabaseAdapter` into a :class:`~four_dim_matrix.hypercube.HyperCube`.

    This bridges the ``DatabaseAdapter``/``TableInfo`` architecture and the
    ``HyperCube``/``DataCell`` architecture.  Each table in the adapter
    becomes one :class:`~four_dim_matrix.data_matrix.DataCell` in the
    data matrix, and a corresponding
    :class:`~four_dim_matrix.hypercube.ColorCell` in the colour matrix.

    The resulting :class:`~four_dim_matrix.hypercube.HyperCube` therefore
    holds **two** four-dimensional matrices simultaneously:

    * **DataMatrix** – stores the table's (t, x, y, z) coordinates and full
      metadata in ``hc.data_matrix``.
    * **ColorMatrix** – stores the HSL colour encoding for the same
      coordinates in ``hc.color_matrix``.

    Args:
        adapter: A pre-populated
            :class:`~four_dim_matrix.DatabaseAdapter` (from
            :meth:`~four_dim_matrix.DatabaseAdapter.from_sqlite` or
            :meth:`~four_dim_matrix.DatabaseAdapter.from_connection`).
        db_label: Human-readable name for the database (used in cell metadata
            and visualisation titles).

    Returns:
        A fully populated :class:`~four_dim_matrix.hypercube.HyperCube` with
        both matrices computed.
    """
    hypercube = HyperCube()
    tables = adapter.tables
    if not tables:
        return hypercube

    max_rows = max((t.row_count for t in tables), default=0)
    snapshot_t = adapter.snapshot_time

    for table in tables:
        z_id, domain_name = _classify_domain(table.name)
        stage, x_val = _classify_lifecycle(table.column_count)
        y_val = _compress_rows(table.row_count, max_rows)

        cell = DataCell(
            t=snapshot_t,
            x=x_val,
            y=y_val,
            z=z_id,
            table_name=table.name,
            schema_name=db_label,
            column_count=table.column_count,
            row_count=table.row_count,
            size_bytes=table.row_count * 100,
            business_domain=domain_name,
            lifecycle_stage=stage,
            tags=[db_label],
            payload=table.to_dict(),
        )
        hypercube.add_cell(cell, compute_color=True)

    hypercube.sync_color_matrix()
    return hypercube


# ---------------------------------------------------------------------------
# Sample SQLite database creators
# ---------------------------------------------------------------------------

def _create_ecommerce_db(path: str) -> None:
    """Create a sample e-commerce SQLite database at *path*."""
    conn = sqlite3.connect(path)
    conn.executescript("""
        CREATE TABLE customers (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE,
            phone TEXT,
            signup_date DATETIME DEFAULT CURRENT_TIMESTAMP,
            loyalty_tier TEXT DEFAULT 'bronze'
        );
        CREATE TABLE categories (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            parent_id INTEGER,
            description TEXT
        );
        CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            sku TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            price REAL,
            stock INTEGER DEFAULT 0,
            category_id INTEGER,
            active BOOLEAN DEFAULT 1,
            FOREIGN KEY (category_id) REFERENCES categories(id)
        );
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            status TEXT DEFAULT 'pending',
            total REAL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            shipped_at DATETIME,
            FOREIGN KEY (customer_id) REFERENCES customers(id)
        );
        CREATE TABLE order_items (
            id INTEGER PRIMARY KEY,
            order_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            quantity INTEGER,
            unit_price REAL,
            FOREIGN KEY (order_id) REFERENCES orders(id),
            FOREIGN KEY (product_id) REFERENCES products(id)
        );
        CREATE TABLE payments (
            id INTEGER PRIMARY KEY,
            order_id INTEGER NOT NULL,
            amount REAL,
            method TEXT,
            status TEXT,
            paid_at DATETIME,
            FOREIGN KEY (order_id) REFERENCES orders(id)
        );
        CREATE TABLE inventory_log (
            id INTEGER PRIMARY KEY,
            product_id INTEGER NOT NULL,
            change_qty INTEGER,
            reason TEXT,
            logged_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE promotions (
            id INTEGER PRIMARY KEY,
            code TEXT UNIQUE,
            discount_pct REAL,
            starts_at DATETIME,
            ends_at DATETIME,
            usage_count INTEGER DEFAULT 0
        );

        INSERT INTO customers VALUES
            (1,'Alice Chen','alice@example.com','555-0101','2024-01-10','gold'),
            (2,'Bob Smith','bob@example.com','555-0102','2024-02-15','silver'),
            (3,'Carol Wang','carol@example.com','555-0103','2024-03-01','bronze'),
            (4,'Dave Lee','dave@example.com','555-0104','2024-03-15','bronze'),
            (5,'Eve Patel','eve@example.com','555-0105','2024-04-01','gold');
        INSERT INTO categories VALUES
            (1,'Electronics',NULL,'Electronic devices'),
            (2,'Clothing',NULL,'Apparel'),
            (3,'Phones',1,'Mobile phones');
        INSERT INTO products VALUES
            (1,'SKU-A','Laptop',999.0,50,1,1),
            (2,'SKU-B','T-Shirt',29.99,200,2,1),
            (3,'SKU-C','Phone',699.0,75,3,1),
            (4,'SKU-D','Headphones',149.99,100,1,1);
        INSERT INTO orders VALUES
            (1,1,'paid',999.0,'2024-03-01','2024-03-03'),
            (2,2,'paid',29.99,'2024-03-05',NULL),
            (3,1,'shipped',849.0,'2024-03-10','2024-03-12'),
            (4,3,'pending',699.0,'2024-04-01',NULL),
            (5,4,'paid',149.99,'2024-04-05','2024-04-07');
        INSERT INTO order_items VALUES
            (1,1,1,1,999.0),(2,2,2,1,29.99),(3,3,3,1,699.0),
            (4,3,4,1,149.99),(5,4,3,1,699.0),(6,5,4,1,149.99);
        INSERT INTO payments VALUES
            (1,1,999.0,'card','paid','2024-03-01'),
            (2,2,29.99,'paypal','paid','2024-03-05'),
            (3,5,149.99,'card','paid','2024-04-05');
        INSERT INTO inventory_log VALUES
            (1,1,-1,'sale','2024-03-01'),
            (2,2,-1,'sale','2024-03-05'),
            (3,3,-1,'sale','2024-04-01');
        INSERT INTO promotions VALUES
            (1,'SAVE10',10.0,'2024-01-01','2024-12-31',25),
            (2,'SUMMER20',20.0,'2024-06-01','2024-08-31',0);
    """)
    conn.close()


def _create_crm_db(path: str) -> None:
    """Create a sample CRM SQLite database at *path*."""
    conn = sqlite3.connect(path)
    conn.executescript("""
        CREATE TABLE contacts (
            id INTEGER PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            email TEXT UNIQUE,
            phone TEXT,
            company TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE leads (
            id INTEGER PRIMARY KEY,
            contact_id INTEGER,
            source TEXT,
            status TEXT DEFAULT 'new',
            score INTEGER DEFAULT 0,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (contact_id) REFERENCES contacts(id)
        );
        CREATE TABLE deals (
            id INTEGER PRIMARY KEY,
            lead_id INTEGER,
            title TEXT,
            value REAL,
            stage TEXT DEFAULT 'prospect',
            probability INTEGER DEFAULT 50,
            expected_close DATETIME,
            FOREIGN KEY (lead_id) REFERENCES leads(id)
        );
        CREATE TABLE campaigns (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            type TEXT,
            budget REAL,
            status TEXT,
            starts_at DATETIME,
            ends_at DATETIME
        );
        CREATE TABLE campaign_contacts (
            id INTEGER PRIMARY KEY,
            campaign_id INTEGER,
            contact_id INTEGER,
            sent_at DATETIME,
            opened BOOLEAN DEFAULT 0,
            clicked BOOLEAN DEFAULT 0
        );
        CREATE TABLE tasks (
            id INTEGER PRIMARY KEY,
            contact_id INTEGER,
            title TEXT,
            due_at DATETIME,
            completed BOOLEAN DEFAULT 0,
            assigned_to TEXT
        );
        CREATE TABLE user_accounts (
            id INTEGER PRIMARY KEY,
            username TEXT UNIQUE,
            role TEXT DEFAULT 'sales_rep',
            email TEXT,
            last_login DATETIME,
            active BOOLEAN DEFAULT 1
        );

        INSERT INTO contacts VALUES
            (1,'John','Doe','jdoe@corp.com','555-1001','Acme Corp','2024-01-05'),
            (2,'Jane','Smith','jsmith@biz.com','555-1002','TechBiz','2024-02-10'),
            (3,'Raj','Kumar','rkumar@start.io','555-1003','StartupIO','2024-03-01'),
            (4,'Li','Wei','lwei@global.cn','555-1004','Global Inc','2024-03-20'),
            (5,'Maria','Garcia','mgarcia@media.es','555-1005','MediaES','2024-04-01');
        INSERT INTO leads VALUES
            (1,1,'web','qualified',85,'2024-01-06'),
            (2,2,'referral','new',60,'2024-02-11'),
            (3,3,'cold_call','contacted',40,'2024-03-02'),
            (4,4,'web','qualified',90,'2024-03-21'),
            (5,5,'email','new',30,'2024-04-02');
        INSERT INTO deals VALUES
            (1,1,'Enterprise License',50000.0,'negotiation',80,'2024-06-30'),
            (2,4,'Cloud Migration',120000.0,'demo',60,'2024-09-30');
        INSERT INTO campaigns VALUES
            (1,'Q1 Email Blast','email',5000.0,'completed','2024-01-01','2024-03-31'),
            (2,'Spring Webinar','event',10000.0,'active','2024-04-01','2024-04-30');
        INSERT INTO campaign_contacts VALUES
            (1,1,1,'2024-01-15',1,1),
            (2,1,2,'2024-01-15',1,0),
            (3,1,3,'2024-01-15',0,0);
        INSERT INTO tasks VALUES
            (1,1,'Follow-up call','2024-04-10',0,'rep1'),
            (2,2,'Send proposal','2024-04-15',1,'rep2'),
            (3,4,'Product demo','2024-04-20',0,'rep1');
        INSERT INTO user_accounts VALUES
            (1,'rep1','sales_rep','rep1@company.com','2024-04-10',1),
            (2,'rep2','sales_rep','rep2@company.com','2024-04-09',1),
            (3,'mgr1','manager','mgr1@company.com','2024-04-10',1);
    """)
    conn.close()


# ---------------------------------------------------------------------------
# HTML visualization export
# ---------------------------------------------------------------------------

def export_dual_matrix_html(
    hc_a: HyperCube,
    hc_b: HyperCube,
    db_a_label: str = "Database A",
    db_b_label: str = "Database B",
    output_path: str = "dual_matrix_demo.html",
) -> None:
    """Export both HyperCubes as a combined interactive HTML analysis page.

    The page shows six views arranged in a 3×2 grid:

    * **Row 1** – DataMatrix scatter plots (x = business phase, y = volume,
      each dot coloured by its mapped hex colour)
    * **Row 2** – ColorMatrix scatter plots (same positions, dot *size*
      encodes row count so the colour encoding is visually prominent)
    * **Row 3** – Business domain distribution bar charts

    A summary banner at the bottom shows cell counts and colour-category
    counts for both hypercubes.

    Args:
        hc_a: First :class:`~four_dim_matrix.hypercube.HyperCube`.
        hc_b: Second :class:`~four_dim_matrix.hypercube.HyperCube`.
        db_a_label: Display name for the first database.
        db_b_label: Display name for the second database.
        output_path: Destination path for the HTML file.

    Raises:
        ImportError: When ``plotly`` is not installed.
    """
    try:
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots
    except ImportError as exc:
        raise ImportError(
            "plotly is required for HTML export.  "
            "Install it with:  pip install plotly"
        ) from exc

    def _scatter(hc: HyperCube, label: str, scale_size: bool = False) -> go.Scatter:
        viz = hc.export_for_visualization()
        pts = viz["data_points"]
        if not pts:
            return go.Scatter(x=[], y=[], mode="markers", name=label)
        x_vals = [p["coordinates"]["x"] for p in pts]
        y_vals = [p["coordinates"]["y"] for p in pts]
        colors = [p["color"]["hex"] for p in pts]
        if scale_size:
            sizes = [max(8, min(30, p["data"]["row_count"] // 5 + 8)) for p in pts]
        else:
            sizes = [14] * len(pts)
        hover = [
            (
                f"<b>{p['data']['table_name']}</b><br>"
                f"域(z={p['coordinates']['z']}): {p['data']['business_domain']}<br>"
                f"阶段(x={p['coordinates']['x']}): {p['data']['lifecycle_stage']}<br>"
                f"量级(y={p['coordinates']['y']:.1f}): {p['data']['row_count']:,} 行<br>"
                f"颜色: {p['color']['hex']}"
            )
            for p in pts
        ]
        return go.Scatter(
            x=x_vals,
            y=y_vals,
            mode="markers",
            marker=dict(
                size=sizes,
                color=colors,
                line=dict(width=1, color="rgba(0,0,0,0.35)"),
            ),
            customdata=hover,
            hovertemplate="%{customdata}<extra></extra>",
            name=label,
        )

    def _bar(hc: HyperCube, label: str) -> go.Bar:
        viz = hc.export_for_visualization()
        counts: Dict[str, int] = {}
        for p in viz["data_points"]:
            d = p["data"]["business_domain"]
            counts[d] = counts.get(d, 0) + 1
        bar_colors = [
            f"hsl({(i * 60) % 360}, 65%, 55%)" for i in range(len(counts))
        ]
        return go.Bar(
            x=list(counts.keys()),
            y=list(counts.values()),
            name=label,
            marker_color=bar_colors,
        )

    fig = make_subplots(
        rows=3,
        cols=2,
        subplot_titles=[
            f"数据矩阵 (DataMatrix) – {db_a_label}",
            f"数据矩阵 (DataMatrix) – {db_b_label}",
            f"颜色矩阵 (ColorMatrix) – {db_a_label}",
            f"颜色矩阵 (ColorMatrix) – {db_b_label}",
            f"业务域分布 – {db_a_label}",
            f"业务域分布 – {db_b_label}",
        ],
        vertical_spacing=0.12,
        horizontal_spacing=0.08,
    )

    # Row 1: data matrices
    fig.add_trace(_scatter(hc_a, db_a_label), row=1, col=1)
    fig.add_trace(_scatter(hc_b, db_b_label), row=1, col=2)
    # Row 2: colour matrices (same coordinates, size encodes row count)
    fig.add_trace(_scatter(hc_a, db_a_label + " 颜色", scale_size=True), row=2, col=1)
    fig.add_trace(_scatter(hc_b, db_b_label + " 颜色", scale_size=True), row=2, col=2)
    # Row 3: domain bar charts
    fig.add_trace(_bar(hc_a, db_a_label), row=3, col=1)
    fig.add_trace(_bar(hc_b, db_b_label), row=3, col=2)

    for col in (1, 2):
        fig.update_xaxes(title_text="业务阶段 X (20=new · 50=growth · 80=mature · 110=legacy)", row=1, col=col)
        fig.update_yaxes(title_text="量级 Y (行数对数压缩, 1–255)", row=1, col=col)
        fig.update_xaxes(title_text="业务阶段 X", row=2, col=col)
        fig.update_yaxes(title_text="量级 Y", row=2, col=col)
        fig.update_xaxes(title_text="业务域 (Z轴分类)", row=3, col=col)
        fig.update_yaxes(title_text="表数量", row=3, col=col)

    # Summary banner
    def _summary(label: str, hc: HyperCube) -> str:
        s = hc.get_summary()
        dm = s.get("data_matrix", {})
        cm = s.get("color_matrix", {})
        if dm.get("empty"):
            return f"<b>{label}</b>: 空"
        return (
            f"<b>{label}</b>: "
            f"数据矩阵 {dm.get('total_cells', 0)} 个单元格 "
            f"({dm.get('unique_tables', 0)} 张表, "
            f"{dm.get('total_rows', 0):,} 行) | "
            f"颜色矩阵 {cm.get('total_cells', 0)} 个单元格 "
            f"({cm.get('color_categories', 0)} 种颜色)"
        )

    summary_text = _summary(db_a_label, hc_a) + "<br>" + _summary(db_b_label, hc_b)

    fig.update_layout(
        title={
            "text": (
                "四维矩阵双数据库可视化分析<br>"
                "<sub>每个数据库各生成 2 个四维矩阵："
                "数据矩阵 (DataMatrix) + 颜色矩阵 (ColorMatrix)</sub>"
            ),
            "x": 0.5,
            "xanchor": "center",
        },
        height=1200,
        showlegend=False,
        annotations=[
            dict(
                x=0.5,
                y=-0.04,
                xref="paper",
                yref="paper",
                text=summary_text,
                showarrow=False,
                font=dict(size=12),
                align="center",
            )
        ],
    )

    fig.write_html(output_path)
    print(f"[demo] 已保存可视化分析页面 → {output_path}")


# ---------------------------------------------------------------------------
# End-to-end demo runner
# ---------------------------------------------------------------------------

def run_demo(
    output_path: str = "dual_matrix_demo.html",
    db_a_path: Optional[str] = None,
    db_b_path: Optional[str] = None,
) -> Tuple[HyperCube, HyperCube]:
    """Run the full dual-matrix demo end-to-end.

    If *db_a_path* / *db_b_path* are not given, temporary sample databases
    are created and removed after the HyperCubes are built.

    Steps:

    1. Create (or open) two SQLite databases.
    2. Use :class:`~four_dim_matrix.DatabaseAdapter` to introspect each
       database schema.
    3. Call :func:`build_hypercube_from_adapter` to generate **2 four-
       dimensional matrices** (DataMatrix + ColorMatrix) for each database.
    4. Call :func:`export_dual_matrix_html` to write the interactive HTML
       analysis page.

    Args:
        output_path: Destination for the HTML file.
        db_a_path: Path to the first SQLite database.  ``None`` creates a
            temporary e-commerce sample database.
        db_b_path: Path to the second SQLite database.  ``None`` creates a
            temporary CRM sample database.

    Returns:
        ``(hc_ecommerce, hc_crm)`` – the two populated
        :class:`~four_dim_matrix.hypercube.HyperCube` instances.
    """
    tmpdir: Optional[str] = None
    created_a = False
    created_b = False

    try:
        if db_a_path is None:
            tmpdir = tempfile.mkdtemp(prefix="four_dim_demo_")
            db_a_path = os.path.join(tmpdir, "ecommerce_demo.db")
            _create_ecommerce_db(db_a_path)
            created_a = True
            print(f"[demo] 已创建电商示例数据库 → {db_a_path}")

        if db_b_path is None:
            if tmpdir is None:
                tmpdir = tempfile.mkdtemp(prefix="four_dim_demo_")
            db_b_path = os.path.join(tmpdir, "crm_demo.db")
            _create_crm_db(db_b_path)
            created_b = True
            print(f"[demo] 已创建 CRM 示例数据库   → {db_b_path}")

        # ---- Build HyperCube A (e-commerce) ----
        print("\n[demo] 正在扫描电商数据库，生成四维矩阵...")
        adapter_a = DatabaseAdapter.from_sqlite(db_a_path)
        hc_a = build_hypercube_from_adapter(adapter_a, "电商数据库")
        sum_a = hc_a.get_summary()
        dm_a = sum_a.get("data_matrix", {})
        cm_a = sum_a.get("color_matrix", {})
        print(
            f"  数据矩阵: {dm_a.get('total_cells', 0)} 单元格"
            f" ({dm_a.get('unique_tables', 0)} 张表, "
            f"{dm_a.get('total_rows', 0):,} 行)"
        )
        print(
            f"  颜色矩阵: {cm_a.get('total_cells', 0)} 单元格"
            f" ({cm_a.get('color_categories', 0)} 种颜色)"
        )

        # ---- Build HyperCube B (CRM) ----
        print("\n[demo] 正在扫描 CRM 数据库，生成四维矩阵...")
        adapter_b = DatabaseAdapter.from_sqlite(db_b_path)
        hc_b = build_hypercube_from_adapter(adapter_b, "CRM数据库")
        sum_b = hc_b.get_summary()
        dm_b = sum_b.get("data_matrix", {})
        cm_b = sum_b.get("color_matrix", {})
        print(
            f"  数据矩阵: {dm_b.get('total_cells', 0)} 单元格"
            f" ({dm_b.get('unique_tables', 0)} 张表, "
            f"{dm_b.get('total_rows', 0):,} 行)"
        )
        print(
            f"  颜色矩阵: {cm_b.get('total_cells', 0)} 单元格"
            f" ({cm_b.get('color_categories', 0)} 种颜色)"
        )

        # ---- Export HTML ----
        print(f"\n[demo] 正在生成可视化分析页面...")
        export_dual_matrix_html(
            hc_a, hc_b,
            db_a_label="电商数据库",
            db_b_label="CRM数据库",
            output_path=output_path,
        )

        return hc_a, hc_b

    finally:
        # Remove temporary SQLite files
        for flag, path in [(created_a, db_a_path), (created_b, db_b_path)]:
            if flag and path and os.path.exists(path):
                try:
                    os.unlink(path)
                except OSError:
                    pass
        if tmpdir and os.path.exists(tmpdir):
            try:
                os.rmdir(tmpdir)
            except OSError:
                pass


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------

def main(argv: Optional[List[str]] = None) -> None:
    """Run the dual-matrix demo from the command line."""
    parser = argparse.ArgumentParser(
        description=(
            "通过两个 SQLite 数据库生成 2 个四维矩阵并输出可视化分析 HTML 页面。\n\n"
            "每个数据库分别生成:\n"
            "  · DataMatrix  (数据矩阵) — 存储表的 (t,x,y,z) 坐标和元数据\n"
            "  · ColorMatrix (颜色矩阵) — 对应的 HSL 颜色编码矩阵"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--output", "-o",
        default="dual_matrix_demo.html",
        help="输出 HTML 文件路径 (默认: dual_matrix_demo.html)",
    )
    parser.add_argument(
        "--db-a",
        metavar="PATH",
        default=None,
        help="第一个 SQLite 数据库路径 (默认: 自动创建电商示例数据库)",
    )
    parser.add_argument(
        "--db-b",
        metavar="PATH",
        default=None,
        help="第二个 SQLite 数据库路径 (默认: 自动创建 CRM 示例数据库)",
    )
    args = parser.parse_args(argv)

    print("=" * 60)
    print("四维矩阵双数据库演示")
    print("=" * 60)
    run_demo(
        output_path=args.output,
        db_a_path=args.db_a,
        db_b_path=args.db_b,
    )
    print("\n请在浏览器中打开生成的 HTML 文件以查看可视化分析页面。")


if __name__ == "__main__":
    main()
