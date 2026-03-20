"""四维矩阵命令行工具 (four-dim-matrix CLI)

用法 / Usage
-----------
  four-dim-matrix demo                       # 运行双矩阵演示，生成 HTML
  four-dim-matrix scan --db sqlite \\
      --sqlite-path mydb.db -o outputs/      # 扫描 SQLite 数据库
  four-dim-matrix scan --db sqlite \\
      --sqlite-path mydb.db \\
      --spec tasks/mydb_spec.md              # 先加载设计说明书再扫描
  four-dim-matrix scan --db postgres \\
      --host localhost --user me --password x --database mydb
  four-dim-matrix visualize -i scan.json     # 从已有 JSON 启动可视化
  four-dim-matrix query -i scan.json -c '#3d6e9e'
  four-dim-matrix history                    # 查看最近扫描记录
  four-dim-matrix history --clear            # 清空历史
  four-dim-matrix task --file tasks/my.yaml  # 按任务文件执行
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from four_dim_matrix.hypercube import HyperCube
from four_dim_matrix.data_matrix import DataCell
from four_dim_matrix.lineage import LineageTracker, PhysicalLocation
from four_dim_matrix.dynamic_classifier import UnknownDatabaseProcessor
from four_dim_matrix.demo import build_hypercube_from_adapter
from four_dim_matrix.db_adapter import DatabaseAdapter
from four_dim_matrix.memory import MemoryStore
from four_dim_matrix.design_spec import DesignSpecParser, DesignSpec


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_spec(spec_path: Optional[str]) -> Optional[DesignSpec]:
    """Load a design spec from *spec_path*; return ``None`` if not provided.

    Prints a brief summary to stdout so the user knows the spec was picked up.
    """
    if not spec_path:
        return None
    spec = DesignSpecParser.parse_file(spec_path)
    if not spec:
        print(f"⚠️  设计说明书文件为空或无法解析: {spec_path}")
        return None
    print(f"\n📋 已加载数据库设计说明书: {spec_path}")
    print(f"   {spec.summary()}")
    return spec

def _get_connector(db_type: str, conn_params: dict):
    """Lazily import and return the appropriate database connector."""
    if db_type == "postgres":
        from four_dim_matrix.connectors.postgres import PostgresConnector
        return PostgresConnector(conn_params)
    from four_dim_matrix.connectors.mysql import MySQLConnector
    return MySQLConnector(conn_params)


def _start_dashboard(hypercube: HyperCube, port: int) -> None:
    """Lazily import Dash dashboard and start the server."""
    from four_dim_matrix.dashboard import create_hypercube_dashboard
    print(f"\n🌐 仪表盘启动中…请在浏览器打开 http://127.0.0.1:{port}")
    print("   按 Ctrl+C 停止服务\n")
    app = create_hypercube_dashboard(hypercube)
    app.run(debug=False, port=port)


def _resolve_output_path(output_arg: Optional[str], suffix: str = ".json") -> Optional[str]:
    """Return an absolute output file path, creating the directory if needed.

    *output_arg* may be:
    - ``None``                → return ``None`` (no export)
    - ``"./outputs/"``        → auto-generate filename inside the directory
    - ``"./outputs/my.json"`` → use as-is

    Returns the resolved file path string, or ``None``.
    """
    if output_arg is None:
        return None
    p = Path(output_arg)
    if str(output_arg).endswith(("/", os.sep)) or p.is_dir():
        p.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        return str(p / f"matrix_scan_{ts}{suffix}")
    p.parent.mkdir(parents=True, exist_ok=True)
    return str(p)


def _print_hypercube_summary(hypercube: HyperCube, label: str = "") -> None:
    """Print a human-readable dual-matrix summary."""
    s = hypercube.get_summary()
    dm = s.get("data_matrix", {})
    cm = s.get("color_matrix", {})
    tag = f"[{label}] " if label else ""
    print(f"\n✅ {tag}四维矩阵生成完成")
    print(f"   数据矩阵 (DataMatrix) : {dm.get('total_cells', 0)} 单元格"
          f"  ({dm.get('unique_tables', 0)} 张表, "
          f"{dm.get('total_rows', 0):,} 行)")
    print(f"   颜色矩阵 (ColorMatrix) : {cm.get('total_cells', 0)} 单元格"
          f"  ({cm.get('color_categories', 0)} 种颜色)")
    domains = dm.get("domains", [])
    if domains:
        print(f"   业务域 : {', '.join(str(d) for d in domains)}")


def _export_json(hypercube: HyperCube, path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(hypercube.export_for_visualization(), f,
                  ensure_ascii=False, indent=2)


def _record_session(
    store: MemoryStore, args: Any, hypercube: HyperCube, source: str,
    out_path: str = "",
) -> None:
    """Persist a scan session in the memory store.

    Parameters:
        store: The :class:`~four_dim_matrix.memory.MemoryStore` instance.
        args: Parsed CLI namespace (used for ``label`` and ``db`` attributes).
        hypercube: The produced :class:`~four_dim_matrix.HyperCube`.
        source: Database path or connection string.
        out_path: The **resolved** output file path that was actually written.
            Must be the same path that was passed to :func:`_export_json`; do
            not re-call :func:`_resolve_output_path` here because a second call
            would generate a *different* timestamped filename.
    """
    dm = hypercube.get_summary().get("data_matrix", {})
    cm = hypercube.get_summary().get("color_matrix", {})
    store.record_session(
        source=source,
        cell_count=dm.get("total_cells", 0),
        color_count=cm.get("total_cells", 0),
        label=getattr(args, "label", "") or source,
        output_file=out_path,
        extra={
            "db_type": getattr(args, "db", ""),
            "domains": dm.get("domains", []),
        },
    )


# ---------------------------------------------------------------------------
# scan
# ---------------------------------------------------------------------------

def scan_database(args) -> HyperCube:
    """扫描数据库并生成四维矩阵（支持 SQLite / PostgreSQL / MySQL）"""
    store = MemoryStore()

    # ----------------------------------------------------------------
    # Load optional design spec (prior knowledge)
    # ----------------------------------------------------------------
    spec = _load_spec(getattr(args, "spec", None))

    # ----------------------------------------------------------------
    # SQLite path – uses the lightweight DatabaseAdapter directly
    # ----------------------------------------------------------------
    if args.db == "sqlite":
        if not args.sqlite_path:
            print("❌ 错误: 使用 --db sqlite 时必须提供 --sqlite-path 参数")
            sys.exit(1)
        db_path = args.sqlite_path
        print(f"🔍 正在打开 SQLite 数据库: {db_path}")
        adapter = DatabaseAdapter.from_sqlite(db_path)
        print(f"   发现 {len(adapter.tables)} 张表")
        hypercube = build_hypercube_from_adapter(adapter, db_path, spec=spec)
        _print_hypercube_summary(hypercube)

        out_path = _resolve_output_path(getattr(args, "output", None))
        if out_path:
            _export_json(hypercube, out_path)
            print(f"📄 JSON 已导出至: {out_path}")

        _record_session(store, args, hypercube, db_path, out_path=out_path or "")

        if getattr(args, "visualize", False):
            _start_dashboard(hypercube, args.viz_port)

        return hypercube

    # ----------------------------------------------------------------
    # PostgreSQL / MySQL path – uses the native connectors
    # ----------------------------------------------------------------
    print(f"🔍 正在连接 {args.db} 数据库 ({args.host}/{args.database})…")

    conn_params = {
        "host": args.host,
        "port": args.db_port,
        "user": args.user,
        "password": args.password,
        "database": args.database,
    }

    if args.db in ("postgres", "mysql"):
        connector = _get_connector(args.db, conn_params)
    else:
        print(f"❌ 不支持的数据库类型: {args.db}")
        sys.exit(1)

    connector.connect()
    print("   连接成功，正在扫描表结构…")

    tables_metadata = connector.get_all_tables_metadata(getattr(args, "schema", None))
    print(f"   发现 {len(tables_metadata)} 张表")

    raw_metadata = []
    for meta in tables_metadata:
        raw_metadata.append({
            "table_name": meta.table_name,
            "schema_name": meta.schema_name,
            "columns": meta.columns,
            "indexes": [{"name": i.get("name", "")} for i in meta.indexes],
            "primary_key": meta.primary_key,
            "foreign_keys": [],
            "row_count": meta.row_count,
            "column_count": meta.column_count,
            "size_bytes": meta.size_bytes,
        })

    print("   正在动态发现业务域…")
    processor = UnknownDatabaseProcessor()
    result = processor.process(raw_metadata, spec=spec)

    stats = result["stats"]
    print(f"\n📊 动态分析结果:")
    print(f"   主题域  : {stats['domain_count']} 个")
    print(f"   总表数  : {stats['total_tables']}")
    print(f"   总列数  : {stats['total_columns']}")
    print(f"   总数据量: {stats['total_rows']:,} 行")
    if stats.get("relationships", 0):
        print(f"   表关联  : {stats['relationships']} 个")

    print("\n🗂️  发现的业务域:")
    for z_id, domain_info in result["domains"].items():
        print(f"   Z={z_id}: {domain_info['name']}"
              f"  ({domain_info['table_count']} 张表)")

    hypercube = HyperCube()
    lineage = LineageTracker()
    stage_to_x = {"new": 20, "growth": 50, "mature": 80, "legacy": 110}

    # Compute max_rows once before the loop (O(n)) instead of inside it (O(n²))
    max_rows = max(
        (s.row_count for s in result["signatures"]), default=1
    ) or 1

    for sig in result["signatures"]:
        table_name = sig.table_name
        z = result["domain_mapping"].get(table_name, 0)
        stage = result["lifecycle_mapping"].get(table_name, "mature")
        x = stage_to_x.get(stage, 50)
        y = 0
        if sig.row_count > 0:
            y = min(255, max(1, int(sig.row_count / max(max_rows / 255, 1))))

        loc = PhysicalLocation(
            db_type=args.db,
            host=args.host,
            port=args.db_port,
            database=args.database,
            schema=sig.schema_name,
            table=table_name,
            snapshot_at=datetime.now(),
        )
        cell_id = f"scan_{table_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        prov = lineage.register_first_stage(cell_id, loc)
        domain_name = result["domains"].get(z, {}).get("name", "unknown")

        cell = DataCell(
            t=datetime.now(),
            x=x, y=y, z=z,
            table_name=table_name,
            schema_name=sig.schema_name,
            column_count=sig.column_count,
            row_count=sig.row_count,
            size_bytes=sig.row_count * 100,
            business_domain=domain_name,
            lifecycle_stage=stage,
            tags=list(sig.name_tokens),
            payload={
                "columns": [{"name": c} for c in sig.column_names],
                "indexes": sig.indexes,
                "primary_key": sig.primary_key,
                "foreign_keys": sig.foreign_keys,
                "has_timestamp": sig.has_timestamp,
                "has_soft_delete": sig.has_soft_delete,
            },
            provenance=prov,
        )
        hypercube.add_cell(cell, compute_color=True)

    hypercube.sync_color_matrix()
    _print_hypercube_summary(hypercube)

    out_path = _resolve_output_path(getattr(args, "output", None))
    if out_path:
        _export_json(hypercube, out_path)
        print(f"📄 JSON 已导出至: {out_path}")

    _record_session(store, args, hypercube, args.database, out_path=out_path or "")

    if getattr(args, "visualize", False):
        _start_dashboard(hypercube, args.viz_port)

    return hypercube


# ---------------------------------------------------------------------------
# demo
# ---------------------------------------------------------------------------

def run_demo_command(args) -> None:
    """运行双矩阵演示（自动创建示例数据库并输出 HTML）"""
    from four_dim_matrix.demo import run_demo

    out_dir = Path(getattr(args, "output_dir", "./outputs"))
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    html_path = str(out_dir / f"dual_matrix_demo_{ts}.html")

    print("=" * 60)
    print("🚀 四维矩阵双数据库演示")
    print("=" * 60)
    print("   将自动创建两个示例 SQLite 数据库（电商 + CRM），")
    print("   各自生成一个包含数据矩阵和颜色矩阵的四维超立方体，")
    print("   并合并输出到一个交互式 HTML 分析页面。\n")

    hc_a, hc_b = run_demo(output_path=html_path)
    _print_hypercube_summary(hc_a, "电商数据库")
    _print_hypercube_summary(hc_b, "CRM 数据库")

    print(f"\n📊 可视化页面已生成: {html_path}")
    print("   请在浏览器中打开该文件查看交互式四维矩阵分析。")

    store = MemoryStore()
    store.record_session(
        source="demo (ecommerce + crm)",
        cell_count=len(hc_a.data_matrix.cells) + len(hc_b.data_matrix.cells),
        color_count=len(hc_a.color_matrix.cells) + len(hc_b.color_matrix.cells),
        label="演示模式",
        output_file=html_path,
        notes="自动创建的电商+CRM示例数据库",
    )


# ---------------------------------------------------------------------------
# visualize
# ---------------------------------------------------------------------------

def visualize_data(args) -> None:
    """从已保存的 JSON 文件加载并启动可视化仪表盘"""
    in_path = args.input
    print(f"📂 正在加载数据文件: {in_path}")
    try:
        with open(in_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"❌ 无法读取文件: {exc}")
        sys.exit(1)

    n = len(data.get("data_points", []))
    print(f"   数据点数量: {n}")
    if n == 0:
        print("⚠️  文件中没有数据点，仪表盘将显示空视图。")

    # Reconstruct HyperCube from the loaded JSON so the dashboard shows real data
    hypercube = HyperCube.from_visualization_dict(data)
    _print_hypercube_summary(hypercube)
    _start_dashboard(hypercube, args.port)


# ---------------------------------------------------------------------------
# query
# ---------------------------------------------------------------------------

def query_by_color(args) -> None:
    """通过颜色相似度反查数据库表"""
    try:
        with open(args.input, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"❌ 无法读取文件: {exc}")
        sys.exit(1)

    target_color = args.color.lower()
    threshold = args.threshold
    print(f"🎨 查询颜色: {target_color}  (相似阈值: {threshold})")

    matching = []
    for point in data.get("data_points", []):
        point_color = point["color"]["hex"].lower()
        distance = _hex_color_distance(target_color, point_color)
        if distance < threshold:
            matching.append({
                "table": point["data"]["table_name"],
                "color": point_color,
                "distance": distance,
                "domain": point["data"].get("business_domain", "—"),
            })

    matching.sort(key=lambda x: x["distance"])
    print(f"\n找到 {len(matching)} 个匹配结果:")
    for m in matching[:20]:
        bar = "█" * max(1, int(20 * (1 - m["distance"] / threshold)))
        print(f"  {m['color']}  {bar:<20}  {m['table']:<30}  域:{m['domain']}"
              f"  距离:{m['distance']:.1f}")
    if len(matching) > 20:
        print(f"  … 还有 {len(matching) - 20} 条结果（仅显示前 20 条）")


def _hex_color_distance(c1: str, c2: str) -> float:
    """Euclidean RGB distance between two hex colour strings."""
    c1, c2 = c1.lstrip("#"), c2.lstrip("#")
    try:
        r1, g1, b1 = int(c1[0:2], 16), int(c1[2:4], 16), int(c1[4:6], 16)
        r2, g2, b2 = int(c2[0:2], 16), int(c2[2:4], 16), int(c2[4:6], 16)
    except (ValueError, IndexError):
        return float("inf")
    return ((r1 - r2) ** 2 + (g1 - g2) ** 2 + (b1 - b2) ** 2) ** 0.5


# ---------------------------------------------------------------------------
# history
# ---------------------------------------------------------------------------

def show_history(args) -> None:
    """显示最近的扫描会话记录"""
    store = MemoryStore()

    if getattr(args, "clear", False):
        store.clear_sessions()
        print("✅ 历史记录已清空。")
        return

    n = getattr(args, "n", 10)
    sessions = store.recent_sessions(n)

    if not sessions:
        print("📭 暂无历史记录。运行 'four-dim-matrix scan' 或 'four-dim-matrix demo' 后将自动记录。")
        return

    print(f"📋 最近 {len(sessions)} 条扫描记录 (最新在前):\n")
    for i, rec in enumerate(sessions, 1):
        ts = rec.timestamp[:19].replace("T", " ")
        label = rec.label or rec.source
        out = f"  → {rec.output_file}" if rec.output_file else ""
        print(f"  {i:2}. [{ts}]  {label}")
        print(f"       数据矩阵: {rec.cell_count} 单元格  "
              f"颜色矩阵: {rec.color_count} 单元格{out}")

    summary = store.summary()
    print(f"\n共 {summary['session_count']} 条记录，"
          f"存储于: {summary['store_path']}")


# ---------------------------------------------------------------------------
# task
# ---------------------------------------------------------------------------

def run_task(args) -> None:
    """执行任务文件 (YAML) 中定义的扫描任务"""
    task_file = args.file
    try:
        import yaml  # type: ignore[import]
        _yaml_load = yaml.safe_load
    except ImportError:
        _yaml_load = None  # type: ignore[assignment]

    print(f"📋 正在读取任务文件: {task_file}")
    try:
        with open(task_file, "r", encoding="utf-8") as f:
            raw = f.read()
    except OSError as exc:
        print(f"❌ 无法读取任务文件: {exc}")
        sys.exit(1)

    if _yaml_load is not None:
        cfg: Dict[str, Any] = _yaml_load(raw) or {}
    else:
        cfg = _minimal_yaml_parse(raw)

    db_cfg = cfg.get("database") or cfg.get("db") or {}
    out_cfg = cfg.get("output") or {}
    viz_cfg = cfg.get("visualization") or {}
    mem_cfg = cfg.get("memory") or {}
    task_meta = cfg.get("task") or {}
    spec_cfg = cfg.get("spec") or {}

    db_type = db_cfg.get("type", "sqlite")
    label = mem_cfg.get("label") or task_meta.get("name", "")

    # Build a namespace that scan_database() understands
    class _Args:
        pass

    a = _Args()
    a.db = db_type
    a.sqlite_path = db_cfg.get("sqlite_path", "") or ""
    a.host = db_cfg.get("host", "localhost")
    a.db_port = db_cfg.get("port")
    a.user = db_cfg.get("user", "")
    a.password = (
        db_cfg.get("password")
        or os.environ.get("FOUR_DIM_DB_PASSWORD", "")
    )
    a.database = db_cfg.get("database", "") or ""
    a.schema = db_cfg.get("schema")

    # Design spec: resolve relative paths against the task file's directory
    spec_file = spec_cfg.get("file") or spec_cfg.get("path") or ""
    if spec_file:
        task_dir = Path(task_file).parent
        spec_file = str(task_dir / spec_file) if not Path(spec_file).is_absolute() else spec_file
    a.spec = spec_file or None

    out_dir = out_cfg.get("dir", "./outputs")
    a.output = out_dir.rstrip("/") + "/"  # trigger directory resolution
    a.visualize = bool(viz_cfg.get("serve", False))
    a.viz_port = int(viz_cfg.get("port", 8050))
    a.label = label

    print(f"   任务名称 : {task_meta.get('name', '—')}")
    print(f"   数据库   : {db_type}  ({a.sqlite_path or a.database})")
    print(f"   输出目录 : {out_dir}")
    if spec_file:
        print(f"   设计说明 : {spec_file}")

    scan_database(a)


def _minimal_yaml_parse(text: str) -> Dict[str, Any]:
    """Parse a very simple flat/2-level YAML without PyYAML dependency."""
    result: Dict[str, Any] = {}
    current_key: Optional[str] = None
    current_section: Optional[Dict[str, Any]] = None
    for line in text.splitlines():
        stripped = line.rstrip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith("  ") or stripped.startswith("\t"):
            if current_key and current_section is not None:
                inner = stripped.strip()
                if ": " in inner:
                    k, v = inner.split(": ", 1)
                    current_section[k.strip()] = _yaml_value(v.strip())
        else:
            if ": " in stripped:
                k, v = stripped.split(": ", 1)
                v = v.strip()
                if v == "":
                    current_key = k.strip()
                    current_section = {}
                    result[current_key] = current_section
                else:
                    result[k.strip()] = _yaml_value(v)
                    current_key = k.strip()
                    current_section = None
            elif stripped.endswith(":"):
                current_key = stripped[:-1].strip()
                current_section = {}
                result[current_key] = current_section
    return result


def _yaml_value(v: str) -> Any:
    """Convert a simple YAML scalar string to a Python value."""
    if v in ("true", "True", "yes"):
        return True
    if v in ("false", "False", "no"):
        return False
    if v in ("null", "~", "None", ""):
        return None
    try:
        return int(v)
    except ValueError:
        pass
    try:
        return float(v)
    except ValueError:
        pass
    return v.strip('"\'')


# ---------------------------------------------------------------------------
# argument parser
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        prog="four-dim-matrix",
        description="四维矩阵数据库可视化工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "示例 / Examples:\n"
            "  four-dim-matrix demo\n"
            "  four-dim-matrix scan --db sqlite --sqlite-path mydb.db -o outputs/\n"
            "  four-dim-matrix scan --db postgres --host localhost \\\n"
            "      --user postgres --password s3cr3t --database prod\n"
            "  four-dim-matrix history\n"
            "  four-dim-matrix task --file tasks/my_scan.yaml\n"
        ),
    )
    subparsers = parser.add_subparsers(dest="command", help="可用命令")

    # ── demo ──────────────────────────────────────────────────────────
    demo_parser = subparsers.add_parser(
        "demo",
        help="运行双矩阵演示 (自动创建示例数据库并生成 HTML)",
    )
    demo_parser.add_argument(
        "--output-dir", default="./outputs",
        help="HTML 输出目录 (默认: ./outputs)",
    )

    # ── scan ──────────────────────────────────────────────────────────
    scan_parser = subparsers.add_parser(
        "scan",
        help="扫描数据库并生成四维矩阵",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=(
            "扫描 SQLite / PostgreSQL / MySQL 数据库，\n"
            "自动构建数据矩阵 (DataMatrix) 和颜色矩阵 (ColorMatrix)。"
        ),
    )
    scan_parser.add_argument(
        "--db", choices=["postgres", "mysql", "sqlite"], required=True,
        help="数据库类型",
    )
    scan_parser.add_argument(
        "--sqlite-path", metavar="FILE",
        help="SQLite 数据库文件路径 (仅 --db sqlite 时使用)",
    )
    scan_parser.add_argument("--host", default="localhost", help="主机地址")
    scan_parser.add_argument(
        "--db-port", type=int, dest="db_port",
        help="数据库端口 (postgres 默认 5432, mysql 默认 3306)",
    )
    scan_parser.add_argument("--user", help="数据库用户名")
    scan_parser.add_argument("--password", default="", help="数据库密码")
    scan_parser.add_argument("--database", help="数据库名")
    scan_parser.add_argument("--schema", help="Schema 名称")
    scan_parser.add_argument(
        "--spec", metavar="FILE",
        help=(
            "数据库设计说明书文件路径 (.md / .yaml / .txt)。"
            "扫描前先解析该文件，将其中描述的业务域和生命周期注入到矩阵生成过程中，"
            "提升分类准确性。(参见 tasks/example_spec.md)"
        ),
    )
    scan_parser.add_argument(
        "--output", "-o",
        help="输出路径：文件名 (如 scan.json) 或目录 (如 ./outputs/)",
    )
    scan_parser.add_argument(
        "--label", default="",
        help="保存到历史记录的可读名称",
    )
    scan_parser.add_argument(
        "--visualize", "-v", action="store_true",
        help="扫描完成后启动可视化仪表盘",
    )
    scan_parser.add_argument(
        "--viz-port", dest="viz_port", type=int, default=8050,
        help="可视化服务端口 (默认: 8050)",
    )

    # ── visualize ─────────────────────────────────────────────────────
    viz_parser = subparsers.add_parser(
        "visualize",
        help="从已保存的 JSON 文件加载并启动可视化仪表盘",
    )
    viz_parser.add_argument("--input", "-i", required=True, help="JSON 文件路径")
    viz_parser.add_argument(
        "--port", type=int, default=8050, help="服务端口 (默认: 8050)",
    )

    # ── query ─────────────────────────────────────────────────────────
    query_parser = subparsers.add_parser(
        "query",
        help="通过颜色相似度反查数据库表",
    )
    query_parser.add_argument("--input", "-i", required=True, help="JSON 文件路径")
    query_parser.add_argument(
        "--color", "-c", required=True, help="目标颜色 (如 #FF5733)",
    )
    query_parser.add_argument(
        "--threshold", "-t", type=float, default=50,
        help="颜色相似度阈值，越小越严格 (默认: 50)",
    )

    # ── history ───────────────────────────────────────────────────────
    hist_parser = subparsers.add_parser(
        "history",
        help="查看 / 清空最近的扫描历史记录",
    )
    hist_parser.add_argument(
        "-n", type=int, default=10,
        help="显示最近 N 条记录 (默认: 10)",
    )
    hist_parser.add_argument(
        "--clear", action="store_true",
        help="清空所有历史记录",
    )

    # ── task ──────────────────────────────────────────────────────────
    task_parser = subparsers.add_parser(
        "task",
        help="按 YAML 任务文件执行扫描任务",
    )
    task_parser.add_argument(
        "--file", "-f", required=True,
        help="任务定义文件路径 (参见 tasks/example_task.yaml)",
    )

    # ── dispatch ──────────────────────────────────────────────────────
    args = parser.parse_args()

    if args.command == "demo":
        run_demo_command(args)
    elif args.command == "scan":
        scan_database(args)
    elif args.command == "visualize":
        visualize_data(args)
    elif args.command == "query":
        query_by_color(args)
    elif args.command == "history":
        show_history(args)
    elif args.command == "task":
        run_task(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
