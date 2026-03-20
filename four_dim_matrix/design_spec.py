"""数据库设计说明书解析器 (DesignSpecParser)

在访问真实数据库之前，先解析一份文本形式的数据库设计文档，从中提取出
表名、业务域、生命周期、列名、外键关系等先验知识。这份先验知识随后被
注入到动态分类器 (:class:`~four_dim_matrix.dynamic_classifier.UnknownDatabaseProcessor`)
中，让矩阵生成过程"先知先觉"，显著提升分类准确性。

支持三种输入格式
---------------
1. **Markdown** (``.md``) — 最自然的设计说明书格式，适合人机共读::

       ## tables

       ### users
       - description: 存储用户账号信息
       - domain: user
       - lifecycle: mature
       - columns: id, email, name, created_at
       - tags: core, auth

2. **YAML** (``.yaml`` / ``.yml``) — 机器友好，适合 CI/CD 流程::

       database:
         name: ecommerce
         description: 电商平台数据库
       tables:
         users:
           description: 用户账号
           domain: user
           lifecycle: mature
           columns: [id, email, name]

3. **Plain text** (``.txt`` 或其他) — 宽松格式，每行 ``key: value``，
   ``[table_name]`` 小节头::

       [users]
       domain: user
       lifecycle: mature
       columns: id, email, name

快速上手
--------
::

    from four_dim_matrix.design_spec import DesignSpecParser, DesignSpec

    spec = DesignSpecParser.parse_file("tasks/mydb_spec.md")
    print(spec.database_name)            # "ecommerce"
    print(spec.tables["users"].domain)   # "user"
    print(spec.tables["users"].columns)  # ["id", "email", "name", ...]

    # 与 UnknownDatabaseProcessor 集成
    from four_dim_matrix.dynamic_classifier import UnknownDatabaseProcessor
    proc = UnknownDatabaseProcessor()
    result = proc.process(raw_metadata, spec=spec)
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class TableSpec:
    """先验知识：单张表的设计规格说明。

    Attributes:
        name: 表名（小写，与数据库实际表名对应）。
        description: 对该表用途的文字描述。
        domain: 业务域名称（如 ``"user"``、``"revenue"``、``"product"``）。
            当提供给 :class:`~four_dim_matrix.dynamic_classifier.UnknownDatabaseProcessor`
            时，该值将覆盖纯启发式算法的结果。
        lifecycle: 生命周期阶段
            (``"new"`` / ``"growth"`` / ``"mature"`` / ``"legacy"``）。
        columns: 已知列名列表；若规格中未列出则为空列表。
        tags: 任意标签，用于颜色映射和搜索。
        foreign_keys: 外键描述列表，每项为
            ``{"column": "...", "ref_table": "...", "ref_column": "..."}``.
        notes: 附加备注，不参与分类计算。
    """

    name: str
    description: str = ""
    domain: str = ""
    lifecycle: str = ""
    columns: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    foreign_keys: List[Dict[str, str]] = field(default_factory=list)
    notes: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "domain": self.domain,
            "lifecycle": self.lifecycle,
            "columns": self.columns,
            "tags": self.tags,
            "foreign_keys": self.foreign_keys,
            "notes": self.notes,
        }


@dataclass
class DesignSpec:
    """数据库设计说明书的结构化表示。

    一个 :class:`DesignSpec` 对象封装了整个数据库的先验知识：

    * ``tables`` — 各表的 :class:`TableSpec` 字典（键为小写表名）。
    * ``database_name`` — 数据库的可读名称（可选）。
    * ``description`` — 数据库用途概述（可选）。
    * ``raw_text`` — 原始文本，便于调试或日志输出。

    通常通过 :class:`DesignSpecParser` 构造，而非直接实例化。
    """

    tables: Dict[str, TableSpec] = field(default_factory=dict)
    database_name: str = ""
    description: str = ""
    raw_text: str = ""

    # -----------------------------------------------------------------------
    # Accessors
    # -----------------------------------------------------------------------

    def get_table(self, table_name: str) -> Optional[TableSpec]:
        """返回 *table_name* 对应的 :class:`TableSpec`，不区分大小写。

        若规格中没有该表的描述则返回 ``None``。
        """
        return self.tables.get(table_name.lower())

    def apply_to_metadata(self, raw_metadata: List[Dict[str, Any]]) -> None:
        """将设计规格中的先验知识写入 *raw_metadata* 列表（原地修改）。

        对于规格中描述过的表，将 ``domain``、``lifecycle``、``description``
        字段注入到元数据字典中，供 :class:`~four_dim_matrix.dynamic_classifier.UnknownDatabaseProcessor`
        使用。未在规格中描述的表不受影响。

        Parameters:
            raw_metadata: 从数据库扫描得到的原始元数据列表，每项至少含
                ``"table_name"`` 键。方法会在原字典上添加 ``"spec_domain"``,
                ``"spec_lifecycle"``, ``"spec_description"`` 键。
        """
        for meta in raw_metadata:
            name = meta.get("table_name", "")
            ts = self.get_table(name)
            if ts is None:
                continue
            if ts.domain:
                meta["spec_domain"] = ts.domain
            if ts.lifecycle:
                meta["spec_lifecycle"] = ts.lifecycle
            if ts.description:
                meta["spec_description"] = ts.description
            if ts.columns and not meta.get("columns"):
                meta["columns"] = [{"name": c} for c in ts.columns]
            if ts.foreign_keys and not meta.get("foreign_keys"):
                meta["foreign_keys"] = ts.foreign_keys

    def summary(self) -> str:
        """返回设计规格的简短摘要字符串（用于 CLI 打印）。"""
        parts = []
        if self.database_name:
            parts.append(f"数据库: {self.database_name}")
        if self.description:
            parts.append(f"说明: {self.description}")
        parts.append(f"已描述的表: {len(self.tables)} 张")
        with_domain = sum(1 for t in self.tables.values() if t.domain)
        with_lifecycle = sum(1 for t in self.tables.values() if t.lifecycle)
        if with_domain:
            parts.append(f"  - 含业务域注解: {with_domain} 张")
        if with_lifecycle:
            parts.append(f"  - 含生命周期注解: {with_lifecycle} 张")
        return "\n".join(parts)

    def __len__(self) -> int:
        return len(self.tables)

    def __bool__(self) -> bool:
        return bool(self.tables)


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------

class DesignSpecParser:
    """将文本格式的数据库设计说明书解析为 :class:`DesignSpec` 对象。

    支持三种格式：Markdown、YAML 和纯文本。格式通过文件扩展名自动检测，
    也可通过 ``fmt`` 参数显式指定。

    所有方法均为类方法，无需实例化：

    ::

        spec = DesignSpecParser.parse_file("tasks/mydb_spec.md")
        spec = DesignSpecParser.parse_text(some_markdown_string, fmt="markdown")
        spec = DesignSpecParser.parse_text(some_yaml_string, fmt="yaml")
    """

    # -----------------------------------------------------------------------
    # Public entry points
    # -----------------------------------------------------------------------

    @classmethod
    def parse_file(cls, path: str | Path) -> DesignSpec:
        """从文件路径解析设计说明书。

        Parameters:
            path: 文件路径（``.md``/``.yaml``/``.yml``/``.txt``）。

        Returns:
            解析完成的 :class:`DesignSpec` 对象。若文件不存在或无法读取，
            返回空的 :class:`DesignSpec`。

        Raises:
            无论任何 I/O 或解析错误均静默返回空 :class:`DesignSpec`，
            不向调用方抛出异常（遵循 graceful-degradation 原则）。
        """
        p = Path(path)
        try:
            text = p.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            return DesignSpec()

        suffix = p.suffix.lower()
        if suffix in (".yaml", ".yml"):
            fmt = "yaml"
        elif suffix == ".md":
            fmt = "markdown"
        else:
            fmt = "text"

        return cls.parse_text(text, fmt=fmt)

    @classmethod
    def parse_text(cls, text: str, fmt: str = "auto") -> DesignSpec:
        """从字符串解析设计说明书。

        Parameters:
            text: 设计说明书文本内容。
            fmt: 格式提示 — ``"markdown"``、``"yaml"``、``"text"``，
                或 ``"auto"``（自动检测）。

        Returns:
            :class:`DesignSpec` 对象。
        """
        text = text or ""
        if fmt == "auto":
            fmt = cls._detect_format(text)

        try:
            if fmt == "yaml":
                return cls._parse_yaml(text)
            if fmt == "markdown":
                return cls._parse_markdown(text)
            return cls._parse_text(text)
        except Exception:  # noqa: BLE001  – graceful degradation
            return DesignSpec(raw_text=text)

    # -----------------------------------------------------------------------
    # Format detection
    # -----------------------------------------------------------------------

    @staticmethod
    def _detect_format(text: str) -> str:
        """Heuristically detect the spec format from content."""
        stripped = text.lstrip()
        # Plain text uses [section] headers — check before YAML so that
        # "database: value" lines in plain-text files aren't mis-classified.
        if re.search(r"^\[[\w]+\]", text, re.MULTILINE):
            return "text"
        # YAML starts with a mapping key at column 0 or explicit YAML markers
        if stripped.startswith("---") or re.match(r"^[a-z_]+\s*:", stripped):
            return "yaml"
        # Markdown has heading markers
        if re.search(r"^#{1,6}\s+", text, re.MULTILINE):
            return "markdown"
        return "text"

    # -----------------------------------------------------------------------
    # Markdown parser
    # -----------------------------------------------------------------------

    @classmethod
    def _parse_markdown(cls, text: str) -> DesignSpec:
        """Parse a Markdown design spec.

        Expected layout::

            # Optional top-level heading (becomes database_name)
            description: Optional single-line description

            ## tables  ← section header (case-insensitive)

            ### table_name
            - description: What this table does
            - domain: user
            - lifecycle: mature
            - columns: id, email, name, created_at
            - tags: core, auth
            - notes: Free-form notes

            ### another_table
            ...
        """
        spec = DesignSpec(raw_text=text)
        lines = text.splitlines()
        idx = 0

        # ── top-level metadata ──────────────────────────────────────────
        while idx < len(lines):
            line = lines[idx].strip()
            if line.startswith("# "):
                spec.database_name = line[2:].strip()
                idx += 1
                continue
            # Bare "description: ..." before any section
            m = re.match(r"^description\s*:\s*(.+)$", line, re.IGNORECASE)
            if m and not spec.description:
                spec.description = m.group(1).strip()
            # Stop at the first h2-level section header
            if line.startswith("## "):
                break
            idx += 1

        # ── locate "tables" section ─────────────────────────────────────
        tables_start = None
        for i, line in enumerate(lines):
            if re.match(r"^#{2}\s+tables?\s*$", line.strip(), re.IGNORECASE):
                tables_start = i + 1
                break

        if tables_start is None:
            # Fall back: treat any h3 block as a table entry
            tables_start = 0

        # ── parse h3 blocks as table entries ────────────────────────────
        current_table: Optional[str] = None
        kv_buf: Dict[str, str] = {}

        def _flush(name: str, kv: Dict[str, str]) -> None:
            ts = cls._kv_to_table_spec(name, kv)
            spec.tables[name.lower()] = ts

        for line in lines[tables_start:]:
            stripped = line.strip()
            # h3 heading → new table
            m3 = re.match(r"^#{3}\s+(\S+.*)", stripped)
            if m3:
                if current_table:
                    _flush(current_table, kv_buf)
                current_table = m3.group(1).strip().split()[0]  # first word only
                kv_buf = {}
                continue
            if current_table is None:
                continue
            # Bullet or bare "key: value" lines
            kv_line = re.match(r"^[-*]\s+(.+)$", stripped)
            raw_line = stripped if not kv_line else None
            content = kv_line.group(1) if kv_line else stripped
            m_kv = re.match(r"^(\w[\w\s]*?)\s*:\s*(.*)$", content)
            if m_kv:
                kv_buf[m_kv.group(1).strip().lower()] = m_kv.group(2).strip()

        if current_table:
            _flush(current_table, kv_buf)

        return spec

    # -----------------------------------------------------------------------
    # YAML parser
    # -----------------------------------------------------------------------

    @classmethod
    def _parse_yaml(cls, text: str) -> DesignSpec:
        """Parse a YAML design spec.

        Expected layout::

            database:
              name: ecommerce
              description: E-commerce platform

            tables:
              users:
                description: Customer accounts
                domain: user
                lifecycle: mature
                columns: [id, email, name, created_at]
                tags: [core, auth]
                foreign_keys:
                  - column: company_id
                    ref_table: companies
                    ref_column: id

        PyYAML is used when available; falls back to the project's own
        minimal YAML parser when it is not installed.
        """
        spec = DesignSpec(raw_text=text)

        # Try PyYAML first
        data: Dict[str, Any] = {}
        try:
            import yaml  # type: ignore[import]
            data = yaml.safe_load(text) or {}
        except ImportError:
            data = cls._minimal_yaml_parse(text)
        except Exception:
            return spec

        if not isinstance(data, dict):
            return spec

        # database metadata
        db_section = data.get("database") or {}
        if isinstance(db_section, dict):
            spec.database_name = str(db_section.get("name") or "")
            spec.description = str(db_section.get("description") or "")

        # tables section
        tables_raw = data.get("tables") or {}
        if not isinstance(tables_raw, dict):
            return spec

        for tname, tdata in tables_raw.items():
            if not isinstance(tdata, dict):
                tdata = {}
            ts = cls._yaml_entry_to_table_spec(str(tname), tdata)
            spec.tables[str(tname).lower()] = ts

        return spec

    @classmethod
    def _yaml_entry_to_table_spec(cls, name: str, data: Dict[str, Any]) -> TableSpec:
        cols_raw = data.get("columns") or []
        cols: List[str] = (
            [c.strip() for c in cols_raw.split(",") if c.strip()]
            if isinstance(cols_raw, str)
            else [str(c).strip() for c in cols_raw if c]
        )
        tags_raw = data.get("tags") or []
        tags: List[str] = (
            [t.strip() for t in tags_raw.split(",") if t.strip()]
            if isinstance(tags_raw, str)
            else [str(t).strip() for t in tags_raw if t]
        )
        fks_raw = data.get("foreign_keys") or []
        fks: List[Dict[str, str]] = []
        if isinstance(fks_raw, list):
            for fk in fks_raw:
                if isinstance(fk, dict):
                    fks.append({
                        "column": str(fk.get("column") or ""),
                        "ref_table": str(fk.get("ref_table") or ""),
                        "ref_column": str(fk.get("ref_column") or ""),
                    })
        return TableSpec(
            name=name,
            description=str(data.get("description") or ""),
            domain=str(data.get("domain") or ""),
            lifecycle=str(data.get("lifecycle") or ""),
            columns=cols,
            tags=tags,
            foreign_keys=fks,
            notes=str(data.get("notes") or ""),
        )

    # -----------------------------------------------------------------------
    # Plain-text parser
    # -----------------------------------------------------------------------

    @classmethod
    def _parse_text(cls, text: str) -> DesignSpec:
        """Parse a plain-text design spec.

        Format::

            database: ecommerce
            description: E-commerce platform

            [users]
            description: Customer accounts
            domain: user
            lifecycle: mature
            columns: id, email, name, created_at
            tags: core, auth

            [orders]
            domain: revenue
            lifecycle: growth
        """
        spec = DesignSpec(raw_text=text)
        current_table: Optional[str] = None
        kv_buf: Dict[str, str] = {}

        def _flush(name: str, kv: Dict[str, str]) -> None:
            ts = cls._kv_to_table_spec(name, kv)
            spec.tables[name.lower()] = ts

        for line in text.splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            # Section header: [table_name]
            m_sec = re.match(r"^\[(\w+)\]$", stripped)
            if m_sec:
                if current_table:
                    _flush(current_table, kv_buf)
                current_table = m_sec.group(1)
                kv_buf = {}
                continue
            # key: value
            m_kv = re.match(r"^(\w[\w\s]*?)\s*:\s*(.*)$", stripped)
            if not m_kv:
                continue
            key = m_kv.group(1).strip().lower()
            val = m_kv.group(2).strip()
            if current_table is None:
                # top-level metadata
                if key == "database":
                    spec.database_name = val
                elif key == "description":
                    spec.description = val
            else:
                kv_buf[key] = val

        if current_table:
            _flush(current_table, kv_buf)

        return spec

    # -----------------------------------------------------------------------
    # Shared helpers
    # -----------------------------------------------------------------------

    @staticmethod
    def _kv_to_table_spec(name: str, kv: Dict[str, str]) -> TableSpec:
        """Build a :class:`TableSpec` from a flat key→value dict."""
        cols_raw = kv.get("columns", "")
        cols = [c.strip() for c in cols_raw.split(",") if c.strip()]

        tags_raw = kv.get("tags", "")
        tags = [t.strip() for t in tags_raw.split(",") if t.strip()]

        # Parse inline foreign-key shorthand: "column_name -> ref_table.ref_col"
        fks: List[Dict[str, str]] = []
        fk_raw = kv.get("foreign_keys", kv.get("foreign key", ""))
        for part in re.split(r"[;|]", fk_raw):
            part = part.strip()
            m = re.match(r"(\w+)\s*->\s*(\w+)\.(\w+)", part)
            if m:
                fks.append({
                    "column": m.group(1),
                    "ref_table": m.group(2),
                    "ref_column": m.group(3),
                })

        return TableSpec(
            name=name,
            description=kv.get("description", ""),
            domain=kv.get("domain", ""),
            lifecycle=kv.get("lifecycle", ""),
            columns=cols,
            tags=tags,
            foreign_keys=fks,
            notes=kv.get("notes", ""),
        )

    @staticmethod
    def _minimal_yaml_parse(text: str) -> Dict[str, Any]:
        """Very small YAML parser — handles 2 levels of indentation.

        Used only when PyYAML is not installed.  Supports:
        * Top-level mapping keys
        * One level of nested mapping (indented by spaces or tab)
        * Scalar values: strings, booleans, null, ints, floats
        * Inline lists: ``[a, b, c]``
        """
        result: Dict[str, Any] = {}
        current_key: Optional[str] = None
        current_section: Optional[Dict[str, Any]] = None

        def _scalar(v: str) -> Any:
            if v in ("true", "True", "yes"):
                return True
            if v in ("false", "False", "no"):
                return False
            if v in ("null", "~", "None", ""):
                return None
            if v.startswith("[") and v.endswith("]"):
                inner = v[1:-1]
                return [i.strip().strip("'\"") for i in inner.split(",") if i.strip()]
            try:
                return int(v)
            except ValueError:
                pass
            try:
                return float(v)
            except ValueError:
                pass
            return v.strip('"\'')

        for line in text.splitlines():
            stripped = line.rstrip()
            if not stripped or stripped.lstrip().startswith("#"):
                continue
            if line.startswith(" ") or line.startswith("\t"):
                if current_key and current_section is not None:
                    inner = stripped.strip()
                    if ": " in inner:
                        k, v = inner.split(": ", 1)
                        current_section[k.strip()] = _scalar(v.strip())
                    elif inner.endswith(":"):
                        # nested sub-section key only
                        current_section[inner[:-1].strip()] = {}
            else:
                if ": " in stripped:
                    k, v = stripped.split(": ", 1)
                    v = v.strip()
                    if v == "":
                        current_key = k.strip()
                        current_section = {}
                        result[current_key] = current_section
                    else:
                        result[k.strip()] = _scalar(v)
                        current_key = k.strip()
                        current_section = None
                elif stripped.endswith(":"):
                    current_key = stripped[:-1].strip()
                    current_section = {}
                    result[current_key] = current_section
        return result
