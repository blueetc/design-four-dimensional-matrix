"""DatabaseAdapter – populate both 4D matrices directly from a database schema.

The adapter introspects a relational database and maps every table into the
dual-matrix knowledge system, so that loading the schema is itself an act of
database cognition:

Coordinate mapping
------------------
* ``t`` – snapshot timestamp (when the introspection was run; repeated
  snapshots let you track schema/volume drift over time)
* ``x`` – column count of the table (schema width / structural complexity)
* ``y`` – row count of the table (data volume)
* ``z`` – table index in alphabetical order (each table = one topic)

Each :class:`~four_dim_matrix.DataPoint` ``payload`` stores the full table
metadata (table name, every column's name/type/nullability/primary-key flag,
and the row count).

Database engine support – pluggable dialect registry
-----------------------------------------------------
Database-engine-specific introspection is handled by
:class:`DialectHandler` subclasses.  Three built-in handlers are
pre-registered:

* **SQLite** (``"sqlite"``) – via the stdlib ``sqlite3`` module.
* **PostgreSQL** (``"postgresql"`` or ``"postgres"``)
* **MySQL / MariaDB** (``"mysql"`` or ``"mariadb"``)

Additional engines can be added **at runtime** without modifying this
source file::

    from four_dim_matrix import DialectHandler, register_dialect

    class DuckDBHandler(DialectHandler):
        def list_table_names(self, conn):
            cursor = conn.cursor()
            cursor.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main' ORDER BY table_name"
            )
            return [row[0] for row in cursor.fetchall()]

        def get_column_info(self, conn, table_name):
            ...  # return List[ColumnInfo]

    register_dialect("duckdb", DuckDBHandler())

    adapter = DatabaseAdapter.from_connection(conn, dialect="duckdb")

Example::

    from four_dim_matrix import DatabaseAdapter

    adapter = DatabaseAdapter.from_sqlite("my_database.db")
    kb = adapter.to_knowledge_base()

    # Every table is now a colour block – hover to inspect full schema
    snap = kb.snapshot(t=adapter.snapshot_time)
    for topic in snap["topics"]:
        print(topic["z"], topic["hex_color"], topic["total_y"], "rows")

    # Trend: how row counts evolve across repeated snapshots
    for t, total_y in kb.trend(z=0).items():
        print(t, total_y)
"""

from __future__ import annotations

import abc
import enum
import math
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from .data_matrix import DataPoint
from .knowledge_base import KnowledgeBase


# ---------------------------------------------------------------------------
# Row-level column mapping
# ---------------------------------------------------------------------------

@dataclass
class ColumnMapping:
    """Defines how a table's columns map to the ``(t, x, y)`` matrix axes.

    Parameters:
        y_column: Name of the column whose numeric value becomes the ``y``
            (quantity) coordinate.  Non-numeric values are coerced to
            ``float``; rows where coercion fails are skipped.
        t_column: Name of the column used as the time coordinate ``t``.
            ISO-8601 strings and Unix timestamps (int/float) are both
            accepted.  Required when ``t_source="column"``; can be omitted
            for non-temporal databases by choosing a different ``t_source``.
        x_column: Optional name of the column used as the ``x`` (phase /
            category) coordinate.  String values are integer-encoded
            per-table (first occurrence = 0, second = 1, …).  When omitted
            the sequential row position within the query result is used.
        limit: Maximum number of rows to load.  ``None`` (default) loads all
            rows.
        where: Optional SQL ``WHERE`` clause fragment (without the
            ``WHERE`` keyword) used to filter rows before loading.
            **Note:** this string is inserted verbatim into the SQL query.
            It must be developer-controlled; never derive it from untrusted
            user input.
        t_source: Strategy for deriving the ``t`` coordinate.

            * ``"column"`` (default) – parse from ``t_column`` (ISO-8601 or
              Unix timestamp).
            * ``"version"`` – treat ``t_column`` as an integer version
              number; ``t = epoch + timedelta(days=version)``.  Useful for
              tables with a schema-version or migration-sequence column.
            * ``"synthetic"`` – ignore ``t_column`` entirely; derive ``t``
              from the sequential row position after sorting by
              ``t_synthetic_order``.  Ideal for static dictionaries, code
              tables, and reference data that have no time dimension.
            * ``"topology"`` – assign ``t`` using the table's topological
              rank in the entity graph (tables with no outgoing FK edges
              rank 0).  Use this when the data's natural order is its
              structural position in the schema rather than a time column.
            * ``"access_log"`` – treat ``t_column`` as an access-count /
              popularity metric; encode it as a date offset so frequently
              accessed entries appear later on the t-axis.
        t_synthetic_order: Column name used to sort rows when
            ``t_source="synthetic"``.  Defaults to ``"alphabetical"``
            which sorts by the first TEXT column found, or by row insertion
            order if no TEXT column exists.
        x_semantic: Optional semantic label that documents what the x-axis
            represents for this table.  Accepted values:

            * ``"funnel"``     – sales / conversion funnel stages (awareness
              → interest → decision → action).
            * ``"lifecycle"``  – entity life-cycle stages (onboarding → active
              → at-risk → churned).
            * ``"progress"``   – generic 0→1 completion progress.
            * ``"stage"``      – arbitrary ordered stages; caller defines the
              semantics via ``x_normalizer``.

            The value is informational and is stored in the DataPoint payload
            for downstream interpretation; it does not change how the adapter
            loads rows unless an ``x_normalizer`` is also provided.
        x_normalizer: Optional callable ``(raw_value: Any) -> float`` that maps
            whatever value appears in ``x_column`` to the closed interval
            ``[0.0, 1.0]``.  When provided, the numeric ``x`` coordinate is
            derived by calling ``x_normalizer(raw_x_value)`` and then
            converting to a non-negative integer by multiplying by 100 and
            rounding.  This enforces a consistent ``x`` scale across topics
            regardless of how each table encodes its business stages.

            Example – map a string status column to a funnel stage::

                def status_to_funnel(raw: str) -> float:
                    stages = {"lead": 0.0, "prospect": 0.33,
                              "demo": 0.66, "closed": 1.0}
                    return stages.get(str(raw).lower(), 0.5)

                ColumnMapping(
                    y_column="order_value",
                    x_column="status",
                    x_semantic="funnel",
                    x_normalizer=status_to_funnel,
                )
    """

    y_column: str
    t_column: Optional[str] = None
    x_column: Optional[str] = None
    limit: Optional[int] = None
    where: Optional[str] = None
    t_source: str = "column"
    t_synthetic_order: str = "alphabetical"
    x_semantic: Optional[str] = None
    x_normalizer: Optional[Callable[[Any], float]] = None

    def normalize_x(self, raw_value: Any) -> float:
        """Map *raw_value* from ``x_column`` to a normalised ``[0, 1]`` float.

        If :attr:`x_normalizer` is set it is called with *raw_value* and the
        result is clamped to ``[0, 1]``.  Otherwise ``0.5`` is returned as a
        neutral default (midpoint of the x range).

        The integer ``x`` coordinate stored in the DataMatrix is obtained by
        ``round(normalize_x(raw) * 100)`` so that the full 0–100 integer range
        represents the 0.0–1.0 normalised progress.

        Parameters:
            raw_value: The value read from the ``x_column`` for a given row.

        Returns:
            A float in ``[0.0, 1.0]``.
        """
        if self.x_normalizer is None:
            return 0.5
        result = self.x_normalizer(raw_value)
        return max(0.0, min(1.0, float(result)))


@dataclass
class TableMapping:
    """Pairs a table name with a :class:`ColumnMapping` for row-level loading.

    Parameters:
        table_name: The name of the database table to load.
        mapping: Column-to-axis mapping rules for this table.

    Example::

        from four_dim_matrix import ColumnMapping, TableMapping, DatabaseAdapter

        tm = TableMapping(
            table_name="orders",
            mapping=ColumnMapping(
                t_column="created_at",
                y_column="total",
                x_column="status",
            ),
        )
        conn = sqlite3.connect("my.db")
        adapter = DatabaseAdapter.from_connection(conn, dialect="sqlite")
        kb = adapter.load_rows(conn, [tm])
    """

    table_name: str
    mapping: ColumnMapping


# ---------------------------------------------------------------------------
# Column type classification
# ---------------------------------------------------------------------------

class ColumnType(enum.IntEnum):
    """Canonical column-type categories used as the ``x`` sub-dimension."""

    INTEGER = 0
    TEXT = 1
    DATETIME = 2
    FLOAT = 3
    BOOLEAN = 4
    BLOB = 5
    OTHER = 6

    @classmethod
    def from_type_string(cls, type_str: str) -> "ColumnType":
        """Map a raw SQL type declaration to a :class:`ColumnType`.

        The mapping is intentionally broad so that vendor-specific type names
        (``TINYINT``, ``VARCHAR``, ``TIMESTAMP WITH TIME ZONE`` …) all resolve
        to one of the canonical buckets.
        """
        t = type_str.upper().split("(")[0].strip()
        if t in {
            "INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT",
            "INT2", "INT4", "INT8", "MEDIUMINT", "UNSIGNED BIG INT",
        }:
            return cls.INTEGER
        if t in {
            "TEXT", "CHAR", "CHARACTER", "VARCHAR", "NCHAR", "NVARCHAR",
            "CLOB", "STRING", "VARYING CHARACTER", "NATIVE CHARACTER",
        }:
            return cls.TEXT
        if t in {
            "DATE", "DATETIME", "TIMESTAMP", "TIME",
        }:
            return cls.DATETIME
        if t in {
            "REAL", "FLOAT", "DOUBLE", "DOUBLE PRECISION",
            "DECIMAL", "NUMERIC", "NUMBER",
        }:
            return cls.FLOAT
        if t in {"BOOLEAN", "BOOL"}:
            return cls.BOOLEAN
        if t in {"BLOB", "BINARY", "VARBINARY"}:
            return cls.BLOB
        return cls.OTHER


# ---------------------------------------------------------------------------
# Schema metadata dataclasses
# ---------------------------------------------------------------------------

@dataclass
class ColumnInfo:
    """Metadata for a single database column."""

    name: str
    type_str: str
    column_type: ColumnType
    nullable: bool = True
    primary_key: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "type_str": self.type_str,
            "column_type": self.column_type.name,
            "nullable": self.nullable,
            "primary_key": self.primary_key,
        }


@dataclass
class TableInfo:
    """Metadata for a single database table."""

    name: str
    columns: List[ColumnInfo] = field(default_factory=list)
    row_count: int = 0

    # ------------------------------------------------------------------
    # Derived properties
    # ------------------------------------------------------------------

    @property
    def column_count(self) -> int:
        """Number of columns in the table."""
        return len(self.columns)

    def columns_by_type(self) -> Dict[ColumnType, List[ColumnInfo]]:
        """Group columns by their :class:`ColumnType`."""
        groups: Dict[ColumnType, List[ColumnInfo]] = {}
        for col in self.columns:
            groups.setdefault(col.column_type, []).append(col)
        return groups

    def type_summary(self) -> Dict[str, int]:
        """Return ``{type_name: count}`` for each column type present."""
        summary: Dict[str, int] = {}
        for col in self.columns:
            key = col.column_type.name
            summary[key] = summary.get(key, 0) + 1
        return summary

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "row_count": self.row_count,
            "column_count": self.column_count,
            "type_summary": self.type_summary(),
            "columns": [c.to_dict() for c in self.columns],
        }


# ---------------------------------------------------------------------------
# Dialect plugin system
# ---------------------------------------------------------------------------

class DialectHandler(abc.ABC):
    """Abstract base class for database-engine dialect handlers.

    Subclass this and register an instance with :func:`register_dialect` to
    add introspection support for any DBAPI-2 compatible database engine
    without modifying this module.

    Minimal implementation
    ----------------------
    You must implement :meth:`list_table_names` and :meth:`get_column_info`.
    :meth:`get_row_count` has a working default that runs
    ``SELECT COUNT(*) FROM <table>``; override it when a faster or
    dialect-specific alternative is available.

    Example::

        from four_dim_matrix import DialectHandler, register_dialect, ColumnInfo, ColumnType

        class DuckDBHandler(DialectHandler):
            def list_table_names(self, conn):
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = 'main' ORDER BY table_name"
                )
                return [row[0] for row in cursor.fetchall()]

            def get_column_info(self, conn, table_name):
                cursor = conn.cursor()
                cursor.execute(f"DESCRIBE {table_name}")
                return [
                    ColumnInfo(
                        name=row[0], type_str=row[1],
                        column_type=ColumnType.from_type_string(row[1]),
                    )
                    for row in cursor.fetchall()
                ]

        register_dialect("duckdb", DuckDBHandler())
    """

    @abc.abstractmethod
    def list_table_names(self, conn: Any) -> List[str]:
        """Return user-visible table names in the order they should be indexed.

        Implementations should exclude internal or system tables and return
        names in a **stable, deterministic** order (alphabetical is
        recommended) so that z-axis indices remain consistent across calls.
        """

    @abc.abstractmethod
    def get_column_info(self, conn: Any, table_name: str) -> List[ColumnInfo]:
        """Return a :class:`ColumnInfo` list for every column in *table_name*.

        Column order should match the physical declaration order in the table
        (i.e. ``ordinal_position`` from ``information_schema.columns``).
        """

    def get_row_count(self, conn: Any, table_name: str) -> int:
        """Return the number of rows in *table_name*.

        The default runs ``SELECT COUNT(*) FROM "<table>"`` which works for
        every SQL-92 compatible engine.  Override when a cheaper alternative
        is available (e.g. reading ``pg_class.reltuples`` for PostgreSQL, or
        ``information_schema.tables.table_rows`` for MySQL).
        """
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {_quote_identifier(table_name)}")
        result = cursor.fetchone()
        return int(result[0]) if result else 0


class SQLiteDialectHandler(DialectHandler):
    """Built-in dialect handler for **SQLite** databases."""

    def list_table_names(self, conn: Any) -> List[str]:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name NOT LIKE 'sqlite_%' "
            "ORDER BY name"
        )
        return [row[0] for row in cursor.fetchall()]

    def get_column_info(self, conn: Any, table_name: str) -> List[ColumnInfo]:
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info({_quote_identifier(table_name)})")
        rows = cursor.fetchall()
        # PRAGMA table_info columns: cid, name, type, notnull, dflt_value, pk
        return [
            ColumnInfo(
                name=row[1],
                type_str=row[2] or "TEXT",
                column_type=ColumnType.from_type_string(row[2] or "TEXT"),
                nullable=row[3] == 0,
                primary_key=row[5] > 0,
            )
            for row in rows
        ]


class PostgreSQLDialectHandler(DialectHandler):
    """Built-in dialect handler for **PostgreSQL** databases (public schema)."""

    def list_table_names(self, conn: Any) -> List[str]:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_type = 'BASE TABLE'
            ORDER BY table_name
            """
        )
        return [row[0] for row in cursor.fetchall()]

    def get_column_info(self, conn: Any, table_name: str) -> List[ColumnInfo]:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position
            """,
            (table_name,),
        )
        col_rows = cursor.fetchall()

        cursor.execute(
            """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            WHERE tc.table_schema = 'public'
              AND tc.table_name = %s
              AND tc.constraint_type = 'PRIMARY KEY'
            """,
            (table_name,),
        )
        pk_cols = {row[0] for row in cursor.fetchall()}

        return [
            ColumnInfo(
                name=row[0],
                type_str=row[1],
                column_type=ColumnType.from_type_string(row[1]),
                nullable=row[2] == "YES",
                primary_key=row[0] in pk_cols,
            )
            for row in col_rows
        ]


class MySQLDialectHandler(DialectHandler):
    """Built-in dialect handler for **MySQL / MariaDB** databases."""

    def list_table_names(self, conn: Any) -> List[str]:
        cursor = conn.cursor()
        cursor.execute("SELECT DATABASE()")
        db_name = cursor.fetchone()[0]
        cursor.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s AND table_type = 'BASE TABLE'
            ORDER BY table_name
            """,
            (db_name,),
        )
        return [row[0] for row in cursor.fetchall()]

    def get_column_info(self, conn: Any, table_name: str) -> List[ColumnInfo]:
        cursor = conn.cursor()
        cursor.execute("SELECT DATABASE()")
        db_name = cursor.fetchone()[0]
        cursor.execute(
            """
            SELECT column_name, data_type, is_nullable, column_key
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            (db_name, table_name),
        )
        col_rows = cursor.fetchall()
        return [
            ColumnInfo(
                name=row[0],
                type_str=row[1],
                column_type=ColumnType.from_type_string(row[1]),
                nullable=row[2] == "YES",
                primary_key=row[3] == "PRI",
            )
            for row in col_rows
        ]


# ---------------------------------------------------------------------------
# Dialect registry
# ---------------------------------------------------------------------------

#: Module-level registry mapping lowercase dialect names to handler instances.
#: Pre-populated with the three built-in handlers and their common aliases.
#: Use :func:`register_dialect` to add new engines at runtime.
_DIALECT_REGISTRY: Dict[str, DialectHandler] = {
    "sqlite":     SQLiteDialectHandler(),
    "postgresql": PostgreSQLDialectHandler(),
    "postgres":   PostgreSQLDialectHandler(),   # alias
    "mysql":      MySQLDialectHandler(),
    "mariadb":    MySQLDialectHandler(),        # alias
}


def register_dialect(name: str, handler: DialectHandler) -> None:
    """Register *handler* as the introspection engine for dialect *name*.

    Calling this function makes the dialect available to
    :meth:`~four_dim_matrix.DatabaseAdapter.from_connection` via the
    ``dialect=`` parameter.  Registration is **module-global** and
    persists for the lifetime of the Python process.

    Parameters:
        name: Case-insensitive dialect identifier (e.g. ``"duckdb"``,
            ``"mssql"``, ``"clickhouse"``).  If *name* already exists in
            the registry the handler is replaced.
        handler: An instance of a :class:`DialectHandler` concrete subclass.

    Raises:
        TypeError: If *handler* is not an instance of :class:`DialectHandler`.

    Example::

        from four_dim_matrix import DialectHandler, register_dialect

        class ClickHouseHandler(DialectHandler):
            def list_table_names(self, conn):
                cursor = conn.cursor()
                cursor.execute("SHOW TABLES")
                return sorted(row[0] for row in cursor.fetchall())

            def get_column_info(self, conn, table_name):
                ...  # query system.columns

        register_dialect("clickhouse", ClickHouseHandler())
    """
    if not isinstance(handler, DialectHandler):
        raise TypeError(
            f"handler must be a DialectHandler instance, "
            f"got {type(handler).__name__!r}"
        )
    _DIALECT_REGISTRY[name.lower()] = handler


def get_dialect_handler(name: str) -> DialectHandler:
    """Return the :class:`DialectHandler` registered for *name*.

    Parameters:
        name: Dialect name (case-insensitive).

    Returns:
        The registered :class:`DialectHandler` instance.

    Raises:
        ValueError: If no handler is registered for *name*.  The error
            message lists all currently-registered dialect names so users
            know what is available and can call :func:`register_dialect` to
            add the missing engine.
    """
    handler = _DIALECT_REGISTRY.get(name.lower())
    if handler is None:
        registered = ", ".join(f"'{k}'" for k in sorted(_DIALECT_REGISTRY))
        raise ValueError(
            f"Unsupported dialect {name!r}. "
            f"Registered dialects: {registered}. "
            "Use register_dialect() to add a new engine handler."
        )
    return handler


# ---------------------------------------------------------------------------
# Main adapter
# ---------------------------------------------------------------------------

class DatabaseAdapter:
    """Introspect a relational database and populate both 4D matrices.

    Parameters:
        tables: List of :class:`TableInfo` objects (populated by a factory
            class method such as :meth:`from_sqlite`).
        snapshot_time: When the introspection was performed.  Defaults to
            ``datetime.utcnow()`` at construction time.

    Factory methods
    ---------------
    * :meth:`from_sqlite` – open an SQLite file (or ``:memory:``).
    * :meth:`from_connection` – use any open DBAPI-2 connection.
    """

    def __init__(
        self,
        tables: List[TableInfo],
        snapshot_time: Optional[datetime] = None,
    ) -> None:
        self.tables = tables
        self.snapshot_time = snapshot_time or datetime.now(timezone.utc).replace(tzinfo=None)

    # ------------------------------------------------------------------
    # Factory methods
    # ------------------------------------------------------------------

    @classmethod
    def from_sqlite(cls, db_path: str) -> "DatabaseAdapter":
        """Open *db_path* as an SQLite database and introspect its schema.

        Use ``db_path=":memory:"`` together with :meth:`from_connection` when
        you already have a live in-memory connection.
        """
        conn = sqlite3.connect(db_path)
        try:
            return cls.from_connection(conn, dialect="sqlite")
        finally:
            conn.close()

    @classmethod
    def from_connection(
        cls,
        conn: Any,
        dialect: str = "sqlite",
        snapshot_time: Optional[datetime] = None,
    ) -> "DatabaseAdapter":
        """Build an adapter from an existing DBAPI-2 *conn*.

        Parameters:
            conn: An open DBAPI-2 database connection.
            dialect: The database engine dialect to use for introspection.
                Built-in values: ``"sqlite"``, ``"postgresql"`` /
                ``"postgres"``, ``"mysql"`` / ``"mariadb"``.  Additional
                dialects can be registered at runtime with
                :func:`register_dialect`.
            snapshot_time: Override the snapshot timestamp.

        Raises:
            ValueError: If *dialect* is not registered.  The error message
                lists all currently-registered dialect names.
        """
        tables = _introspect(conn, dialect)
        return cls(tables=tables, snapshot_time=snapshot_time)

    # ------------------------------------------------------------------
    # Core conversion
    # ------------------------------------------------------------------

    def to_data_points(self) -> List[DataPoint]:
        """Convert the introspected schema to a list of :class:`DataPoint` objects.

        One DataPoint is generated per table, using the coordinate mapping::

            t = snapshot_time
            x = column_count        (schema width / structural complexity)
            y = float(row_count)    (data volume)
            z = table_index         (alphabetical rank → unique topic per table)
        """
        sorted_tables = sorted(self.tables, key=lambda tb: tb.name)
        points: List[DataPoint] = []
        for z_index, table in enumerate(sorted_tables):
            points.append(
                DataPoint(
                    t=self.snapshot_time,
                    x=table.column_count,
                    y=float(table.row_count),
                    z=z_index,
                    payload=table.to_dict(),
                )
            )
        return points

    def to_knowledge_base(self) -> KnowledgeBase:
        """Build a fully populated :class:`~four_dim_matrix.KnowledgeBase`.

        Both matrices are populated in a single call.  The resulting
        knowledge base can immediately answer questions like:

        * *"Which table has the most rows?"*  → brightest colour block
        * *"Which tables have a similar structure?"*  → similar hues/saturation
        * *"What does table X look like?"*  → hover on colour block → payload
        """
        kb = KnowledgeBase()
        points = self.to_data_points()
        kb.insert_many(points)
        return kb

    def load_rows(
        self,
        conn: Any,
        table_mappings: List[TableMapping],
    ) -> KnowledgeBase:
        """Load actual row data from tables and populate both 4D matrices.

        Unlike :meth:`to_knowledge_base` (which creates one point *per table*
        summarising schema metadata), this method creates one
        :class:`~four_dim_matrix.DataPoint` *per row*, so the matrices reflect
        the real contents of the database.

        Coordinate assignment
        ~~~~~~~~~~~~~~~~~~~~~
        * ``z`` – stable topic index: tables are sorted alphabetically across
          all supplied ``table_mappings`` so each table always gets the same
          hue, even across repeated calls.
        * ``t`` – parsed from the row's ``t_column`` value (ISO-8601 strings
          and Unix timestamps are both supported; falls back to
          :attr:`snapshot_time` when parsing fails).
        * ``y`` – the numeric value of the row's ``y_column``; rows where
          the value cannot be cast to ``float`` are skipped.
        * ``x`` – when ``x_column`` is given, unique values are
          integer-encoded in first-seen order (``"pending"`` → 0,
          ``"paid"`` → 1, …); when omitted, the sequential row position
          within the table is used.
        * ``payload`` – the complete row as a plain dictionary, keyed by
          column names.

        Parameters:
            conn: An open DBAPI-2 connection to the database.
            table_mappings: List of :class:`TableMapping` objects describing
                which columns to use for each table.

        Returns:
            A :class:`~four_dim_matrix.KnowledgeBase` populated with one
            DataPoint / ColorPoint per successfully loaded row.

        Example::

            import sqlite3
            from four_dim_matrix import (
                DatabaseAdapter, ColumnMapping, TableMapping,
            )

            conn = sqlite3.connect("sales.db")
            adapter = DatabaseAdapter.from_connection(conn, dialect="sqlite")
            kb = adapter.load_rows(conn, [
                TableMapping("orders", ColumnMapping(
                    t_column="created_at",
                    y_column="total",
                    x_column="status",
                )),
                TableMapping("customers", ColumnMapping(
                    t_column="signup_date",
                    y_column="id",   # row-count proxy
                )),
            ])
            # See the full colour snapshot for the loaded timestamp range
            snap = kb.snapshot(t=next(iter(kb.trend())))
        """
        # Assign stable z-values by sorting table names alphabetically.
        sorted_names = sorted(tm.table_name for tm in table_mappings)
        z_by_table: Dict[str, int] = {name: idx for idx, name in enumerate(sorted_names)}

        points: List[DataPoint] = []
        for tm in table_mappings:
            z = z_by_table[tm.table_name]
            rows = _fetch_rows(conn, tm)
            x_encoder: Dict[Any, int] = {}
            for row_idx, (col_names, row_values) in enumerate(rows):
                row_dict = dict(zip(col_names, row_values))
                # Parse y – skip rows where value is not numeric
                raw_y = row_dict.get(tm.mapping.y_column)
                try:
                    y = float(raw_y)  # type: ignore[arg-type]
                except (TypeError, ValueError):
                    continue
                # Derive t according to the configured t_source strategy
                t = _derive_t(row_dict, row_idx, tm.mapping, self.snapshot_time)
                # Encode x
                if tm.mapping.x_column is not None:
                    raw_x = row_dict.get(tm.mapping.x_column)
                    if raw_x not in x_encoder:
                        x_encoder[raw_x] = len(x_encoder)
                    x = x_encoder[raw_x]
                else:
                    x = row_idx
                points.append(
                    DataPoint(t=t, x=x, y=y, z=z, payload=row_dict)
                )

        kb = KnowledgeBase()
        kb.insert_many(points)
        return kb

    # ------------------------------------------------------------------
    # Diff / change detection
    # ------------------------------------------------------------------

    def diff(self, other: "DatabaseAdapter") -> Dict[str, Any]:
        """Compare this snapshot with *other* and report schema/volume changes.

        Returns a dictionary with three keys:

        * ``"added"`` – tables present in *other* but not in *self*.
        * ``"removed"`` – tables present in *self* but not in *other*.
        * ``"changed"`` – tables present in both with a different
          ``row_count`` or ``column_count``.
        """
        self_by_name = {t.name: t for t in self.tables}
        other_by_name = {t.name: t for t in other.tables}

        added = [
            other_by_name[n].to_dict()
            for n in sorted(other_by_name)
            if n not in self_by_name
        ]
        removed = [
            self_by_name[n].to_dict()
            for n in sorted(self_by_name)
            if n not in other_by_name
        ]
        changed: List[Dict[str, Any]] = []
        for name in sorted(self_by_name):
            if name not in other_by_name:
                continue
            old = self_by_name[name]
            new = other_by_name[name]
            if old.row_count != new.row_count or old.column_count != new.column_count:
                changed.append(
                    {
                        "table": name,
                        "old": {"row_count": old.row_count, "column_count": old.column_count},
                        "new": {"row_count": new.row_count, "column_count": new.column_count},
                        "row_delta": new.row_count - old.row_count,
                        "column_delta": new.column_count - old.column_count,
                    }
                )
        return {"added": added, "removed": removed, "changed": changed}

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------

    def table_names(self) -> List[str]:
        """Return table names in alphabetical order."""
        return sorted(t.name for t in self.tables)

    def get_table(self, name: str) -> Optional[TableInfo]:
        """Return the :class:`TableInfo` for *name*, or ``None``."""
        for t in self.tables:
            if t.name == name:
                return t
        return None

    def summary(self) -> Dict[str, Any]:
        """Return a high-level summary of the database schema."""
        total_rows = sum(t.row_count for t in self.tables)
        total_cols = sum(t.column_count for t in self.tables)
        type_counts: Dict[str, int] = {}
        for table in self.tables:
            for type_name, count in table.type_summary().items():
                type_counts[type_name] = type_counts.get(type_name, 0) + count
        return {
            "snapshot_time": self.snapshot_time.isoformat(),
            "table_count": len(self.tables),
            "total_rows": total_rows,
            "total_columns": total_cols,
            "column_type_distribution": type_counts,
            "tables": sorted(
                [
                    {
                        "name": t.name,
                        "row_count": t.row_count,
                        "column_count": t.column_count,
                        "type_summary": t.type_summary(),
                    }
                    for t in self.tables
                ],
                key=lambda d: d["row_count"],
                reverse=True,
            ),
        }

    # ------------------------------------------------------------------
    # Dunder helpers
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        return (
            f"DatabaseAdapter("
            f"tables={len(self.tables)}, "
            f"snapshot_time={self.snapshot_time.isoformat()})"
        )


# ---------------------------------------------------------------------------
# Internal introspection helpers
# ---------------------------------------------------------------------------

def _quote_identifier(name: str) -> str:
    """Return *name* as a safely quoted SQL identifier.

    Double-quotes are escaped by doubling them (standard SQL).  This prevents
    SQL injection through table and column names.

    >>> _quote_identifier("my_table")
    '"my_table"'
    >>> _quote_identifier('weird"name')
    '"weird""name"'
    """
    return '"' + name.replace('"', '""') + '"'


def _parse_t_value(raw: Any, fallback: datetime) -> datetime:
    """Parse *raw* into a :class:`datetime`, falling back to *fallback*.

    Accepted formats:
    * ``datetime`` object – returned as-is.
    * ISO-8601 string (e.g. ``"2024-03-01"`` or ``"2024-03-01T12:00:00"``).
    * Numeric Unix timestamp (int or float).
    """
    if isinstance(raw, datetime):
        return raw
    if isinstance(raw, str):
        # Strip trailing timezone info that fromisoformat doesn't handle on
        # Python < 3.11 (e.g. " UTC" suffix from some databases).
        clean = raw.strip()
        for suffix in (" UTC", " utc", "Z"):
            if clean.endswith(suffix):
                clean = clean[: -len(suffix)]
        try:
            return datetime.fromisoformat(clean)
        except ValueError:
            pass
    if isinstance(raw, (int, float)):
        try:
            return datetime.fromtimestamp(float(raw), tz=timezone.utc).replace(tzinfo=None)
        except (OSError, OverflowError, ValueError):
            pass
    return fallback


def _derive_t(
    row_dict: Dict[str, Any],
    row_idx: int,
    mapping: "ColumnMapping",
    fallback: datetime,
) -> datetime:
    """Derive the t-coordinate for a single row using the configured strategy.

    Dispatches on ``mapping.t_source``:

    * ``"column"``    – parse from ``mapping.t_column`` (ISO-8601 / Unix ts).
    * ``"version"``   – treat ``mapping.t_column`` as an integer version
      number; ``t = epoch + timedelta(days=version)``.
    * ``"synthetic"`` – ``t = epoch + timedelta(days=row_idx)``.  The
      ``epoch`` is ``datetime(2000, 1, 1)`` for all offset-based strategies.
    * ``"topology"``  – same as ``"synthetic"``; the topological rank is
      assigned externally; here we fall back to row position.
    * ``"access_log"``– treat ``mapping.t_column`` as a popularity count
      and encode as a date offset from the epoch.
    * Anything else   – return *fallback* unchanged.
    """
    _EPOCH = datetime(2000, 1, 1)
    src = mapping.t_source

    if src == "column":
        raw = row_dict.get(mapping.t_column) if mapping.t_column else None
        return _parse_t_value(raw, fallback)

    if src == "version" and mapping.t_column:
        raw = row_dict.get(mapping.t_column)
        try:
            return _EPOCH + timedelta(days=int(float(str(raw))))
        except (TypeError, ValueError):
            return fallback

    if src in ("synthetic", "topology"):
        return _EPOCH + timedelta(days=row_idx)

    if src == "access_log" and mapping.t_column:
        raw = row_dict.get(mapping.t_column)
        try:
            return _EPOCH + timedelta(days=int(float(str(raw))))
        except (TypeError, ValueError):
            return fallback

    return fallback


def _fetch_rows(
    conn: Any, tm: "TableMapping"
) -> List[Tuple[List[str], tuple]]:
    """Execute a SELECT on *tm.table_name* and return ``[(col_names, row), …]``.

    Respects the ``limit`` and ``where`` constraints from
    :class:`ColumnMapping`.
    """
    m = tm.mapping
    sql = f"SELECT * FROM {_quote_identifier(tm.table_name)}"
    if m.where:
        # NOTE: ``where`` is a developer-controlled SQL fragment; it is not
        # sanitized.  Only pass trusted values here.
        sql += f" WHERE {m.where}"
    if m.limit is not None:
        sql += f" LIMIT {int(m.limit)}"
    cursor = conn.cursor()
    cursor.execute(sql)
    col_names: List[str] = [desc[0] for desc in cursor.description]
    return [(col_names, row) for row in cursor.fetchall()]

def _introspect(conn: Any, dialect: str) -> List[TableInfo]:
    """Use the dialect registry to introspect *conn* and return table metadata."""
    handler = get_dialect_handler(dialect)
    table_names = handler.list_table_names(conn)
    tables: List[TableInfo] = []
    for name in table_names:
        columns = handler.get_column_info(conn, name)
        row_count = handler.get_row_count(conn, name)
        tables.append(TableInfo(name=name, columns=columns, row_count=row_count))
    return tables
