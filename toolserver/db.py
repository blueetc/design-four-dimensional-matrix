"""SQLite database adapter (extensible to PostgreSQL / MySQL / SQL Server)."""

from __future__ import annotations

import re
import sqlite3
from typing import Any


class SQLiteDB:
    """Thin wrapper around :mod:`sqlite3` exposing schema / query / exec."""

    def __init__(self, path: str) -> None:
        self.path = path

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        return conn

    def schema(self) -> dict[str, Any]:
        """Return table/view definitions from ``sqlite_master``."""
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT name, sql FROM sqlite_master "
                "WHERE type IN ('table','view') ORDER BY name"
            ).fetchall()
            return {"tables": [{"name": r["name"], "sql": r["sql"]} for r in rows]}

    def query(self, sql: str, params: list[Any] | None = None) -> dict[str, Any]:
        """Execute a read-only query and return all rows."""
        params = params or []
        with self.connect() as conn:
            cur = conn.execute(sql, params)
            data = [dict(r) for r in cur.fetchall()]
            return {"rows": data, "rowcount": len(data)}

    def estimate_affected_rows(
        self, sql: str, params: list[Any] | None = None,
    ) -> int | None:
        """Estimate the number of rows affected by an UPDATE/DELETE.

        Rewrites the statement into a ``SELECT COUNT(*)`` using the
        same ``WHERE`` clause.  Returns ``None`` when the statement
        cannot be rewritten (e.g. INSERT, DDL).
        """
        params = params or []
        # Only attempt for UPDATE / DELETE
        m = re.match(
            r"(?is)^\s*(?:UPDATE\s+\S+\s+SET\s+.+?|DELETE\s+FROM\s+\S+)"
            r"\s+(WHERE\s+.+?)\s*;?\s*$",
            sql,
        )
        if not m:
            return None
        where_clause = m.group(1)
        # Extract table name
        tbl_m = re.match(
            r"(?i)^\s*(?:UPDATE\s+(\S+)|DELETE\s+FROM\s+(\S+))", sql,
        )
        if not tbl_m:
            return None
        table = tbl_m.group(1) or tbl_m.group(2)
        count_sql = f"SELECT COUNT(*) AS cnt FROM {table} {where_clause}"
        try:
            with self.connect() as conn:
                row = conn.execute(count_sql, params).fetchone()
                return int(row["cnt"]) if row else None
        except Exception:
            return None

    def exec(
        self,
        sql: str,
        params: list[Any] | None = None,
        force_transaction: bool = True,
        row_limit: int | None = None,
    ) -> dict[str, Any]:
        """Execute a write statement inside an explicit transaction.

        When *row_limit* is set and the estimated affected row count
        exceeds the limit, the statement is rejected **before** execution.
        """
        params = params or []

        # Pre-flight row-count check
        if row_limit is not None:
            est = self.estimate_affected_rows(sql, params)
            if est is not None and est > row_limit:
                raise ValueError(
                    f"Estimated affected rows ({est}) exceeds limit ({row_limit})"
                )

        conn = self.connect()
        try:
            if force_transaction:
                conn.execute("BEGIN")
            cur = conn.execute(sql, params)
            affected = cur.rowcount
            if force_transaction:
                conn.commit()
            return {"rowcount": affected}
        except Exception:
            if force_transaction:
                conn.rollback()
            raise
        finally:
            conn.close()
