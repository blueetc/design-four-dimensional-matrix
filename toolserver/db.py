"""SQLite database adapter (extensible to PostgreSQL / MySQL / SQL Server)."""

from __future__ import annotations

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

    def exec(
        self,
        sql: str,
        params: list[Any] | None = None,
        force_transaction: bool = True,
    ) -> dict[str, Any]:
        """Execute a write statement inside an explicit transaction."""
        params = params or []
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
