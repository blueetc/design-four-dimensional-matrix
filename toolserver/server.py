"""FastAPI tool server – local HTTP endpoint for the agent to call."""

from __future__ import annotations

import getpass
import os
import platform
import shutil

from fastapi import FastAPI
from pydantic import BaseModel

from .audit import append_audit
from .config import load_policy
from .db import SQLiteDB
from .files import read_file as _read
from .files import write_file as _write
from .policy import (
    check_command,
    check_sensitive_path,
    check_sql_write,
    enforce_workspace,
)
from .shell import run_command as _run

_POLICY_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "policy.yaml")
POLICY = load_policy(_POLICY_PATH)
AUDIT_LOG = os.path.join(POLICY["workspace_root"], "audit.jsonl")
DB = SQLiteDB(os.path.join(POLICY["workspace_root"], "agent.sqlite3"))

app = FastAPI(title="Ollama Local Agent – Tool Server")


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------

class RunCommandIn(BaseModel):
    command: str
    cwd: str | None = None
    timeout_s: int | None = None


class PathIn(BaseModel):
    path: str


class WriteFileIn(BaseModel):
    path: str
    content: str
    backup: bool = True


class SQLIn(BaseModel):
    sql: str
    params: list | None = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ok(tool: str, result: object) -> dict:
    return {"ok": True, "tool": tool, "result": result, "error": None}


def _fail(tool: str, error: str) -> dict:
    return {"ok": False, "tool": tool, "result": None, "error": error}


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.post("/tool/get_system_info")
def get_system_info() -> dict:
    res = {
        "platform": platform.platform(),
        "system": platform.system(),
        "release": platform.release(),
        "machine": platform.machine(),
        "workspace_root": POLICY["workspace_root"],
        "user": getpass.getuser(),
        "shell": _detect_shell(),
        "disk_free": _disk_free(POLICY["workspace_root"]),
    }
    append_audit(AUDIT_LOG, {"tool": "get_system_info", "args": {}, "ok": True})
    return _ok("get_system_info", res)


def _detect_shell() -> str:
    """Return the preferred shell for the current platform."""
    if platform.system().lower() == "windows":
        return "pwsh" if shutil.which("pwsh") else "powershell"
    return os.environ.get("SHELL", "/bin/bash")


def _disk_free(path: str) -> dict | None:
    """Return disk usage stats (total/used/free in bytes) or ``None``."""
    try:
        usage = shutil.disk_usage(path)
        return {"total": usage.total, "used": usage.used, "free": usage.free}
    except OSError:
        return None


@app.post("/tool/run_command")
def run_command(inp: RunCommandIn) -> dict:
    cwd = inp.cwd or POLICY["workspace_root"]
    ok, msg, cwd_abs = enforce_workspace(POLICY, cwd)
    if not ok:
        append_audit(AUDIT_LOG, {"tool": "run_command", "args": inp.model_dump(), "ok": False, "error": msg})
        return _fail("run_command", msg)

    ok, msg = check_command(POLICY, inp.command)
    if not ok:
        append_audit(AUDIT_LOG, {"tool": "run_command", "args": inp.model_dump(), "ok": False, "error": msg})
        return _fail("run_command", msg)

    res = _run(
        inp.command,
        cwd=cwd_abs,
        timeout_s=inp.timeout_s or POLICY["max_exec_seconds"],
        max_output_bytes=POLICY["max_output_bytes"],
    )
    append_audit(AUDIT_LOG, {
        "tool": "run_command",
        "args": inp.model_dump(),
        "ok": True,
        "result": {"exit_code": res["exit_code"]},
    })
    return _ok("run_command", res)


@app.post("/tool/read_file")
def read_file(inp: PathIn) -> dict:
    ok, msg, abs_path = enforce_workspace(POLICY, inp.path)
    if not ok:
        append_audit(AUDIT_LOG, {"tool": "read_file", "args": inp.model_dump(), "ok": False, "error": msg})
        return _fail("read_file", msg)
    try:
        res = _read(abs_path)
    except OSError as exc:
        err = str(exc)
        append_audit(AUDIT_LOG, {"tool": "read_file", "args": {"path": abs_path}, "ok": False, "error": err})
        return _fail("read_file", err)
    append_audit(AUDIT_LOG, {"tool": "read_file", "args": {"path": abs_path}, "ok": True})
    return _ok("read_file", res)


@app.post("/tool/write_file")
def write_file(inp: WriteFileIn) -> dict:
    ok, msg, abs_path = enforce_workspace(POLICY, inp.path)
    if not ok:
        append_audit(AUDIT_LOG, {"tool": "write_file", "args": inp.model_dump(), "ok": False, "error": msg})
        return _fail("write_file", msg)

    # Block writes to sensitive system directories
    ok, msg = check_sensitive_path(POLICY, abs_path)
    if not ok:
        append_audit(AUDIT_LOG, {"tool": "write_file", "args": {"path": abs_path}, "ok": False, "error": msg})
        return _fail("write_file", msg)

    ext = os.path.splitext(abs_path)[1].lower()
    allowed_exts = set(POLICY["files"]["allow_write_extensions"])
    if ext and ext not in allowed_exts:
        err = f"File extension not allowed: {ext}"
        append_audit(AUDIT_LOG, {"tool": "write_file", "args": {"path": abs_path}, "ok": False, "error": err})
        return _fail("write_file", err)

    try:
        res = _write(abs_path, inp.content, backup=inp.backup, max_bytes=POLICY["files"]["max_write_bytes"])
    except (ValueError, OSError) as exc:
        err = str(exc)
        append_audit(AUDIT_LOG, {"tool": "write_file", "args": {"path": abs_path}, "ok": False, "error": err})
        return _fail("write_file", err)

    append_audit(AUDIT_LOG, {
        "tool": "write_file",
        "args": {"path": abs_path, "backup": inp.backup},
        "ok": True,
        "result": {"skipped": res.get("skipped", False)},
    })
    return _ok("write_file", res)


@app.post("/tool/list_dir")
def list_dir(inp: PathIn) -> dict:
    ok, msg, abs_path = enforce_workspace(POLICY, inp.path)
    if not ok:
        append_audit(AUDIT_LOG, {"tool": "list_dir", "args": inp.model_dump(), "ok": False, "error": msg})
        return _fail("list_dir", msg)
    try:
        entries = os.listdir(abs_path)
    except OSError as exc:
        err = str(exc)
        append_audit(AUDIT_LOG, {"tool": "list_dir", "args": {"path": abs_path}, "ok": False, "error": err})
        return _fail("list_dir", err)
    append_audit(AUDIT_LOG, {"tool": "list_dir", "args": {"path": abs_path}, "ok": True})
    return _ok("list_dir", {"path": abs_path, "entries": entries})


@app.post("/tool/stat")
def stat_path(inp: PathIn) -> dict:
    ok, msg, abs_path = enforce_workspace(POLICY, inp.path)
    if not ok:
        append_audit(AUDIT_LOG, {"tool": "stat", "args": inp.model_dump(), "ok": False, "error": msg})
        return _fail("stat", msg)
    try:
        st = os.stat(abs_path)
    except OSError as exc:
        err = str(exc)
        append_audit(AUDIT_LOG, {"tool": "stat", "args": {"path": abs_path}, "ok": False, "error": err})
        return _fail("stat", err)
    info = {"path": abs_path, "size": st.st_size, "mode": oct(st.st_mode)}
    append_audit(AUDIT_LOG, {"tool": "stat", "args": {"path": abs_path}, "ok": True})
    return _ok("stat", info)


@app.post("/tool/db_schema")
def db_schema() -> dict:
    res = DB.schema()
    append_audit(AUDIT_LOG, {"tool": "db_schema", "args": {}, "ok": True})
    return _ok("db_schema", res)


@app.post("/tool/db_query")
def db_query(inp: SQLIn) -> dict:
    try:
        res = DB.query(inp.sql, inp.params)
    except Exception as exc:
        err = str(exc)
        append_audit(AUDIT_LOG, {"tool": "db_query", "args": {"sql": inp.sql}, "ok": False, "error": err})
        return _fail("db_query", err)
    append_audit(AUDIT_LOG, {"tool": "db_query", "args": {"sql": inp.sql}, "ok": True})
    return _ok("db_query", res)


@app.post("/tool/db_exec")
def db_exec(inp: SQLIn) -> dict:
    # --- SQL policy check ---
    ok, msg = check_sql_write(POLICY, inp.sql)
    if not ok:
        append_audit(AUDIT_LOG, {"tool": "db_exec", "args": {"sql": inp.sql}, "ok": False, "error": msg})
        return _fail("db_exec", msg)

    row_limit = POLICY.get("db", {}).get("write_row_limit")
    try:
        res = DB.exec(
            inp.sql,
            inp.params,
            force_transaction=POLICY["db"]["force_transaction"],
            row_limit=row_limit,
        )
    except (ValueError, OSError) as exc:
        err = str(exc)
        append_audit(AUDIT_LOG, {"tool": "db_exec", "args": {"sql": inp.sql}, "ok": False, "error": err})
        return _fail("db_exec", err)

    append_audit(AUDIT_LOG, {
        "tool": "db_exec",
        "args": {"sql": inp.sql},
        "ok": True,
        "result": {"rowcount": res.get("rowcount")},
    })
    return _ok("db_exec", res)
