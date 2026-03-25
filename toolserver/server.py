"""FastAPI tool server – local HTTP endpoint for the agent to call."""

from __future__ import annotations

import getpass
import os
import platform
import shutil

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from starlette.responses import StreamingResponse
from pydantic import BaseModel, Field

from .audit import append_audit
from .config import load_policy
from .db import SQLiteDB
from .field_analyzer import analyze_database as _analyze_db
from .files import read_file as _read
from .files import write_file as _write
from .policy import (
    check_command,
    check_sensitive_path,
    check_sql_write,
    enforce_workspace,
)
from .shell import run_command as _run
from .visualizer import save_3d_html
from .wide_table import create_wide_table, design_wide_table, incremental_etl
from .dashboard import render_dashboard

# Ollama model discovery
from agent.ollama_client import list_models as _list_ollama_models

_POLICY_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "policy.yaml")
POLICY = load_policy(_POLICY_PATH)
AUDIT_LOG = os.path.join(POLICY["workspace_root"], "audit.jsonl")
DB = SQLiteDB(os.path.join(POLICY["workspace_root"], "agent.sqlite3"))

app = FastAPI(title="Ollama Local Agent – Tool Server")


# ---------------------------------------------------------------------------
# Dashboard (root path)
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
def dashboard() -> str:
    """Serve the interactive dashboard at the root URL."""
    return render_dashboard()


# ---------------------------------------------------------------------------
# Chat API – natural-language conversation with the agent
# ---------------------------------------------------------------------------

import threading

# In-memory session store: session_id → messages list.
# Allows multi-turn conversations from the web UI.
_CHAT_SESSIONS: dict[str, list[dict]] = {}
_CHAT_LOCK = threading.Lock()

# Set of session_ids that have been requested to cancel.
_CANCEL_REQUESTS: set[str] = set()

# Maximum sessions kept in memory to prevent unbounded growth.
_MAX_SESSIONS = 64


class ChatIn(BaseModel):
    """User message sent to the chat API."""
    message: str
    model: str = Field(default="")
    session_id: str = Field(default="default")


class ChatOut(BaseModel):
    """Agent reply returned from the chat API."""
    reply: str
    model: str
    session_id: str
    tool_calls: list[dict] = Field(default_factory=list)
    error: str | None = None


@app.post("/api/chat", response_model=ChatOut)
def chat_endpoint(inp: ChatIn) -> ChatOut:
    """Run the agent loop for one user message and return the reply.

    Maintains per-session conversation history so the LLM sees prior
    context, enabling follow-up questions.
    """
    import json as _json

    from agent.main import _init_messages, _try_parse_tool_call, call_tool
    from agent.main import MAX_ITERATIONS, TOOLS as AGENT_TOOLS
    from agent.ollama_client import OllamaConnectionError, ollama_chat

    model = inp.model or os.environ.get("OLLAMA_MODEL", "qwen2.5:7b")

    # Retrieve or create session history (thread-safe).
    with _CHAT_LOCK:
        if inp.session_id not in _CHAT_SESSIONS:
            # Evict oldest session if at capacity.
            if len(_CHAT_SESSIONS) >= _MAX_SESSIONS:
                oldest = next(iter(_CHAT_SESSIONS))
                del _CHAT_SESSIONS[oldest]
            _CHAT_SESSIONS[inp.session_id] = _init_messages()
        messages = _CHAT_SESSIONS[inp.session_id]

    messages.append({"role": "user", "content": inp.message})

    tool_calls: list[dict] = []

    try:
        for _ in range(MAX_ITERATIONS):
            resp = ollama_chat(model=model, messages=messages)
            content: str = resp["message"]["content"].strip()

            call = _try_parse_tool_call(content)
            if call is not None:
                tool_name = call["tool"]
                args = call.get("args", {})

                if tool_name not in AGENT_TOOLS:
                    messages.append({"role": "assistant", "content": content})
                    break

                tool_result = call_tool(tool_name, args)
                ok = tool_result.get("ok", False)
                tool_calls.append({
                    "tool": tool_name,
                    "args": args,
                    "ok": ok,
                })
                messages.append({"role": "assistant", "content": content})
                messages.append({
                    "role": "assistant",
                    "content": f"[tool result] {_json.dumps(tool_result, ensure_ascii=False)}",
                })
                continue

            # Natural-language reply – done.
            messages.append({"role": "assistant", "content": content})
            append_audit(AUDIT_LOG, {
                "tool": "chat",
                "args": {"message": inp.message[:200], "model": model},
                "ok": True,
            })
            return ChatOut(
                reply=content,
                model=model,
                session_id=inp.session_id,
                tool_calls=tool_calls,
            )

        # Max iterations reached – return last content.
        last = messages[-1]["content"] if messages else ""
        return ChatOut(
            reply=last,
            model=model,
            session_id=inp.session_id,
            tool_calls=tool_calls,
        )
    except OllamaConnectionError as exc:
        return ChatOut(
            reply="",
            model=model,
            session_id=inp.session_id,
            tool_calls=tool_calls,
            error=str(exc),
        )


@app.get("/api/chat/models")
def chat_models() -> dict:
    """Return available models for the chat UI model selector."""
    models = _list_ollama_models()
    default = os.environ.get("OLLAMA_MODEL", "qwen2.5:7b")
    return {
        "models": [m.get("name", "?") for m in models],
        "default": default,
    }


@app.post("/api/chat/reset")
def chat_reset(session_id: str = "default") -> dict:
    """Clear conversation history for a session."""
    with _CHAT_LOCK:
        if session_id in _CHAT_SESSIONS:
            del _CHAT_SESSIONS[session_id]
    return {"ok": True, "session_id": session_id}


@app.post("/api/chat/cancel")
def chat_cancel(session_id: str = "default") -> dict:
    """Request cancellation of a running agent loop for a session."""
    with _CHAT_LOCK:
        _CANCEL_REQUESTS.add(session_id)
    return {"ok": True, "session_id": session_id}


@app.post("/api/chat/stream")
def chat_stream(inp: ChatIn) -> StreamingResponse:
    """Run the agent loop with Server-Sent Events for real-time progress.

    Emits the following SSE event types:
    - ``thinking``   – agent is calling the LLM (before tool detection)
    - ``tool_start`` – a tool call is about to begin
    - ``tool_done``  – a tool call finished (with ok/error status)
    - ``reply``      – final natural-language answer
    - ``error``      – an error occurred
    - ``cancelled``  – the user cancelled the task
    - ``done``       – stream finished (always sent last)
    """
    import json as _json

    from agent.main import _init_messages, _try_parse_tool_call, call_tool
    from agent.main import MAX_ITERATIONS, TOOLS as AGENT_TOOLS
    from agent.ollama_client import OllamaConnectionError, ollama_chat

    model = inp.model or os.environ.get("OLLAMA_MODEL", "qwen2.5:7b")

    # Retrieve or create session history.
    with _CHAT_LOCK:
        if inp.session_id not in _CHAT_SESSIONS:
            if len(_CHAT_SESSIONS) >= _MAX_SESSIONS:
                oldest = next(iter(_CHAT_SESSIONS))
                del _CHAT_SESSIONS[oldest]
            _CHAT_SESSIONS[inp.session_id] = _init_messages()
        messages = _CHAT_SESSIONS[inp.session_id]
        # Clear any stale cancel request for this session.
        _CANCEL_REQUESTS.discard(inp.session_id)

    messages.append({"role": "user", "content": inp.message})

    def _sse(event: str, data: dict) -> str:
        """Format one Server-Sent Event."""
        payload = _json.dumps(data, ensure_ascii=False)
        return f"event: {event}\ndata: {payload}\n\n"

    def generate():
        tool_calls: list[dict] = []
        step = 0

        try:
            for _ in range(MAX_ITERATIONS):
                # Check for cancellation.
                with _CHAT_LOCK:
                    if inp.session_id in _CANCEL_REQUESTS:
                        _CANCEL_REQUESTS.discard(inp.session_id)
                        yield _sse("cancelled", {"message": "任务已取消"})
                        yield _sse("done", {})
                        return

                step += 1
                yield _sse("thinking", {"step": step, "message": "思考中…"})

                resp = ollama_chat(model=model, messages=messages)
                content: str = resp["message"]["content"].strip()

                call = _try_parse_tool_call(content)
                if call is not None:
                    tool_name = call["tool"]
                    args = call.get("args", {})

                    if tool_name not in AGENT_TOOLS:
                        messages.append({"role": "assistant", "content": content})
                        yield _sse("reply", {
                            "reply": content,
                            "model": model,
                            "tool_calls": tool_calls,
                        })
                        break

                    yield _sse("tool_start", {
                        "step": step,
                        "tool": tool_name,
                        "args": args,
                    })

                    tool_result = call_tool(tool_name, args)
                    ok = tool_result.get("ok", False)
                    tool_calls.append({
                        "tool": tool_name,
                        "args": args,
                        "ok": ok,
                    })

                    yield _sse("tool_done", {
                        "step": step,
                        "tool": tool_name,
                        "ok": ok,
                        "preview": _json.dumps(
                            tool_result.get("result", tool_result.get("error", "")),
                            ensure_ascii=False,
                        )[:300],
                    })

                    messages.append({"role": "assistant", "content": content})
                    messages.append({
                        "role": "assistant",
                        "content": f"[tool result] {_json.dumps(tool_result, ensure_ascii=False)}",
                    })
                    continue

                # Natural-language reply.
                messages.append({"role": "assistant", "content": content})
                append_audit(AUDIT_LOG, {
                    "tool": "chat",
                    "args": {"message": inp.message[:200], "model": model},
                    "ok": True,
                })
                yield _sse("reply", {
                    "reply": content,
                    "model": model,
                    "tool_calls": tool_calls,
                })
                break
            else:
                # Max iterations reached.
                last = messages[-1]["content"] if messages else ""
                yield _sse("reply", {
                    "reply": last,
                    "model": model,
                    "tool_calls": tool_calls,
                })
        except OllamaConnectionError as exc:
            yield _sse("error", {"error": str(exc)})
        except Exception as exc:
            yield _sse("error", {"error": str(exc)})

        yield _sse("done", {})

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


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


class AnalyzeFieldsIn(BaseModel):
    sample_size: int = 200


class DesignWideTableIn(BaseModel):
    analysis: list | None = None  # If None, run analyze first


class EtlIn(BaseModel):
    design: dict | None = None  # If None, auto-design first
    batch_size: int = 500


class Visualize3dIn(BaseModel):
    time_col: str | None = None
    measure_col: str | None = None
    theme_col: str | None = None
    title: str = "Wide Table – 3D Business Space"
    limit: int = 5000


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


@app.post("/tool/list_models")
def list_models_endpoint() -> dict:
    """Return all models available on the local Ollama instance."""
    models = _list_ollama_models()
    append_audit(AUDIT_LOG, {
        "tool": "list_models",
        "args": {},
        "ok": True,
        "result": {"count": len(models)},
    })
    return _ok("list_models", {"models": models})


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


# ---------------------------------------------------------------------------
# Wide-table pipeline endpoints
# ---------------------------------------------------------------------------

# Module-level cache for the latest wide-table design so the agent can call
# tools step-by-step without re-passing the design dict every time.
_LAST_ANALYSIS: list | None = None
_LAST_DESIGN: dict | None = None


@app.post("/tool/analyze_fields")
def analyze_fields(inp: AnalyzeFieldsIn | None = None) -> dict:
    """Sample all tables in the database and infer field semantics."""
    global _LAST_ANALYSIS  # noqa: PLW0603
    sample_size = inp.sample_size if inp else 200
    try:
        conn = DB.connect()
        result = _analyze_db(conn, sample_size=sample_size)
        _LAST_ANALYSIS = result
    except Exception as exc:
        err = str(exc)
        append_audit(AUDIT_LOG, {"tool": "analyze_fields", "args": {"sample_size": sample_size}, "ok": False, "error": err})
        return _fail("analyze_fields", err)
    append_audit(AUDIT_LOG, {
        "tool": "analyze_fields",
        "args": {"sample_size": sample_size},
        "ok": True,
        "result": {"tables": len(result)},
    })
    return _ok("analyze_fields", result)


@app.post("/tool/design_wide_table")
def design_wide_table_endpoint(inp: DesignWideTableIn | None = None) -> dict:
    """Design a wide table schema from field analysis."""
    global _LAST_DESIGN  # noqa: PLW0603
    analysis = (inp.analysis if inp and inp.analysis else None) or _LAST_ANALYSIS
    if not analysis:
        return _fail("design_wide_table", "No analysis data. Call analyze_fields first.")
    try:
        result = design_wide_table(analysis)
        _LAST_DESIGN = result
    except Exception as exc:
        err = str(exc)
        append_audit(AUDIT_LOG, {"tool": "design_wide_table", "args": {}, "ok": False, "error": err})
        return _fail("design_wide_table", err)
    append_audit(AUDIT_LOG, {
        "tool": "design_wide_table",
        "args": {},
        "ok": True,
        "result": {
            "columns": len(result.get("columns", [])),
            "time_column": result.get("time_column"),
        },
    })
    return _ok("design_wide_table", result)


@app.post("/tool/create_wide_table")
def create_wide_table_endpoint() -> dict:
    """Create the wide table in the database using the latest design."""
    design = _LAST_DESIGN
    if not design:
        return _fail("create_wide_table", "No design. Call design_wide_table first.")
    try:
        conn = DB.connect()
        ddl = create_wide_table(conn, design)
    except Exception as exc:
        err = str(exc)
        append_audit(AUDIT_LOG, {"tool": "create_wide_table", "args": {}, "ok": False, "error": err})
        return _fail("create_wide_table", err)
    append_audit(AUDIT_LOG, {"tool": "create_wide_table", "args": {}, "ok": True})
    return _ok("create_wide_table", {"ddl": ddl})


@app.post("/tool/etl_to_wide_table")
def etl_to_wide_table(inp: EtlIn | None = None) -> dict:
    """Incrementally load new rows from source tables into the wide table."""
    design = (inp.design if inp and inp.design else None) or _LAST_DESIGN
    if not design:
        return _fail("etl_to_wide_table", "No design. Call design_wide_table first.")
    batch_size = inp.batch_size if inp else 500
    try:
        conn = DB.connect()
        result = incremental_etl(conn, design, batch_size=batch_size)
    except Exception as exc:
        err = str(exc)
        append_audit(AUDIT_LOG, {"tool": "etl_to_wide_table", "args": {"batch_size": batch_size}, "ok": False, "error": err})
        return _fail("etl_to_wide_table", err)
    append_audit(AUDIT_LOG, {
        "tool": "etl_to_wide_table",
        "args": {"batch_size": batch_size},
        "ok": True,
        "result": result,
    })
    return _ok("etl_to_wide_table", result)


@app.post("/tool/visualize_3d")
def visualize_3d(inp: Visualize3dIn | None = None) -> dict:
    """Generate an interactive 3-D scatter HTML from the wide table."""
    design = _LAST_DESIGN
    if not design:
        return _fail("visualize_3d", "No design. Run the pipeline first.")

    time_col = (inp.time_col if inp and inp.time_col else None) or design.get("time_column", "")
    if not time_col:
        return _fail("visualize_3d", "No time column identified. Specify time_col.")

    measure_cols = design.get("measure_columns", [])
    measure_col = (inp.measure_col if inp and inp.measure_col else None) or (measure_cols[0] if measure_cols else "")
    if not measure_col:
        return _fail("visualize_3d", "No measure column identified. Specify measure_col.")

    dim_cols = design.get("dimension_columns", [])
    theme_col = (inp.theme_col if inp and inp.theme_col else None) or (dim_cols[0] if dim_cols else "")
    if not theme_col:
        return _fail("visualize_3d", "No dimension column for themes. Specify theme_col.")

    title = inp.title if inp else "Wide Table – 3D Business Space"
    limit = inp.limit if inp else 5000
    out_path = os.path.join(POLICY["workspace_root"], "wide_table_3d.html")

    try:
        conn = DB.connect()
        from .wide_table import WIDE_TABLE_NAME
        saved = save_3d_html(conn, WIDE_TABLE_NAME, time_col, measure_col, theme_col, out_path, title=title, limit=limit)
    except Exception as exc:
        err = str(exc)
        append_audit(AUDIT_LOG, {"tool": "visualize_3d", "args": {"time_col": time_col}, "ok": False, "error": err})
        return _fail("visualize_3d", err)

    append_audit(AUDIT_LOG, {
        "tool": "visualize_3d",
        "args": {"time_col": time_col, "measure_col": measure_col, "theme_col": theme_col},
        "ok": True,
        "result": {"path": saved},
    })
    return _ok("visualize_3d", {
        "path": saved,
        "time_col": time_col,
        "measure_col": measure_col,
        "theme_col": theme_col,
    })
