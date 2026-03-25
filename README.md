# Ollama Local Agent

A fully local automation agent powered by [Ollama](https://ollama.com) with a
policy-controlled tool server, cross-platform command execution, file
management, and database access — all guarded by strict security policies and
an immutable audit log.

## Architecture

```
┌──────────────┐       ┌──────────────────┐       ┌──────────────────┐
│  Ollama LLM  │◄─────►│  Agent (Python)  │◄─────►│  Tool Server     │
│ localhost:    │       │  Conversation    │       │  FastAPI :7331   │
│    11434      │       │  Loop            │       │  Policy Engine   │
└──────────────┘       └──────────────────┘       │  Audit Log       │
                                                  └──────────────────┘
```

1. **Ollama** — local LLM inference (`http://localhost:11434`)
2. **Agent** (`agent/`) — drives the conversation loop, parses tool calls from
   the model, forwards them to the tool server, and feeds results back.
3. **Tool Server** (`toolserver/`) — FastAPI service on `127.0.0.1:7331` that
   actually executes operations, with every call validated against the policy
   engine and recorded in the audit log.

## Quick Start

### Prerequisites

* Python ≥ 3.10
* [Ollama](https://ollama.com) installed and running
* A model pulled (e.g. `ollama pull qwen2.5:7b`)

### 1. Install dependencies

```bash
pip install -e .
```

### 2. Start the Tool Server

```bash
uvicorn toolserver.server:app --host 127.0.0.1 --port 7331
```

### 3. Run the Agent

```bash
# Default demo task (single-shot)
python -m agent.main

# Custom task (single-shot)
python -m agent.main "列出workspace目录内容"

# Interactive REPL — continuous tasks with follow-up questions
python -m agent.main -i
python -m agent.main --interactive

# Interactive mode with a specific model
python -m agent.main --interactive --model qwen2.5:14b
```

In interactive mode the conversation history is preserved across turns, so you
can ask follow-up questions that reference previous results without the agent
repeating earlier work.

## Directory Layout

```
├── agent/
│   ├── main.py            # Conversation loop
│   ├── ollama_client.py   # Ollama HTTP client
│   └── prompts.py         # System / developer prompts
├── toolserver/
│   ├── server.py          # FastAPI tool endpoints
│   ├── policy.py          # Security policy enforcement
│   ├── shell.py           # Cross-platform command execution
│   ├── files.py           # File read/write with backup + idempotency
│   ├── db.py              # SQLite (extensible to PG/MySQL/MSSQL)
│   ├── audit.py           # JSONL audit logger
│   └── config.py          # YAML config loader
├── config/
│   ├── policy.yaml        # Security policy (allowlists, deny patterns, etc.)
│   └── databases.yaml     # Database connection definitions
├── pyproject.toml
└── README.md
```

## Available Tools

| Tool | Endpoint | Description |
|------|----------|-------------|
| `get_system_info` | `POST /tool/get_system_info` | OS, shell, user, workspace, disk info |
| `run_command` | `POST /tool/run_command` | Execute shell commands (allow-listed) |
| `read_file` | `POST /tool/read_file` | Read a file inside workspace |
| `write_file` | `POST /tool/write_file` | Write a file (with auto-backup + idempotency) |
| `list_dir` | `POST /tool/list_dir` | List directory contents |
| `stat` | `POST /tool/stat` | File/directory metadata |
| `db_schema` | `POST /tool/db_schema` | Show database schema |
| `db_query` | `POST /tool/db_query` | Run a read-only SQL query |
| `db_exec` | `POST /tool/db_exec` | Execute a write SQL statement (policy-checked) |
| `analyze_fields` | `POST /tool/analyze_fields` | Sample tables and infer field semantics (time/dimension/measure) |
| `design_wide_table` | `POST /tool/design_wide_table` | Auto-design a wide-table schema from analysis |
| `create_wide_table` | `POST /tool/create_wide_table` | Create the wide table in the database |
| `etl_to_wide_table` | `POST /tool/etl_to_wide_table` | Incrementally load source data into the wide table |
| `visualize_3d` | `POST /tool/visualize_3d` | Generate an interactive 3-D scatter HTML |
| `list_models` | `POST /tool/list_models` | List locally available Ollama models |

## Interactive REPL Commands

In interactive mode (`-i`/`--interactive`), the following slash commands are
available:

| Command | Description |
|---------|-------------|
| `/models` | List all locally available Ollama models |
| `/model [name]` | Show or switch the active model |
| `/ask <model> <question>` | One-shot question to a different model |
| `/panel <question>` | Ask all Panel models the same question |
| `/panel+ <model>` | Add a model to the Panel list |
| `/panel- <model>` | Remove a model from the Panel list |
| `/orch <task>` | Multi-model orchestration (director–worker pattern) |
| `/knowledge` | Display the built-in knowledge base |
| `/status` | Show current session status (model, panel, connectivity) |
| `/help` | Show available commands |
| `exit`/`quit`/`q`/`退出`/`结束` | Exit the session |

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `OLLAMA_MODEL` | `qwen2.5:7b` | Default Ollama model for the agent |

## Security Model

All tool calls pass through the **Policy Engine** (`toolserver/policy.py`)
before execution:

### Command Execution (`run_command`)

* **Command allowlist** — per-OS list of permitted executables (Linux, macOS,
  Windows each have separate lists)
* **Deny patterns** — regex patterns that block dangerous commands
  (`rm -r*`, `mkfs`, `diskpart`, `shutdown`, `passwd`, `visudo`, firewall
  changes, …)
* **Workspace sandbox** — `cwd` is restricted to `workspace_root`
* **Execution limits** — per-command timeout and max output size

### File Operations (`write_file`)

* **Workspace sandbox** — only files inside `workspace_root` may be written
* **Sensitive-path blocklist** — writes to system directories (`/etc`,
  `/boot`, `C:\Windows`, …) are always blocked
* **File-extension allowlist** — only approved extensions may be written
* **Auto-backup** — existing files are backed up to `.bak` before overwrite
* **Idempotency** — if the content is unchanged the write is skipped

### Database Operations (`db_exec`)

* **SQL deny patterns** — `DROP DATABASE`, `TRUNCATE`, `ALTER USER`,
  `GRANT`, `REVOKE` are blocked
* **Forced WHERE clause** — `UPDATE` and `DELETE` without `WHERE` are rejected
* **Row-count estimation** — before executing an `UPDATE`/`DELETE`, the tool
  server estimates the affected rows; if the count exceeds
  `db.write_row_limit` the statement is rejected
* **Forced transactions** — every write runs inside `BEGIN` / `COMMIT` with
  automatic `ROLLBACK` on failure

### Audit

Every tool invocation is recorded in an append-only **JSONL audit log** at
`<workspace_root>/audit.jsonl`.

## Configuration

### `config/policy.yaml`

Controls all security boundaries:

| Section | Purpose |
|---------|---------|
| `workspace_root` | Root directory for all file operations |
| `allowlist` | Per-OS command executable allowlists |
| `deny_patterns` | Regex patterns blocking dangerous commands |
| `sensitive_paths` | System directories where writes are always blocked |
| `files` | Extension allowlist, max write size |
| `db` | Transaction policy, row-count limit, SQL deny patterns |
| `max_exec_seconds` | Per-command timeout |
| `max_output_bytes` | Max captured output per command |

### `config/databases.yaml`

Defines database connections. Ships with SQLite by default; PostgreSQL, MySQL,
and SQL Server examples are included as comments.

## Extending Database Support

The `toolserver/db.py` module currently implements SQLite. To add another
backend, create a class that exposes the same interface (`schema()`, `query()`,
`exec()`, `estimate_affected_rows()`) and register it in `server.py`.
Recommended drivers:

| Database | Driver |
|----------|--------|
| PostgreSQL | `psycopg` |
| MySQL | `pymysql` or `mysqlclient` |
| SQL Server | `pyodbc` |

## License

MIT
