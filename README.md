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
# Default demo task
python -m agent.main

# Custom task
python -m agent.main "列出workspace目录内容"
```

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
│   ├── files.py           # File read/write with backup
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
| `get_system_info` | `POST /tool/get_system_info` | OS, user, workspace info |
| `run_command` | `POST /tool/run_command` | Execute shell commands (allow-listed) |
| `read_file` | `POST /tool/read_file` | Read a file inside workspace |
| `write_file` | `POST /tool/write_file` | Write a file (with auto-backup) |
| `list_dir` | `POST /tool/list_dir` | List directory contents |
| `stat` | `POST /tool/stat` | File/directory metadata |
| `db_schema` | `POST /tool/db_schema` | Show database schema |
| `db_query` | `POST /tool/db_query` | Run a read-only SQL query |
| `db_exec` | `POST /tool/db_exec` | Execute a write SQL statement |

## Security Model

All tool calls pass through the **Policy Engine** (`toolserver/policy.py`)
before execution:

* **Command allowlist** — per-OS list of permitted executables
* **Deny patterns** — regex patterns that block dangerous commands
  (`rm -rf /`, `mkfs`, `diskpart`, `shutdown`, …)
* **Workspace sandbox** — file operations are restricted to `workspace_root`
* **File-extension allowlist** — only approved extensions may be written
* **Execution limits** — command timeout and max output size
* **Database safeguards** — forced transactions, row-count limits

Every tool invocation is recorded in an append-only **JSONL audit log** at
`<workspace_root>/audit.jsonl`.

## Configuration

### `config/policy.yaml`

Controls all security boundaries — workspace root, command allowlists, deny
patterns, file extension limits, database transaction policy, and more.

### `config/databases.yaml`

Defines database connections. Ships with SQLite by default; PostgreSQL, MySQL,
and SQL Server examples are included as comments.

## Extending Database Support

The `toolserver/db.py` module currently implements SQLite. To add another
backend, create a class that exposes the same interface (`schema()`, `query()`,
`exec()`) and register it in `server.py`. Recommended drivers:

| Database | Driver |
|----------|--------|
| PostgreSQL | `psycopg` |
| MySQL | `pymysql` or `mysqlclient` |
| SQL Server | `pyodbc` |

## License

MIT
