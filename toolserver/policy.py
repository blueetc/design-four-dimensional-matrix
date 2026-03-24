"""Security policy enforcement for commands, file paths, and SQL statements."""

from __future__ import annotations

import os
import platform
import re
import shlex
from typing import Tuple


# ---------------------------------------------------------------------------
# OS detection
# ---------------------------------------------------------------------------

def current_os_key() -> str:
    """Return a normalised OS key: ``linux``, ``darwin``, or ``windows``."""
    sys_name = platform.system().lower()
    if "windows" in sys_name:
        return "windows"
    if "darwin" in sys_name:
        return "darwin"
    return "linux"


# ---------------------------------------------------------------------------
# Command policy
# ---------------------------------------------------------------------------

def check_command(policy: dict, command: str) -> Tuple[bool, str]:
    """Validate *command* against deny-patterns and the per-OS allowlist.

    Returns ``(True, "ok")`` on success or ``(False, reason)`` on denial.
    """
    for pat in policy.get("deny_patterns", []):
        if re.search(pat, command):
            return False, f"Command denied by pattern: {pat}"

    os_key = current_os_key()
    allow = set(policy["allowlist"].get(os_key, []))
    posix = os_key != "windows"
    try:
        parts = shlex.split(command, posix=posix)
    except ValueError:
        return False, "Failed to parse command"
    if not parts:
        return False, "Empty command"
    exe = os.path.basename(parts[0]).lower()
    if exe not in {a.lower() for a in allow}:
        return False, f"Executable not in allowlist for {os_key}: {exe}"
    return True, "ok"


# ---------------------------------------------------------------------------
# File-path policy
# ---------------------------------------------------------------------------

def enforce_workspace(policy: dict, path: str) -> Tuple[bool, str, str]:
    """Ensure *path* resolves inside ``workspace_root``.

    Returns ``(True, "ok", abs_path)`` on success or
    ``(False, reason, abs_path)`` on denial.
    """
    root = os.path.realpath(policy["workspace_root"])
    abs_path = os.path.realpath(os.path.abspath(os.path.expanduser(path)))
    if not (abs_path == root or abs_path.startswith(root + os.sep)):
        return False, f"Path outside workspace_root: {abs_path}", abs_path
    return True, "ok", abs_path


def check_sensitive_path(policy: dict, abs_path: str) -> Tuple[bool, str]:
    """Block writes to OS-sensitive directories listed in policy.

    Returns ``(True, "ok")`` when the path is safe, or
    ``(False, reason)`` when it falls inside a blocked prefix.
    """
    blocked: list[str] = policy.get("sensitive_paths", [])
    for prefix in blocked:
        expanded = os.path.realpath(os.path.expanduser(prefix))
        if abs_path == expanded or abs_path.startswith(expanded + os.sep):
            return False, f"Path inside sensitive directory: {expanded}"
    return True, "ok"


# ---------------------------------------------------------------------------
# SQL policy
# ---------------------------------------------------------------------------

# Patterns that match dangerous DDL / bulk-destructive statements.
_SQL_DENY_PATTERNS: list[str] = [
    r"(?i)\bDROP\s+DATABASE\b",
    r"(?i)\bTRUNCATE\b",
]

# Statements that *require* a WHERE clause.
_SQL_REQUIRE_WHERE: list[str] = [
    r"(?i)^\s*(UPDATE|DELETE)\b",
]


def check_sql_write(policy: dict, sql: str) -> Tuple[bool, str]:
    """Validate a write-SQL statement against the configured deny list.

    Checks performed (in order):
    1. Reject statements matching ``db.sql_deny_patterns`` (or built-in
       defaults like ``DROP DATABASE``, ``TRUNCATE``).
    2. Require a ``WHERE`` clause for ``UPDATE`` / ``DELETE``.

    Returns ``(True, "ok")`` or ``(False, reason)``.
    """
    deny = policy.get("db", {}).get("sql_deny_patterns", _SQL_DENY_PATTERNS)
    for pat in deny:
        if re.search(pat, sql):
            return False, f"SQL denied by pattern: {pat}"

    for pat in _SQL_REQUIRE_WHERE:
        if re.search(pat, sql) and not re.search(r"(?i)\bWHERE\b", sql):
            return False, "UPDATE/DELETE without WHERE clause is not allowed"

    return True, "ok"
