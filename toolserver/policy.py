"""Security policy enforcement for commands and file paths."""

from __future__ import annotations

import os
import platform
import re
import shlex
from typing import Tuple


def current_os_key() -> str:
    """Return a normalised OS key: ``linux``, ``darwin``, or ``windows``."""
    sys_name = platform.system().lower()
    if "windows" in sys_name:
        return "windows"
    if "darwin" in sys_name:
        return "darwin"
    return "linux"


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


def enforce_workspace(policy: dict, path: str) -> Tuple[bool, str, str]:
    """Ensure *path* resolves inside ``workspace_root``.

    Returns ``(True, "ok", abs_path)`` on success or
    ``(False, reason, abs_path)`` on denial.
    """
    root = policy["workspace_root"]
    abs_path = os.path.abspath(os.path.expanduser(path))
    if not (abs_path == root or abs_path.startswith(root + os.sep)):
        return False, f"Path outside workspace_root: {abs_path}", abs_path
    return True, "ok", abs_path
