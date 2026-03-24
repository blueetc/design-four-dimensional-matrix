"""Cross-platform shell command execution with timeout and output limits."""

from __future__ import annotations

import os
import platform
import subprocess


def run_command(
    command: str,
    cwd: str,
    timeout_s: int,
    max_output_bytes: int,
) -> dict:
    """Execute *command* via the system shell and return structured output."""
    cwd = os.path.abspath(os.path.expanduser(cwd))
    proc = subprocess.run(
        command,
        cwd=cwd,
        shell=True,
        capture_output=True,
        text=True,
        timeout=timeout_s,
    )
    stdout = proc.stdout or ""
    stderr = proc.stderr or ""
    combined = stdout + stderr
    truncated = False
    encoded = combined.encode("utf-8", errors="ignore")
    if len(encoded) > max_output_bytes:
        combined = encoded[:max_output_bytes].decode("utf-8", errors="ignore")
        truncated = True
    return {
        "exit_code": proc.returncode,
        "stdout": stdout if not truncated else combined,
        "stderr": stderr if not truncated else "",
        "output": combined,
        "truncated": truncated,
        "platform": platform.platform(),
        "cwd": cwd,
        "command": command,
    }
