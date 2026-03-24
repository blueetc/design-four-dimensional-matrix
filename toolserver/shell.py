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
    out = (proc.stdout or "") + (proc.stderr or "")
    truncated = False
    if len(out.encode("utf-8", errors="ignore")) > max_output_bytes:
        out = out[:max_output_bytes]
        truncated = True
    return {
        "exit_code": proc.returncode,
        "output": out,
        "truncated": truncated,
        "platform": platform.platform(),
        "cwd": cwd,
        "command": command,
    }
