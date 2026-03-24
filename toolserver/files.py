"""File read/write helpers with optional backup and size guard."""

from __future__ import annotations

import os
import shutil


def read_file(path: str) -> dict:
    """Read *path* and return its content."""
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        return {"path": path, "content": fh.read()}


def write_file(
    path: str,
    content: str,
    backup: bool = True,
    max_bytes: int = 500_000,
) -> dict:
    """Write *content* to *path* with optional backup and size check."""
    encoded = content.encode("utf-8")
    if len(encoded) > max_bytes:
        raise ValueError(f"content too large: {len(encoded)} bytes > {max_bytes}")
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    backup_path = None
    if backup and os.path.exists(path):
        backup_path = path + ".bak"
        shutil.copy2(path, backup_path)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(content)
    return {
        "path": path,
        "bytes": len(encoded),
        "backup": backup,
        "backup_path": backup_path,
    }
