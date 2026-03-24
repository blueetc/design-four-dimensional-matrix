"""File read/write helpers with optional backup, size guard, and idempotency."""

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
    """Write *content* to *path* with optional backup, size check, and diff.

    If the file already exists and its content is identical to *content*,
    the write is skipped (idempotency) and ``skipped`` is set to ``True``
    in the return value.
    """
    encoded = content.encode("utf-8")
    if len(encoded) > max_bytes:
        raise ValueError(f"content too large: {len(encoded)} bytes > {max_bytes}")

    # Idempotency: skip if content unchanged
    if os.path.isfile(path):
        try:
            with open(path, "r", encoding="utf-8", errors="replace") as fh:
                existing = fh.read()
            if existing == content:
                return {
                    "path": path,
                    "bytes": len(encoded),
                    "backup": False,
                    "backup_path": None,
                    "skipped": True,
                }
        except OSError:
            pass  # proceed to write

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
        "skipped": False,
    }
