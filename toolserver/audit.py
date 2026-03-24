"""Immutable append-only JSONL audit log."""

from __future__ import annotations

import json
import os
import time
from typing import Any


def append_audit(log_path: str, event: dict[str, Any]) -> None:
    """Append a single audit event (with UTC timestamp) to *log_path*."""
    os.makedirs(os.path.dirname(log_path) or ".", exist_ok=True)
    event = dict(event)
    event["ts"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    with open(log_path, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(event, ensure_ascii=False) + "\n")
