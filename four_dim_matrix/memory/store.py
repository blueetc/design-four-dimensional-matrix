"""Persistent JSON-backed memory store for four-dimensional matrix sessions.

The store lives at ``~/.four_dim_matrix/memory.json`` (or a custom path)
and keeps a rolling history of the last *N* sessions together with a flat
key/value preferences dictionary.

Design goals
------------
* **Zero extra dependencies** – uses only the Python standard library.
* **Thread-safe reads** – the file is read fresh on every public query so
  that concurrent processes see consistent data.
* **Graceful degradation** – any I/O or JSON error silently produces an
  empty store; the rest of the application is never disrupted.
* **Minimal footprint** – the entire store is a single ≈ few-KB JSON file.
"""

from __future__ import annotations

import json
import os
import threading
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Default paths
# ---------------------------------------------------------------------------

_DEFAULT_STORE_DIR = Path.home() / ".four_dim_matrix"
_DEFAULT_STORE_FILE = _DEFAULT_STORE_DIR / "memory.json"

#: Maximum number of session records retained in the store.
MAX_SESSIONS: int = 100


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

@dataclass
class SessionRecord:
    """A single database-scan session persisted in memory.

    Attributes:
        source: Path or connection string for the scanned database.
        timestamp: ISO-8601 string of when the scan was performed.
        cell_count: Number of DataMatrix cells generated.
        color_count: Number of ColorMatrix cells generated.
        label: Human-readable label for the database (e.g. ``"E-commerce DB"``).
        output_file: Path to the exported JSON/HTML file, if any.
        notes: Free-form notes added by the user or the wizard.
        extra: Arbitrary additional metadata (domain counts, lifecycle stages, …).
    """

    source: str
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    cell_count: int = 0
    color_count: int = 0
    label: str = ""
    output_file: str = ""
    notes: str = ""
    extra: Dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Store
# ---------------------------------------------------------------------------

class MemoryStore:
    """JSON-backed persistent memory store.

    Parameters:
        path: Path to the JSON file.  Defaults to
            ``~/.four_dim_matrix/memory.json``.

    The file has the following schema::

        {
            "sessions": [<SessionRecord as dict>, ...],
            "preferences": {"output_dir": "./outputs", ...}
        }
    """

    def __init__(self, path: Optional[os.PathLike] = None) -> None:
        self._path = Path(path) if path else _DEFAULT_STORE_FILE
        self._lock = threading.Lock()
        self._ensure_dir()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _ensure_dir(self) -> None:
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
        except OSError:
            pass

    def _load(self) -> Dict[str, Any]:
        """Load the JSON file; return an empty scaffold on any error."""
        try:
            with open(self._path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            if not isinstance(data, dict):
                return self._empty()
            data.setdefault("sessions", [])
            data.setdefault("preferences", {})
            return data
        except (OSError, json.JSONDecodeError):
            return self._empty()

    def _save(self, data: Dict[str, Any]) -> None:
        """Write *data* to the JSON file, silently ignoring I/O errors."""
        try:
            tmp = self._path.with_suffix(".tmp")
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(data, fh, ensure_ascii=False, indent=2)
            tmp.replace(self._path)
        except OSError:
            pass

    @staticmethod
    def _empty() -> Dict[str, Any]:
        return {"sessions": [], "preferences": {}}

    # ------------------------------------------------------------------
    # Session history
    # ------------------------------------------------------------------

    def record_session(
        self,
        source: str,
        *,
        cell_count: int = 0,
        color_count: int = 0,
        label: str = "",
        output_file: str = "",
        notes: str = "",
        extra: Optional[Dict[str, Any]] = None,
    ) -> SessionRecord:
        """Append a new session record and return it.

        The store retains at most :data:`MAX_SESSIONS` records; the oldest
        entries are discarded when the limit is exceeded.

        Parameters:
            source: Database path or connection string.
            cell_count: Number of DataMatrix cells produced.
            color_count: Number of ColorMatrix cells produced.
            label: Human-readable name for this session.
            output_file: Path to the output HTML/JSON file.
            notes: Free-form notes.
            extra: Any additional metadata to persist.

        Returns:
            The newly created :class:`SessionRecord`.
        """
        record = SessionRecord(
            source=source,
            cell_count=cell_count,
            color_count=color_count,
            label=label or source,
            output_file=output_file,
            notes=notes,
            extra=extra or {},
        )
        with self._lock:
            data = self._load()
            data["sessions"].append(asdict(record))
            # Trim to MAX_SESSIONS (keep most recent)
            if len(data["sessions"]) > MAX_SESSIONS:
                data["sessions"] = data["sessions"][-MAX_SESSIONS:]
            self._save(data)
        return record

    def recent_sessions(self, n: int = 10) -> List[SessionRecord]:
        """Return the *n* most recent :class:`SessionRecord` objects.

        Returns an empty list if the store is empty or the file cannot be
        read.

        Parameters:
            n: Maximum number of records to return (most recent first).
        """
        data = self._load()
        raw = data["sessions"][-n:]
        raw.reverse()  # most recent first
        result: List[SessionRecord] = []
        for item in raw:
            try:
                result.append(SessionRecord(**item))
            except (TypeError, KeyError):
                pass
        return result

    def clear_sessions(self) -> None:
        """Remove all session records from the store."""
        with self._lock:
            data = self._load()
            data["sessions"] = []
            self._save(data)

    # ------------------------------------------------------------------
    # Preferences
    # ------------------------------------------------------------------

    def set_preference(self, key: str, value: Any) -> None:
        """Persist a user preference.

        Parameters:
            key: Preference name (e.g. ``"output_dir"``).
            value: JSON-serialisable value.
        """
        with self._lock:
            data = self._load()
            data["preferences"][key] = value
            self._save(data)

    def get_preference(self, key: str, default: Any = None) -> Any:
        """Retrieve a previously persisted preference.

        Parameters:
            key: Preference name.
            default: Value to return when *key* is not found.
        """
        return self._load()["preferences"].get(key, default)

    def all_preferences(self) -> Dict[str, Any]:
        """Return a copy of the entire preferences dictionary."""
        return dict(self._load()["preferences"])

    def clear_preferences(self) -> None:
        """Remove all stored preferences."""
        with self._lock:
            data = self._load()
            data["preferences"] = {}
            self._save(data)

    # ------------------------------------------------------------------
    # Convenience
    # ------------------------------------------------------------------

    def summary(self) -> Dict[str, Any]:
        """Return a high-level summary of the store contents."""
        data = self._load()
        sessions = data["sessions"]
        return {
            "store_path": str(self._path),
            "session_count": len(sessions),
            "preference_count": len(data["preferences"]),
            "most_recent": sessions[-1]["timestamp"] if sessions else None,
        }

    def __repr__(self) -> str:  # pragma: no cover
        s = self.summary()
        return (
            f"MemoryStore(path={s['store_path']!r}, "
            f"sessions={s['session_count']}, "
            f"prefs={s['preference_count']})"
        )
