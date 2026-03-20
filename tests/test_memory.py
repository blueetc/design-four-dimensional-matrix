"""Tests for four_dim_matrix.memory – persistent session & preference store.

All tests use a temporary directory so they never touch the user's real
~/.four_dim_matrix/ store.
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

import pytest

from four_dim_matrix.memory import MemoryStore, SessionRecord
from four_dim_matrix.memory.store import MAX_SESSIONS


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def store(tmp_path: Path) -> MemoryStore:
    """Return a MemoryStore backed by a temporary file."""
    return MemoryStore(path=tmp_path / "memory.json")


# ---------------------------------------------------------------------------
# SessionRecord dataclass
# ---------------------------------------------------------------------------

class TestSessionRecord:
    def test_default_timestamp_is_iso(self):
        rec = SessionRecord(source="test.db")
        assert "T" in rec.timestamp  # ISO-8601

    def test_defaults(self):
        rec = SessionRecord(source="test.db")
        assert rec.cell_count == 0
        assert rec.color_count == 0
        assert rec.label == ""
        assert rec.extra == {}

    def test_explicit_fields(self):
        rec = SessionRecord(
            source="prod.db",
            cell_count=10,
            color_count=10,
            label="Production",
            extra={"domains": ["user", "revenue"]},
        )
        assert rec.cell_count == 10
        assert rec.label == "Production"
        assert rec.extra["domains"] == ["user", "revenue"]


# ---------------------------------------------------------------------------
# MemoryStore – file lifecycle
# ---------------------------------------------------------------------------

class TestMemoryStoreFile:
    def test_creates_file_on_first_write(self, tmp_path: Path):
        path = tmp_path / "sub" / "memory.json"
        s = MemoryStore(path=path)
        s.record_session("test.db")
        assert path.exists()

    def test_file_is_valid_json(self, store: MemoryStore):
        store.record_session("a.db")
        with open(store._path, encoding="utf-8") as f:
            data = json.load(f)
        assert "sessions" in data
        assert "preferences" in data

    def test_graceful_on_corrupt_file(self, tmp_path: Path):
        path = tmp_path / "bad.json"
        path.write_text("NOT JSON !!!")
        s = MemoryStore(path=path)
        # Should not raise; returns empty scaffold
        assert s.recent_sessions() == []
        assert s.all_preferences() == {}

    def test_graceful_on_missing_file(self, tmp_path: Path):
        s = MemoryStore(path=tmp_path / "nonexistent.json")
        assert s.recent_sessions() == []


# ---------------------------------------------------------------------------
# MemoryStore – session history
# ---------------------------------------------------------------------------

class TestMemoryStoreSessions:
    def test_record_and_retrieve(self, store: MemoryStore):
        rec = store.record_session("my.db", cell_count=5, color_count=5, label="My DB")
        sessions = store.recent_sessions()
        assert len(sessions) == 1
        assert sessions[0].source == "my.db"
        assert sessions[0].cell_count == 5
        assert sessions[0].label == "My DB"

    def test_returns_most_recent_first(self, store: MemoryStore):
        store.record_session("a.db")
        store.record_session("b.db")
        store.record_session("c.db")
        sessions = store.recent_sessions(3)
        assert sessions[0].source == "c.db"
        assert sessions[1].source == "b.db"
        assert sessions[2].source == "a.db"

    def test_n_limits_results(self, store: MemoryStore):
        for i in range(10):
            store.record_session(f"db{i}.sqlite")
        assert len(store.recent_sessions(3)) == 3
        assert len(store.recent_sessions(10)) == 10

    def test_n_larger_than_stored(self, store: MemoryStore):
        store.record_session("only.db")
        assert len(store.recent_sessions(100)) == 1

    def test_clear_sessions(self, store: MemoryStore):
        store.record_session("a.db")
        store.record_session("b.db")
        store.clear_sessions()
        assert store.recent_sessions() == []

    def test_max_sessions_rolling_trim(self, tmp_path: Path):
        s = MemoryStore(path=tmp_path / "max.json")
        for i in range(MAX_SESSIONS + 20):
            s.record_session(f"db{i}.sqlite")
        sessions = s.recent_sessions(MAX_SESSIONS + 50)
        assert len(sessions) == MAX_SESSIONS

    def test_record_returns_session_record(self, store: MemoryStore):
        rec = store.record_session("test.db")
        assert isinstance(rec, SessionRecord)
        assert rec.source == "test.db"

    def test_extra_metadata_persisted(self, store: MemoryStore):
        store.record_session("x.db", extra={"domains": ["user"], "count": 3})
        rec = store.recent_sessions(1)[0]
        assert rec.extra["domains"] == ["user"]
        assert rec.extra["count"] == 3

    def test_output_file_persisted(self, store: MemoryStore):
        store.record_session("x.db", output_file="/tmp/out.html")
        rec = store.recent_sessions(1)[0]
        assert rec.output_file == "/tmp/out.html"

    def test_notes_persisted(self, store: MemoryStore):
        store.record_session("x.db", notes="Some notes here")
        rec = store.recent_sessions(1)[0]
        assert rec.notes == "Some notes here"

    def test_multiple_sessions_accumulate(self, store: MemoryStore):
        for i in range(5):
            store.record_session(f"db{i}.db")
        assert len(store.recent_sessions(10)) == 5


# ---------------------------------------------------------------------------
# MemoryStore – preferences
# ---------------------------------------------------------------------------

class TestMemoryStorePreferences:
    def test_set_and_get_string(self, store: MemoryStore):
        store.set_preference("output_dir", "./outputs")
        assert store.get_preference("output_dir") == "./outputs"

    def test_set_and_get_int(self, store: MemoryStore):
        store.set_preference("viz_port", 8080)
        assert store.get_preference("viz_port") == 8080

    def test_set_and_get_bool(self, store: MemoryStore):
        store.set_preference("auto_open", True)
        assert store.get_preference("auto_open") is True

    def test_default_when_missing(self, store: MemoryStore):
        assert store.get_preference("missing_key", "default_val") == "default_val"

    def test_default_none_when_missing(self, store: MemoryStore):
        assert store.get_preference("nope") is None

    def test_overwrite(self, store: MemoryStore):
        store.set_preference("k", "v1")
        store.set_preference("k", "v2")
        assert store.get_preference("k") == "v2"

    def test_all_preferences(self, store: MemoryStore):
        store.set_preference("a", 1)
        store.set_preference("b", "x")
        prefs = store.all_preferences()
        assert prefs == {"a": 1, "b": "x"}

    def test_clear_preferences(self, store: MemoryStore):
        store.set_preference("a", 1)
        store.clear_preferences()
        assert store.all_preferences() == {}

    def test_preferences_independent_of_sessions(self, store: MemoryStore):
        store.set_preference("key", "value")
        store.record_session("db.sqlite")
        assert store.get_preference("key") == "value"
        assert len(store.recent_sessions()) == 1

    def test_clear_preferences_keeps_sessions(self, store: MemoryStore):
        store.record_session("db.sqlite")
        store.set_preference("k", "v")
        store.clear_preferences()
        assert len(store.recent_sessions()) == 1


# ---------------------------------------------------------------------------
# MemoryStore – summary
# ---------------------------------------------------------------------------

class TestMemoryStoreSummary:
    def test_empty_summary(self, store: MemoryStore):
        s = store.summary()
        assert s["session_count"] == 0
        assert s["preference_count"] == 0
        assert s["most_recent"] is None

    def test_summary_after_records(self, store: MemoryStore):
        store.record_session("a.db")
        store.record_session("b.db")
        store.set_preference("x", 1)
        s = store.summary()
        assert s["session_count"] == 2
        assert s["preference_count"] == 1
        assert s["most_recent"] is not None

    def test_summary_store_path(self, tmp_path: Path):
        path = tmp_path / "mem.json"
        s = MemoryStore(path=path)
        assert s.summary()["store_path"] == str(path)


# ---------------------------------------------------------------------------
# Package-level import
# ---------------------------------------------------------------------------

class TestPackageImport:
    def test_importable_from_package(self):
        from four_dim_matrix import MemoryStore as MS, SessionRecord as SR
        assert MS is MemoryStore
        assert SR is SessionRecord

    def test_memory_subpackage_import(self):
        from four_dim_matrix.memory import MemoryStore as MS
        assert MS is MemoryStore
