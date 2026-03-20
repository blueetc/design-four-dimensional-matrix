"""four_dim_matrix.memory – lightweight session & preference store.

This subpackage provides a pure-stdlib, zero-dependency persistent memory
layer for the four-dimensional matrix system.  It stores JSON files in
``~/.four_dim_matrix/`` so that the CLI and dashboard can remember:

* **Session history** – recent database scans (source, time, cell counts).
* **User preferences** – default output directory, preferred colour preset, etc.
* **Cached analysis results** – last-seen matrix summaries keyed by database path.

Public API::

    from four_dim_matrix.memory import MemoryStore, SessionRecord

    store = MemoryStore()               # opens ~/.four_dim_matrix/memory.json
    store.record_session("mydb.db", cells=12, colors=12, label="My DB")
    for rec in store.recent_sessions(5):
        print(rec.label, rec.timestamp, rec.cell_count)

    store.set_preference("output_dir", "./outputs")
    print(store.get_preference("output_dir"))
"""

from .store import MemoryStore, SessionRecord

__all__ = ["MemoryStore", "SessionRecord"]
