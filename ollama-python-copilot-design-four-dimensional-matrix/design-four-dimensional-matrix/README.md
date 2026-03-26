# four-dim-matrix

A dual-matrix knowledge system that converts relational databases into an
intuitive four-dimensional colour space.

## Concept

Two mirrored four-dimensional matrices let you *see* an entire database at a
glance:

| Coordinate | Meaning (schema mode) | Visual encoding |
|---|---|---|
| `t` | Snapshot timestamp | Colour temperature shift over time |
| `x` | Column count (schema width) | Saturation |
| `y` | Row count (data volume) | Lightness |
| `z` | Table index (each table = one topic) | Hue |

**Matrix 1 – Data Matrix**: stores full column/table metadata as a JSON
payload at every `(t, x, y, z)` address.

**Matrix 2 – Colour Matrix**: stores a `#rrggbb` colour at the same address.
Hovering over any colour block in Matrix 2 reveals the corresponding data
record in Matrix 1.

Loading a database schema into the two matrices *is* the act of rapid
database cognition: large, wide tables appear as bright, vivid blocks; small
lookup tables appear as muted specks; schema changes between snapshots are
detected automatically via `diff()`.

## Quick start

```python
import sqlite3
from four_dim_matrix import DatabaseAdapter

# Point at any SQLite file (or use from_connection for PostgreSQL / MySQL)
adapter = DatabaseAdapter.from_sqlite("my_database.db")
kb = adapter.to_knowledge_base()

# Inspect the colour snapshot – one topic per table
snap = kb.snapshot(t=adapter.snapshot_time)
for topic in snap["topics"]:
    print(topic["hex_color"], topic["total_y"], "rows –", topic)

# Reverse-lookup: colour block → full table metadata
results = kb.lookup_by_color("#3d6e9e")
print(results[0].payload)

# Detect schema drift between two snapshots
adapter2 = DatabaseAdapter.from_sqlite("my_database.db")  # re-introspect later
print(adapter.diff(adapter2))
```

## Package layout

```
four_dim_matrix/
├── data_matrix.py    # DataPoint + DataMatrix (sparse 4D data store)
├── color_matrix.py   # ColorPoint + ColorMatrix (4D colour store)
├── color_mapping.py  # ColorConfig + ColorMapper (HSL colour mapping)
├── knowledge_base.py # KnowledgeBase (high-level dual-matrix API)
└── db_adapter.py     # DatabaseAdapter (DB → both matrices in one call)
```
