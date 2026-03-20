"""Track A: Primary-key discovery and entity clustering.

Two-phase process:

1. :class:`KeyDiscoveryEngine` – scores each column of a single table as a
   primary-key candidate using a combination of database metadata, naming
   conventions, and cross-table reference analysis.

2. :class:`EntityClusteringEngine` – builds a weighted undirected graph
   where nodes are tables and edges represent inferred FK relationships or
   shared-column signatures, then partitions the graph into business-entity
   clusters using Louvain community detection (via *python-louvain* +
   *networkx*) or a simpler Union-Find fallback when those libraries are
   not available.

The output – a list of :class:`CoreEntity` objects – feeds directly into
Track C (:class:`~four_dim_matrix.z_axis_encoding.ZAxisAllocator`) to assign
each table its hierarchical z-coordinate.

Example::

    from four_dim_matrix import DatabaseAdapter
    from four_dim_matrix.key_discovery import (
        KeyDiscoveryEngine, EntityClusteringEngine,
    )

    adapter  = DatabaseAdapter.from_sqlite("erp.db")
    ke       = KeyDiscoveryEngine()
    for table in adapter.tables:
        keys = ke.discover_table_keys(table, all_tables=adapter.tables)
        print(table.name, "PK candidate:", keys[0].column_name if keys else "—")

    clustering = EntityClusteringEngine(adapter.tables)
    entities   = clustering.cluster_entities(target_clusters=15)
    for e in entities:
        print(f"Entity {e.z0_index}: {e.name} ({len(e.member_tables)} tables)")
"""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from .db_adapter import ColumnInfo, ColumnType, TableInfo


# ---------------------------------------------------------------------------
# Key scoring
# ---------------------------------------------------------------------------

@dataclass
class KeyScore:
    """Primary-key candidate score for a single column.

    Attributes:
        column_name: The column being evaluated.
        score: Raw composite score (higher = more likely a primary key).
        reasons: List of string tags explaining the contribution to the score.
        confidence: Score normalised to ``[0, 1]``.
    """

    column_name: str
    score: float
    reasons: List[str] = field(default_factory=list)
    confidence: float = 0.0  # 0.0–1.0


class KeyDiscoveryEngine:
    """Score every column in a table as a primary-key candidate.

    Scoring criteria
    ----------------
    * Explicit DB ``PRIMARY KEY`` flag: **+100**
    * Name matches strong-ID patterns (``id``, ``pk``, ``uuid`` …): **+50**
    * Name matches business-key patterns (``guid``, ``serial``, …): **+40**
    * Column type is INTEGER (common surrogate key): **+20**
    * Column is NOT NULL: **+15**
    * Cross-table FK reference count (inferred): **+15 per table**
    * Table name embedded in column name with ``_id`` suffix: **+25**
    """

    _STRONG_ID = re.compile(
        r"^(id|pk|key|code|no|num)$"
        r"|^.+(id|pk|key|code|no|num)$"
        r"|^(id|pk|key|code|no|num).+",
        re.I,
    )
    _BUSINESS_KEY = re.compile(r"(uuid|guid|sn|serial|biz|business|ident)", re.I)
    _FK_SUFFIX    = re.compile(r"_id$|_fk$|_key$|_ref$", re.I)

    def score_column(
        self,
        col: ColumnInfo,
        table: TableInfo,
        all_tables: Optional[List[TableInfo]] = None,
    ) -> KeyScore:
        """Return a :class:`KeyScore` for *col* within *table*."""
        score = 0.0
        reasons: List[str] = []
        name_lower = col.name.lower()

        if col.primary_key:
            score += 100
            reasons.append("DB_PRIMARY_KEY")

        if self._STRONG_ID.match(name_lower):
            score += 50
            reasons.append("NAMING_PATTERN_ID")
        elif self._BUSINESS_KEY.search(name_lower):
            score += 40
            reasons.append("NAMING_PATTERN_BUSINESS_KEY")

        if col.column_type == ColumnType.INTEGER:
            score += 20
            reasons.append("INTEGER_TYPE")

        if not col.nullable:
            score += 15
            reasons.append("NOT_NULL")

        if all_tables:
            ref_count = _count_references(col, table, all_tables)
            if ref_count > 0:
                score += ref_count * 15
                reasons.append(f"REFERENCED_BY_{ref_count}_TABLES")

        # Table name embedded in column (e.g. ``customer_id`` in ``customers``)
        table_base = table.name.lower().rstrip("s")
        if (
            (table_base in name_lower or table.name.lower() in name_lower)
            and "id" in name_lower
            and not self._FK_SUFFIX.search(name_lower)
        ):
            score += 25
            reasons.append("TABLE_NAME_IN_COLUMN")

        confidence = min(score / 200.0, 1.0)
        return KeyScore(
            column_name=col.name,
            score=score,
            reasons=reasons,
            confidence=confidence,
        )

    def discover_table_keys(
        self,
        table: TableInfo,
        all_tables: Optional[List[TableInfo]] = None,
        min_score: float = 30.0,
    ) -> List[KeyScore]:
        """Return key candidates for *table* with score ≥ *min_score*.

        Results are sorted by score descending.
        """
        candidates = [
            self.score_column(col, table, all_tables)
            for col in table.columns
        ]
        filtered = [c for c in candidates if c.score >= min_score]
        filtered.sort(key=lambda c: c.score, reverse=True)
        return filtered


def _count_references(
    col: ColumnInfo,
    table: TableInfo,
    all_tables: List[TableInfo],
) -> int:
    """Count how many other tables have a column that looks like a FK to *col*."""
    table_lower = table.name.lower()
    singular = table_lower.rstrip("s")  # simple singularisation

    # Match: product_id, product_pk, products_id, product_id (from singular)
    target = re.compile(
        rf"^{re.escape(table_lower)}_(id|pk|key)$"
        rf"|^{re.escape(singular)}_(id|pk|key)$"
        rf"|^{re.escape(table_lower)}s_(id|pk|key)$",
        re.I,
    )
    count = 0
    for other in all_tables:
        if other.name == table.name:
            continue
        for other_col in other.columns:
            if target.match(other_col.name.lower()):
                count += 1
                break  # count one FK edge per other-table
    return count


# ---------------------------------------------------------------------------
# Core entity (result of clustering)
# ---------------------------------------------------------------------------

@dataclass
class CoreEntity:
    """A discovered business-entity cluster.

    Attributes:
        z0_index: Stable entity ID used as the z₀ layer of the z-axis.
        name: Inferred business name (defaults to the center table's name).
        center_table: The most-referenced / highest-PageRank table in the
            cluster.
        member_tables: All table names that belong to this cluster, sorted
            alphabetically.
        primary_key: Best :class:`KeyScore` from the center table, or
            ``None`` if no candidate was found.
        estimated_cardinality: Maximum ``row_count`` across member tables;
            used as a y-axis calibration hint.
    """

    z0_index: int
    name: str
    center_table: str
    member_tables: List[str] = field(default_factory=list)
    primary_key: Optional[KeyScore] = None
    estimated_cardinality: int = 0

    def get_z0_hue(self, total_entities: int = 15) -> float:
        """Hue in degrees, evenly spaced for this entity on the colour wheel."""
        return (self.z0_index * 360.0 / max(total_entities, 1)) % 360.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "z0_index": self.z0_index,
            "name": self.name,
            "center_table": self.center_table,
            "member_tables": self.member_tables,
            "primary_key": self.primary_key.column_name if self.primary_key else None,
            "estimated_cardinality": self.estimated_cardinality,
        }


# ---------------------------------------------------------------------------
# Entity clustering engine
# ---------------------------------------------------------------------------

class EntityClusteringEngine:
    """Cluster tables into core business entities using graph community detection.

    The algorithm:

    1. Build a weighted undirected graph: tables = nodes.

       * **FK edges (weight 3)**: if table A has a column ``{B}_id`` and
         table B exists, add an edge A–B with weight 3.
       * **Implicit same-column edges (weight 1)**: if two tables share a
         column with identical name *and* base type (excluding common
         housekeeping columns like ``created_at``), add a weak edge.

    2. Run Louvain community detection (*python-louvain* + *networkx*).
       If those libraries are unavailable, fall back to Union-Find
       connected components.

    3. Within each cluster find the **centre table**: highest PageRank (if
       networkx is available) or highest inferred in-degree otherwise.

    Parameters:
        tables: All :class:`~four_dim_matrix.TableInfo` objects to cluster.
    """

    # Housekeeping column names to ignore for implicit-edge construction.
    _COMMON_COLUMNS: frozenset = frozenset([
        "created", "updated", "modified", "deleted", "status",
        "remark", "note", "sort", "order", "type", "flag",
    ])

    def __init__(self, tables: List[TableInfo]) -> None:
        self.tables = tables
        self._table_map: Dict[str, TableInfo] = {t.name: t for t in tables}

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    def cluster_entities(
        self,
        target_clusters: Optional[int] = None,
    ) -> List[CoreEntity]:
        """Partition tables into :class:`CoreEntity` clusters.

        Parameters:
            target_clusters: Desired number of entity clusters.  When
                ``None`` the natural community count from Louvain is used.

        Returns:
            List of :class:`CoreEntity` objects sorted by cluster size
            (largest cluster first → smallest ``z0_index``).
        """
        partition = self._partition(target_clusters)

        groups: Dict[int, List[str]] = defaultdict(list)
        for table_name, cluster_id in partition.items():
            groups[cluster_id].append(table_name)

        key_engine = KeyDiscoveryEngine()
        entities: List[CoreEntity] = []

        for entity_id, (_, members) in enumerate(
            sorted(groups.items(), key=lambda kv: -len(kv[1]))
        ):
            member_infos = [self._table_map[m] for m in members if m in self._table_map]
            center = self._find_center(member_infos)

            pk_candidates = key_engine.discover_table_keys(
                self._table_map[center],
                all_tables=self.tables,
            )
            primary_key = pk_candidates[0] if pk_candidates else None
            cardinality = max((t.row_count for t in member_infos), default=0)

            entities.append(CoreEntity(
                z0_index=entity_id,
                name=center,
                center_table=center,
                member_tables=sorted(members),
                primary_key=primary_key,
                estimated_cardinality=cardinality,
            ))

        return entities

    # ------------------------------------------------------------------
    # Graph building
    # ------------------------------------------------------------------

    def _build_nx_graph(self) -> "networkx.Graph":  # type: ignore[name-defined]
        """Construct the weighted table-relationship graph."""
        import networkx as nx  # type: ignore[import]

        G: networkx.Graph = nx.Graph()  # type: ignore[name-defined]
        for table in self.tables:
            G.add_node(table.name)

        # ---- FK edges (weight 3) ----
        table_names_lower = {t.name.lower(): t.name for t in self.tables}
        singular_map: Dict[str, str] = {}
        for name in table_names_lower.values():
            low = name.lower()
            if low.endswith("ies"):
                singular_map[low[:-3] + "y"] = name
            elif low.endswith("s") and not low.endswith("ss"):
                singular_map[low[:-1]] = name

        for table in self.tables:
            for col in table.columns:
                if not col.name.lower().endswith("_id"):
                    continue
                prefix = col.name.lower()[:-3]
                target = (
                    table_names_lower.get(prefix)
                    or table_names_lower.get(prefix + "s")
                    or table_names_lower.get(prefix + "es")
                    or singular_map.get(prefix)
                )
                if target and target != table.name:
                    if G.has_edge(table.name, target):
                        G[table.name][target]["weight"] += 3
                    else:
                        G.add_edge(table.name, target, weight=3)

        # ---- Implicit same-column edges (weight 1) ----
        col_index: Dict[Tuple[str, str], List[str]] = defaultdict(list)
        for table in self.tables:
            for col in table.columns:
                name_lower = col.name.lower()
                if any(kw in name_lower for kw in self._COMMON_COLUMNS):
                    continue
                sig = (name_lower, col.type_str.upper().split("(")[0])
                col_index[sig].append(table.name)

        for _sig, tnames in col_index.items():
            if len(tnames) < 2:
                continue
            for i in range(len(tnames)):
                for j in range(i + 1, len(tnames)):
                    t1, t2 = tnames[i], tnames[j]
                    if not G.has_edge(t1, t2):
                        G.add_edge(t1, t2, weight=1)

        return G

    # ------------------------------------------------------------------
    # Partitioning (Louvain or Union-Find)
    # ------------------------------------------------------------------

    def _partition(self, target_clusters: Optional[int]) -> Dict[str, int]:
        """Return ``{table_name: cluster_id}`` via the best available method."""
        try:
            import networkx as nx  # type: ignore[import]
            import community as community_louvain  # type: ignore[import]

            G = self._build_nx_graph()
            if G.number_of_edges() == 0:
                return {t.name: i for i, t in enumerate(self.tables)}

            if target_clusters is not None:
                return self._tune_resolution(G, target_clusters, community_louvain)
            return community_louvain.best_partition(G, weight="weight")
        except ImportError:
            return self._union_find_partition()

    def _tune_resolution(
        self, G: Any, target: int, community_louvain: Any
    ) -> Dict[str, int]:
        """Binary-search for a Louvain resolution that produces ~*target* clusters."""
        lo, hi = 0.1, 10.0
        best = community_louvain.best_partition(G, weight="weight", resolution=1.0)
        for _ in range(12):
            mid = (lo + hi) / 2.0
            partition = community_louvain.best_partition(G, weight="weight", resolution=mid)
            n = len(set(partition.values()))
            if n == target:
                return partition
            if n < target:
                hi = mid
            else:
                lo = mid
            if abs(n - target) < abs(len(set(best.values())) - target):
                best = partition
        return best

    def _union_find_partition(self) -> Dict[str, int]:
        """Fallback: Union-Find connected-component partitioning."""
        parent = {t.name: t.name for t in self.tables}

        def find(x: str) -> str:
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        def union(a: str, b: str) -> None:
            ra, rb = find(a), find(b)
            if ra != rb:
                parent[rb] = ra

        table_names_lower = {t.name.lower(): t.name for t in self.tables}
        singular_map: Dict[str, str] = {}
        for name in table_names_lower.values():
            low = name.lower()
            if low.endswith("s") and not low.endswith("ss"):
                singular_map[low[:-1]] = name

        for table in self.tables:
            for col in table.columns:
                if not col.name.lower().endswith("_id"):
                    continue
                prefix = col.name.lower()[:-3]
                target = (
                    table_names_lower.get(prefix)
                    or table_names_lower.get(prefix + "s")
                    or singular_map.get(prefix)
                )
                if target and target != table.name:
                    union(table.name, target)

        root_to_id: Dict[str, int] = {}
        result: Dict[str, int] = {}
        for t in self.tables:
            root = find(t.name)
            if root not in root_to_id:
                root_to_id[root] = len(root_to_id)
            result[t.name] = root_to_id[root]
        return result

    # ------------------------------------------------------------------
    # Centre-table selection
    # ------------------------------------------------------------------

    def _find_center(self, members: List[TableInfo]) -> str:
        """Return the name of the most central table in *members*.

        Uses PageRank (networkx) when available; falls back to highest
        inferred in-degree.
        """
        if len(members) == 1:
            return members[0].name

        try:
            import networkx as nx  # type: ignore[import]

            G = self._build_nx_graph()
            sub = G.subgraph([m.name for m in members])
            if sub.number_of_edges() == 0:
                return max(members, key=lambda t: t.row_count).name
            pr = nx.pagerank(sub, weight="weight")
            return max(pr, key=pr.get)  # type: ignore[arg-type]
        except ImportError:
            pass

        # Fallback: in-degree by inferred FK references
        in_degree: Dict[str, int] = {m.name: 0 for m in members}
        for table in members:
            for col in table.columns:
                if not col.name.lower().endswith("_id"):
                    continue
                prefix = col.name.lower()[:-3]
                for other in members:
                    if other.name == table.name:
                        continue
                    ol = other.name.lower()
                    if ol == prefix or ol == prefix + "s" or ol.rstrip("s") == prefix:
                        in_degree[other.name] = in_degree.get(other.name, 0) + 1
        return max(in_degree, key=lambda n: (in_degree[n], -len(n)))
