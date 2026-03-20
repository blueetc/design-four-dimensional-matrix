"""Tests for seebook-integrated modules: DataCell, HyperCube, lineage,
dynamic_classifier, quality, relationship_extractor, changelog, optimizer."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime

import pytest

from four_dim_matrix import (
    DataCell,
    HyperCube,
    ColorCell,
    ColorScheme,
    RichDataMatrix,
    RichColorMatrix,
    LineageTracker,
    PhysicalLocation,
    LineageEdge,
    TransformationType,
    DynamicDomainDiscoverer,
    AdaptiveLifecycleClassifier,
    UnknownDatabaseProcessor,
    QualityEngine,
    QualityIssueType,
    RelationshipExtractor,
    ChangeTracker,
    ChangeType,
)
from four_dim_matrix.dynamic_classifier import TableSignature


# ---------------------------------------------------------------------------
# Helper fixtures
# ---------------------------------------------------------------------------

_T0 = datetime(2024, 1, 1, 0, 0, 0)
_T1 = datetime(2024, 2, 1, 0, 0, 0)


def _make_cell(
    *,
    t=None,
    x=50,
    y=0.5,
    z=0,
    table_name="users",
    schema_name="public",
    row_count=1000,
    column_count=5,
    size_bytes=8192,
    business_domain="user",
    lifecycle_stage="mature",
) -> DataCell:
    return DataCell(
        t=t or _T0,
        x=x,
        y=y,
        z=z,
        table_name=table_name,
        schema_name=schema_name,
        row_count=row_count,
        column_count=column_count,
        size_bytes=size_bytes,
        business_domain=business_domain,
        lifecycle_stage=lifecycle_stage,
        payload={"columns": [{"name": "id"}, {"name": "email"}]},
    )


# ===========================================================================
# DataCell
# ===========================================================================


class TestDataCell:
    def test_basic_construction(self):
        cell = _make_cell()
        assert cell.t == _T0
        assert cell.x == 50
        assert cell.y == 0.5
        assert cell.z == 0
        assert cell.table_name == "users"
        assert cell.business_domain == "user"
        assert cell.lifecycle_stage == "mature"

    def test_string_t_is_parsed(self):
        cell = DataCell(t="2024-06-15", x=0, y=0.0, z=0)
        assert cell.t == datetime(2024, 6, 15)

    def test_negative_x_raises(self):
        with pytest.raises(ValueError):
            DataCell(t=_T0, x=-1, y=0.0, z=0)

    def test_negative_z_raises(self):
        with pytest.raises(ValueError):
            DataCell(t=_T0, x=0, y=0.0, z=-1)

    def test_to_dict_structure(self):
        cell = _make_cell()
        d = cell.to_dict()
        assert "coordinates" in d
        assert "metrics" in d
        assert d["metrics"]["row_count"] == 1000
        assert d["classification"]["business_domain"] == "user"

    def test_to_data_point(self):
        from four_dim_matrix import DataPoint

        cell = _make_cell(x=10, y=2.5, z=3)
        dp = cell.to_data_point()
        assert isinstance(dp, DataPoint)
        assert dp.x == 10
        assert dp.y == 2.5
        assert dp.z == 3

    def test_human_readable_size(self):
        cell = _make_cell(size_bytes=1024 * 1024)  # 1 MB
        d = cell.to_dict()
        assert "MB" in d["metrics"]["size_human"]


# ===========================================================================
# HyperCube
# ===========================================================================


class TestHyperCube:
    def _populated_cube(self) -> HyperCube:
        cube = HyperCube()
        for i in range(3):
            cube.add_cell(
                _make_cell(
                    t=_T0,
                    x=30 + i * 20,
                    y=float(i) * 0.3 + 0.1,
                    z=i,
                    table_name=f"table_{i}",
                    business_domain=["user", "revenue", "product"][i],
                    lifecycle_stage="mature",
                )
            )
        cube.sync_color_matrix()
        return cube

    def test_add_cell_populates_data_matrix(self):
        cube = HyperCube()
        cube.add_cell(_make_cell())
        assert len(cube.data_matrix.cells) == 1

    def test_add_cell_computes_color(self):
        cube = HyperCube()
        cc = cube.add_cell(_make_cell())
        assert cc is not None
        assert isinstance(cc, ColorCell)

    def test_sync_color_matrix(self):
        cube = self._populated_cube()
        assert cube.synced
        assert len(cube.color_matrix.cells) == len(cube.data_matrix.cells)

    def test_get_dual_cell(self):
        cube = HyperCube()
        cell = _make_cell(x=10, y=0.5, z=0)
        cube.add_cell(cell)
        dc, cc = cube.get_dual_cell(_T0, 10, 0.5, 0)
        assert dc is not None
        assert cc is not None

    def test_query_by_color_returns_list(self):
        cube = self._populated_cube()
        # get the actual hex of first cell so we can find it
        first_cc = next(iter(cube.color_matrix.cells.values()))
        results = cube.query_by_color(first_cc.to_hex(), threshold=10)
        # should find at least the exact match
        assert isinstance(results, list)

    def test_query_by_visual_region(self):
        cube = self._populated_cube()
        region = cube.query_by_visual_region(z=0)
        assert "statistics" in region
        assert region["statistics"]["count"] == 1

    def test_export_for_visualization(self):
        cube = self._populated_cube()
        data = cube.export_for_visualization()
        assert "data_points" in data
        assert len(data["data_points"]) == 3

    def test_get_summary(self):
        cube = self._populated_cube()
        summary = cube.get_summary()
        assert summary["sync_status"] is True
        assert summary["data_matrix"]["total_cells"] == 3

    def test_get_color_flow(self):
        cube = HyperCube()
        cube.add_cell(_make_cell(t=_T0, z=0))
        cube.add_cell(_make_cell(t=_T1, z=0, table_name="users2"))
        flow = cube.get_color_flow(z=0)
        assert len(flow) == 2
        assert "time" in flow[0]
        assert "color" in flow[0]


# ===========================================================================
# ColorScheme
# ===========================================================================


class TestColorScheme:
    def test_get_hue_for_known_domain(self):
        scheme = ColorScheme()
        z_cats = {0: "user", 1: "revenue"}
        hue_user = scheme.get_hue_for_z(0, z_cats)
        hue_rev = scheme.get_hue_for_z(1, z_cats)
        assert hue_user == 200.0
        assert hue_rev == 120.0

    def test_get_hue_for_unknown_domain_uses_modulo(self):
        scheme = ColorScheme()
        hue = scheme.get_hue_for_z(7, {})
        assert 0 <= hue < 360

    def test_lightness_range(self):
        scheme = ColorScheme()
        for y in [0.0, 0.5, 1.0, 100.0]:
            l = scheme.get_lightness_for_y(y)
            assert 0.15 <= l <= 0.85


# ===========================================================================
# RichDataMatrix / RichColorMatrix
# ===========================================================================


class TestRichMatrices:
    def test_rich_data_matrix_add_and_get(self):
        dm = RichDataMatrix()
        cell = _make_cell()
        dm.add_cell(cell)
        retrieved = dm.get_cell(_T0, 50, 0.5, 0)
        assert retrieved is not None
        assert retrieved.table_name == "users"

    def test_rich_data_matrix_slice_by_z(self):
        dm = RichDataMatrix()
        dm.add_cell(_make_cell(z=0, table_name="a"))
        dm.add_cell(_make_cell(z=0, x=60, table_name="b"))
        dm.add_cell(_make_cell(z=1, table_name="c"))
        sliced = dm.slice_by_z(0)
        assert len(sliced) == 2

    def test_rich_data_matrix_summary(self):
        dm = RichDataMatrix()
        dm.add_cell(_make_cell())
        s = dm.get_summary()
        assert not s["empty"]
        assert s["total_cells"] == 1

    def test_rich_color_matrix_add_and_get(self):
        cm = RichColorMatrix()
        cc = cm.add_cell(_T0, 50, 0.5, 0)
        assert cc is not None
        assert 0 <= cc.r <= 255
        retrieved = cm.get_cell(_T0, 50, 0.5, 0)
        assert retrieved is not None


# ===========================================================================
# Lineage
# ===========================================================================


class TestLineage:
    def test_register_first_stage(self):
        tracker = LineageTracker()
        loc = PhysicalLocation(
            db_type="postgres",
            host="localhost",
            port=5432,
            database="mydb",
            schema="public",
            table="users",
        )
        prov = tracker.register_first_stage("cell_1", loc)
        assert prov.is_first_stage()
        assert not prov.is_second_stage()
        assert prov.cell_id == "cell_1"

    def test_physical_location_uri(self):
        loc = PhysicalLocation(
            db_type="mysql",
            host="db.example.com",
            port=3306,
            database="prod",
            schema="app",
            table="orders",
        )
        uri = loc.to_uri()
        assert "mysql://" in uri
        assert "orders" in uri

    def test_register_second_stage(self):
        tracker = LineageTracker()
        loc = PhysicalLocation("postgres", "localhost", 5432, "db", "public", "src")
        tracker.register_first_stage("src_cell", loc)
        edge = LineageEdge(
            source_id="src_cell",
            target_id="derived_cell",
            transform_type=TransformationType.DIRECT,
            transform_reason="Test",
        )
        prov = tracker.register_second_stage("derived_cell", [edge])
        assert prov.is_second_stage()
        assert not prov.is_first_stage()

    def test_get_upstream(self):
        tracker = LineageTracker()
        loc = PhysicalLocation("postgres", "localhost", 5432, "db", "public", "tbl")
        tracker.register_first_stage("stage1_cell", loc)
        edge = LineageEdge("stage1_cell", "stage2_cell", TransformationType.AGGREGATE, "agg")
        tracker.register_second_stage("stage2_cell", [edge])
        upstream = tracker.get_upstream("stage2_cell")
        assert any(p.cell_id == "stage1_cell" for p in upstream)

    def test_export_lineage_graph(self):
        tracker = LineageTracker()
        loc = PhysicalLocation("sqlite", "localhost", 0, "file.db", "main", "tbl")
        tracker.register_first_stage("c1", loc)
        graph = tracker.export_lineage_graph()
        assert "nodes" in graph
        assert "edges" in graph
        assert graph["stats"]["total_nodes"] == 1


# ===========================================================================
# DynamicClassifier
# ===========================================================================


class TestDynamicClassifier:
    def _make_signatures(self):
        return [
            TableSignature(
                table_name="users",
                schema_name="public",
                column_names=["id", "email", "created_at"],
                column_types=["int", "varchar", "timestamp"],
                primary_key="id",
                foreign_keys=[],
                indexes=["idx_email"],
                row_count=50000,
                column_count=3,
                has_timestamp=True,
                has_soft_delete=False,
            ),
            TableSignature(
                table_name="user_profiles",
                schema_name="public",
                column_names=["id", "user_id", "bio"],
                column_types=["int", "int", "text"],
                primary_key="id",
                foreign_keys=[{"column": "user_id", "ref_table": "users", "ref_column": "id"}],
                indexes=[],
                row_count=45000,
                column_count=3,
                has_timestamp=False,
                has_soft_delete=False,
            ),
            TableSignature(
                table_name="orders",
                schema_name="public",
                column_names=["id", "user_id", "amount", "created_at"],
                column_types=["int", "int", "decimal", "timestamp"],
                primary_key="id",
                foreign_keys=[{"column": "user_id", "ref_table": "users", "ref_column": "id"}],
                indexes=["idx_user"],
                row_count=200000,
                column_count=4,
                has_timestamp=True,
                has_soft_delete=False,
            ),
        ]

    def test_discover_domains_returns_mapping(self):
        discoverer = DynamicDomainDiscoverer()
        sigs = self._make_signatures()
        mapping = discoverer.discover_domains(sigs)
        assert isinstance(mapping, dict)
        # all tables should be assigned
        assert set(mapping.keys()) == {"users", "user_profiles", "orders"}
        # z-indices are non-negative integers
        for v in mapping.values():
            assert v >= 0

    def test_discover_domains_empty(self):
        discoverer = DynamicDomainDiscoverer()
        result = discoverer.discover_domains([])
        assert result == {}

    def test_adaptive_lifecycle_classifier(self):
        classifier = AdaptiveLifecycleClassifier()
        sigs = self._make_signatures()
        result = classifier.classify(sigs)
        assert set(result.keys()) == {"users", "user_profiles", "orders"}
        for stage in result.values():
            assert stage in {"new", "growth", "mature", "legacy"}

    def test_unknown_database_processor(self):
        processor = UnknownDatabaseProcessor()
        raw = [
            {
                "table_name": "products",
                "schema_name": "public",
                "columns": [
                    {"name": "id", "type": "int"},
                    {"name": "name", "type": "varchar"},
                    {"name": "created_at", "type": "timestamp"},
                ],
                "foreign_keys": [],
                "indexes": [],
                "primary_key": "id",
                "row_count": 5000,
            },
            {
                "table_name": "categories",
                "schema_name": "public",
                "columns": [
                    {"name": "id", "type": "int"},
                    {"name": "name", "type": "varchar"},
                ],
                "foreign_keys": [],
                "indexes": [],
                "primary_key": "id",
                "row_count": 50,
            },
        ]
        result = processor.process(raw)
        assert "domain_mapping" in result
        assert "lifecycle_mapping" in result
        assert "stats" in result
        assert result["stats"]["total_tables"] == 2

    def test_table_signature_extract_tokens(self):
        sig = TableSignature(
            table_name="user_profiles",
            schema_name="public",
            column_names=[],
            column_types=[],
            primary_key=None,
            foreign_keys=[],
            indexes=[],
            row_count=0,
            column_count=0,
            has_timestamp=False,
            has_soft_delete=False,
        )
        assert "user" in sig.name_tokens
        assert "profiles" in sig.name_tokens


# ===========================================================================
# QualityEngine
# ===========================================================================


class TestQualityEngine:
    def _make_cube_with_issues(self) -> HyperCube:
        cube = HyperCube()
        # Table with no primary key
        cube.add_cell(
            DataCell(
                t=_T0, x=50, y=0.5, z=0,
                table_name="bad_table",
                schema_name="public",
                row_count=1000,
                column_count=5,
                size_bytes=8192,
                business_domain="user",
                lifecycle_stage="mature",
                payload={"columns": [{"name": "col1"}], "indexes": []},
            )
        )
        # Normal table
        cube.add_cell(
            DataCell(
                t=_T0, x=60, y=0.6, z=0,
                table_name="good_table",
                schema_name="public",
                row_count=5000,
                column_count=8,
                size_bytes=40960,
                business_domain="user",
                lifecycle_stage="mature",
                payload={
                    "columns": [{"name": "id"}, {"name": "email"}],
                    "primary_key": "id",
                    "indexes": [{"name": "idx_email"}],
                },
            )
        )
        cube.sync_color_matrix()
        return cube

    def test_evaluate_returns_scores(self):
        engine = QualityEngine()
        cube = self._make_cube_with_issues()
        scores = engine.evaluate(cube)
        assert len(scores) == 2
        for score in scores.values():
            assert 0 <= score.overall_score <= 100

    def test_generate_report(self):
        engine = QualityEngine()
        cube = self._make_cube_with_issues()
        scores = engine.evaluate(cube)
        report = engine.generate_report(scores)
        assert "summary" in report
        assert report["summary"]["total_cells"] == 2


# ===========================================================================
# RelationshipExtractor (naming inference only – no live DB)
# ===========================================================================


class TestRelationshipExtractor:
    def test_infer_from_naming_user_id(self):
        extractor = RelationshipExtractor(infer_missing=True)
        columns = [
            {"name": "id"},
            {"name": "user_id"},
            {"name": "amount"},
        ]
        all_tables = ["users", "orders", "payments"]
        fks = extractor.infer_from_naming("orders", columns, all_tables)
        names = [fk.column_name for fk in fks]
        assert "user_id" in names

    def test_find_orphan_tables(self):
        extractor = RelationshipExtractor()
        all_tables = ["standalone", "connected_a", "connected_b"]
        graph = {"connected_a": {"connected_b"}, "connected_b": {"connected_a"}}
        orphans = extractor.find_orphan_tables(all_tables, graph)
        assert "standalone" in orphans
        assert "connected_a" not in orphans


# ===========================================================================
# ChangeTracker
# ===========================================================================


class TestChangeTracker:
    def test_take_snapshot_creates_version(self):
        tracker = ChangeTracker()
        cube = HyperCube()
        cube.add_cell(_make_cell())
        cube.sync_color_matrix()
        # create_snapshot takes explicit args: cell_count, domain_dist, stage_dist, changes
        cell_count = len(cube.data_matrix.cells)
        domain_dist = {"user": 1}
        stage_dist = {"mature": 1}
        snapshot = tracker.create_snapshot(
            cell_count=cell_count,
            domain_dist=domain_dist,
            stage_dist=stage_dist,
            changes=[],
            description="Initial",
        )
        assert snapshot is not None
        assert len(tracker.versions) == 1

    def test_detect_changes_between_snapshots(self):
        tracker = ChangeTracker()
        prev = {
            "cell_users": {"table_name": "users", "row_count": 1000, "column_count": 5},
        }
        curr = {
            "cell_users": {"table_name": "users", "row_count": 2000, "column_count": 5},
            "cell_new": {"table_name": "new_table", "row_count": 500, "column_count": 3},
        }
        changes = tracker.detect_changes(prev, curr)
        assert isinstance(changes, list)
        change_types = {c.change_type for c in changes}
        # new table was added, existing table was modified
        assert ChangeType.ADDED in change_types or ChangeType.MODIFIED in change_types
