"""Tests for Track C: ZCoordinate, RelationType, and ZAxisAllocator."""

from __future__ import annotations

import pytest

from four_dim_matrix import ColumnInfo, ColumnType, TableInfo
from four_dim_matrix.z_axis_encoding import (
    RelationType,
    ZAxisAllocator,
    ZCoordinate,
    _infer_relation_type,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _col(name: str, col_type: ColumnType = ColumnType.INTEGER,
         nullable: bool = True, pk: bool = False) -> ColumnInfo:
    return ColumnInfo(name=name, type_str=col_type.name,
                      column_type=col_type, nullable=nullable, primary_key=pk)


def _table(name: str, columns: list, row_count: int = 0) -> TableInfo:
    return TableInfo(name=name, columns=columns, row_count=row_count)


# ---------------------------------------------------------------------------
# ZCoordinate
# ---------------------------------------------------------------------------

class TestZCoordinate:
    def test_scalar_encoding(self):
        assert ZCoordinate(z0=1, z1=2, z2=3).to_scalar() == 123
        assert ZCoordinate(z0=0, z1=0, z2=0).to_scalar() == 0
        assert ZCoordinate(z0=14, z1=9, z2=9).to_scalar() == 1499

    def test_scalar_decoding(self):
        coord = ZCoordinate.from_scalar(123)
        assert coord.z0 == 1
        assert coord.z1 == 2
        assert coord.z2 == 3

    def test_round_trip(self):
        for scalar in [0, 50, 100, 210, 999, 1499]:
            assert ZCoordinate.from_scalar(scalar).to_scalar() == scalar

    def test_invalid_z1_raises(self):
        with pytest.raises(ValueError):
            ZCoordinate(z0=0, z1=10, z2=0)

    def test_invalid_z2_raises(self):
        with pytest.raises(ValueError):
            ZCoordinate(z0=0, z1=0, z2=10)

    def test_get_hue_first_entity(self):
        hue = ZCoordinate(z0=0, z1=0, z2=0).get_hue(total_entities=15)
        # z1=0 → shift = (0-4.5)*3 = -13.5; z2=0 → shift = (0-4.5)*1 = -4.5
        expected_base = 0.0
        expected = (expected_base - 13.5 - 4.5) % 360.0
        assert hue == pytest.approx(expected, abs=0.01)

    def test_hues_differ_between_entities(self):
        h0 = ZCoordinate(z0=0, z1=0, z2=0).get_hue(5)
        h1 = ZCoordinate(z0=1, z1=0, z2=0).get_hue(5)
        assert h0 != h1

    def test_to_hex_color_format(self):
        color = ZCoordinate(z0=0, z1=0, z2=0).to_hex_color()
        assert color.startswith("#")
        assert len(color) == 7
        # Validate all hex digits
        int(color[1:], 16)

    def test_primary_vs_extension_saturation(self):
        # Primary (z1=0) should not be equal to extension (z1=1) in hex
        c_primary   = ZCoordinate(z0=2, z1=0, z2=0).to_hex_color()
        c_extension = ZCoordinate(z0=2, z1=1, z2=0).to_hex_color()
        assert c_primary != c_extension

    def test_color_family_name(self):
        for z0 in range(15):
            fam = ZCoordinate(z0=z0, z1=0, z2=0).color_family(15)
            assert isinstance(fam, str)
            assert len(fam) > 0

    def test_to_dict_keys(self):
        d = ZCoordinate(z0=3, z1=2, z2=1).to_dict()
        assert set(d.keys()) == {"z0", "z1", "z2", "scalar"}
        assert d["scalar"] == 321

    def test_frozen(self):
        coord = ZCoordinate(z0=1, z1=0, z2=0)
        with pytest.raises((AttributeError, TypeError)):
            coord.z0 = 99  # type: ignore[misc]


# ---------------------------------------------------------------------------
# RelationType
# ---------------------------------------------------------------------------

class TestRelationType:
    def test_primary_is_zero(self):
        assert int(RelationType.PRIMARY) == 0

    def test_values_are_unique(self):
        vals = [int(rt) for rt in RelationType]
        assert len(vals) == len(set(vals))

    def test_all_values_in_0_to_9(self):
        for rt in RelationType:
            assert 0 <= int(rt) <= 9

    def test_can_construct_zcoordinate_with_relation_type(self):
        coord = ZCoordinate(z0=1, z1=int(RelationType.ONE_TO_MANY), z2=0)
        assert coord.z1 == 2


# ---------------------------------------------------------------------------
# ZAxisAllocator
# ---------------------------------------------------------------------------

class TestZAxisAllocator:
    def test_allocate_core_table(self):
        allocator = ZAxisAllocator()
        coord = allocator.allocate("customers", z0=0, relation_type=RelationType.MISC,
                                   is_core=True)
        assert coord.z0 == 0
        assert coord.z1 == RelationType.PRIMARY
        assert coord.z2 == 0

    def test_allocate_non_core_uses_given_relation(self):
        allocator = ZAxisAllocator()
        coord = allocator.allocate("order_items", z0=1,
                                   relation_type=RelationType.ONE_TO_MANY)
        assert coord.z1 == int(RelationType.ONE_TO_MANY)

    def test_z2_increments_within_bucket(self):
        allocator = ZAxisAllocator()
        c1 = allocator.allocate("t1", z0=0, relation_type=RelationType.MISC)
        c2 = allocator.allocate("t2", z0=0, relation_type=RelationType.MISC)
        assert c1.z2 == 0
        assert c2.z2 == 1

    def test_different_z1_buckets_independent_z2(self):
        allocator = ZAxisAllocator()
        c_onetomany = allocator.allocate("items", z0=0, relation_type=RelationType.ONE_TO_MANY)
        c_misc      = allocator.allocate("other", z0=0, relation_type=RelationType.MISC)
        # Each bucket starts at z2=0
        assert c_onetomany.z2 == 0
        assert c_misc.z2 == 0

    def test_bucket_overflow_raises(self):
        allocator = ZAxisAllocator()
        for i in range(10):
            allocator.allocate(f"t{i}", z0=0, relation_type=RelationType.MISC)
        with pytest.raises(ValueError, match="full"):
            allocator.allocate("overflow", z0=0, relation_type=RelationType.MISC)

    def test_allocated_property(self):
        allocator = ZAxisAllocator()
        allocator.allocate("customers", z0=0, relation_type=RelationType.MISC, is_core=True)
        allocator.allocate("orders",    z0=1, relation_type=RelationType.MISC, is_core=True)
        assert set(allocator.allocated.keys()) == {"customers", "orders"}

    def test_allocate_cluster(self):
        from four_dim_matrix.key_discovery import CoreEntity

        allocator = ZAxisAllocator()
        pk_col    = _col("id", ColumnType.INTEGER, nullable=False, pk=True)
        customers = _table("customers", [pk_col, _col("name", ColumnType.TEXT)])
        orders    = _table("orders", [pk_col, _col("customers_id"), _col("total", ColumnType.FLOAT)])

        entity = CoreEntity(
            z0_index=0,
            name="customers",
            center_table="customers",
            member_tables=["customers", "orders"],
        )
        result = allocator.allocate_cluster(entity, all_tables=[customers, orders])
        assert "customers" in result
        assert "orders" in result
        assert result["customers"].z1 == RelationType.PRIMARY

    def test_allocation_report_structure(self):
        allocator = ZAxisAllocator()
        allocator.allocate("a", z0=0, relation_type=RelationType.MISC, is_core=True)
        allocator.allocate("b", z0=0, relation_type=RelationType.ONE_TO_MANY)
        report = allocator.allocation_report()
        assert "total_tables_allocated" in report
        assert "entities" in report
        assert report["total_tables_allocated"] == 2


# ---------------------------------------------------------------------------
# _infer_relation_type helper
# ---------------------------------------------------------------------------

class TestInferRelationType:
    def test_aggregation_table(self):
        table = _table("sales_stats", [_col("id", pk=True), _col("count")])
        rel = _infer_relation_type(table, "sales", {})
        assert rel == RelationType.AGGREGATION

    def test_reference_table(self):
        table = _table("country_codes", [_col("id", pk=True), _col("name", ColumnType.TEXT)])
        rel = _infer_relation_type(table, "orders", {})
        assert rel == RelationType.REFERENCE

    def test_many_to_many_junction(self):
        table = _table("user_roles", [
            _col("users_id"),
            _col("roles_id"),
        ])
        rel = _infer_relation_type(table, "users", {"users": _table("users", [])})
        assert rel == RelationType.MANY_TO_MANY

    def test_extension_table(self):
        core   = _table("customers", [_col("id", pk=True)])
        ext    = _table("customer_extra", [_col("customers_id"), _col("bio", ColumnType.TEXT)])
        rel = _infer_relation_type(ext, "customers", {"customers": core})
        assert rel in (RelationType.EXTENSION, RelationType.ONE_TO_MANY)

    def test_misc_fallback(self):
        table = _table("random_data", [_col("value", ColumnType.FLOAT)])
        rel = _infer_relation_type(table, "main", {})
        assert rel == RelationType.MISC
