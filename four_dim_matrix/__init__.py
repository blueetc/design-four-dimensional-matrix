"""
Four-Dimensional Matrix Knowledge System
=========================================

Implements a dual-matrix architecture for database-to-knowledge-base conversion:

* **DataMatrix** – stores business records indexed by four dimensions:
    - ``t`` (global time)
    - ``x`` (business cycle / phase)
    - ``y`` (total quantity / value)
    - ``z`` (topic / category)

* **ColorMatrix** – mirrors DataMatrix coordinates with a colour encoding so that
  users can *see* the four-dimensional data space as an animated colour cloud.

* **ColorMapper** – translates ``(t, x, y, z)`` coordinates to HSL colours.

* **KnowledgeBase** – high-level API that keeps the two matrices in sync and
  exposes query/trend-analysis helpers.
"""

from .data_matrix import DataMatrix, DataPoint
from .color_matrix import ColorMatrix, ColorPoint
from .color_mapping import ColorMapper, ColorConfig, ColorPreset
from .knowledge_base import KnowledgeBase
from .db_adapter import (
    DatabaseAdapter,
    TableInfo,
    ColumnInfo,
    ColumnType,
    ColumnMapping,
    TableMapping,
    DialectHandler,
    SQLiteDialectHandler,
    PostgreSQLDialectHandler,
    MySQLDialectHandler,
    register_dialect,
    get_dialect_handler,
)
from .schema_analyzer import SchemaAnalyzer, TableAnalysis, ColumnGroup
from .normalization_pipeline import NormalizationPipeline, SubTablePlan
from .key_discovery import KeyDiscoveryEngine, KeyScore, CoreEntity, EntityClusteringEngine
from .temporal_discovery import (
    TemporalDiscoveryEngine,
    TemporalType,
    TemporalColumn,
    TMappingStrategy,
)
from .z_axis_encoding import ZCoordinate, RelationType, ZAxisAllocator
from .four_d_mapper import FourDimensionalMapper, MatrixConfig
from .visualizer import MatrixVisualizer, render_snapshot
from .aggregation_layer import HierarchicalAggregator

__all__ = [
    # Core dual-matrix
    "DataMatrix",
    "DataPoint",
    "ColorMatrix",
    "ColorPoint",
    "ColorMapper",
    "ColorConfig",
    "ColorPreset",
    "KnowledgeBase",
    # Database adapter
    "DatabaseAdapter",
    "TableInfo",
    "ColumnInfo",
    "ColumnType",
    "ColumnMapping",
    "TableMapping",
    "DialectHandler",
    "SQLiteDialectHandler",
    "PostgreSQLDialectHandler",
    "MySQLDialectHandler",
    "register_dialect",
    "get_dialect_handler",
    # Schema analysis / normalization
    "SchemaAnalyzer",
    "TableAnalysis",
    "ColumnGroup",
    "NormalizationPipeline",
    "SubTablePlan",
    # Track A: key discovery & entity clustering
    "KeyDiscoveryEngine",
    "KeyScore",
    "CoreEntity",
    "EntityClusteringEngine",
    # Track B: temporal discovery
    "TemporalDiscoveryEngine",
    "TemporalType",
    "TemporalColumn",
    "TMappingStrategy",
    # Track C: z-axis encoding
    "ZCoordinate",
    "RelationType",
    "ZAxisAllocator",
    # Integration: four-dimensional mapper
    "FourDimensionalMapper",
    "MatrixConfig",
    # Visualization
    "MatrixVisualizer",
    "render_snapshot",
    # LOD / performance
    "HierarchicalAggregator",
]
