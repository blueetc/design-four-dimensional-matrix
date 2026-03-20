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

seebook 集成新增能力
--------------------
* **DataCell** – 富数据单元格：在 DataPoint 基础上增加表元数据、业务域/生命周期
  标签和血缘溯源信息。
* **HyperCube** – 字典型双矩阵管理器，支持按颜色反查、框选查询、趋势分析。
* **DynamicDomainDiscoverer / UnknownDatabaseProcessor** – 无需预设业务域，
  基于外键关系+命名相似度+结构特征动态聚类。
* **LineageTracker** – 数据血缘追踪，记录从物理库到矩阵单元格的完整转换链。
* **QualityEngine** – 颜色异常检测 + 结构合规检查，输出质量评分报告。
* **MatrixOptimizer** – 分析第一阶段矩阵，给出合并/归档/重分类建议并生成第二
  阶段矩阵。
* **RelationshipExtractor** – 从 MySQL/PostgreSQL 提取真实外键或基于命名推断。
* **ChangeTracker** – 版本快照与增量变更追踪。
* **connectors** – PostgreSQL / MySQL 数据库连接器（SQLAlchemy 驱动）。
"""

from .data_matrix import DataMatrix, DataPoint, DataCell
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

# ── seebook 集成模块 ──────────────────────────────────────────────────────────
from .hypercube import HyperCube, ColorCell, ColorScheme, RichDataMatrix, RichColorMatrix
from .lineage import (
    LineageTracker,
    Provenance,
    PhysicalLocation,
    LineageEdge,
    FieldMapping,
    TransformationType,
)
from .dynamic_classifier import (
    TableSignature,
    DynamicDomainDiscoverer,
    AdaptiveLifecycleClassifier,
    UnknownDatabaseProcessor,
)
from .quality import (
    QualityEngine,
    QualityScore,
    QualityIssue,
    QualityIssueType,
    ColorAnomalyDetector,
    StructureAnomalyDetector,
)
from .relationship_extractor import (
    RelationshipExtractor,
    ForeignKey,
    TableRelationship,
)
from .changelog import ChangeTracker, CellChange, VersionSnapshot, ChangeType
from .optimizer import MatrixOptimizer, OptimizationSuggestion

__all__ = [
    # Core dual-matrix (original)
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
    # ── seebook 集成新增 ──────────────────────────────────────────────────
    # Rich cell & HyperCube dual-matrix manager
    "DataCell",
    "HyperCube",
    "ColorCell",
    "ColorScheme",
    "RichDataMatrix",
    "RichColorMatrix",
    # Lineage / provenance
    "LineageTracker",
    "Provenance",
    "PhysicalLocation",
    "LineageEdge",
    "FieldMapping",
    "TransformationType",
    # Dynamic domain discovery (no preset domains)
    "TableSignature",
    "DynamicDomainDiscoverer",
    "AdaptiveLifecycleClassifier",
    "UnknownDatabaseProcessor",
    # Data quality engine
    "QualityEngine",
    "QualityScore",
    "QualityIssue",
    "QualityIssueType",
    "ColorAnomalyDetector",
    "StructureAnomalyDetector",
    # Relationship extraction
    "RelationshipExtractor",
    "ForeignKey",
    "TableRelationship",
    # Change / version tracking
    "ChangeTracker",
    "CellChange",
    "VersionSnapshot",
    "ChangeType",
    # Matrix optimizer (stage-1 → stage-2)
    "MatrixOptimizer",
    "OptimizationSuggestion",
]
