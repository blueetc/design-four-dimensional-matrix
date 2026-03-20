"""
数据质量评分与异常检测模块

基于四维矩阵的颜色模式发现数据质量问题：
- 颜色异常点检测（孤立点）
- 时序颜色突变检测
- 跨维度一致性检查
"""

from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import numpy as np
from collections import defaultdict

from .data_matrix import DataCell


class QualityIssueType(Enum):
    """质量问题类型"""
    # 颜色异常
    COLOR_OUTLIER = "color_outlier"           # 颜色孤立点
    COLOR_DRIFT = "color_drift"               # 颜色漂移
    
    # 结构异常
    SCHEMA_ANOMALY = "schema_anomaly"         # 结构异常
    SIZE_ANOMALY = "size_anomaly"             # 数据量异常
    GROWTH_ANOMALY = "growth_anomaly"         # 增长异常
    
    # 元数据异常
    DOMAIN_MISMATCH = "domain_mismatch"       # 主题域错配
    STAGE_MISMATCH = "stage_mismatch"         # 生命周期误判
    
    # 关联异常
    ORPHAN_TABLE = "orphan_table"             # 孤儿表（无关联）
    CIRCULAR_REF = "circular_ref"             # 循环依赖
    
    # 合规异常
    NO_PRIMARY_KEY = "no_primary_key"         # 无主键
    NO_INDEX = "no_index"                     # 无索引
    SENSITIVE_DATA = "sensitive_data"         # 敏感数据风险


@dataclass
class QualityIssue:
    """质量问题记录"""
    issue_id: str
    cell_id: str
    issue_type: QualityIssueType
    severity: str  # critical/high/medium/low
    
    # 问题描述
    title: str
    description: str
    
    # 检测详情
    detected_at: datetime = field(default_factory=datetime.now)
    detector: str = "system"  # system/ai/manual
    
    # 证据
    evidence: Dict[str, Any] = field(default_factory=dict)
    
    # 建议
    suggestion: str = ""
    auto_fixable: bool = False
    
    # 状态
    status: str = "open"  # open/confirmed/fixed/ignored
    fixed_at: Optional[datetime] = None
    fixed_by: Optional[str] = None


@dataclass
class QualityScore:
    """质量评分"""
    cell_id: str
    overall_score: float  # 0-100
    
    # 分项评分
    schema_score: float = 100.0      # 结构完整性
    consistency_score: float = 100.0 # 一致性
    freshness_score: float = 100.0   # 新鲜度
    lineage_score: float = 100.0     # 血缘完整度
    
    # 评分依据
    issues: List[QualityIssue] = field(default_factory=list)
    
    def to_dict(self) -> Dict:
        return {
            "cell_id": self.cell_id,
            "overall_score": self.overall_score,
            "breakdown": {
                "schema": self.schema_score,
                "consistency": self.consistency_score,
                "freshness": self.freshness_score,
                "lineage": self.lineage_score,
            },
            "issue_count": len(self.issues),
            "critical_issues": len([i for i in self.issues if i.severity == "critical"]),
        }


class ColorAnomalyDetector:
    """颜色异常检测器"""
    
    def __init__(self, threshold_std: float = 2.0):
        self.threshold_std = threshold_std  # 标准差阈值
    
    def detect_outliers(self, color_matrix) -> List[QualityIssue]:
        """
        检测颜色孤立点
        
        原理：在颜色空间中与其他点距离过远的点
        """
        issues = []
        cells = list(color_matrix.cells.values())
        
        if len(cells) < 3:
            return issues
        
        # 按Z轴分组检测
        z_groups = defaultdict(list)
        for key, cell in color_matrix.cells.items():
            z_groups[key[3]].append(cell)
        
        for z, group_cells in z_groups.items():
            if len(group_cells) < 3:
                continue
            
            # 计算组内颜色分布
            rgb_array = np.array([[c.r, c.g, c.b] for c in group_cells])
            mean_rgb = np.mean(rgb_array, axis=0)
            std_rgb = np.std(rgb_array, axis=0)
            
            # 检测离群点
            for cell in group_cells:
                rgb = np.array([cell.r, cell.g, cell.b])
                z_score = np.abs((rgb - mean_rgb) / (std_rgb + 1e-6))
                max_z = np.max(z_score)
                
                if max_z > self.threshold_std:
                    distance = np.linalg.norm(rgb - mean_rgb)
                    
                    issue = QualityIssue(
                        issue_id=f"color_outlier_{cell.source_coordinates}_{datetime.now().strftime('%Y%m%d%H%M%S')}",
                        cell_id=str(cell.source_coordinates),
                        issue_type=QualityIssueType.COLOR_OUTLIER,
                        severity="medium" if max_z < 3 else "high",
                        title="颜色异常孤立点",
                        description=f"该表在颜色空间中偏离同域其他表 {distance:.1f} 个单位，可能存在分类错误",
                        evidence={
                            "z_score": float(max_z),
                            "distance": float(distance),
                            "mean_rgb": mean_rgb.tolist(),
                            "actual_rgb": [cell.r, cell.g, cell.b],
                        },
                        suggestion="检查该表的主题域分类是否正确",
                    )
                    issues.append(issue)
        
        return issues
    
    def detect_drift(self, 
                     current_matrix, 
                     previous_matrix,
                     time_window: int = 7) -> List[QualityIssue]:
        """
        检测颜色漂移
        
        原理：同一物理表在不同时间的颜色发生显著变化
        """
        issues = []
        
        # 获取共同表
        current_ids = set(current_matrix.cells.keys())
        previous_ids = set(previous_matrix.cells.keys())
        common_ids = current_ids & previous_ids
        
        for cell_id in common_ids:
            curr_cell = current_matrix.cells[cell_id]
            prev_cell = previous_matrix.cells[cell_id]
            
            # 计算颜色变化
            curr_rgb = np.array([curr_cell.r, curr_cell.g, curr_cell.b])
            prev_rgb = np.array([prev_cell.r, prev_cell.g, prev_cell.b])
            color_distance = np.linalg.norm(curr_rgb - prev_rgb)
            
            # 变化过大（>50为显著变化）
            if color_distance > 50:
                issue = QualityIssue(
                    issue_id=f"color_drift_{cell_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}",
                    cell_id=str(cell_id),
                    issue_type=QualityIssueType.COLOR_DRIFT,
                    severity="high" if color_distance > 100 else "medium",
                    title="颜色显著漂移",
                    description=f"该表颜色在短时间内变化 {color_distance:.1f}，可能经历了重大变更",
                    evidence={
                        "color_distance": float(color_distance),
                        "previous_rgb": prev_rgb.tolist(),
                        "current_rgb": curr_rgb.tolist(),
                    },
                    suggestion="检查该表是否经历了结构变更或数据迁移",
                )
                issues.append(issue)
        
        return issues


class StructureAnomalyDetector:
    """结构异常检测器"""
    
    def detect_schema_issues(self, data_matrix) -> List[QualityIssue]:
        """检测结构问题"""
        issues = []
        
        for key, cell in data_matrix.cells.items():
            # 1. 无主键
            if not cell.payload.get("primary_key"):
                issues.append(QualityIssue(
                    issue_id=f"no_pk_{cell.table_name}",
                    cell_id=str(key),
                    issue_type=QualityIssueType.NO_PRIMARY_KEY,
                    severity="high",
                    title="表缺少主键",
                    description=f"表 {cell.table_name} 没有定义主键，可能影响数据一致性和查询性能",
                    suggestion="建议为表添加主键约束",
                    auto_fixable=False,
                ))
            
            # 2. 无索引
            indexes = cell.payload.get("indexes", [])
            if len(indexes) == 0:
                issues.append(QualityIssue(
                    issue_id=f"no_idx_{cell.table_name}",
                    cell_id=str(key),
                    issue_type=QualityIssueType.NO_INDEX,
                    severity="medium",
                    title="表缺少索引",
                    description=f"表 {cell.table_name} 没有索引，大数据量时查询性能可能受影响",
                    suggestion="建议根据查询模式添加适当的索引",
                ))
            
            # 3. 列数异常（过多或过少）
            col_count = cell.column_count
            if col_count > 100:
                issues.append(QualityIssue(
                    issue_id=f"too_many_cols_{cell.table_name}",
                    cell_id=str(key),
                    issue_type=QualityIssueType.SCHEMA_ANOMALY,
                    severity="medium",
                    title="表列数过多",
                    description=f"表 {cell.table_name} 有 {col_count} 列，可能存在宽表反模式",
                    suggestion="考虑将表拆分为多个相关表",
                ))
            elif col_count < 2 and cell.row_count > 100:
                issues.append(QualityIssue(
                    issue_id=f"too_few_cols_{cell.table_name}",
                    cell_id=str(key),
                    issue_type=QualityIssueType.SCHEMA_ANOMALY,
                    severity="low",
                    title="表列数过少",
                    description=f"表 {cell.table_name} 只有 {col_count} 列但数据量较大",
                ))
        
        return issues
    
    def detect_size_anomalies(self, data_matrix) -> List[QualityIssue]:
        """检测数据量异常"""
        issues = []
        
        # 计算全局统计
        row_counts = [c.row_count for c in data_matrix.cells.values() if c.row_count > 0]
        if not row_counts:
            return issues
        
        mean_rows = np.mean(row_counts)
        std_rows = np.std(row_counts)
        
        for key, cell in data_matrix.cells.items():
            if cell.row_count == 0:
                continue
            
            # Z-score检测
            z_score = abs(cell.row_count - mean_rows) / (std_rows + 1e-6)
            
            if z_score > 3:  # 3个标准差以外
                if cell.row_count > mean_rows:
                    severity = "medium"
                    title = "表数据量异常大"
                    desc = f"表 {cell.table_name} 有 {cell.row_count:,} 行，远超平均水平 ({mean_rows:,.0f})"
                    suggestion = "考虑分区或归档策略"
                else:
                    severity = "low"
                    title = "表数据量异常小"
                    desc = f"表 {cell.table_name} 只有 {cell.row_count:,} 行，远低于平均水平"
                    suggestion = "检查是否为测试表或废弃表"
                
                issues.append(QualityIssue(
                    issue_id=f"size_anomaly_{cell.table_name}",
                    cell_id=str(key),
                    issue_type=QualityIssueType.SIZE_ANOMALY,
                    severity=severity,
                    title=title,
                    description=desc,
                    evidence={
                        "row_count": cell.row_count,
                        "mean_rows": float(mean_rows),
                        "z_score": float(z_score),
                    },
                    suggestion=suggestion,
                ))
        
        return issues


class QualityEngine:
    """
    质量引擎
    
    整合多种检测器，提供统一质量评分
    """
    
    def __init__(self):
        self.color_detector = ColorAnomalyDetector()
        self.structure_detector = StructureAnomalyDetector()
        
        # 质量规则权重
        self.weights = {
            "schema": 0.3,
            "consistency": 0.3,
            "freshness": 0.2,
            "lineage": 0.2,
        }
    
    def evaluate(self, hypercube, previous_hypercube=None) -> Dict[str, QualityScore]:
        """
        评估整个超立方体的质量
        
        Returns:
            {cell_id: QualityScore}
        """
        scores = {}
        all_issues = []
        
        # 1. 颜色异常检测
        color_issues = self.color_detector.detect_outliers(hypercube.color_matrix)
        all_issues.extend(color_issues)
        
        # 2. 颜色漂移检测（如果有历史）
        if previous_hypercube:
            drift_issues = self.color_detector.detect_drift(
                hypercube.color_matrix, 
                previous_hypercube.color_matrix
            )
            all_issues.extend(drift_issues)
        
        # 3. 结构异常检测
        schema_issues = self.structure_detector.detect_schema_issues(hypercube.data_matrix)
        all_issues.extend(schema_issues)
        
        # 4. 数据量异常检测
        size_issues = self.structure_detector.detect_size_anomalies(hypercube.data_matrix)
        all_issues.extend(size_issues)
        
        # 5. 按单元格聚合评分
        cell_issues = defaultdict(list)
        for issue in all_issues:
            cell_issues[issue.cell_id].append(issue)
        
        # 6. 计算每个单元格的评分
        for key, cell in hypercube.data_matrix.cells.items():
            cell_id = str(key)
            issues = cell_issues.get(cell_id, [])
            
            score = self._calculate_score(cell, issues)
            scores[cell_id] = score
        
        return scores
    
    def _calculate_score(self, cell: DataCell, issues: List[QualityIssue]) -> QualityScore:
        """计算单个单元格的质量评分"""
        
        score = QualityScore(
            cell_id=str((cell.t, cell.x, cell.y, cell.z)),
            overall_score=100.0,
            issues=issues,
        )
        
        # 根据问题扣减分数
        for issue in issues:
            deduction = {
                "critical": 30,
                "high": 15,
                "medium": 8,
                "low": 3,
            }.get(issue.severity, 5)
            
            if issue.issue_type in [QualityIssueType.NO_PRIMARY_KEY, 
                                     QualityIssueType.SCHEMA_ANOMALY]:
                score.schema_score -= deduction
            elif issue.issue_type in [QualityIssueType.COLOR_OUTLIER,
                                      QualityIssueType.DOMAIN_MISMATCH]:
                score.consistency_score -= deduction
            elif issue.issue_type in [QualityIssueType.COLOR_DRIFT]:
                score.freshness_score -= deduction
            else:
                score.overall_score -= deduction
        
        # 确保分数不低于0
        score.schema_score = max(0, score.schema_score)
        score.consistency_score = max(0, score.consistency_score)
        score.freshness_score = max(0, score.freshness_score)
        score.lineage_score = max(0, score.lineage_score)
        
        # 计算综合得分
        score.overall_score = (
            score.schema_score * self.weights["schema"] +
            score.consistency_score * self.weights["consistency"] +
            score.freshness_score * self.weights["freshness"] +
            score.lineage_score * self.weights["lineage"]
        )
        
        return score
    
    def generate_report(self, scores: Dict[str, QualityScore]) -> Dict:
        """生成质量报告"""
        if not scores:
            return {"error": "No scores available"}
        
        all_issues = []
        for score in scores.values():
            all_issues.extend(score.issues)
        
        # 按严重程度统计
        severity_count = defaultdict(int)
        type_count = defaultdict(int)
        
        for issue in all_issues:
            severity_count[issue.severity] += 1
            type_count[issue.issue_type.value] += 1
        
        # 评分分布
        score_values = [s.overall_score for s in scores.values()]
        
        return {
            "summary": {
                "total_cells": len(scores),
                "total_issues": len(all_issues),
                "severity_distribution": dict(severity_count),
                "issue_type_distribution": dict(type_count),
                "average_score": np.mean(score_values),
                "min_score": min(score_values),
                "max_score": max(score_values),
            },
            "critical_issues": [
                {
                    "id": i.issue_id,
                    "cell": i.cell_id,
                    "type": i.issue_type.value,
                    "title": i.title,
                }
                for i in all_issues if i.severity == "critical"
            ],
            "low_scoring_cells": [
                {
                    "cell_id": s.cell_id,
                    "score": s.overall_score,
                    "issues": len(s.issues),
                }
                for s in sorted(scores.values(), key=lambda x: x.overall_score)[:10]
            ],
        }
