"""
变更追踪与版本控制模块

管理矩阵的历史版本，支持：
- 增量更新检测
- 版本对比
- 变更通知
- 回滚能力
"""

from typing import Dict, List, Optional, Any, Set
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import hashlib
import json


class ChangeType(Enum):
    """变更类型"""
    ADDED = "added"              # 新增表
    REMOVED = "removed"          # 删除表
    MODIFIED = "modified"        # 结构变更
    RENAMED = "renamed"          # 重命名
    ARCHIVED = "archived"        # 归档
    RESTORED = "restored"        # 恢复
    
    # 元数据变更
    DOMAIN_CHANGED = "domain_changed"      # 主题域调整
    STAGE_CHANGED = "stage_changed"        # 生命周期变更
    SIZE_CHANGED = "size_changed"          # 数据量变化


@dataclass
class FieldChange:
    """字段级变更"""
    field_name: str
    change_type: ChangeType
    old_value: Any = None
    new_value: Any = None


@dataclass
class CellChange:
    """单元格变更记录"""
    change_id: str
    cell_id: str
    change_type: ChangeType
    timestamp: datetime = field(default_factory=datetime.now)
    
    # 变更详情
    old_state: Optional[Dict] = None
    new_state: Optional[Dict] = None
    field_changes: List[FieldChange] = field(default_factory=list)
    
    # 变更来源
    triggered_by: str = "system"    # system/manual/sync
    reason: str = ""
    
    # 影响评估
    impact_score: float = 0.0       # 0-1，影响程度
    affected_downstream: List[str] = field(default_factory=list)


@dataclass
class VersionSnapshot:
    """版本快照"""
    version_id: str                 # 版本哈希
    timestamp: datetime
    description: str
    
    # 矩阵状态摘要
    cell_count: int
    domain_distribution: Dict[str, int]
    stage_distribution: Dict[str, int]
    
    # 完整状态哈希（用于快速对比）
    state_hash: str
    
    # 变更列表（相对于上一版本）
    changes: List[CellChange] = field(default_factory=list)


class ChangeTracker:
    """
    变更追踪器
    
    管理矩阵的历史版本和增量更新
    """
    
    def __init__(self):
        # 版本历史
        self.versions: List[VersionSnapshot] = []
        
        # 单元格历史（cell_id -> 变更列表）
        self.cell_history: Dict[str, List[CellChange]] = {}
        
        # 当前状态指纹（用于快速检测变更）
        self.current_fingerprints: Dict[str, str] = {}
        
        # 订阅者（谁关心变更）
        self.subscribers: Dict[str, List[str]] = {}  # cell_id -> subscriber_ids
    
    def compute_fingerprint(self, cell_data: Dict) -> str:
        """计算单元格指纹"""
        # 关键字段哈希
        key_fields = {
            "table_name": cell_data.get("table_name"),
            "row_count": cell_data.get("row_count"),
            "column_count": cell_data.get("column_count"),
            "size_bytes": cell_data.get("size_bytes"),
            "business_domain": cell_data.get("business_domain"),
            "lifecycle_stage": cell_data.get("lifecycle_stage"),
        }
        content = json.dumps(key_fields, sort_keys=True)
        return hashlib.md5(content.encode()).hexdigest()[:16]
    
    def detect_changes(self, 
                       previous_cells: Dict[str, Dict], 
                       current_cells: Dict[str, Dict]) -> List[CellChange]:
        """
        检测两次扫描之间的变更
        
        Args:
            previous_cells: 上次扫描的单元格 {cell_id: data}
            current_cells: 本次扫描的单元格 {cell_id: data}
        
        Returns:
            变更列表
        """
        changes = []
        
        previous_ids = set(previous_cells.keys())
        current_ids = set(current_cells.keys())
        
        # 1. 检测新增
        for cell_id in current_ids - previous_ids:
            change = CellChange(
                change_id=f"{cell_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}",
                cell_id=cell_id,
                change_type=ChangeType.ADDED,
                new_state=current_cells[cell_id],
                reason="新表发现",
            )
            changes.append(change)
        
        # 2. 检测删除
        for cell_id in previous_ids - current_ids:
            change = CellChange(
                change_id=f"{cell_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}",
                cell_id=cell_id,
                change_type=ChangeType.REMOVED,
                old_state=previous_cells[cell_id],
                reason="表被删除或不可访问",
            )
            changes.append(change)
        
        # 3. 检测修改
        for cell_id in current_ids & previous_ids:
            old_data = previous_cells[cell_id]
            new_data = current_cells[cell_id]
            
            old_fp = self.compute_fingerprint(old_data)
            new_fp = self.compute_fingerprint(new_data)
            
            if old_fp != new_fp:
                # 详细比较字段变更
                field_changes = self._compare_fields(old_data, new_data)
                
                # 确定主要变更类型
                change_type = self._determine_change_type(field_changes)
                
                change = CellChange(
                    change_id=f"{cell_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}",
                    cell_id=cell_id,
                    change_type=change_type,
                    old_state=old_data,
                    new_state=new_data,
                    field_changes=field_changes,
                    reason=self._generate_change_reason(field_changes),
                    impact_score=self._calculate_impact(field_changes),
                )
                changes.append(change)
        
        return changes
    
    def _compare_fields(self, old_data: Dict, new_data: Dict) -> List[FieldChange]:
        """比较字段变更"""
        field_changes = []
        
        # 监控的关键字段
        key_fields = [
            "row_count", "column_count", "size_bytes",
            "business_domain", "lifecycle_stage", "column_list"
        ]
        
        for field in key_fields:
            old_val = old_data.get(field)
            new_val = new_data.get(field)
            
            if old_val != new_val:
                # 确定变更类型
                if field == "business_domain":
                    change_type = ChangeType.DOMAIN_CHANGED
                elif field == "lifecycle_stage":
                    change_type = ChangeType.STAGE_CHANGED
                elif field in ["row_count", "size_bytes"]:
                    change_type = ChangeType.SIZE_CHANGED
                else:
                    change_type = ChangeType.MODIFIED
                
                field_changes.append(FieldChange(
                    field_name=field,
                    change_type=change_type,
                    old_value=old_val,
                    new_value=new_val,
                ))
        
        return field_changes
    
    def _determine_change_type(self, field_changes: List[FieldChange]) -> ChangeType:
        """根据字段变更确定主要变更类型"""
        if not field_changes:
            return ChangeType.MODIFIED
        
        # 优先级顺序
        priority = [
            ChangeType.DOMAIN_CHANGED,
            ChangeType.STAGE_CHANGED,
            ChangeType.SIZE_CHANGED,
            ChangeType.MODIFIED,
        ]
        
        for p in priority:
            if any(fc.change_type == p for fc in field_changes):
                return p
        
        return ChangeType.MODIFIED
    
    def _generate_change_reason(self, field_changes: List[FieldChange]) -> str:
        """生成变更原因描述"""
        reasons = []
        
        for fc in field_changes:
            if fc.change_type == ChangeType.SIZE_CHANGED:
                if isinstance(fc.old_value, (int, float)) and isinstance(fc.new_value, (int, float)):
                    delta = fc.new_value - fc.old_value
                    pct = (delta / fc.old_value * 100) if fc.old_value else 0
                    reasons.append(f"{fc.field_name}: {delta:+,} ({pct:+.1f}%)")
            elif fc.change_type in [ChangeType.DOMAIN_CHANGED, ChangeType.STAGE_CHANGED]:
                reasons.append(f"{fc.field_name}: {fc.old_value} → {fc.new_value}")
        
        return "; ".join(reasons) if reasons else "元数据变更"
    
    def _calculate_impact(self, field_changes: List[FieldChange]) -> float:
        """计算变更影响程度 0-1"""
        impact = 0.0
        
        for fc in field_changes:
            if fc.change_type == ChangeType.DOMAIN_CHANGED:
                impact += 0.5  # 主题域变更是重大变更
            elif fc.change_type == ChangeType.STAGE_CHANGED:
                impact += 0.3
            elif fc.change_type == ChangeType.SIZE_CHANGED:
                if isinstance(fc.old_value, (int, float)) and fc.old_value > 0:
                    change_ratio = abs(fc.new_value - fc.old_value) / fc.old_value
                    impact += min(0.2, change_ratio * 0.1)
            else:
                impact += 0.1
        
        return min(impact, 1.0)
    
    def create_snapshot(self, 
                        cell_count: int,
                        domain_dist: Dict[str, int],
                        stage_dist: Dict[str, int],
                        changes: List[CellChange],
                        description: str = "") -> VersionSnapshot:
        """创建版本快照"""
        
        # 计算状态哈希
        state_content = json.dumps({
            "cell_count": cell_count,
            "domains": domain_dist,
            "stages": stage_dist,
        }, sort_keys=True)
        state_hash = hashlib.sha256(state_content.encode()).hexdigest()[:16]
        
        snapshot = VersionSnapshot(
            version_id=state_hash,
            timestamp=datetime.now(),
            description=description,
            cell_count=cell_count,
            domain_distribution=domain_dist,
            stage_distribution=stage_dist,
            state_hash=state_hash,
            changes=changes,
        )
        
        self.versions.append(snapshot)
        
        # 记录到单元格历史
        for change in changes:
            if change.cell_id not in self.cell_history:
                self.cell_history[change.cell_id] = []
            self.cell_history[change.cell_id].append(change)
        
        return snapshot
    
    def get_cell_history(self, cell_id: str) -> List[CellChange]:
        """获取单元格的历史变更"""
        return self.cell_history.get(cell_id, [])
    
    def compare_versions(self, version1_id: str, version2_id: str) -> Dict:
        """对比两个版本"""
        v1 = next((v for v in self.versions if v.version_id == version1_id), None)
        v2 = next((v for v in self.versions if v.version_id == version2_id), None)
        
        if not v1 or not v2:
            return {"error": "Version not found"}
        
        return {
            "version1": {
                "id": v1.version_id,
                "timestamp": v1.timestamp.isoformat(),
                "cell_count": v1.cell_count,
            },
            "version2": {
                "id": v2.version_id,
                "timestamp": v2.timestamp.isoformat(),
                "cell_count": v2.cell_count,
            },
            "cell_count_delta": v2.cell_count - v1.cell_count,
            "domain_changes": self._compare_distributions(
                v1.domain_distribution, v2.domain_distribution
            ),
            "stage_changes": self._compare_distributions(
                v1.stage_distribution, v2.stage_distribution
            ),
        }
    
    def _compare_distributions(self, d1: Dict, d2: Dict) -> Dict:
        """对比分布变化"""
        all_keys = set(d1.keys()) | set(d2.keys())
        changes = {}
        
        for key in all_keys:
            old_val = d1.get(key, 0)
            new_val = d2.get(key, 0)
            if old_val != new_val:
                changes[key] = {
                    "old": old_val,
                    "new": new_val,
                    "delta": new_val - old_val,
                }
        
        return changes
    
    def subscribe(self, cell_id: str, subscriber_id: str):
        """订阅单元格变更通知"""
        if cell_id not in self.subscribers:
            self.subscribers[cell_id] = []
        if subscriber_id not in self.subscribers[cell_id]:
            self.subscribers[cell_id].append(subscriber_id)
    
    def get_notifications(self, cell_id: str) -> List[str]:
        """获取订阅者列表"""
        return self.subscribers.get(cell_id, [])
    
    def export_changelog(self, since: Optional[datetime] = None) -> List[Dict]:
        """导出变更日志"""
        changes = []
        
        for cell_id, history in self.cell_history.items():
            for change in history:
                if since is None or change.timestamp >= since:
                    changes.append({
                        "cell_id": cell_id,
                        "change_id": change.change_id,
                        "type": change.change_type.value,
                        "timestamp": change.timestamp.isoformat(),
                        "reason": change.reason,
                        "impact": change.impact_score,
                    })
        
        # 按时间排序
        changes.sort(key=lambda x: x["timestamp"])
        return changes
