"""多流矩阵比较器 (StreamComparator)

灵感来自视频编辑软件中的多轨时间轴编辑模式：在视频编辑软件里，可以同时
加载多条视频流，拖动同步时间轴来对比各流在同一帧的画面。本模块将相同的
理念移植到四维矩阵数据流上——

* **数据流** = 一个 :class:`~four_dim_matrix.hypercube.HyperCube` 快照
* **时间轴** = x 轴（生命周期阶段：new→growth→mature→legacy）或
  z 轴（业务域）作为"播放位置"
* **同步游标** = :class:`StreamCursor`，移动它会同步更新所有流的可见范围
* **轨道对比** = :class:`StreamComparator` 的 :meth:`~StreamComparator.diff`
  方法，输出两条流之间的结构性差异

主要使用场景
-----------
1. **版本比较** — 同一数据库在不同时间点（两次扫描）之间的差异
2. **系统比较** — 两个不同数据库（如 A 系统 vs B 系统）的横向对比
3. **规划辅助** — 对比"现状矩阵"与"目标矩阵"（由设计说明书生成），
   帮助更高质量地设计两套四维矩阵

快速上手
--------
::

    from four_dim_matrix.stream_comparator import StreamComparator

    # 注册两条流
    cmp = StreamComparator()
    cmp.add_stream("before", hypercube_v1)
    cmp.add_stream("after",  hypercube_v2)

    # 查看差异
    diff = cmp.diff("before", "after")
    print(diff.summary())

    # 将游标移到 "mature" 生命周期阶段，同步观察两条流
    cmp.cursor.move_to_lifecycle("mature")
    view = cmp.at_cursor()
    # view["before"] = [DataCell, ...]
    # view["after"]  = [DataCell, ...]

    # 游标在所有生命周期阶段上逐步滑动（类似拖动时间轴）
    for frame in cmp.cursor.frames():
        view = cmp.at_cursor()
        print(f"  [{frame}] before={len(view['before'])} after={len(view['after'])}")
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, List, Optional, Tuple

from .data_matrix import DataCell
from .hypercube import HyperCube


# ---------------------------------------------------------------------------
# X-axis lifecycle stage ordering
# ---------------------------------------------------------------------------

#: Canonical lifecycle stage names ordered from newest to most deprecated.
LIFECYCLE_ORDER: List[str] = ["new", "growth", "mature", "legacy", "deprecated"]

#: Typical x-coordinate values assigned to lifecycle stages in demo.py /
#: build_hypercube_from_adapter().
LIFECYCLE_X: Dict[str, int] = {
    "new":        20,
    "growth":     50,
    "mature":     80,
    "legacy":    110,
    "deprecated": 140,
}


# ---------------------------------------------------------------------------
# StreamCursor – synchronized playhead
# ---------------------------------------------------------------------------

class StreamCursor:
    """同步游标，类似视频编辑器的播放头（playhead）。

    游标在"位置序列"上滑动，每个位置对应一个生命周期阶段（x 轴）。
    移动游标会影响 :class:`StreamComparator` 的 :meth:`~StreamComparator.at_cursor`
    输出，使所有已注册的流同步展示当前位置的数据。

    Examples::

        cursor = StreamCursor()
        cursor.move_to_lifecycle("mature")
        print(cursor.lifecycle)   # "mature"
        print(cursor.x)           # 80

        for frame in cursor.frames():
            print(f"Frame: {frame}")  # "new", "growth", "mature", ...
    """

    def __init__(self, lifecycle: str = "mature") -> None:
        self._lifecycle = lifecycle if lifecycle in LIFECYCLE_ORDER else LIFECYCLE_ORDER[0]

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def lifecycle(self) -> str:
        """当前游标所在的生命周期阶段名称。"""
        return self._lifecycle

    @property
    def x(self) -> int:
        """当前位置对应的 x 轴坐标值。"""
        return LIFECYCLE_X.get(self._lifecycle, LIFECYCLE_X["mature"])

    @property
    def index(self) -> int:
        """在 :data:`LIFECYCLE_ORDER` 序列中的位置索引（0-based）。"""
        try:
            return LIFECYCLE_ORDER.index(self._lifecycle)
        except ValueError:
            return 0

    # ------------------------------------------------------------------
    # Navigation
    # ------------------------------------------------------------------

    def move_to_lifecycle(self, lifecycle: str) -> "StreamCursor":
        """将游标移动到指定的生命周期阶段。

        Parameters:
            lifecycle: 阶段名称（``"new"``, ``"growth"``, ``"mature"``,
                ``"legacy"``, ``"deprecated"``）。未知名称被静默忽略。

        Returns:
            ``self`` — 支持链式调用。
        """
        if lifecycle in LIFECYCLE_ORDER:
            self._lifecycle = lifecycle
        return self

    def advance(self) -> "StreamCursor":
        """将游标向右（更成熟）移动一步。到达末尾后不再前进。"""
        idx = self.index
        if idx < len(LIFECYCLE_ORDER) - 1:
            self._lifecycle = LIFECYCLE_ORDER[idx + 1]
        return self

    def rewind(self) -> "StreamCursor":
        """将游标向左（更新）移动一步。到达起点后不再后退。"""
        idx = self.index
        if idx > 0:
            self._lifecycle = LIFECYCLE_ORDER[idx - 1]
        return self

    def reset(self) -> "StreamCursor":
        """将游标重置到序列开头（``"new"``）。"""
        self._lifecycle = LIFECYCLE_ORDER[0]
        return self

    def frames(self) -> Iterator[str]:
        """遍历所有生命周期阶段（从当前位置到末尾），类似视频帧序列。

        Yields:
            每个生命周期阶段名称，同时将游标移动到该阶段。
        """
        start = self.index
        for stage in LIFECYCLE_ORDER[start:]:
            self._lifecycle = stage
            yield stage

    def __repr__(self) -> str:
        return f"StreamCursor(lifecycle={self._lifecycle!r}, x={self.x})"


# ---------------------------------------------------------------------------
# StreamDiff – structured difference between two HyperCube streams
# ---------------------------------------------------------------------------

@dataclass
class TableChange:
    """记录单张表在两条流之间的变化详情。"""

    table_name: str
    old_domain: str = ""
    new_domain: str = ""
    old_lifecycle: str = ""
    new_lifecycle: str = ""
    old_row_count: int = 0
    new_row_count: int = 0

    @property
    def domain_changed(self) -> bool:
        return bool(self.old_domain) and bool(self.new_domain) and self.old_domain != self.new_domain

    @property
    def lifecycle_changed(self) -> bool:
        return bool(self.old_lifecycle) and bool(self.new_lifecycle) and self.old_lifecycle != self.new_lifecycle

    @property
    def volume_changed(self) -> bool:
        """如果行数变化超过 20% 则视为"量级变化"。"""
        if self.old_row_count == 0 and self.new_row_count == 0:
            return False
        if self.old_row_count == 0:
            return self.new_row_count > 0
        return abs(self.new_row_count - self.old_row_count) / self.old_row_count >= 0.2

    def to_dict(self) -> Dict[str, Any]:
        return {
            "table_name": self.table_name,
            "domain": {"before": self.old_domain, "after": self.new_domain,
                       "changed": self.domain_changed},
            "lifecycle": {"before": self.old_lifecycle, "after": self.new_lifecycle,
                          "changed": self.lifecycle_changed},
            "row_count": {"before": self.old_row_count, "after": self.new_row_count,
                          "changed": self.volume_changed},
        }


@dataclass
class StreamDiff:
    """两条矩阵流之间的结构性差异，类似视频帧 diff。

    Attributes:
        stream_a: 第一条流（"before"）的名称。
        stream_b: 第二条流（"after"）的名称。
        added: 仅出现在 stream_b 中的表名列表（新增）。
        removed: 仅出现在 stream_a 中的表名列表（已删除）。
        changes: 两条流中均存在但有属性变化的表的详情列表。

    Examples::

        diff = comparator.diff("v1", "v2")
        print(diff.summary())
        d = diff.to_dict()
    """

    stream_a: str = ""
    stream_b: str = ""
    added: List[str] = field(default_factory=list)
    removed: List[str] = field(default_factory=list)
    changes: List[TableChange] = field(default_factory=list)

    # ------------------------------------------------------------------
    # Convenience views
    # ------------------------------------------------------------------

    @property
    def domain_changes(self) -> List[TableChange]:
        """仅返回业务域发生变化的 :class:`TableChange` 条目。"""
        return [c for c in self.changes if c.domain_changed]

    @property
    def lifecycle_changes(self) -> List[TableChange]:
        """仅返回生命周期阶段发生变化的 :class:`TableChange` 条目。"""
        return [c for c in self.changes if c.lifecycle_changed]

    @property
    def volume_changes(self) -> List[TableChange]:
        """仅返回数据量发生显著变化（≥20%）的 :class:`TableChange` 条目。"""
        return [c for c in self.changes if c.volume_changed]

    @property
    def has_differences(self) -> bool:
        """如果两条流之间存在任何差异则返回 ``True``。"""
        return bool(self.added or self.removed or self.changes)

    # ------------------------------------------------------------------
    # Reporting
    # ------------------------------------------------------------------

    def summary(self) -> str:
        """返回人类可读的差异摘要字符串。

        Returns:
            多行文本摘要，覆盖新增/删除/属性变化三类差异。
        """
        lines = [
            f"=== 矩阵流比较：{self.stream_a!r} → {self.stream_b!r} ===",
        ]
        if not self.has_differences:
            lines.append("  ✅ 两条流完全一致，无任何差异。")
            return "\n".join(lines)

        if self.added:
            lines.append(f"\n新增表（{len(self.added)} 张）：")
            for t in sorted(self.added):
                lines.append(f"  + {t}")

        if self.removed:
            lines.append(f"\n删除表（{len(self.removed)} 张）：")
            for t in sorted(self.removed):
                lines.append(f"  - {t}")

        if self.domain_changes:
            lines.append(f"\n业务域变更（{len(self.domain_changes)} 张）：")
            for ch in sorted(self.domain_changes, key=lambda c: c.table_name):
                lines.append(f"  ~ {ch.table_name}: {ch.old_domain!r} → {ch.new_domain!r}")

        if self.lifecycle_changes:
            lines.append(f"\n生命周期变更（{len(self.lifecycle_changes)} 张）：")
            for ch in sorted(self.lifecycle_changes, key=lambda c: c.table_name):
                lines.append(f"  ~ {ch.table_name}: {ch.old_lifecycle!r} → {ch.new_lifecycle!r}")

        if self.volume_changes:
            lines.append(f"\n数据量显著变化（{len(self.volume_changes)} 张）：")
            for ch in sorted(self.volume_changes, key=lambda c: c.table_name):
                pct = (
                    f"{(ch.new_row_count - ch.old_row_count) / ch.old_row_count * 100:+.0f}%"
                    if ch.old_row_count
                    else "∞"
                )
                lines.append(
                    f"  ~ {ch.table_name}: {ch.old_row_count:,} → {ch.new_row_count:,} ({pct})"
                )

        return "\n".join(lines)

    def to_dict(self) -> Dict[str, Any]:
        """将差异结果序列化为可 JSON 化的字典。"""
        return {
            "streams": {"a": self.stream_a, "b": self.stream_b},
            "summary": {
                "added_count": len(self.added),
                "removed_count": len(self.removed),
                "domain_changes": len(self.domain_changes),
                "lifecycle_changes": len(self.lifecycle_changes),
                "volume_changes": len(self.volume_changes),
            },
            "added": sorted(self.added),
            "removed": sorted(self.removed),
            "changes": [c.to_dict() for c in sorted(self.changes, key=lambda c: c.table_name)],
        }


# ---------------------------------------------------------------------------
# StreamComparator – multi-track matrix timeline
# ---------------------------------------------------------------------------

class StreamComparator:
    """多轨矩阵时间轴比较器 — 类比视频编辑软件中的多轨编辑界面。

    每条"轨道"（stream）是一个 :class:`~four_dim_matrix.hypercube.HyperCube`
    快照，代表某个数据库在某一时刻的四维矩阵状态。
    :class:`StreamCursor` 充当同步游标，可以在生命周期阶段（x 轴）上
    滑动，令所有轨道同步展示当前"帧"的数据。

    典型工作流::

                  new ──── growth ──── mature ──── legacy
                  │                    ↑                │
        track A:  ██   ██    ████     [cursor]  ████   █
        track B:  ██   ████  ████     [cursor]  ███    ██
                                      ↑
                               当前游标位置

    Examples::

        cmp = StreamComparator()
        cmp.add_stream("before", hc_v1)
        cmp.add_stream("after",  hc_v2)

        # 同步游标到 "mature" 阶段
        cmp.cursor.move_to_lifecycle("mature")
        view = cmp.at_cursor()

        # 逐帧扫描（类似拖动时间轴）
        for frame in cmp.scan():
            print(frame["cursor"], [len(v) for v in frame["streams"].values()])

        # 计算两条轨道的差异
        diff = cmp.diff("before", "after")
        print(diff.summary())
    """

    def __init__(self) -> None:
        self._streams: Dict[str, HyperCube] = {}
        self.cursor = StreamCursor()

    # ------------------------------------------------------------------
    # Stream registration
    # ------------------------------------------------------------------

    def add_stream(self, name: str, hypercube: HyperCube) -> "StreamComparator":
        """注册一条矩阵数据流（轨道）。

        Parameters:
            name: 轨道名称（如 ``"before"``、``"v2"``、``"db_a"``）。
                同名注册会覆盖旧轨道。
            hypercube: 代表该轨道的 :class:`~four_dim_matrix.hypercube.HyperCube`。

        Returns:
            ``self`` — 支持链式调用。
        """
        self._streams[name] = hypercube
        return self

    def remove_stream(self, name: str) -> "StreamComparator":
        """移除一条已注册的流。未找到时静默忽略。"""
        self._streams.pop(name, None)
        return self

    @property
    def stream_names(self) -> List[str]:
        """按注册顺序返回所有流的名称列表。"""
        return list(self._streams.keys())

    def __len__(self) -> int:
        return len(self._streams)

    # ------------------------------------------------------------------
    # Cursor-based synchronized view
    # ------------------------------------------------------------------

    def at_cursor(self, lifecycle: Optional[str] = None) -> Dict[str, List[DataCell]]:
        """返回所有流在当前游标位置（生命周期阶段）的数据单元格。

        这是"同步拖动时间轴观察变化"的核心方法：所有轨道使用同一游标位置
        进行过滤，返回的结果可直接对比展示。

        Parameters:
            lifecycle: 可选，临时覆盖游标位置（不修改游标状态）。

        Returns:
            ``{stream_name: [DataCell, ...]}`` — 每条流中属于当前游标
            生命周期阶段的单元格列表。若某流中该阶段无数据则返回空列表。
        """
        stage = lifecycle or self.cursor.lifecycle
        result: Dict[str, List[DataCell]] = {}
        for name, hc in self._streams.items():
            result[name] = [
                c for c in hc.data_matrix.cells.values()
                if c.lifecycle_stage == stage
            ]
        return result

    def sync_filter(
        self,
        domain: Optional[str] = None,
        lifecycle: Optional[str] = None,
    ) -> Dict[str, List[DataCell]]:
        """对所有流应用相同的过滤条件并返回结果。

        类似在视频编辑器中给所有轨道同时加上同一个颜色遮罩或时间范围限制。

        Parameters:
            domain: 按业务域名称过滤（精确匹配，大小写不敏感）。
            lifecycle: 按生命周期阶段名称过滤（精确匹配，大小写不敏感）。

        Returns:
            ``{stream_name: [DataCell, ...]}`` — 每条流中满足条件的单元格。
        """
        domain_lc = domain.lower() if domain else None
        lifecycle_lc = lifecycle.lower() if lifecycle else None

        result: Dict[str, List[DataCell]] = {}
        for name, hc in self._streams.items():
            cells: List[DataCell] = list(hc.data_matrix.cells.values())
            if domain_lc is not None:
                cells = [c for c in cells if c.business_domain.lower() == domain_lc]
            if lifecycle_lc is not None:
                cells = [c for c in cells if c.lifecycle_stage.lower() == lifecycle_lc]
            result[name] = cells
        return result

    # ------------------------------------------------------------------
    # Timeline scan
    # ------------------------------------------------------------------

    def scan(
        self, start: Optional[str] = None
    ) -> Iterator[Dict[str, Any]]:
        """逐帧扫描所有生命周期阶段，返回每一"帧"的多流视图。

        类比在视频编辑器中从头到尾拖动时间轴，依次观察每帧的多轨画面。

        Parameters:
            start: 起始生命周期阶段。默认从游标当前位置开始。

        Yields:
            每一帧的字典：

            .. code-block:: python

                {
                    "cursor": "mature",          # 当前阶段名称
                    "x": 80,                     # x 轴坐标
                    "streams": {                 # 所有流在此帧的数据
                        "before": [DataCell, …],
                        "after":  [DataCell, …],
                    },
                    "counts": {"before": 3, "after": 4},  # 各流表数量
                }
        """
        if start is not None:
            self.cursor.move_to_lifecycle(start)

        for stage in self.cursor.frames():
            view = self.at_cursor(lifecycle=stage)
            yield {
                "cursor": stage,
                "x": LIFECYCLE_X.get(stage, 0),
                "streams": view,
                "counts": {k: len(v) for k, v in view.items()},
            }

    # ------------------------------------------------------------------
    # Diff
    # ------------------------------------------------------------------

    def diff(self, name_a: str, name_b: str) -> StreamDiff:
        """计算两条流之间的结构性差异。

        类比在视频编辑器中将两条轨道做"视觉 diff"，高亮显示不同的帧。

        Parameters:
            name_a: 第一条流（"before"）的名称。
            name_b: 第二条流（"after"）的名称。

        Returns:
            :class:`StreamDiff` — 包含新增、删除和属性变化的完整差异报告。

        Raises:
            KeyError: 若 ``name_a`` 或 ``name_b`` 未注册。
        """
        if name_a not in self._streams:
            raise KeyError(f"流 {name_a!r} 未注册，已注册的流：{self.stream_names}")
        if name_b not in self._streams:
            raise KeyError(f"流 {name_b!r} 未注册，已注册的流：{self.stream_names}")

        hc_a = self._streams[name_a]
        hc_b = self._streams[name_b]

        # Build lookup: table_name → DataCell (one cell per table)
        # For simplicity: last cell wins if the same table appears multiple times
        # (e.g., multiple schema snapshots in one HyperCube)
        def _index(hc: HyperCube) -> Dict[str, DataCell]:
            idx: Dict[str, DataCell] = {}
            for c in hc.data_matrix.cells.values():
                if c.table_name:
                    idx[c.table_name.lower()] = c
            return idx

        idx_a = _index(hc_a)
        idx_b = _index(hc_b)

        tables_a = set(idx_a.keys())
        tables_b = set(idx_b.keys())

        added = sorted(tables_b - tables_a)
        removed = sorted(tables_a - tables_b)

        changes: List[TableChange] = []
        for tname in sorted(tables_a & tables_b):
            ca = idx_a[tname]
            cb = idx_b[tname]
            change = TableChange(
                table_name=tname,
                old_domain=ca.business_domain,
                new_domain=cb.business_domain,
                old_lifecycle=ca.lifecycle_stage,
                new_lifecycle=cb.lifecycle_stage,
                old_row_count=ca.row_count,
                new_row_count=cb.row_count,
            )
            # Only include in changes if something actually changed
            if change.domain_changed or change.lifecycle_changed or change.volume_changed:
                changes.append(change)

        return StreamDiff(
            stream_a=name_a,
            stream_b=name_b,
            added=added,
            removed=removed,
            changes=changes,
        )

    # ------------------------------------------------------------------
    # Reporting
    # ------------------------------------------------------------------

    def summary(self) -> str:
        """返回所有已注册流的摘要。"""
        if not self._streams:
            return "StreamComparator: 当前没有已注册的流。"

        lines = [
            f"StreamComparator — {len(self._streams)} 条流  |  游标：{self.cursor!r}",
            "",
        ]
        for name, hc in self._streams.items():
            sm = hc.data_matrix.get_summary()
            if sm.get("empty"):
                lines.append(f"  [{name}]  (空)")
            else:
                lines.append(
                    f"  [{name}]  {sm['total_cells']} 张表"
                    f"  域：{sm.get('domains', [])}  "
                    f"  阶段：{sm.get('stages', [])}"
                )
        return "\n".join(lines)
