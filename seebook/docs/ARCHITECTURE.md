# 四维矩阵系统架构

## 核心架构

```
物理数据库 → 第一阶段矩阵 → 分析优化 → 第二阶段矩阵 → 输出
     │              │             │              │         │
     │         (发现层)      (AI/质量)      (目标层)   (DDL/可视化)
     │              │             │              │         │
     └──────────────┴─────────────┴──────────────┴─────────┘
                         ↑
                    完整溯源链
```

## 双矩阵设计

### 第一阶段：发现层（不完美但真实）

```python
DataCell {
    t: 扫描时间
    x: 生命周期阶段 (启发式推断)
    y: 数据量级
    z: 主题域 (可能错配)
    payload: {完整元数据}
    provenance: PhysicalLocation {  # ← 关键：直接溯源
        uri: "postgres://host/db/schema/table"
    }
}
```

### 第二阶段：目标层（规范化）

```python
DataCell {
    ...
    provenance: {
        sources: [  # ← 关键：转换血缘
            {
                from: "stage1_cell_id",
                type: MERGE/SPLIT/RENAME,
                reason: "颜色相似度85%, XY距离<50",
                field_mappings: [{source→target}]
            }
        ]
    }
}
```

## 核心能力清单

✅ 已完成：
- 双矩阵架构 (DataMatrix + ColorMatrix)
- 数据库连接器 (PostgreSQL, MySQL)
- 完整溯源链 (URI → 转换 → 字段级)
- AI分类优化 (规则+启发式)
- 质量检测 (颜色异常、结构问题)
- 变更追踪 (增量更新、版本控制)
- 可视化仪表盘

📋 待扩展：
- LLM API集成 (OpenAI/Claude)
- 更多连接器 (Oracle, SQLServer, MongoDB)
- 实时流数据支持
- 敏感数据自动发现
- 与DBT/SQLMesh集成
