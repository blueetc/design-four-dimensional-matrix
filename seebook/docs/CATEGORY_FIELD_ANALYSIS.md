# 分类字段分析 - 五维矩阵设计

## 核心命题

> **主域的核心表一定有分类，分类多少是Z轴的重要信息**

## 问题背景

传统四维矩阵的Z轴仅基于表名相似度划分主题域，存在缺陷：
- `users` 和 `user_profiles` 可能分到同一域
- 无法区分核心业务表和附属表
- 忽略了表内部结构的业务含义

## 解决方案：五维矩阵

在五维矩阵中增加 **Z' 轴（分类复杂度）**：

```
原始四维矩阵: (t, x, y, z)
增强五维矩阵: (t, x, y, z_domain, z_category)

其中:
  t: 时间维度 (更新时间)
  x: 生命周期维度 (new/growth/mature/legacy)
  y: 数据量级维度 (行数)
  z_domain: 主题域维度 (命名+外键聚类)
  z_category: 分类复杂度维度 (分类字段数量和深度)
```

## 分类字段识别

### 什么是分类字段？

```sql
-- 典型的分类字段特征
role_type:       admin | user | guest | vip                -- 角色分类
order_status:    pending | paid | shipped | completed      -- 状态分类
priority_level:  high | normal | low                       -- 优先级分类
product_type:    physical | digital | service              -- 类型分类
```

### 识别标准

```python
def is_classification_field(column_stats):
    """
    判断是否为分类字段
    
    标准:
    1. 唯一值比例 < 10% (distinct_count / total_rows)
    2. 不同值数量 >= 2
    3. 有明确的分布模式（非随机）
    """
    uniqueness_ratio = column_stats['distinct_count'] / column_stats['total_rows']
    return uniqueness_ratio < 0.1 and column_stats['distinct_count'] >= 2
```

## 分类复杂度评分

### 计算公式

```python
def get_category_complexity_score(table_profile):
    """
    计算表的分类复杂度评分 (0-100)
    """
    # 基础分：分类字段数 * 10
    base_score = min(50, num_category_fields * 10)
    
    # 密度加分：分类字段占比 * 20
    density_bonus = category_density * 20
    
    # 类别复杂度：总类别数的对数 * 5
    total_categories = sum(cf.distinct_count for cf in category_fields)
    complexity_bonus = min(30, log2(total_categories + 1) * 5)
    
    return min(100, base_score + density_bonus + complexity_bonus)
```

### 复杂度分级

| 复杂度 | 等级 | 说明 | 示例 |
|--------|------|------|------|
| 0-20 | 支撑表 | 配置、日志 | system_config |
| 20-40 | 辅助表 | 关联表、明细表 | order_items |
| 40-60 | 重要表 | 有业务状态管理 | users, products |
| 60-80 | 核心表 | 复杂业务状态机 | orders, workitem |
| 80-100 | 枢纽表 | 多维度分类 | 流程中心、调度中心 |

## OA数据库实例分析

### 原始四维坐标

| 表名 | Z(域) | X(周期) | Y(量级) |
|------|-------|---------|---------|
| mv_form_data_inst | 0 | 80 | 254 |
| mv_form_file | 0 | 80 | 254 |
| mv_formset_inst | 0 | 110 | 127 |
| mv_opinion_inst | 0 | 110 | 127 |
| mv_workitem | 1 | 80 | 254 |

### 增强五维坐标（加入分类复杂度）

| 表名 | Z(域) | X(周期) | Y(量级) | **Z'(分类复杂度)** | 核心业务度 |
|------|-------|---------|---------|-------------------|------------|
| mv_workitem | 1 | 80 | 254 | **62.0** | 核心流程表 |
| mv_form_data_inst | 0 | 80 | 254 | **60.0** | 核心实例表 |
| mv_form_file | 0 | 80 | 254 | **50.6** | 重要附件表 |
| mv_opinion_inst | 0 | 110 | 127 | **50.6** | 重要意见表 |
| mv_formset_inst | 0 | 110 | 127 | **47.3** | 配置表 |

### 洞察

```
仅看四维矩阵：
  mv_form_data_inst 和 mv_formset_inst 都是Z=0，难以区分重要性

加入Z'维度后：
  - mv_form_data_inst: Z'=60.0（高复杂度，有3个业务状态分类）→ 核心表
  - mv_formset_inst: Z'=47.3（较低，数据量小，可能是配置）→ 辅助表
  
  - mv_workitem: Z'=62.0（最高，4个分类字段）→ OA系统最核心表
```

## 分类特征洞察

### 1. 核心业务表识别

```python
if complexity_score > 60:
    insight = {
        "type": "core_table_candidate",
        "reason": f"该表有{category_count}个分类字段，{total_categories}个类别，"
                  f"包含复杂的业务状态管理，是主题域的核心表"
    }
```

### 2. 分类结构相似性

```python
# users 和 orders 有相似的分类结构
users:     [role_type, account_status, vip_level]
orders:    [order_status, payment_type, priority_level]

→ 都是核心业务表
→ 都有多维度状态管理
→ 都应该高Z'值
```

### 3. 无分类表识别

```python
if category_count == 0:
    insight = {
        "type": "no_category_table",
        "reason": "该表没有分类字段，可能是："
                  "1) 纯配置表 2) 日志表 3) 关联表 4) 待完善的新表"
    }
```

## 可视化增强

### 颜色编码增强

原始颜色编码：
```
H: 由Z_domain决定 (主题域)
S: 由X决定 (生命周期)
L: 由Y决定 (数据量)
```

增强颜色编码（加入Z'）：
```
H: 由Z_domain决定 (主题域色相)
S: 由X决定 (生命周期饱和度) + Z'微调 (复杂度越高越鲜艳)
L: 由Y决定 (数据量亮度)
边框: 由Z'决定 (核心表加粗边框)
```

### 3D可视化增强

```
原始3D: (X, Y, Z)
增强3D: (X, Y, Z_domain)

增加第四维Z'的展示方式：
- 点的大小: Z'值越大点越大
- 发光效果: Z'>60的表有发光效果
- 标签: 显示Z'值
```

## 实现代码

```python
from hypercube.core.category_analyzer import (
    CategoryAnalyzer,
    EnhancedDomainDiscoverer
)

# 1. 分析分类特征
analyzer = CategoryAnalyzer()
profiles = analyzer.analyze_database(db_stats)

# 2. 获取复杂度评分
for table_name, profile in profiles.items():
    z_category = profile.get_category_complexity_score()
    print(f"{table_name}: Z'={z_category}")

# 3. 整合到五维矩阵
enhanced = EnhancedDomainDiscoverer()
result = enhanced.discover_with_categories(signatures, db_stats)
```

## 业务价值

### 1. 精准识别核心资产

```
传统方式：
  认为所有表同等重要

加入Z'后：
  - Z'>60: 核心业务表，需要重点保护
  - Z'<20: 支撑表，可以简化架构
```

### 2. 优化宽表设计

```
发现：
  users (Z'=58.6) + user_profiles (Z'=26.9)

决策：
  - users是高价值核心表，保持独立
  - user_profiles是低复杂度表，可以合并到users
```

### 3. 数据治理优先级

```
按Z'排序处理：
  1. 先治理Z'>60的表（影响大）
  2. 后处理Z'<30的表（影响小）
```

## 总结

**分类字段是业务语义的载体**。通过分析：
1. 分类字段数量
2. 分类值分布
3. 分类复杂度

我们能够：
- 更精准地识别核心业务表
- 区分核心表和附属表
- 形成更准确的五维矩阵

这不仅改进了主题域发现，更重要的是**理解了业务本质**。
