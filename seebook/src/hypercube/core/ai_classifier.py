"""
AI辅助分类优化模块

利用LLM改进主题域和生命周期的推断准确性
"""

from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
import json
import re


@dataclass
class ClassificationContext:
    """分类上下文"""
    table_name: str
    schema_name: str
    columns: List[Dict[str, Any]]
    existing_tags: List[str]
    sample_data: Optional[List[Dict]] = None
    
    def to_prompt(self) -> str:
        """转换为LLM提示"""
        prompt = f"""分析数据库表的业务分类：

表名: {self.schema_name}.{self.table_name}
列结构:
"""
        for col in self.columns[:10]:  # 限制列数
            prompt += f"  - {col.get('name')}: {col.get('type')}\n"
        
        if self.existing_tags:
            prompt += f"\n现有标签: {', '.join(self.existing_tags)}\n"
        
        return prompt


class AIClassifier:
    """
    AI分类器
    
    使用规则+启发式+可选LLM提升分类准确性
    """
    
    # 业务域定义（含关键词）
    DOMAIN_DEFINITIONS = {
        "user": {
            "keywords": ["user", "account", "profile", "member", "customer", "consumer", "person"],
            "columns": ["user_id", "username", "email", "phone", "password", "nickname", "avatar"],
            "description": "用户信息、账户管理、会员体系",
        },
        "revenue": {
            "keywords": ["order", "payment", "transaction", "bill", "invoice", "revenue", "sale", "price", "amount", "fee", "charge"],
            "columns": ["order_id", "amount", "price", "quantity", "total", "currency", "discount"],
            "description": "订单、支付、交易、收入相关",
        },
        "product": {
            "keywords": ["product", "item", "sku", "goods", "merchandise", "catalog", "category", "inventory", "stock", "commodity"],
            "columns": ["product_id", "sku", "name", "description", "category_id", "brand", "spec"],
            "description": "商品、SKU、库存、类目管理",
        },
        "marketing": {
            "keywords": ["campaign", "ad", "advertisement", "promotion", "coupon", "discount", "channel", "traffic", "conversion", "lead"],
            "columns": ["campaign_id", "source", "medium", "channel", "cost", "impression", "click"],
            "description": "营销活动、广告投放、优惠券、渠道",
        },
        "content": {
            "keywords": ["content", "article", "post", "media", "video", "image", "comment", "review", "feedback"],
            "columns": ["content_id", "title", "body", "author_id", "status", "publish_time"],
            "description": "内容管理、UGC、媒体资源",
        },
        "logistics": {
            "keywords": ["delivery", "shipping", "logistics", "warehouse", "express", "courier", "tracking", "shipment"],
            "columns": ["tracking_no", "carrier", "warehouse_id", "address", "status", "deliver_time"],
            "description": "物流、配送、仓储、快递",
        },
        "tech": {
            "keywords": ["log", "event", "metric", "monitor", "system", "config", "setting", "task", "job", "queue"],
            "columns": ["log_id", "level", "message", "timestamp", "service", "trace_id"],
            "description": "技术日志、监控、系统配置",
        },
    }
    
    # 生命周期阶段定义
    STAGE_DEFINITIONS = {
        "new": {
            "max_age_days": 30,
            "characteristics": ["表结构可能不稳定", "数据量小但增长快"],
        },
        "growth": {
            "max_age_days": 180,
            "characteristics": ["结构趋于稳定", "数据量快速增长"],
        },
        "mature": {
            "max_age_days": 730,
            "characteristics": ["结构稳定", "数据量大", "访问频繁"],
        },
        "legacy": {
            "max_age_days": 1095,  # 3年
            "characteristics": ["很少更新", "数据量大", "可能即将归档"],
        },
    }
    
    def __init__(self, use_llm: bool = False):
        self.use_llm = use_llm
        self.confidence_threshold = 0.6
    
    def classify_table(self, context: ClassificationContext) -> Dict[str, Any]:
        """
        对表进行智能分类
        
        Returns:
            {
                "domain": str,
                "domain_confidence": float,
                "stage": str,
                "stage_confidence": float,
                "reasoning": str,
                "alternative_domains": List[Tuple[str, float]],
            }
        """
        # 1. 基于规则的分类
        rule_result = self._rule_based_classify(context)
        
        # 2. 如果需要且启用LLM，进行LLM分类
        if self.use_llm:
            llm_result = self._llm_classify(context)
            # 融合两种结果
            return self._fuse_results(rule_result, llm_result)
        
        return rule_result
    
    def _rule_based_classify(self, context: ClassificationContext) -> Dict[str, Any]:
        """基于规则的分类"""
        
        # 域分类打分
        domain_scores = {}
        table_name_lower = f"{context.schema_name}_{context.table_name}".lower()
        column_names = [c.get("name", "").lower() for c in context.columns]
        
        for domain, definition in self.DOMAIN_DEFINITIONS.items():
            score = 0.0
            reasons = []
            
            # 表名匹配
            for keyword in definition["keywords"]:
                if keyword in table_name_lower:
                    score += 0.3
                    reasons.append(f"表名包含'{keyword}'")
                    break
            
            # 列名匹配
            matched_columns = set()
            for col_pattern in definition["columns"]:
                for col_name in column_names:
                    if col_pattern in col_name or col_name in col_pattern:
                        matched_columns.add(col_name)
            
            if matched_columns:
                col_score = min(0.5, len(matched_columns) * 0.1)
                score += col_score
                reasons.append(f"列名匹配: {', '.join(list(matched_columns)[:3])}")
            
            domain_scores[domain] = {
                "score": score,
                "reasons": reasons,
            }
        
        # 选择最高分
        sorted_domains = sorted(
            domain_scores.items(),
            key=lambda x: x[1]["score"],
            reverse=True
        )
        
        top_domain = sorted_domains[0]
        
        # 推断生命周期（简化版，实际需要更多信息）
        stage = self._infer_stage(context)
        
        return {
            "domain": top_domain[0],
            "domain_confidence": top_domain[1]["score"],
            "stage": stage,
            "stage_confidence": 0.5,  # 规则推断置信度较低
            "reasoning": f"基于规则: {'; '.join(top_domain[1]['reasons'])}",
            "alternative_domains": [
                (d, s["score"]) for d, s in sorted_domains[1:3]
            ],
        }
    
    def _infer_stage(self, context: ClassificationContext) -> str:
        """推断生命周期阶段"""
        # 这里简化处理，实际应该基于表的创建时间和更新频率
        # 返回一个基于表名特征的启发式判断
        table_name = context.table_name.lower()
        
        if any(x in table_name for x in ["temp", "tmp", "test", "bak"]):
            return "new"  # 临时表视为新建
        elif any(x in table_name for x in ["old", "legacy", "deprecated", "history"]):
            return "legacy"
        elif any(x in table_name for x in ["log", "event", "metric"]):
            return "growth"  # 日志类通常持续增长
        else:
            return "mature"  # 默认成熟
    
    def _llm_classify(self, context: ClassificationContext) -> Dict[str, Any]:
        """
        使用LLM进行分类
        
        注意：这里是一个模拟实现，实际应该调用OpenAI/Claude等API
        """
        # 模拟LLM响应（实际项目中替换为真实API调用）
        prompt = context.to_prompt()
        
        # 模拟推理过程
        # 实际实现：
        # response = openai.ChatCompletion.create(...)
        # result = parse_response(response)
        
        # 这里返回一个与规则结果相近但略有不同的模拟结果
        rule_result = self._rule_based_classify(context)
        
        # 模拟LLM可能改进的地方
        # 例如：user_logs 应该属于 user 域而非 tech 域
        if "log" in context.table_name.lower() and "user" in context.table_name.lower():
            return {
                "domain": "user",
                "domain_confidence": 0.85,
                "stage": rule_result["stage"],
                "stage_confidence": 0.7,
                "reasoning": "LLM分析: 虽然表名包含log，但从列结构看主要是用户行为日志，应归属用户域",
                "alternative_domains": [("tech", 0.6)],
            }
        
        # 默认返回规则结果但提高置信度
        return {
            **rule_result,
            "domain_confidence": min(0.9, rule_result["domain_confidence"] + 0.2),
            "reasoning": f"LLM验证: {rule_result['reasoning']}",
        }
    
    def _fuse_results(self, rule_result: Dict, llm_result: Dict) -> Dict[str, Any]:
        """融合规则和LLM的结果"""
        
        # 如果两者一致，提高置信度
        if rule_result["domain"] == llm_result["domain"]:
            return {
                "domain": rule_result["domain"],
                "domain_confidence": min(0.95, 
                    (rule_result["domain_confidence"] + llm_result["domain_confidence"]) / 2 + 0.1),
                "stage": llm_result.get("stage", rule_result["stage"]),
                "stage_confidence": llm_result.get("stage_confidence", rule_result["stage_confidence"]),
                "reasoning": f"规则与AI一致: {rule_result['reasoning']}",
                "alternative_domains": rule_result.get("alternative_domains", []),
            }
        
        # 如果不一致，选择置信度高的
        if llm_result["domain_confidence"] > rule_result["domain_confidence"] + 0.2:
            return {
                **llm_result,
                "reasoning": f"AI覆盖规则: {llm_result['reasoning']} (规则建议: {rule_result['domain']})",
            }
        else:
            return {
                **rule_result,
                "reasoning": f"规则优先: {rule_result['reasoning']} (AI建议: {llm_result['domain']})",
            }
    
    def batch_classify(self, contexts: List[ClassificationContext]) -> List[Dict[str, Any]]:
        """批量分类"""
        return [self.classify_table(ctx) for ctx in contexts]
    
    def suggest_optimizations(self, 
                              current_classifications: List[Dict],
                              hypercube_summary: Dict) -> List[Dict]:
        """
        基于分类结果提出优化建议
        
        例如：
        - 发现同一域的表分散在不同schema
        - 发现命名不规范的表
        - 建议合并相似表
        """
        suggestions = []
        
        # 1. 检查schema一致性
        domain_schema_map = {}
        for cls in current_classifications:
            domain = cls["domain"]
            # 这里需要更多信息，简化处理
        
        # 2. 检查命名规范
        for cls in current_classifications:
            table_name = cls.get("table_name", "")
            
            # 检查是否使用复数形式
            if table_name.endswith("s") and not table_name.endswith("ss"):
                pass  # 正确
            elif not table_name.endswith("_info") and not table_name.endswith("_detail"):
                suggestions.append({
                    "type": "naming_convention",
                    "table": table_name,
                    "suggestion": f"考虑将表名改为复数形式",
                    "current": table_name,
                    "recommended": f"{table_name}s" if not table_name.endswith("s") else table_name,
                })
        
        return suggestions
    
    def explain_classification(self, context: ClassificationContext) -> str:
        """
        生成可解释的分类说明
        
        用于用户理解为什么这样分类
        """
        result = self.classify_table(context)
        
        explanation = f"""表 {context.schema_name}.{context.table_name} 的分类解释：

【主题域】{result['domain']} (置信度: {result['domain_confidence']:.0%})
{self.DOMAIN_DEFINITIONS.get(result['domain'], {}).get('description', '')}

【推理过程】
{result['reasoning']}

【生命周期】{result['stage']} (置信度: {result['stage_confidence']:.0%})
{self.STAGE_DEFINITIONS.get(result['stage'], {}).get('characteristics', [''])[0]}

【其他可能】
"""
        for alt_domain, score in result.get("alternative_domains", []):
            explanation += f"  - {alt_domain}: {score:.0%} 概率\n"
        
        return explanation
