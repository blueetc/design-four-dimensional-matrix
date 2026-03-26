"""
LLM分类器

集成OpenAI/Claude等大模型，利用AI理解业务语义
"""

import os
import json
import re
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from enum import Enum


class LLMProvider(Enum):
    OPENAI = "openai"
    ANTHROPIC = "anthropic"
    AZURE = "azure"
    LOCAL = "local"


@dataclass
class LLMClassificationResult:
    """LLM分类结果"""
    table_name: str
    business_domain: str
    confidence: float
    reasoning: str
    suggested_tags: List[str]
    lifecycle_stage: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "table_name": self.table_name,
            "business_domain": self.business_domain,
            "confidence": self.confidence,
            "reasoning": self.reasoning,
            "suggested_tags": self.suggested_tags,
            "lifecycle_stage": self.lifecycle_stage,
        }


class LLMClassifier:
    """
    LLM分类器
    
    使用大模型理解表的业务含义
    """
    
    def __init__(self, 
                 provider: LLMProvider = None,
                 api_key: str = None,
                 model: str = None,
                 use_cache: bool = True):
        """
        初始化LLM分类器
        
        Args:
            provider: LLM提供商
            api_key: API密钥（默认从环境变量读取）
            model: 模型名称
            use_cache: 是否缓存结果（避免重复调用）
        """
        self.provider = provider or self._detect_provider()
        self.api_key = api_key or self._get_api_key()
        self.model = model or self._get_default_model()
        self.use_cache = use_cache
        self.cache = {}
        
        # 初始化客户端
        self.client = None
        if self.api_key:
            self._init_client()
        else:
            print("  [LLM] 未配置API密钥，使用模拟模式")
    
    def _detect_provider(self) -> LLMProvider:
        """自动检测提供商"""
        if os.getenv("OPENAI_API_KEY"):
            return LLMProvider.OPENAI
        elif os.getenv("ANTHROPIC_API_KEY"):
            return LLMProvider.ANTHROPIC
        elif os.getenv("AZURE_OPENAI_KEY"):
            return LLMProvider.AZURE
        else:
            return LLMProvider.LOCAL
    
    def _get_api_key(self) -> Optional[str]:
        """获取API密钥"""
        env_vars = {
            LLMProvider.OPENAI: "OPENAI_API_KEY",
            LLMProvider.ANTHROPIC: "ANTHROPIC_API_KEY",
            LLMProvider.AZURE: "AZURE_OPENAI_KEY",
        }
        
        if self.provider in env_vars:
            key = os.getenv(env_vars[self.provider])
            if key:
                return key
        
        # 如果没有密钥，返回None（将进入模拟模式）
        return None
    
    def _get_default_model(self) -> str:
        """获取默认模型"""
        defaults = {
            LLMProvider.OPENAI: "gpt-3.5-turbo",
            LLMProvider.ANTHROPIC: "claude-3-sonnet-20240229",
            LLMProvider.AZURE: "gpt-35-turbo",
            LLMProvider.LOCAL: "llama2",
        }
        return defaults.get(self.provider, "gpt-3.5-turbo")
    
    def _init_client(self):
        """初始化LLM客户端"""
        try:
            if self.provider == LLMProvider.OPENAI:
                import openai
                openai.api_key = self.api_key
                self.client = openai
                
            elif self.provider == LLMProvider.ANTHROPIC:
                try:
                    from anthropic import Anthropic
                    self.client = Anthropic(api_key=self.api_key)
                except ImportError:
                    print("警告: 未安装anthropic包，使用模拟模式")
                    self.client = None
                    
            elif self.provider == LLMProvider.AZURE:
                import openai
                openai.api_type = "azure"
                openai.api_key = self.api_key
                openai.api_base = os.getenv("AZURE_OPENAI_ENDPOINT", "")
                openai.api_version = "2023-05-15"
                self.client = openai
                
            elif self.provider == LLMProvider.LOCAL:
                # 本地模型，如Ollama
                self.client = None
                
        except ImportError as e:
            print(f"警告: 导入LLM库失败: {e}")
            print("将使用模拟模式（基于规则）")
            self.client = None
    
    def _get_cache_key(self, table_info: Dict) -> str:
        """生成缓存键"""
        key_data = {
            "name": table_info.get("table_name"),
            "columns": sorted([c.get("name", "") for c in table_info.get("columns", [])]),
        }
        return json.dumps(key_data, sort_keys=True)
    
    def classify_table(self, table_info: Dict) -> LLMClassificationResult:
        """
        使用LLM分类表
        
        Args:
            table_info: 表信息 {
                "table_name": "users",
                "columns": [{"name": "...", "type": "..."}],
                "row_count": 1000,
                "comment": "用户表"
            }
        
        Returns:
            LLMClassificationResult
        """
        # 检查缓存
        if self.use_cache:
            cache_key = self._get_cache_key(table_info)
            if cache_key in self.cache:
                return self.cache[cache_key]
        
        # 如果没有LLM客户端，使用模拟模式
        if self.client is None:
            result = self._simulate_llm_classification(table_info)
        else:
            # 调用真实LLM
            try:
                if self.provider == LLMProvider.OPENAI:
                    result = self._call_openai(table_info)
                elif self.provider == LLMProvider.ANTHROPIC:
                    result = self._call_anthropic(table_info)
                else:
                    result = self._simulate_llm_classification(table_info)
            except Exception as e:
                print(f"LLM调用失败: {e}，使用模拟模式")
                result = self._simulate_llm_classification(table_info)
        
        # 存入缓存
        if self.use_cache:
            self.cache[cache_key] = result
        
        return result
    
    def _build_prompt(self, table_info: Dict) -> str:
        """构建提示词"""
        table_name = table_info.get("table_name", "")
        columns = table_info.get("columns", [])
        row_count = table_info.get("row_count", 0)
        comment = table_info.get("comment", "")
        
        column_desc = "\n".join([
            f"  - {col.get('name', '')}: {col.get('type', 'unknown')}"
            for col in columns[:20]  # 限制列数
        ])
        
        prompt = f"""作为数据库架构专家，请分析以下表的业务分类。

表名: {table_name}
表注释: {comment}
数据量: {row_count} 行

列结构:
{column_desc}

请分析并返回JSON格式:
{{
    "business_domain": "业务域名称（如：用户管理、订单交易、商品管理、日志监控等）",
    "confidence": 0.95,
    "reasoning": "详细的分析推理过程",
    "suggested_tags": ["标签1", "标签2"],
    "lifecycle_stage": "new/growth/mature/legacy（基于数据量和结构判断）"
}}

注意:
1. business_domain应该是通用的业务领域，不要包含表名本身
2. confidence表示置信度，0-1之间
3. 如果有comment字段，请重点参考
4. 根据列名推断业务含义，如user_id/phone/email通常是用户域
"""
        return prompt
    
    def _call_openai(self, table_info: Dict) -> LLMClassificationResult:
        """调用OpenAI API"""
        prompt = self._build_prompt(table_info)
        
        response = self.client.ChatCompletion.create(
            model=self.model,
            messages=[
                {"role": "system", "content": "你是一个数据库架构专家，擅长分析表的业务含义。"},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            max_tokens=500
        )
        
        content = response.choices[0].message.content
        return self._parse_response(table_info["table_name"], content)
    
    def _call_anthropic(self, table_info: Dict) -> LLMClassificationResult:
        """调用Anthropic Claude API"""
        prompt = self._build_prompt(table_info)
        
        response = self.client.messages.create(
            model=self.model,
            max_tokens=500,
            temperature=0.3,
            system="你是一个数据库架构专家，擅长分析表的业务含义。",
            messages=[{"role": "user", "content": prompt}]
        )
        
        content = response.content[0].text
        return self._parse_response(table_info["table_name"], content)
    
    def _parse_response(self, table_name: str, content: str) -> LLMClassificationResult:
        """解析LLM响应"""
        try:
            # 提取JSON
            json_match = re.search(r'\{[^}]+\}', content, re.DOTALL)
            if json_match:
                json_str = json_match.group()
                data = json.loads(json_str)
                
                return LLMClassificationResult(
                    table_name=table_name,
                    business_domain=data.get("business_domain", "其他"),
                    confidence=data.get("confidence", 0.5),
                    reasoning=data.get("reasoning", ""),
                    suggested_tags=data.get("suggested_tags", []),
                    lifecycle_stage=data.get("lifecycle_stage", "mature"),
                )
        except Exception as e:
            print(f"解析LLM响应失败: {e}")
        
        # 返回默认值
        return self._simulate_llm_classification({"table_name": table_name})
    
    def _simulate_llm_classification(self, table_info: Dict) -> LLMClassificationResult:
        """
        模拟LLM分类（基于增强规则）
        
        当没有LLM API时使用，比原始规则更智能
        """
        table_name = table_info.get("table_name", "").lower()
        columns = [c.get("name", "").lower() for c in table_info.get("columns", [])]
        
        # 业务域推断
        domain_scores = {}
        
        # 用户域特征
        user_keywords = ["user", "account", "member", "customer", "profile", "auth", "login"]
        user_score = sum(2 for kw in user_keywords if kw in table_name)
        user_score += sum(1 for col in columns for kw in user_keywords if kw in col)
        if user_score > 0:
            domain_scores["用户管理"] = user_score
        
        # 订单域特征
        order_keywords = ["order", "payment", "transaction", "trade", "pay"]
        order_score = sum(2 for kw in order_keywords if kw in table_name)
        order_score += sum(1 for col in columns for kw in order_keywords if kw in col)
        if order_score > 0:
            domain_scores["订单交易"] = order_score
        
        # 商品域特征
        product_keywords = ["product", "goods", "item", "sku", "category", "brand", "inventory"]
        product_score = sum(2 for kw in product_keywords if kw in table_name)
        product_score += sum(1 for col in columns for kw in product_keywords if kw in col)
        if product_score > 0:
            domain_scores["商品管理"] = product_score
        
        # 日志/技术域
        log_keywords = ["log", "event", "metric", "config", "system"]
        log_score = sum(2 for kw in log_keywords if kw in table_name)
        if log_score > 0:
            domain_scores["系统日志"] = log_score
        
        # 选择最高分
        if domain_scores:
            best_domain = max(domain_scores.items(), key=lambda x: x[1])
            business_domain = best_domain[0]
            confidence = min(0.95, 0.5 + best_domain[1] * 0.1)
        else:
            business_domain = "其他"
            confidence = 0.3
        
        # 生成推理
        reasoning = f"基于表名'{table_name}'和列名分析，"
        if "user" in table_name or "account" in table_name:
            reasoning += "包含用户信息相关字段，判定为用户管理域。"
        elif "order" in table_name or "pay" in table_name:
            reasoning += "包含订单和支付相关字段，判定为订单交易域。"
        elif "product" in table_name or "goods" in table_name:
            reasoning += "包含商品信息相关字段，判定为商品管理域。"
        else:
            reasoning += "未识别到明确的业务特征，暂归为其他域。"
        
        # 生命周期推断
        row_count = table_info.get("row_count", 0)
        if row_count < 100:
            lifecycle = "new"
        elif row_count > 1000000:
            lifecycle = "mature"
        else:
            lifecycle = "growth"
        
        # 标签
        tags = [business_domain]
        if "status" in columns or "state" in columns:
            tags.append("状态管理")
        if "created_at" in columns:
            tags.append("时序数据")
        
        return LLMClassificationResult(
            table_name=table_info.get("table_name", ""),
            business_domain=business_domain,
            confidence=confidence,
            reasoning=reasoning,
            suggested_tags=tags,
            lifecycle_stage=lifecycle,
        )
    
    def batch_classify(self, 
                       tables_info: List[Dict],
                       progress_callback=None) -> List[LLMClassificationResult]:
        """
        批量分类表
        
        Args:
            tables_info: 表信息列表
            progress_callback: 进度回调函数 (current, total)
        
        Returns:
            LLMClassificationResult列表
        """
        results = []
        total = len(tables_info)
        
        for i, table_info in enumerate(tables_info):
            result = self.classify_table(table_info)
            results.append(result)
            
            if progress_callback:
                progress_callback(i + 1, total)
            else:
                print(f"  进度: {i+1}/{total} - {table_info.get('table_name')}")
        
        return results


# 便捷函数
def create_llm_classifier() -> LLMClassifier:
    """
    创建LLM分类器（自动检测配置）
    """
    return LLMClassifier()


def classify_with_llm(table_info: Dict, api_key: str = None) -> Dict[str, Any]:
    """
    便捷函数：使用LLM分类单表
    
    Args:
        table_info: 表信息
        api_key: API密钥（可选，默认从环境变量读取）
    
    Returns:
        分类结果字典
    """
    classifier = LLMClassifier(api_key=api_key)
    result = classifier.classify_table(table_info)
    return result.to_dict()
