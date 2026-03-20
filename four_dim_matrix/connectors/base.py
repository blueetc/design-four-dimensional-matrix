"""
数据库连接器基类
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from datetime import datetime


@dataclass
class TableMetadata:
    """表元数据"""
    table_name: str
    schema_name: str
    column_count: int
    row_count: int
    size_bytes: int
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    # 额外信息
    columns: List[Dict[str, Any]] = None
    indexes: List[Dict[str, Any]] = None
    primary_key: Optional[str] = None
    
    def __post_init__(self):
        if self.columns is None:
            self.columns = []
        if self.indexes is None:
            self.indexes = []


class BaseConnector(ABC):
    """数据库连接器基类"""
    
    def __init__(self, connection_params: Dict[str, Any]):
        self.params = connection_params
        self.connection = None
    
    @abstractmethod
    def connect(self):
        """建立连接"""
        pass
    
    @abstractmethod
    def disconnect(self):
        """断开连接"""
        pass
    
    @abstractmethod
    def get_tables(self, schema: Optional[str] = None) -> List[str]:
        """获取表列表"""
        pass
    
    @abstractmethod
    def get_table_metadata(self, table_name: str, schema: Optional[str] = None) -> TableMetadata:
        """获取表元数据"""
        pass
    
    @abstractmethod
    def get_all_tables_metadata(self, schema: Optional[str] = None) -> List[TableMetadata]:
        """获取所有表的元数据"""
        pass
    
    @abstractmethod
    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """执行查询"""
        pass
    
    def infer_business_domain(self, table_name: str, schema_name: str = "") -> str:
        """
        推断业务域
        
        基于表名和schema名进行启发式分类
        """
        name_lower = (schema_name + "_" + table_name).lower()
        
        domain_keywords = {
            "user": ["user", "account", "profile", "member", "customer", "login", "auth"],
            "revenue": ["order", "payment", "transaction", "bill", "invoice", "revenue", "sale", "price", "amount"],
            "product": ["product", "item", "sku", "goods", "catalog", "category", "inventory"],
            "tech": ["log", "event", "metric", "monitor", "system", "config", "setting"],
            "marketing": ["campaign", "ad", "promotion", "coupon", "channel", "traffic"],
            "operations": ["task", "workflow", "job", "schedule", "report", "dashboard"],
        }
        
        for domain, keywords in domain_keywords.items():
            if any(kw in name_lower for kw in keywords):
                return domain
        
        return "other"
    
    def infer_lifecycle_stage(self, table_metadata: TableMetadata) -> str:
        """
        推断生命周期阶段
        
        基于表元数据进行启发式分类
        """
        # 基于表大小和更新时间的启发式规则
        if table_metadata.row_count == 0:
            return "new"
        
        if table_metadata.updated_at and table_metadata.created_at:
            age_days = (table_metadata.updated_at - table_metadata.created_at).days
            
            if age_days < 30:
                return "new"
            elif table_metadata.row_count > 1000000 and age_days > 365:
                return "mature"
            elif age_days > 730:  # 2年未更新
                return "legacy"
        
        # 基于行数判断
        if table_metadata.row_count > 10000000:
            return "mature"
        elif table_metadata.row_count < 1000:
            return "new"
        
        return "growth"
