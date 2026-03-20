"""
PostgreSQL 连接器
"""

from typing import List, Dict, Any, Optional
from datetime import datetime

from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import Engine

from hypercube.connectors.base import BaseConnector, TableMetadata


class PostgresConnector(BaseConnector):
    """PostgreSQL数据库连接器"""
    
    def __init__(self, connection_params: Dict[str, Any]):
        super().__init__(connection_params)
        self.engine: Optional[Engine] = None
    
    def connect(self):
        """建立连接"""
        host = self.params.get("host", "localhost")
        port = self.params.get("port", 5432)
        user = self.params.get("user", "postgres")
        password = self.params.get("password", "")
        database = self.params.get("database", "postgres")
        
        connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        self.engine = create_engine(connection_string)
        return self
    
    def disconnect(self):
        """断开连接"""
        if self.engine:
            self.engine.dispose()
            self.engine = None
    
    def get_tables(self, schema: Optional[str] = None) -> List[str]:
        """获取表列表"""
        if not self.engine:
            self.connect()
        
        inspector = inspect(self.engine)
        schemas = [schema] if schema else inspector.get_schema_names()
        
        tables = []
        for s in schemas:
            if s not in ["information_schema", "pg_catalog", "pg_toast"]:
                tables.extend(inspector.get_table_names(schema=s))
        
        return tables
    
    def get_table_metadata(self, table_name: str, schema: Optional[str] = None) -> TableMetadata:
        """获取表元数据"""
        if not self.engine:
            self.connect()
        
        schema = schema or "public"
        
        with self.engine.connect() as conn:
            # 获取列信息
            columns_query = text("""
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_schema = :schema AND table_name = :table
            """)
            columns_result = conn.execute(columns_query, {"schema": schema, "table": table_name})
            columns = [
                {
                    "name": row[0],
                    "type": row[1],
                    "nullable": row[2] == "YES",
                    "default": row[3],
                }
                for row in columns_result
            ]
            
            # 获取行数和大小
            size_query = text(f"""
                SELECT 
                    reltuples::bigint as row_count,
                    pg_total_relation_size(:qualified_name) as size_bytes
                FROM pg_class
                WHERE oid = :qualified_name::regclass
            """)
            qualified_name = f"{schema}.{table_name}"
            
            try:
                size_result = conn.execute(size_query, {"qualified_name": qualified_name})
                row = size_result.fetchone()
                row_count = row[0] if row else 0
                size_bytes = row[1] if row else 0
            except:
                # 回退：直接COUNT
                count_query = text(f'SELECT COUNT(*) FROM {qualified_name}')
                row_count = conn.execute(count_query).scalar()
                size_bytes = 0
            
            # 获取索引信息
            index_query = text("""
                SELECT indexname, indexdef
                FROM pg_indexes
                WHERE schemaname = :schema AND tablename = :table
            """)
            index_result = conn.execute(index_query, {"schema": schema, "table": table_name})
            indexes = [{"name": row[0], "definition": row[1]} for row in index_result]
            
            # 获取主键
            pk_query = text("""
                SELECT kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                WHERE tc.constraint_type = 'PRIMARY KEY'
                    AND tc.table_schema = :schema
                    AND tc.table_name = :table
            """)
            pk_result = conn.execute(pk_query, {"schema": schema, "table": table_name})
            pk_row = pk_result.fetchone()
            primary_key = pk_row[0] if pk_row else None
            
            return TableMetadata(
                table_name=table_name,
                schema_name=schema,
                column_count=len(columns),
                row_count=row_count,
                size_bytes=size_bytes,
                columns=columns,
                indexes=indexes,
                primary_key=primary_key,
            )
    
    def get_all_tables_metadata(self, schema: Optional[str] = None) -> List[TableMetadata]:
        """获取所有表的元数据"""
        tables = self.get_tables(schema)
        metadata_list = []
        
        for table_name in tables:
            try:
                # 尝试推断schema
                table_schema = schema
                if not table_schema and "." in table_name:
                    parts = table_name.split(".")
                    table_schema = parts[0]
                    table_name = parts[1]
                
                metadata = self.get_table_metadata(table_name, table_schema)
                metadata_list.append(metadata)
            except Exception as e:
                print(f"Error getting metadata for {table_name}: {e}")
        
        return metadata_list
    
    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """执行查询"""
        if not self.engine:
            self.connect()
        
        with self.engine.connect() as conn:
            result = conn.execute(text(query))
            columns = result.keys()
            return [dict(zip(columns, row)) for row in result]
    
    def get_schemas(self) -> List[str]:
        """获取schema列表"""
        if not self.engine:
            self.connect()
        
        inspector = inspect(self.engine)
        return [s for s in inspector.get_schema_names() 
                if s not in ["information_schema", "pg_catalog", "pg_toast"]]
