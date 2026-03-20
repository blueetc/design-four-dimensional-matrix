"""
MySQL 连接器
"""

from typing import List, Dict, Any, Optional

from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import Engine

from hypercube.connectors.base import BaseConnector, TableMetadata


class MySQLConnector(BaseConnector):
    """MySQL数据库连接器"""
    
    def __init__(self, connection_params: Dict[str, Any]):
        super().__init__(connection_params)
        self.engine: Optional[Engine] = None
    
    def connect(self):
        """建立连接"""
        from urllib.parse import quote_plus
        
        host = self.params.get("host", "localhost")
        port = self.params.get("port", 3306)
        user = self.params.get("user", "root")
        password = self.params.get("password", "")
        database = self.params.get("database", "")
        
        # URL编码密码（处理@等特殊字符）
        encoded_password = quote_plus(password)
        
        connection_string = f"mysql+pymysql://{user}:{encoded_password}@{host}:{port}/{database}"
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
        database = schema or self.params.get("database")
        return inspector.get_table_names(schema=database)
    
    def get_table_metadata(self, table_name: str, schema: Optional[str] = None) -> TableMetadata:
        """获取表元数据"""
        if not self.engine:
            self.connect()
        
        database = schema or self.params.get("database")
        
        with self.engine.connect() as conn:
            # 获取列信息
            columns_query = text("""
                SELECT column_name, data_type, is_nullable, column_default, column_comment
                FROM information_schema.columns
                WHERE table_schema = :schema AND table_name = :table
            """)
            columns_result = conn.execute(columns_query, {"schema": database, "table": table_name})
            columns = [
                {
                    "name": row[0],
                    "type": row[1],
                    "nullable": row[2] == "YES",
                    "default": row[3],
                    "comment": row[4],
                }
                for row in columns_result
            ]
            
            # 获取表统计信息
            stats_query = text("""
                SELECT table_rows, data_length + index_length as size_bytes
                FROM information_schema.tables
                WHERE table_schema = :schema AND table_name = :table
            """)
            stats_result = conn.execute(stats_query, {"schema": database, "table": table_name})
            stats_row = stats_result.fetchone()
            
            row_count = stats_row[0] if stats_row else 0
            size_bytes = stats_row[1] if stats_row else 0
            
            # 获取索引信息
            index_query = text("""
                SELECT index_name, column_name, non_unique
                FROM information_schema.statistics
                WHERE table_schema = :schema AND table_name = :table
            """)
            index_result = conn.execute(index_query, {"schema": database, "table": table_name})
            indexes = [
                {"name": row[0], "column": row[1], "unique": row[2] == 0}
                for row in index_result
            ]
            
            # 获取主键
            pk_query = text("""
                SELECT column_name
                FROM information_schema.key_column_usage
                WHERE table_schema = :schema 
                    AND table_name = :table
                    AND constraint_name = 'PRIMARY'
            """)
            pk_result = conn.execute(pk_query, {"schema": database, "table": table_name})
            pk_row = pk_result.fetchone()
            primary_key = pk_row[0] if pk_row else None
            
            return TableMetadata(
                table_name=table_name,
                schema_name=database,
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
                metadata = self.get_table_metadata(table_name, schema)
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
