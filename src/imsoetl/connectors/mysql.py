"""MySQL connector implementation."""

import asyncio
import aiomysql
from typing import Dict, List, Any, Optional
from datetime import datetime

from .base import BaseConnector, ConnectionConfig, TableMetadata, ConnectionStatus


class MySQLConnector(BaseConnector):
    """MySQL database connector."""
    
    def __init__(self, config: ConnectionConfig):
        super().__init__(config)
        self.pool = None
    
    async def connect(self) -> bool:
        """Establish connection pool to MySQL."""
        try:
            self.status = ConnectionStatus.CONNECTING
            self.logger.info(f"Connecting to MySQL at {self.config.host}:{self.config.port}")
            
            # Create connection pool
            self.pool = await aiomysql.create_pool(
                host=self.config.host,
                port=self.config.port,
                db=self.config.database,
                user=self.config.username,
                password=self.config.password,
                minsize=1,
                maxsize=10,
                autocommit=True,
                **(self.config.additional_params or {})
            )
            
            # Test the connection
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("SELECT 1")
            
            self.status = ConnectionStatus.CONNECTED
            self.logger.info("Successfully connected to MySQL")
            return True
            
        except Exception as e:
            self.status = ConnectionStatus.ERROR
            self.logger.error(f"Failed to connect to MySQL: {str(e)}")
            return False
    
    async def disconnect(self) -> bool:
        """Close connection pool."""
        try:
            if self.pool:
                self.pool.close()
                await self.pool.wait_closed()
                self.pool = None
            self.status = ConnectionStatus.DISCONNECTED
            self.logger.info("Disconnected from MySQL")
            return True
        except Exception as e:
            self.logger.error(f"Error disconnecting from MySQL: {str(e)}")
            return False
    
    async def test_connection(self) -> bool:
        """Test MySQL connection."""
        try:
            conn = await aiomysql.connect(
                host=self.config.host,
                port=self.config.port,
                db=self.config.database,
                user=self.config.username,
                password=self.config.password,
                **(self.config.additional_params or {})
            )
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT 1")
            conn.close()
            return True
        except Exception as e:
            self.logger.error(f"Connection test failed: {str(e)}")
            return False
    
    async def get_tables(self, schema: Optional[str] = None) -> List[str]:
        """Get list of tables in MySQL."""
        if not self.pool:
            raise RuntimeError("Not connected to database")
        
        database = schema or self.config.database
        query = "SHOW TABLES FROM `%s`" % database
        
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(query)
                    rows = await cursor.fetchall()
                    return [row[0] for row in rows]
        except Exception as e:
            self.logger.error(f"Error getting tables: {str(e)}")
            raise
    
    async def get_table_metadata(self, table_name: str, schema: Optional[str] = None) -> TableMetadata:
        """Get metadata for a MySQL table."""
        if not self.pool:
            raise RuntimeError("Not connected to database")
        
        database = schema or self.config.database
        
        # Get column information
        column_query = """
            SELECT 
                COLUMN_NAME,
                DATA_TYPE,
                IS_NULLABLE,
                COLUMN_DEFAULT,
                CHARACTER_MAXIMUM_LENGTH,
                NUMERIC_PRECISION,
                NUMERIC_SCALE,
                COLUMN_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION
        """
        
        # Get table statistics
        stats_query = """
            SELECT 
                TABLE_ROWS,
                DATA_LENGTH + INDEX_LENGTH as size_bytes
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        """
        
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    # Get columns
                    await cursor.execute(column_query, (database, table_name))
                    column_rows = await cursor.fetchall()
                    
                    columns = []
                    for row in column_rows:
                        column_info = {
                            'name': row['COLUMN_NAME'],
                            'type': row['DATA_TYPE'],
                            'column_type': row['COLUMN_TYPE'],
                            'nullable': row['IS_NULLABLE'] == 'YES',
                            'default': row['COLUMN_DEFAULT'],
                            'max_length': row['CHARACTER_MAXIMUM_LENGTH'],
                            'precision': row['NUMERIC_PRECISION'],
                            'scale': row['NUMERIC_SCALE']
                        }
                        columns.append(column_info)
                    
                    # Get statistics
                    await cursor.execute(stats_query, (database, table_name))
                    stats_row = await cursor.fetchone()
                    
                    row_count = None
                    size_bytes = None
                    if stats_row:
                        row_count = stats_row['TABLE_ROWS']
                        size_bytes = stats_row['size_bytes']
                    
                    return TableMetadata(
                        name=table_name,
                        schema=database,
                        columns=columns,
                        row_count=row_count,
                        size_bytes=size_bytes
                    )
                    
        except Exception as e:
            self.logger.error(f"Error getting table metadata: {str(e)}")
            raise
    
    async def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """Execute a query and return results."""
        if not self.pool:
            raise RuntimeError("Not connected to database")
        
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    await cursor.execute(query)
                    rows = await cursor.fetchall()
                    return rows
        except Exception as e:
            self.logger.error(f"Error executing query: {str(e)}")
            raise
    
    async def get_sample_data(self, table_name: str, limit: int = 100, schema: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get sample data from a MySQL table."""
        database = schema or self.config.database
        query = f"SELECT * FROM `{database}`.`{table_name}` LIMIT {limit}"
        return await self.execute_query(query)
