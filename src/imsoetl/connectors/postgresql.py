"""PostgreSQL connector implementation."""

import asyncio
import asyncpg
from typing import Dict, List, Any, Optional
from datetime import datetime

from .base import BaseConnector, ConnectionConfig, TableMetadata, ConnectionStatus


class PostgreSQLConnector(BaseConnector):
    """PostgreSQL database connector."""
    
    def __init__(self, config: ConnectionConfig):
        super().__init__(config)
        self.pool = None
    
    async def connect(self) -> bool:
        """Establish connection pool to PostgreSQL."""
        try:
            self.status = ConnectionStatus.CONNECTING
            self.logger.info(f"Connecting to PostgreSQL at {self.config.host}:{self.config.port}")
            
            # Create connection pool
            self.pool = await asyncpg.create_pool(
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.username,
                password=self.config.password,
                min_size=1,
                max_size=10,
                **(self.config.additional_params or {})
            )
            
            # Test the connection
            async with self.pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
            
            self.status = ConnectionStatus.CONNECTED
            self.logger.info("Successfully connected to PostgreSQL")
            return True
            
        except Exception as e:
            self.status = ConnectionStatus.ERROR
            self.logger.error(f"Failed to connect to PostgreSQL: {str(e)}")
            return False
    
    async def disconnect(self) -> bool:
        """Close connection pool."""
        try:
            if self.pool:
                await self.pool.close()
                self.pool = None
            self.status = ConnectionStatus.DISCONNECTED
            self.logger.info("Disconnected from PostgreSQL")
            return True
        except Exception as e:
            self.logger.error(f"Error disconnecting from PostgreSQL: {str(e)}")
            return False
    
    async def test_connection(self) -> bool:
        """Test PostgreSQL connection."""
        try:
            conn = await asyncpg.connect(
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.username,
                password=self.config.password,
                **(self.config.additional_params or {})
            )
            await conn.fetchval("SELECT 1")
            await conn.close()
            return True
        except Exception as e:
            self.logger.error(f"Connection test failed: {str(e)}")
            return False
    
    async def get_tables(self, schema: Optional[str] = None) -> List[str]:
        """Get list of tables in PostgreSQL."""
        if not self.pool:
            raise RuntimeError("Not connected to database")
        
        query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_type = 'BASE TABLE'
        """
        
        if schema:
            query += " AND table_schema = $1"
            params = [schema]
        else:
            query += " AND table_schema NOT IN ('information_schema', 'pg_catalog')"
            params = []
        
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(query, *params)
                return [row['table_name'] for row in rows]
        except Exception as e:
            self.logger.error(f"Error getting tables: {str(e)}")
            raise
    
    async def get_table_metadata(self, table_name: str, schema: Optional[str] = None) -> TableMetadata:
        """Get metadata for a PostgreSQL table."""
        if not self.pool:
            raise RuntimeError("Not connected to database")
        
        if not schema:
            schema = 'public'
        
        # Get column information
        column_query = """
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default,
                character_maximum_length,
                numeric_precision,
                numeric_scale
            FROM information_schema.columns
            WHERE table_name = $1 AND table_schema = $2
            ORDER BY ordinal_position
        """
        
        # Get table statistics
        stats_query = """
            SELECT 
                schemaname,
                tablename,
                n_tup_ins as row_count,
                pg_total_relation_size(schemaname||'.'||tablename) as size_bytes
            FROM pg_stat_user_tables
            WHERE tablename = $1 AND schemaname = $2
        """
        
        try:
            async with self.pool.acquire() as conn:
                # Get columns
                column_rows = await conn.fetch(column_query, table_name, schema)
                columns = []
                for row in column_rows:
                    column_info = {
                        'name': row['column_name'],
                        'type': row['data_type'],
                        'nullable': row['is_nullable'] == 'YES',
                        'default': row['column_default'],
                        'max_length': row['character_maximum_length'],
                        'precision': row['numeric_precision'],
                        'scale': row['numeric_scale']
                    }
                    columns.append(column_info)
                
                # Get statistics
                stats_rows = await conn.fetch(stats_query, table_name, schema)
                row_count = None
                size_bytes = None
                if stats_rows:
                    row_count = stats_rows[0]['row_count']
                    size_bytes = stats_rows[0]['size_bytes']
                
                return TableMetadata(
                    name=table_name,
                    schema=schema,
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
                rows = await conn.fetch(query)
                return [dict(row) for row in rows]
        except Exception as e:
            self.logger.error(f"Error executing query: {str(e)}")
            raise
    
    async def get_sample_data(self, table_name: str, limit: int = 100, schema: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get sample data from a PostgreSQL table."""
        if not schema:
            schema = 'public'
        
        query = f'SELECT * FROM "{schema}"."{table_name}" LIMIT {limit}'
        return await self.execute_query(query)
