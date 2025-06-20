"""SQLite connector implementation."""

import asyncio
import aiosqlite
from typing import Dict, List, Any, Optional
from pathlib import Path
import os

from .base import BaseConnector, ConnectionConfig, TableMetadata, ConnectionStatus


class SQLiteConnector(BaseConnector):
    """SQLite database connector."""
    
    def __init__(self, config: ConnectionConfig):
        super().__init__(config)
        # For SQLite, we use the database field as the file path
        self.db_path = config.database
        self.connection = None
    
    async def connect(self) -> bool:
        """Establish connection to SQLite."""
        try:
            self.status = ConnectionStatus.CONNECTING
            self.logger.info(f"Connecting to SQLite at {self.db_path}")
            
            # Check if database file exists (for file-based SQLite)
            if self.db_path != ":memory:" and not Path(self.db_path).exists():
                self.logger.warning(f"SQLite database file does not exist: {self.db_path}")
            
            # Test connection
            async with aiosqlite.connect(self.db_path) as conn:
                await conn.execute("SELECT 1")
            
            self.status = ConnectionStatus.CONNECTED
            self.logger.info("Successfully connected to SQLite")
            return True
            
        except Exception as e:
            self.status = ConnectionStatus.ERROR
            self.logger.error(f"Failed to connect to SQLite: {str(e)}")
            return False
    
    async def disconnect(self) -> bool:
        """Close connection to SQLite."""
        try:
            # SQLite connections are opened per operation, so no persistent connection to close
            self.status = ConnectionStatus.DISCONNECTED
            self.logger.info("Disconnected from SQLite")
            return True
        except Exception as e:
            self.logger.error(f"Error disconnecting from SQLite: {str(e)}")
            return False
    
    async def test_connection(self) -> bool:
        """Test SQLite connection."""
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                await conn.execute("SELECT 1")
            return True
        except Exception as e:
            self.logger.error(f"Connection test failed: {str(e)}")
            return False
    
    async def get_tables(self, schema: Optional[str] = None) -> List[str]:
        """Get list of tables in SQLite."""
        query = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                async with conn.execute(query) as cursor:
                    rows = await cursor.fetchall()
                    return [row[0] for row in rows]
        except Exception as e:
            self.logger.error(f"Error getting tables: {str(e)}")
            raise
    
    async def get_table_metadata(self, table_name: str, schema: Optional[str] = None) -> TableMetadata:
        """Get metadata for a SQLite table."""
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                # Get column information
                async with conn.execute(f"PRAGMA table_info({table_name})") as cursor:
                    column_rows = await cursor.fetchall()
                
                columns = []
                for row in column_rows:
                    column_info = {
                        'name': row[1],  # column name
                        'type': row[2],  # data type
                        'nullable': not bool(row[3]),  # not null flag
                        'default': row[4],  # default value
                        'primary_key': bool(row[5])  # primary key flag
                    }
                    columns.append(column_info)
                
                # Get row count
                async with conn.execute(f"SELECT COUNT(*) FROM {table_name}") as cursor:
                    count_result = await cursor.fetchone()
                    row_count = count_result[0] if count_result else None
                
                return TableMetadata(
                    name=table_name,
                    schema=schema or 'main',
                    columns=columns,
                    row_count=row_count
                )
                
        except Exception as e:
            self.logger.error(f"Error getting table metadata: {str(e)}")
            raise
    
    async def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """Execute a query and return results."""
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                conn.row_factory = aiosqlite.Row
                async with conn.execute(query) as cursor:
                    rows = await cursor.fetchall()
                    return [dict(row) for row in rows]
        except Exception as e:
            self.logger.error(f"Error executing query: {str(e)}")
            raise
    
    async def get_sample_data(self, table_name: str, limit: int = 100, schema: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get sample data from a SQLite table."""
        query = f"SELECT * FROM {table_name} LIMIT {limit}"
        return await self.execute_query(query)
