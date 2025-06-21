"""
DuckDB Execution Engine for IMSOETL

DuckDB is perfect for analytical workloads and provides:
- Fast analytical queries
- SQL compatibility
- No external dependencies
- Columnar storage
- Multi-threaded execution
"""

import asyncio
import logging
import tempfile
import time
from typing import Dict, List, Any, Optional, Union
import pandas as pd

try:
    import duckdb
    from duckdb import DuckDBPyConnection
except ImportError:
    duckdb = None
    DuckDBPyConnection = None

from . import BaseExecutionEngine, ExecutionEngineType, ExecutionResult


class DuckDBExecutionEngine(BaseExecutionEngine):
    """DuckDB-based execution engine for fast analytical processing."""
    
    def __init__(self, engine_type: ExecutionEngineType, config: Dict[str, Any]):
        super().__init__(engine_type, config)
        self.connection = None  # Will be set to DuckDBPyConnection when initialized
        self.database_path = config.get("database_path", ":memory:")
        self.read_only = config.get("read_only", False)
        self.tables: Dict[str, str] = {}  # table_name -> creation_sql
        
    async def initialize(self) -> bool:
        """Initialize DuckDB engine."""
        try:
            if duckdb is None:
                self.logger.error("DuckDB not available - install with: pip install duckdb")
                return False
                
            self.connection = duckdb.connect(
                database=self.database_path,
                read_only=self.read_only
            )
            
            # Set some performance optimizations
            self.connection.execute("SET threads TO 4")
            self.connection.execute("SET memory_limit = '2GB'")
            
            self.is_initialized = True
            self.logger.info("DuckDB execution engine initialized")
            return True
            
        except ImportError:
            self.logger.error("DuckDB not available - install with: pip install duckdb")
            return False
        except Exception as e:
            self.logger.error(f"DuckDB initialization failed: {e}")
            return False
            
    async def execute_sql(self, sql: str, params: Optional[Dict[str, Any]] = None) -> ExecutionResult:
        """Execute SQL query using DuckDB."""
        if not self.is_initialized:
            return ExecutionResult(success=False, error="DuckDB not initialized")
            
        if not self.connection:
            # Try to reinitialize connection
            self.logger.warning("DuckDB connection is None, attempting to reinitialize")
            if not await self.initialize():
                return ExecutionResult(success=False, error="DuckDB connection failed to reinitialize")
        
        try:
            start_time = time.time()
            
            # Handle data parameters by creating temporary tables
            if params:
                for key, value in params.items():
                    if isinstance(value, list):
                        # Convert list of dicts to DataFrame and register as table
                        df = pd.DataFrame(value)
                        self.connection.register(key, df)
                    elif isinstance(value, pd.DataFrame):
                        # Register DataFrame directly
                        self.connection.register(key, value)
                    elif isinstance(value, str) and value.endswith('.csv'):
                        # Load CSV and register
                        df = pd.read_csv(value)
                        self.connection.register(key, df)
            
            # Execute the query (without params since we registered tables)
            if not self.connection:
                return ExecutionResult(success=False, error="DuckDB connection is None")
                
            result = self.connection.execute(sql).fetchdf()
                
            execution_time = time.time() - start_time
            rows_processed = len(result) if isinstance(result, pd.DataFrame) else 0
            
            # Convert DataFrame to list of dicts for consistency
            if isinstance(result, pd.DataFrame):
                data = result.to_dict('records')
            else:
                data = result
            
            return ExecutionResult(
                success=True,
                data=data,
                execution_time=execution_time,
                rows_processed=rows_processed,
                metadata={
                    "engine": "duckdb",
                    "sql": sql,
                    "database_path": self.database_path
                }
            )
            
        except Exception as e:
            self.logger.error(f"DuckDB SQL execution failed: {e}")
            return ExecutionResult(
                success=False,
                error=str(e),
                metadata={"sql": sql}
            )
            
    async def execute_transformation(
        self, 
        source_data: Any, 
        transformation_spec: Dict[str, Any]
    ) -> ExecutionResult:
        """Execute transformations using DuckDB SQL."""
        if not self.is_initialized:
            return ExecutionResult(success=False, error="DuckDB not initialized")
            
        try:
            start_time = time.time()
            
            # Convert source data to DuckDB table
            table_name = "source_table"
            if isinstance(source_data, pd.DataFrame):
                self.connection.register(table_name, source_data)
            elif isinstance(source_data, dict):
                df = pd.DataFrame(source_data)
                self.connection.register(table_name, df)
            else:
                raise ValueError(f"Unsupported source data type: {type(source_data)}")
                
            # Generate SQL from transformation spec
            sql = self._generate_transformation_sql(table_name, transformation_spec)
            
            # Execute the transformation
            result = self.connection.execute(sql).fetchdf()
            
            execution_time = time.time() - start_time
            
            return ExecutionResult(
                success=True,
                data=result,
                execution_time=execution_time,
                rows_processed=len(result),
                metadata={
                    "engine": "duckdb",
                    "transformations": transformation_spec,
                    "generated_sql": sql
                }
            )
            
        except Exception as e:
            self.logger.error(f"DuckDB transformation failed: {e}")
            return ExecutionResult(
                success=False,
                error=str(e)
            )
            
    def _generate_transformation_sql(self, table_name: str, spec: Dict[str, Any]) -> str:
        """Generate SQL from transformation specification."""
        operations = spec.get("operations", [])
        
        # Start with basic SELECT
        select_parts = []
        from_clause = f"FROM {table_name}"
        where_conditions = []
        group_by_columns = []
        order_by_columns = []
        
        for operation in operations:
            op_type = operation.get("type")
            config = operation.get("config", {})
            source_columns = operation.get("source_columns", [])
            
            if op_type == "select":
                if source_columns:
                    select_parts.extend(source_columns)
                    
            elif op_type == "filter":
                condition = config.get("condition")
                if condition:
                    where_conditions.append(condition)
                    
            elif op_type == "aggregate":
                agg_func = config.get("function", "COUNT").upper()
                group_by = config.get("group_by", [])
                
                if source_columns:
                    for col in source_columns:
                        select_parts.append(f"{agg_func}({col}) as {col}_{agg_func.lower()}")
                else:
                    select_parts.append(f"{agg_func}(*) as total_{agg_func.lower()}")
                    
                if group_by:
                    group_by_columns.extend(group_by)
                    select_parts.extend(group_by)
                    
            elif op_type == "sort":
                ascending = config.get("ascending", True)
                direction = "ASC" if ascending else "DESC"
                if source_columns:
                    for col in source_columns:
                        order_by_columns.append(f"{col} {direction}")
                        
        # Build the complete SQL
        if not select_parts:
            select_parts = ["*"]
            
        sql = f"SELECT {', '.join(select_parts)} {from_clause}"
        
        if where_conditions:
            sql += f" WHERE {' AND '.join(where_conditions)}"
            
        if group_by_columns:
            sql += f" GROUP BY {', '.join(group_by_columns)}"
            
        if order_by_columns:
            sql += f" ORDER BY {', '.join(order_by_columns)}"
            
        return sql
        
    async def load_data(
        self, 
        source: str, 
        format_type: str = "auto",
        options: Optional[Dict[str, Any]] = None
    ) -> ExecutionResult:
        """Load data into DuckDB."""
        if not self.is_initialized:
            return ExecutionResult(success=False, error="DuckDB not initialized")
            
        try:
            start_time = time.time()
            options = options or {}
            
            if format_type == "auto":
                format_type = self._detect_format(source)
                
            table_name = options.get("table_name", f"table_{len(self.tables)}")
            
            # DuckDB can read various formats directly
            if format_type == "csv":
                sql = f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{source}')"
            elif format_type == "parquet":
                sql = f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{source}')"
            elif format_type == "json":
                sql = f"CREATE TABLE {table_name} AS SELECT * FROM read_json_auto('{source}')"
            else:
                # Fallback to pandas loading
                if format_type == "excel":
                    df = pd.read_excel(source, **options)
                else:
                    df = pd.read_csv(source, **options)
                    
                self.connection.register(table_name, df)
                sql = f"-- Loaded via pandas: {table_name}"
                
            if sql.startswith("CREATE"):
                self.connection.execute(sql)
                
            # Get the loaded data for return
            result = self.connection.execute(f"SELECT * FROM {table_name}").fetchdf()
            self.tables[table_name] = sql
            
            execution_time = time.time() - start_time
            
            return ExecutionResult(
                success=True,
                data=result,
                execution_time=execution_time,
                rows_processed=len(result),
                metadata={
                    "engine": "duckdb",
                    "format": format_type,
                    "source": source,
                    "table_name": table_name,
                    "sql": sql
                }
            )
            
        except Exception as e:
            self.logger.error(f"DuckDB data loading failed: {e}")
            return ExecutionResult(
                success=False,
                error=str(e)
            )
            
    async def save_data(
        self, 
        data: Any, 
        destination: str, 
        format_type: str = "auto",
        options: Optional[Dict[str, Any]] = None
    ) -> ExecutionResult:
        """Save data from DuckDB."""
        if not self.is_initialized:
            return ExecutionResult(success=False, error="DuckDB not initialized")
            
        try:
            start_time = time.time()
            options = options or {}
            
            if format_type == "auto":
                format_type = self._detect_format(destination)
                
            # Register data if it's not already a table
            if isinstance(data, pd.DataFrame):
                temp_table = "temp_export_table"
                self.connection.register(temp_table, data)
                source_table = temp_table
                rows_processed = len(data)
            elif isinstance(data, str):
                # Assume it's a table name
                source_table = data
                result = self.connection.execute(f"SELECT COUNT(*) FROM {source_table}").fetchone()
                rows_processed = result[0] if result else 0
            else:
                raise ValueError(f"Unsupported data type for export: {type(data)}")
                
            # Use DuckDB's native export capabilities
            if format_type == "csv":
                sql = f"COPY {source_table} TO '{destination}' (FORMAT CSV, HEADER)"
            elif format_type == "parquet":
                sql = f"COPY {source_table} TO '{destination}' (FORMAT PARQUET)"
            elif format_type == "json":
                sql = f"COPY {source_table} TO '{destination}' (FORMAT JSON)"
            else:
                # Fallback to pandas export
                export_df = self.connection.execute(f"SELECT * FROM {source_table}").fetchdf()
                if format_type == "excel":
                    export_df.to_excel(destination, index=False, **options)
                else:
                    export_df.to_csv(destination, index=False, **options)
                sql = f"-- Exported via pandas to {destination}"
                
            if sql.startswith("COPY"):
                self.connection.execute(sql)
                
            execution_time = time.time() - start_time
            
            return ExecutionResult(
                success=True,
                execution_time=execution_time,
                rows_processed=rows_processed,
                metadata={
                    "engine": "duckdb",
                    "format": format_type,
                    "destination": destination,
                    "sql": sql
                }
            )
            
        except Exception as e:
            self.logger.error(f"DuckDB data saving failed: {e}")
            return ExecutionResult(
                success=False,
                error=str(e)
            )
            
    async def create_index(self, table_name: str, columns: List[str], index_name: Optional[str] = None) -> ExecutionResult:
        """Create index on table."""
        if not self.is_initialized:
            return ExecutionResult(success=False, error="DuckDB not initialized")
            
        try:
            if not index_name:
                index_name = f"idx_{table_name}_{'_'.join(columns)}"
                
            sql = f"CREATE INDEX {index_name} ON {table_name} ({', '.join(columns)})"
            self.connection.execute(sql)
            
            return ExecutionResult(
                success=True,
                metadata={
                    "engine": "duckdb",
                    "operation": "create_index",
                    "table": table_name,
                    "columns": columns,
                    "index_name": index_name
                }
            )
            
        except Exception as e:
            self.logger.error(f"DuckDB index creation failed: {e}")
            return ExecutionResult(
                success=False,
                error=str(e)
            )
            
    async def get_table_info(self, table_name: str) -> ExecutionResult:
        """Get table schema information."""
        if not self.is_initialized:
            return ExecutionResult(success=False, error="DuckDB not initialized")
            
        try:
            # Get table schema
            schema_sql = f"DESCRIBE {table_name}"
            schema_df = self.connection.execute(schema_sql).fetchdf()
            
            # Get row count
            count_sql = f"SELECT COUNT(*) as row_count FROM {table_name}"
            count_result = self.connection.execute(count_sql).fetchone()
            row_count = count_result[0] if count_result else 0
            
            return ExecutionResult(
                success=True,
                data={
                    "schema": schema_df,
                    "row_count": row_count,
                    "table_name": table_name
                },
                metadata={
                    "engine": "duckdb",
                    "operation": "table_info"
                }
            )
            
        except Exception as e:
            self.logger.error(f"DuckDB table info failed: {e}")
            return ExecutionResult(
                success=False,
                error=str(e)
            )
            
    async def cleanup(self) -> None:
        """Cleanup DuckDB resources."""
        if self.connection:
            self.connection.close()
            self.connection = None
            self.is_initialized = False
            self.logger.info("DuckDB connection closed")
            
    def _detect_format(self, path: str) -> str:
        """Detect file format from extension."""
        import os
        ext = os.path.splitext(path)[1].lower()
        format_map = {
            ".csv": "csv",
            ".json": "json",
            ".parquet": "parquet",
            ".xlsx": "excel",
            ".xls": "excel"
        }
        return format_map.get(ext, "csv")
        
    def get_capabilities(self) -> Dict[str, Any]:
        """Get DuckDB engine capabilities."""
        return {
            "engine_type": "duckdb",
            "type": "Analytical Database",
            "available": self.is_initialized,
            "capabilities": ["SQL", "Analytics", "Columnar", "CSV", "Parquet", "JSON"],
            "supports_sql": True,
            "supports_streaming": False,
            "supports_distributed": False,
            "max_memory_usage": "2GB default",
            "best_for": "Medium to large datasets (10K - 10M rows)"
        }
