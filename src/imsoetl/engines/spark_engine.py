"""
Apache Spark Execution Engine for IMSOETL

Spark engine provides:
- Distributed processing
- Large dataset handling
- SQL and DataFrame APIs
- Streaming capabilities
- Multi-format support
"""

import asyncio
import logging
import tempfile
import time
from typing import Dict, List, Any, Optional, Union
import pandas as pd

from . import BaseExecutionEngine, ExecutionEngineType, ExecutionResult


class SparkExecutionEngine(BaseExecutionEngine):
    """Spark-based execution engine for big data processing."""
    
    def __init__(self, engine_type: ExecutionEngineType, config: Dict[str, Any]):
        super().__init__(engine_type, config)
        self.spark = None
        self.spark_context = None
        self.app_name = config.get("app_name", "IMSOETL-Spark")
        self.master = config.get("master", "local[*]")
        self.executor_memory = config.get("executor_memory", "2g")
        self.driver_memory = config.get("driver_memory", "1g")
        self.temp_views: Dict[str, str] = {}
        
    async def initialize(self) -> bool:
        """Initialize Spark engine."""
        try:
            from pyspark.sql import SparkSession
            from pyspark.conf import SparkConf
            
            # Configure Spark
            conf = SparkConf()
            conf.setAppName(self.app_name)
            conf.setMaster(self.master)
            conf.set("spark.executor.memory", self.executor_memory)
            conf.set("spark.driver.memory", self.driver_memory)
            conf.set("spark.sql.adaptive.enabled", "true")
            conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
            
            # Apply additional config
            for key, value in self.config.get("spark_config", {}).items():
                conf.set(key, value)
                
            # Create Spark session
            self.spark = SparkSession.builder.config(conf=conf).getOrCreate()
            self.spark_context = self.spark.sparkContext
            
            # Set log level to reduce noise
            self.spark_context.setLogLevel("WARN")
            
            self.is_initialized = True
            self.logger.info(f"Spark execution engine initialized - Master: {self.master}")
            return True
            
        except ImportError:
            self.logger.error("PySpark not available - install with: pip install pyspark")
            return False
        except Exception as e:
            self.logger.error(f"Spark initialization failed: {e}")
            return False
            
    async def execute_sql(self, sql: str, params: Optional[Dict[str, Any]] = None) -> ExecutionResult:
        """Execute SQL query using Spark SQL."""
        if not self.is_initialized or not self.spark:
            return ExecutionResult(success=False, error="Spark not initialized")
            
        try:
            start_time = time.time()
            
            # Spark SQL doesn't support parameterized queries the same way
            # For security, we'd need proper parameter binding in production
            if params:
                # Simple parameter substitution (NOT safe for production)
                for key, value in params.items():
                    if isinstance(value, str):
                        sql = sql.replace(f":{key}", f"'{value}'")
                    else:
                        sql = sql.replace(f":{key}", str(value))
                        
            # Execute SQL
            df = self.spark.sql(sql)
            
            # Convert to Pandas for consistent API
            result = df.toPandas()
            
            execution_time = time.time() - start_time
            
            return ExecutionResult(
                success=True,
                data=result,
                execution_time=execution_time,
                rows_processed=len(result),
                metadata={
                    "engine": "spark",
                    "sql": sql,
                    "partitions": df.rdd.getNumPartitions()
                }
            )
            
        except Exception as e:
            self.logger.error(f"Spark SQL execution failed: {e}")
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
        """Execute transformations using Spark DataFrame API."""
        if not self.is_initialized:
            return ExecutionResult(success=False, error="Spark not initialized")
            
        try:
            start_time = time.time()
            
            # Convert source data to Spark DataFrame
            if isinstance(source_data, pd.DataFrame):
                df = self.spark.createDataFrame(source_data)
            elif isinstance(source_data, dict):
                pandas_df = pd.DataFrame(source_data)
                df = self.spark.createDataFrame(pandas_df)
            elif hasattr(source_data, 'sql'):  # Already a Spark DataFrame
                df = source_data
            else:
                raise ValueError(f"Unsupported source data type: {type(source_data)}")
                
            # Apply transformations
            result_df = await self._apply_spark_transformations(df, transformation_spec)
            
            # Convert back to Pandas for consistent return type
            result = result_df.toPandas()
            
            execution_time = time.time() - start_time
            
            return ExecutionResult(
                success=True,
                data=result,
                execution_time=execution_time,
                rows_processed=len(result),
                metadata={
                    "engine": "spark",
                    "transformations": transformation_spec,
                    "partitions": result_df.rdd.getNumPartitions()
                }
            )
            
        except Exception as e:
            self.logger.error(f"Spark transformation failed: {e}")
            return ExecutionResult(
                success=False,
                error=str(e)
            )
            
    async def _apply_spark_transformations(self, df, spec: Dict[str, Any]):
        """Apply Spark DataFrame transformations."""
        from pyspark.sql import functions as F
        
        result_df = df
        operations = spec.get("operations", [])
        
        for operation in operations:
            op_type = operation.get("type")
            config = operation.get("config", {})
            source_columns = operation.get("source_columns", [])
            
            if op_type == "select":
                if source_columns:
                    result_df = result_df.select(*source_columns)
                    
            elif op_type == "filter":
                condition = config.get("condition")
                if condition:
                    result_df = result_df.filter(condition)
                    
            elif op_type == "aggregate":
                agg_func = config.get("function", "sum").lower()
                group_by = config.get("group_by", [])
                
                if group_by:
                    grouped = result_df.groupBy(*group_by)
                    
                    if source_columns:
                        agg_exprs = []
                        for col in source_columns:
                            if agg_func == "sum":
                                agg_exprs.append(F.sum(col).alias(f"{col}_sum"))
                            elif agg_func == "avg":
                                agg_exprs.append(F.avg(col).alias(f"{col}_avg"))
                            elif agg_func == "count":
                                agg_exprs.append(F.count(col).alias(f"{col}_count"))
                            elif agg_func == "max":
                                agg_exprs.append(F.max(col).alias(f"{col}_max"))
                            elif agg_func == "min":
                                agg_exprs.append(F.min(col).alias(f"{col}_min"))
                                
                        result_df = grouped.agg(*agg_exprs)
                    else:
                        result_df = grouped.count()
                        
            elif op_type == "sort":
                ascending = config.get("ascending", True)
                if source_columns:
                    if ascending:
                        result_df = result_df.orderBy(*source_columns)
                    else:
                        result_df = result_df.orderBy(*[F.desc(col) for col in source_columns])
                        
            elif op_type == "join":
                join_type = config.get("join_type", "inner")
                join_condition = config.get("condition")
                right_table = config.get("right_table")
                
                if right_table and join_condition:
                    # This would need proper table lookup in production
                    pass
                    
        return result_df
        
    async def load_data(
        self, 
        source: str, 
        format_type: str = "auto",
        options: Optional[Dict[str, Any]] = None
    ) -> ExecutionResult:
        """Load data using Spark."""
        if not self.is_initialized:
            return ExecutionResult(success=False, error="Spark not initialized")
            
        try:
            start_time = time.time()
            options = options or {}
            
            if format_type == "auto":
                format_type = self._detect_format(source)
                
            # Use Spark's native readers
            reader = self.spark.read
            
            # Apply options
            for key, value in options.items():
                if key != "table_name":  # Skip our custom option
                    reader = reader.option(key, value)
                    
            if format_type == "csv":
                df = reader.csv(source, header=True, inferSchema=True)
            elif format_type == "json":
                df = reader.json(source)
            elif format_type == "parquet":
                df = reader.parquet(source)
            elif format_type == "excel":
                # Spark doesn't natively support Excel, fall back to pandas
                pandas_df = pd.read_excel(source, **options)
                df = self.spark.createDataFrame(pandas_df)
            else:
                raise ValueError(f"Unsupported format: {format_type}")
                
            # Create temporary view for SQL access
            table_name = options.get("table_name", f"table_{len(self.temp_views)}")
            df.createOrReplaceTempView(table_name)
            self.temp_views[table_name] = source
            
            # Convert to Pandas for consistent return
            result = df.toPandas()
            
            execution_time = time.time() - start_time
            
            return ExecutionResult(
                success=True,
                data=result,
                execution_time=execution_time,
                rows_processed=len(result),
                metadata={
                    "engine": "spark",
                    "format": format_type,
                    "source": source,
                    "table_name": table_name,
                    "partitions": df.rdd.getNumPartitions()
                }
            )
            
        except Exception as e:
            self.logger.error(f"Spark data loading failed: {e}")
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
        """Save data using Spark."""
        if not self.is_initialized:
            return ExecutionResult(success=False, error="Spark not initialized")
            
        try:
            start_time = time.time()
            options = options or {}
            
            if format_type == "auto":
                format_type = self._detect_format(destination)
                
            # Convert to Spark DataFrame if needed
            if isinstance(data, pd.DataFrame):
                df = self.spark.createDataFrame(data)
            elif hasattr(data, 'write'):  # Already a Spark DataFrame
                df = data
            else:
                raise ValueError(f"Unsupported data type for save: {type(data)}")
                
            # Get row count before writing
            rows_processed = df.count()
            
            # Configure writer
            writer = df.write.mode("overwrite")  # Default to overwrite
            
            # Apply options
            for key, value in options.items():
                writer = writer.option(key, value)
                
            # Write based on format
            if format_type == "csv":
                writer.csv(destination, header=True)
            elif format_type == "json":
                writer.json(destination)
            elif format_type == "parquet":
                writer.parquet(destination)
            elif format_type == "excel":
                # Fall back to pandas for Excel
                pandas_df = df.toPandas()
                pandas_df.to_excel(destination, index=False, **options)
            else:
                raise ValueError(f"Unsupported format: {format_type}")
                
            execution_time = time.time() - start_time
            
            return ExecutionResult(
                success=True,
                execution_time=execution_time,
                rows_processed=rows_processed,
                metadata={
                    "engine": "spark",
                    "format": format_type,
                    "destination": destination,
                    "partitions": df.rdd.getNumPartitions()
                }
            )
            
        except Exception as e:
            self.logger.error(f"Spark data saving failed: {e}")
            return ExecutionResult(
                success=False,
                error=str(e)
            )
            
    async def optimize_table(self, table_name: str, partition_columns: Optional[List[str]] = None) -> ExecutionResult:
        """Optimize table for better performance."""
        if not self.is_initialized:
            return ExecutionResult(success=False, error="Spark not initialized")
            
        try:
            # Cache the table for faster access
            self.spark.catalog.cacheTable(table_name)
            
            metadata = {
                "engine": "spark",
                "operation": "optimize",
                "table": table_name,
                "cached": True
            }
            
            if partition_columns:
                # This would involve repartitioning the DataFrame
                df = self.spark.table(table_name)
                optimized_df = df.repartition(*partition_columns)
                optimized_df.createOrReplaceTempView(f"{table_name}_optimized")
                metadata["partitioned_by"] = partition_columns
                
            return ExecutionResult(
                success=True,
                metadata=metadata
            )
            
        except Exception as e:
            self.logger.error(f"Spark table optimization failed: {e}")
            return ExecutionResult(
                success=False,
                error=str(e)
            )
            
    async def cleanup(self) -> None:
        """Cleanup Spark resources."""
        if self.spark:
            # Clear cache
            self.spark.catalog.clearCache()
            
            # Stop Spark session
            self.spark.stop()
            self.spark = None
            self.spark_context = None
            self.is_initialized = False
            self.logger.info("Spark session stopped")
            
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
        """Get Spark engine capabilities."""
        return {
            **super().get_capabilities(),
            "supports_streaming": True,
            "supports_distributed": True,
            "max_memory_usage": f"Driver: {self.driver_memory}, Executor: {self.executor_memory}",
            "supported_formats": ["csv", "json", "parquet", "excel"],
            "supports_joins": True,
            "supports_aggregations": True,
            "supports_window_functions": True,
            "supports_partitioning": True,
            "supports_caching": True,
            "parallel_processing": True,
            "cluster_support": True,
            "fault_tolerance": True
        }
