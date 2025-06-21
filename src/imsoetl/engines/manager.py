"""
Execution Engine Manager for IMSOETL

This manager handles multiple execution engines and provides:
- Engine selection based on data size and complexity
- Automatic fallback between engines
- Performance optimization
- Resource management
"""

import asyncio
import logging
from typing import Dict, List, Any, Optional, Union, Type
from datetime import datetime
from enum import Enum

from . import BaseExecutionEngine, ExecutionEngineType, ExecutionResult, PandasExecutionEngine
from .duckdb_engine import DuckDBExecutionEngine
from .spark_engine import SparkExecutionEngine
from ..core.errors import IMSOETLError


class EngineSelectionStrategy(str, Enum):
    """Engine selection strategies."""
    AUTO = "auto"
    PERFORMANCE = "performance"
    MEMORY_EFFICIENT = "memory_efficient"
    DISTRIBUTED = "distributed"
    MANUAL = "manual"


class ExecutionEngineManager:
    """Manages multiple execution engines and provides intelligent selection."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger("ExecutionEngineManager")
        
        # Available engines
        self.engines: Dict[ExecutionEngineType, BaseExecutionEngine] = {}
        self.primary_engine: Optional[BaseExecutionEngine] = None
        self.fallback_engines: List[BaseExecutionEngine] = []
        
        # Selection strategy
        self.selection_strategy = EngineSelectionStrategy(
            config.get("selection_strategy", "auto")
        )
        
        # Performance thresholds
        self.small_data_threshold = config.get("small_data_threshold", 10_000)  # rows
        self.medium_data_threshold = config.get("medium_data_threshold", 1_000_000)  # rows
        self.memory_limit = config.get("memory_limit", "2GB")
        
    async def initialize(self) -> bool:
        """Initialize all configured execution engines."""
        engines_config = self.config.get("engines", {})
        initialized_count = 0
        
        # Initialize Pandas engine (always available)
        if engines_config.get("pandas", {}).get("enabled", True):
            pandas_engine = PandasExecutionEngine(
                ExecutionEngineType.PANDAS, 
                engines_config.get("pandas", {})
            )
            if await pandas_engine.initialize():
                self.engines[ExecutionEngineType.PANDAS] = pandas_engine
                initialized_count += 1
                self.logger.info("Pandas engine initialized")
                
        # Initialize DuckDB engine
        if engines_config.get("duckdb", {}).get("enabled", True):
            duckdb_engine = DuckDBExecutionEngine(
                ExecutionEngineType.DUCKDB,
                engines_config.get("duckdb", {})
            )
            if await duckdb_engine.initialize():
                self.engines[ExecutionEngineType.DUCKDB] = duckdb_engine
                initialized_count += 1
                self.logger.info("DuckDB engine initialized")
                
        # Initialize Spark engine
        if engines_config.get("spark", {}).get("enabled", False):
            spark_engine = SparkExecutionEngine(
                ExecutionEngineType.SPARK,
                engines_config.get("spark", {})
            )
            if await spark_engine.initialize():
                self.engines[ExecutionEngineType.SPARK] = spark_engine
                initialized_count += 1
                self.logger.info("Spark engine initialized")
                
        if not self.engines:
            self.logger.error("No execution engines could be initialized")
            return False
            
        # Set primary engine based on preference
        self._set_primary_engine()
        
        self.logger.info(f"Initialized {initialized_count} execution engine(s)")
        return True
        
    def _set_primary_engine(self) -> None:
        """Set primary engine based on availability and strategy."""
        # Priority order based on strategy
        if self.selection_strategy == EngineSelectionStrategy.PERFORMANCE:
            preferred_order = [ExecutionEngineType.DUCKDB, ExecutionEngineType.SPARK, ExecutionEngineType.PANDAS]
        elif self.selection_strategy == EngineSelectionStrategy.DISTRIBUTED:
            preferred_order = [ExecutionEngineType.SPARK, ExecutionEngineType.DUCKDB, ExecutionEngineType.PANDAS]
        elif self.selection_strategy == EngineSelectionStrategy.MEMORY_EFFICIENT:
            preferred_order = [ExecutionEngineType.DUCKDB, ExecutionEngineType.PANDAS, ExecutionEngineType.SPARK]
        else:  # AUTO
            preferred_order = [ExecutionEngineType.DUCKDB, ExecutionEngineType.PANDAS, ExecutionEngineType.SPARK]
            
        for engine_type in preferred_order:
            if engine_type in self.engines:
                self.primary_engine = self.engines[engine_type]
                break
                
        # Set fallback engines
        self.fallback_engines = [
            engine for engine_type, engine in self.engines.items() 
            if engine != self.primary_engine
        ]
        
        if self.primary_engine:
            self.logger.info(f"Primary engine: {self.primary_engine.engine_type.value}")
            
    async def execute_sql(
        self, 
        sql: str, 
        params: Optional[Dict[str, Any]] = None,
        engine_hint: Optional[ExecutionEngineType] = None
    ) -> ExecutionResult:
        """Execute SQL with automatic engine selection."""
        engine = self._select_engine(engine_hint=engine_hint, operation_type="sql")
        
        if not engine:
            return ExecutionResult(
                success=False,
                error="No suitable execution engine available"
            )
            
        try:
            result = await engine.execute_sql(sql, params)
            if result.success:
                self.logger.debug(f"SQL executed successfully using {engine.engine_type.value}")
                return result
            else:
                self.logger.warning(f"SQL execution failed on {engine.engine_type.value}: {result.error}")
                
        except Exception as e:
            self.logger.error(f"SQL execution error on {engine.engine_type.value}: {e}")
            
        # Try fallback engines
        for fallback_engine in self.fallback_engines:
            if fallback_engine != engine:
                try:
                    result = await fallback_engine.execute_sql(sql, params)
                    if result.success:
                        self.logger.info(f"SQL executed successfully using fallback {fallback_engine.engine_type.value}")
                        return result
                except Exception as e:
                    self.logger.warning(f"Fallback engine {fallback_engine.engine_type.value} also failed: {e}")
                    
        return ExecutionResult(
            success=False,
            error="All execution engines failed"
        )
        
    async def execute_transformation(
        self,
        source_data: Any,
        transformation_spec: Dict[str, Any],
        engine_hint: Optional[ExecutionEngineType] = None
    ) -> ExecutionResult:
        """Execute transformation with automatic engine selection."""
        # Estimate data size for engine selection
        data_size = self._estimate_data_size(source_data)
        
        engine = self._select_engine(
            engine_hint=engine_hint,
            operation_type="transformation",
            data_size=data_size
        )
        
        if not engine:
            return ExecutionResult(
                success=False,
                error="No suitable execution engine available"
            )
            
        try:
            result = await engine.execute_transformation(source_data, transformation_spec)
            if result.success:
                self.logger.debug(f"Transformation executed successfully using {engine.engine_type.value}")
                return result
            else:
                self.logger.warning(f"Transformation failed on {engine.engine_type.value}: {result.error}")
                
        except Exception as e:
            self.logger.error(f"Transformation error on {engine.engine_type.value}: {e}")
            
        # Try fallback engines
        for fallback_engine in self.fallback_engines:
            if fallback_engine != engine:
                try:
                    result = await fallback_engine.execute_transformation(source_data, transformation_spec)
                    if result.success:
                        self.logger.info(f"Transformation executed successfully using fallback {fallback_engine.engine_type.value}")
                        return result
                except Exception as e:
                    self.logger.warning(f"Fallback engine {fallback_engine.engine_type.value} also failed: {e}")
                    
        return ExecutionResult(
            success=False,
            error="All execution engines failed"
        )
        
    async def load_data(
        self,
        source: str,
        format_type: str = "auto",
        options: Optional[Dict[str, Any]] = None,
        engine_hint: Optional[ExecutionEngineType] = None
    ) -> ExecutionResult:
        """Load data with automatic engine selection."""
        engine = self._select_engine(
            engine_hint=engine_hint,
            operation_type="load",
            file_path=source
        )
        
        if not engine:
            return ExecutionResult(
                success=False,
                error="No suitable execution engine available"
            )
            
        try:
            result = await engine.load_data(source, format_type, options)
            if result.success:
                self.logger.debug(f"Data loaded successfully using {engine.engine_type.value}")
                return result
            else:
                self.logger.warning(f"Data loading failed on {engine.engine_type.value}: {result.error}")
                
        except Exception as e:
            self.logger.error(f"Data loading error on {engine.engine_type.value}: {e}")
            
        # Try fallback engines
        for fallback_engine in self.fallback_engines:
            if fallback_engine != engine:
                try:
                    result = await fallback_engine.load_data(source, format_type, options)
                    if result.success:
                        self.logger.info(f"Data loaded successfully using fallback {fallback_engine.engine_type.value}")
                        return result
                except Exception as e:
                    self.logger.warning(f"Fallback engine {fallback_engine.engine_type.value} also failed: {e}")
                    
        return ExecutionResult(
            success=False,
            error="All execution engines failed"
        )
        
    async def save_data(
        self,
        data: Any,
        destination: str,
        format_type: str = "auto",
        options: Optional[Dict[str, Any]] = None,
        engine_hint: Optional[ExecutionEngineType] = None
    ) -> ExecutionResult:
        """Save data with automatic engine selection."""
        data_size = self._estimate_data_size(data)
        
        engine = self._select_engine(
            engine_hint=engine_hint,
            operation_type="save",
            data_size=data_size,
            file_path=destination
        )
        
        if not engine:
            return ExecutionResult(
                success=False,
                error="No suitable execution engine available"
            )
            
        try:
            result = await engine.save_data(data, destination, format_type, options)
            if result.success:
                self.logger.debug(f"Data saved successfully using {engine.engine_type.value}")
                return result
            else:
                self.logger.warning(f"Data saving failed on {engine.engine_type.value}: {result.error}")
                
        except Exception as e:
            self.logger.error(f"Data saving error on {engine.engine_type.value}: {e}")
            
        # Try fallback engines
        for fallback_engine in self.fallback_engines:
            if fallback_engine != engine:
                try:
                    result = await fallback_engine.save_data(data, destination, format_type, options)
                    if result.success:
                        self.logger.info(f"Data saved successfully using fallback {fallback_engine.engine_type.value}")
                        return result
                except Exception as e:
                    self.logger.warning(f"Fallback engine {fallback_engine.engine_type.value} also failed: {e}")
                    
        return ExecutionResult(
            success=False,
            error="All execution engines failed"
        )
        
    def _select_engine(
        self,
        engine_hint: Optional[ExecutionEngineType] = None,
        operation_type: str = "general",
        data_size: Optional[int] = None,
        file_path: Optional[str] = None
    ) -> Optional[BaseExecutionEngine]:
        """Select the best engine for the operation."""
        
        # If specific engine requested and available, use it
        if engine_hint and engine_hint in self.engines:
            return self.engines[engine_hint]
            
        # Auto-selection based on operation characteristics
        if self.selection_strategy == EngineSelectionStrategy.MANUAL:
            return self.primary_engine
            
        # Data size-based selection
        if data_size is not None:
            if data_size <= self.small_data_threshold:
                # Small data: Pandas is efficient
                if ExecutionEngineType.PANDAS in self.engines:
                    return self.engines[ExecutionEngineType.PANDAS]
            elif data_size <= self.medium_data_threshold:
                # Medium data: DuckDB is optimal
                if ExecutionEngineType.DUCKDB in self.engines:
                    return self.engines[ExecutionEngineType.DUCKDB]
            else:
                # Large data: Spark for distributed processing
                if ExecutionEngineType.SPARK in self.engines:
                    return self.engines[ExecutionEngineType.SPARK]
                    
        # Operation-specific selection
        if operation_type == "sql":
            # DuckDB excels at analytical SQL
            if ExecutionEngineType.DUCKDB in self.engines:
                return self.engines[ExecutionEngineType.DUCKDB]
                
        # File format considerations
        if file_path:
            if file_path.endswith('.parquet'):
                # DuckDB and Spark handle Parquet very well
                if ExecutionEngineType.DUCKDB in self.engines:
                    return self.engines[ExecutionEngineType.DUCKDB]
                elif ExecutionEngineType.SPARK in self.engines:
                    return self.engines[ExecutionEngineType.SPARK]
                    
        # Default to primary engine
        return self.primary_engine
        
    def _estimate_data_size(self, data: Any) -> int:
        """Estimate data size (number of rows)."""
        if hasattr(data, '__len__'):
            return len(data)
        elif hasattr(data, 'count'):
            try:
                return data.count()
            except:
                pass
        elif isinstance(data, dict):
            if data:
                first_key = next(iter(data))
                if isinstance(data[first_key], list):
                    return len(data[first_key])
                    
        # Default to medium size if cannot estimate
        return self.medium_data_threshold // 2
        
    def get_available_engines(self) -> List[str]:
        """Get list of available engine names."""
        return [engine.engine_type.value for engine in self.engines.values()]
        
    def get_engine_capabilities(self) -> Dict[str, Dict[str, Any]]:
        """Get capabilities of all engines."""
        return {
            engine.engine_type.value: engine.get_capabilities()
            for engine in self.engines.values()
        }
        
    def get_primary_engine(self) -> Optional[str]:
        """Get the name of the primary engine."""
        return self.primary_engine.engine_type.value if self.primary_engine else None
        
    async def cleanup(self) -> None:
        """Cleanup all engines."""
        for engine in self.engines.values():
            try:
                await engine.cleanup()
            except Exception as e:
                self.logger.warning(f"Error cleaning up {engine.engine_type.value}: {e}")
                
        self.engines.clear()
        self.primary_engine = None
        self.fallback_engines.clear()
        self.logger.info("All execution engines cleaned up")
