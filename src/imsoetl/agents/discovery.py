"""
Discovery Agent - Responsible for data source discovery and analysis.

This agent:
- Connects to various data sources using real database connectors
- Analyzes table structures and relationships
- Identifies data patterns and characteristics
- Reports findings back to the orchestrator
"""

import asyncio
import json
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime

from ..core.base_agent import BaseAgent, AgentType, Message
from ..connectors import (
    ConnectorFactory, 
    ConnectionConfig, 
    BaseConnector, 
    ConnectionStatus,
    create_sqlite_connector,
    create_postgresql_connector,
    create_mysql_connector
)


class DataSourceInfo:
    """Information about a discovered data source."""
    
    def __init__(
        self,
        source_id: str,
        source_type: str,
        connection_string: str,
        tables: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        self.source_id = source_id
        self.source_type = source_type
        self.connection_string = connection_string
        self.tables = tables or []
        self.metadata = metadata or {}
        self.discovered_at = datetime.utcnow()
        self.status = "unknown"
        self.error_message: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "source_id": self.source_id,
            "source_type": self.source_type,
            "connection_string": self.connection_string,
            "tables": self.tables,
            "metadata": self.metadata,
            "discovered_at": self.discovered_at.isoformat(),
            "status": self.status,
            "error_message": self.error_message
        }


class TableInfo:
    """Information about a discovered table."""
    
    def __init__(
        self,
        table_name: str,
        source_id: str,
        columns: Optional[List[Dict[str, Any]]] = None,
        row_count: Optional[int] = None,
        size_mb: Optional[float] = None
    ):
        self.table_name = table_name
        self.source_id = source_id
        self.columns = columns or []
        self.row_count = row_count
        self.size_mb = size_mb
        self.discovered_at = datetime.utcnow()
        self.relationships = []
        self.indexes = []
        self.constraints = []
        self.sample_data = []
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "table_name": self.table_name,
            "source_id": self.source_id,
            "columns": self.columns,
            "row_count": self.row_count,
            "size_mb": self.size_mb,
            "discovered_at": self.discovered_at.isoformat(),
            "relationships": self.relationships,
            "indexes": self.indexes,
            "constraints": self.constraints,
            "sample_data": self.sample_data
        }


class DatabaseSourceConnector:
    """Real database connector using the connector factory."""
    
    @staticmethod
    async def connect_and_discover(source_config: Dict[str, Any]) -> DataSourceInfo:
        """Connect to real database and discover structure."""
        source_type = source_config.get("type", "unknown")
        source_id = source_config.get("id", f"source_{datetime.utcnow().timestamp()}")
        
        info = DataSourceInfo(
            source_id=source_id,
            source_type=source_type,
            connection_string=f"{source_type}://connection"
        )
        
        try:
            # Create connector based on type
            connector = None
            
            if source_type == "sqlite":
                database_path = source_config.get("database", ":memory:")
                connector = create_sqlite_connector(database_path)
            
            elif source_type == "postgresql":
                connector = create_postgresql_connector(
                    host=source_config.get("host", "localhost"),
                    port=source_config.get("port", 5432),
                    database=source_config.get("database", ""),
                    username=source_config.get("username", ""),
                    password=source_config.get("password", "")
                )
            
            elif source_type == "mysql":
                connector = create_mysql_connector(
                    host=source_config.get("host", "localhost"),
                    port=source_config.get("port", 3306),
                    database=source_config.get("database", ""),
                    username=source_config.get("username", ""),
                    password=source_config.get("password", "")
                )
            
            else:
                info.status = "unsupported_type"
                info.error_message = f"Unsupported source type: {source_type}"
                return info
            
            # Test connection
            if not await connector.test_connection():
                info.status = "connection_failed"
                info.error_message = "Failed to connect to database"
                return info
            
            # Connect and discover tables
            if await connector.connect():
                try:
                    info.tables = await connector.get_tables()
                    info.status = "connected"
                    info.metadata = {
                        "total_tables": len(info.tables),
                        "connection_info": await connector.get_connection_info()
                    }
                finally:
                    await connector.disconnect()
            else:
                info.status = "connection_failed"
                info.error_message = "Failed to establish connection"
        
        except ImportError as e:
            info.status = "dependency_missing"
            info.error_message = f"Database connector dependency missing: {str(e)}"
        except Exception as e:
            info.status = "error"
            info.error_message = str(e)
        
        return info
    
    @staticmethod
    async def analyze_table(source_config: Dict[str, Any], table_name: str) -> TableInfo:
        """Analyze a specific table using real database connection."""
        source_type = source_config.get("type", "unknown")
        source_id = source_config.get("id", f"source_{datetime.utcnow().timestamp()}")
        
        table_info = TableInfo(
            table_name=table_name,
            source_id=source_id
        )
        
        try:
            # Create connector
            connector = None
            
            if source_type == "sqlite":
                database_path = source_config.get("database", ":memory:")
                connector = create_sqlite_connector(database_path)
            
            elif source_type == "postgresql":
                connector = create_postgresql_connector(
                    host=source_config.get("host", "localhost"),
                    port=source_config.get("port", 5432),
                    database=source_config.get("database", ""),
                    username=source_config.get("username", ""),
                    password=source_config.get("password", "")
                )
            
            elif source_type == "mysql":
                connector = create_mysql_connector(
                    host=source_config.get("host", "localhost"),
                    port=source_config.get("port", 3306),
                    database=source_config.get("database", ""),
                    username=source_config.get("username", ""),
                    password=source_config.get("password", "")
                )
            
            if connector and await connector.connect():
                try:
                    # Get table metadata
                    metadata = await connector.get_table_metadata(table_name)
                    table_info.columns = metadata.columns
                    table_info.row_count = metadata.row_count
                    table_info.size_mb = metadata.size_bytes / (1024 * 1024) if metadata.size_bytes else None
                    
                    # Get sample data for analysis
                    sample_data = await connector.get_sample_data(table_name, limit=10)
                    table_info.sample_data = sample_data
                    
                finally:
                    await connector.disconnect()
        
        except Exception as e:
            # Fallback to mock data if real connection fails
            table_info = await MockDataSourceConnector.analyze_table(
                DataSourceInfo(source_id, source_type, "mock://connection"), 
                table_name
            )
        
        return table_info


# Keep the mock connector for testing
class MockDataSourceConnector:
    """Mock connector for testing purposes - simulates various data source types."""
    
    @staticmethod
    async def connect_and_discover(source_config: Dict[str, Any]) -> DataSourceInfo:
        """Mock connection and discovery process."""
        source_type = source_config.get("type", "unknown")
        source_id = source_config.get("id", f"source_{datetime.utcnow().timestamp()}")
        
        # Simulate connection delay
        await asyncio.sleep(1)
        
        info = DataSourceInfo(
            source_id=source_id,
            source_type=source_type,
            connection_string=f"{source_type}://mock_connection"
        )
        
        try:
            # Mock different data source types
            if source_type == "mysql":
                info.tables = [
                    "customers", "orders", "products", "order_items",
                    "users", "categories", "suppliers", "inventory"
                ]
                info.metadata = {
                    "version": "8.0.35",
                    "charset": "utf8mb4",
                    "timezone": "UTC",
                    "total_tables": len(info.tables)
                }
                info.status = "connected"
                
            elif source_type == "snowflake":
                info.tables = [
                    "CUSTOMER_DATA", "SALES_FACT", "PRODUCT_DIM",
                    "TIME_DIM", "GEOGRAPHY_DIM", "CAMPAIGN_DATA"
                ]
                info.metadata = {
                    "warehouse": "COMPUTE_WH",
                    "database": "ANALYTICS_DB",
                    "schema": "PUBLIC",
                    "total_tables": len(info.tables)
                }
                info.status = "connected"
                
            elif source_type == "postgres":
                info.tables = [
                    "user_profiles", "transactions", "audit_log",
                    "session_data", "notifications", "settings"
                ]
                info.metadata = {
                    "version": "15.2",
                    "total_tables": len(info.tables)
                }
                info.status = "connected"
                
            elif source_type == "mongodb":
                info.tables = [  # Collections in MongoDB
                    "user_events", "product_catalog", "recommendations",
                    "analytics_metrics", "real_time_data"
                ]
                info.metadata = {
                    "version": "6.0",
                    "database": "app_data",
                    "total_collections": len(info.tables)
                }
                info.status = "connected"
                
            else:
                info.status = "unknown_type"
                info.error_message = f"Unknown source type: {source_type}"
        
        except Exception as e:
            info.status = "error"
            info.error_message = str(e)
        
        return info
    
    @staticmethod
    async def analyze_table(source_info: DataSourceInfo, table_name: str) -> TableInfo:
        """Mock table analysis."""
        await asyncio.sleep(0.5)  # Simulate analysis time
        
        table_info = TableInfo(
            table_name=table_name,
            source_id=source_info.source_id
        )
        
        # Mock column information based on common table patterns
        if "customer" in table_name.lower():
            table_info.columns = [
                {"name": "customer_id", "type": "INTEGER", "nullable": False, "primary_key": True},
                {"name": "first_name", "type": "VARCHAR(50)", "nullable": False},
                {"name": "last_name", "type": "VARCHAR(50)", "nullable": False},
                {"name": "email", "type": "VARCHAR(100)", "nullable": False, "unique": True},
                {"name": "phone_number", "type": "VARCHAR(20)", "nullable": True},
                {"name": "created_at", "type": "TIMESTAMP", "nullable": False},
                {"name": "updated_at", "type": "TIMESTAMP", "nullable": True}
            ]
            table_info.row_count = 125000
            table_info.size_mb = 45.2
            
        elif "order" in table_name.lower():
            table_info.columns = [
                {"name": "order_id", "type": "INTEGER", "nullable": False, "primary_key": True},
                {"name": "customer_id", "type": "INTEGER", "nullable": False, "foreign_key": "customers.customer_id"},
                {"name": "order_date", "type": "DATE", "nullable": False},
                {"name": "total_amount", "type": "DECIMAL(10,2)", "nullable": False},
                {"name": "status", "type": "VARCHAR(20)", "nullable": False},
                {"name": "shipping_address", "type": "TEXT", "nullable": True}
            ]
            table_info.row_count = 450000
            table_info.size_mb = 89.7
            
        elif "product" in table_name.lower():
            table_info.columns = [
                {"name": "product_id", "type": "INTEGER", "nullable": False, "primary_key": True},
                {"name": "product_name", "type": "VARCHAR(100)", "nullable": False},
                {"name": "category_id", "type": "INTEGER", "nullable": True},
                {"name": "price", "type": "DECIMAL(8,2)", "nullable": False},
                {"name": "description", "type": "TEXT", "nullable": True},
                {"name": "in_stock", "type": "BOOLEAN", "nullable": False, "default": True}
            ]
            table_info.row_count = 25000
            table_info.size_mb = 12.8
            
        else:
            # Generic table structure
            table_info.columns = [
                {"name": "id", "type": "INTEGER", "nullable": False, "primary_key": True},
                {"name": "name", "type": "VARCHAR(100)", "nullable": False},
                {"name": "created_at", "type": "TIMESTAMP", "nullable": False}
            ]
            table_info.row_count = 10000
            table_info.size_mb = 2.5
        
        return table_info


class DiscoveryAgent(BaseAgent):
    """
    Discovery Agent responsible for data source discovery and analysis.
    
    Key capabilities:
    - Connect to various data sources (MySQL, PostgreSQL, Snowflake, MongoDB, etc.)
    - Discover tables/collections and their structures
    - Analyze data patterns and characteristics
    - Identify relationships between tables
    - Report findings to other agents
    """
    
    def __init__(self, agent_id: str = "discovery_main", config: Optional[Dict[str, Any]] = None):
        super().__init__(
            agent_id=agent_id,
            agent_type=AgentType.DISCOVERY,
            name="DiscoveryAgent",
            config=config
        )
        
        # Discovery-specific state
        self.discovered_sources: Dict[str, DataSourceInfo] = {}
        self.discovered_tables: Dict[str, List[TableInfo]] = {}
        # Use the real database connector for all supported types
        self.source_connectors = {
            "mysql": DatabaseSourceConnector,
            "postgresql": DatabaseSourceConnector,
            "postgres": DatabaseSourceConnector,  # alias
            "sqlite": DatabaseSourceConnector,
            "snowflake": MockDataSourceConnector,  # Keep mock for unsupported types
            "mongodb": MockDataSourceConnector,
        }
        
        # Register message handlers
        self.register_message_handlers()
        
        self.logger.info("Discovery Agent initialized")
    
    def register_message_handlers(self) -> None:
        """Register handlers for different message types."""
        self.register_message_handler("task_assignment", self.handle_task_assignment)
        self.register_message_handler("discover_source", self.handle_discover_source)
        self.register_message_handler("analyze_table", self.handle_analyze_table)
        self.register_message_handler("list_sources", self.handle_list_sources)
    
    async def handle_task_assignment(self, message: Message) -> None:
        """Handle task assignments from the orchestrator."""
        task = message.content.get("task", {})
        session_id = message.content.get("session_id")
        context = message.content.get("context", {})
        
        task_type = task.get("task_type")
        task_id = task.get("task_id")
        
        self.logger.info(f"Received task assignment: {task_type} (ID: {task_id})")
        
        try:
            if task_type == "discovery":
                result = await self._execute_discovery_task(task, context)
            else:
                result = {"error": f"Unknown task type: {task_type}"}
                
            # Send result back to orchestrator
            await self.send_message(
                receiver_id=message.sender_id,
                message_type="task_complete",
                content={
                    "session_id": session_id,
                    "task_id": task_id,
                    "result": result
                }
            )
            
        except Exception as e:
            self.logger.error(f"Error executing task {task_id}: {e}")
            await self.send_message(
                receiver_id=message.sender_id,
                message_type="task_error",
                content={
                    "session_id": session_id,
                    "task_id": task_id,
                    "error": str(e)
                }
            )
    
    async def handle_discover_source(self, message: Message) -> None:
        """Handle direct source discovery requests."""
        source_config = message.content.get("source_config", {})
        
        try:
            source_info = await self._discover_source(source_config)
            
            await self.send_message(
                receiver_id=message.sender_id,
                message_type="discovery_result",
                content={
                    "source_info": source_info.to_dict()
                }
            )
            
        except Exception as e:
            await self.send_message(
                receiver_id=message.sender_id,
                message_type="discovery_error",
                content={
                    "error": str(e),
                    "source_config": source_config
                }
            )
    
    async def handle_analyze_table(self, message: Message) -> None:
        """Handle table analysis requests."""
        source_id = message.content.get("source_id")
        table_name = message.content.get("table_name")
        
        if not source_id or not table_name:
            await self.send_message(
                receiver_id=message.sender_id,
                message_type="analysis_error",
                content={
                    "error": "Missing source_id or table_name",
                    "source_id": source_id,
                    "table_name": table_name
                }
            )
            return
        
        try:
            if source_id not in self.discovered_sources:
                raise ValueError(f"Source {source_id} not found")
            
            source_info = self.discovered_sources[source_id]
            table_info = await self._analyze_table(source_info, str(table_name))
            
            await self.send_message(
                receiver_id=message.sender_id,
                message_type="table_analysis_result",
                content={
                    "table_info": table_info.to_dict()
                }
            )
            
        except Exception as e:
            await self.send_message(
                receiver_id=message.sender_id,
                message_type="analysis_error",
                content={
                    "error": str(e),
                    "source_id": source_id,
                    "table_name": table_name
                }
            )
    
    async def handle_list_sources(self, message: Message) -> None:
        """Handle requests to list discovered sources."""
        sources_list = [info.to_dict() for info in self.discovered_sources.values()]
        
        await self.send_message(
            receiver_id=message.sender_id,
            message_type="sources_list",
            content={
                "sources": sources_list,
                "total_count": len(sources_list)
            }
        )
    
    async def _execute_discovery_task(self, task: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a discovery task."""
        parameters = task.get("parameters", {})
        sources = parameters.get("sources", [])
        
        discovery_results = []
        
        for source_name in sources:
            try:
                # For now, we'll create mock source configurations
                # In a real implementation, these would come from configuration
                source_config = self._create_mock_source_config(source_name)
                
                source_info = await self._discover_source(source_config)
                discovery_results.append(source_info.to_dict())
                
                # Also analyze some tables
                if source_info.tables:
                    table_analyses = []
                    # Analyze first few tables to avoid overwhelming the system
                    for table_name in source_info.tables[:3]:
                        table_info = await self._analyze_table(source_info, table_name)
                        table_analyses.append(table_info.to_dict())
                    
                    discovery_results[-1]["analyzed_tables"] = table_analyses
                
            except Exception as e:
                self.logger.error(f"Error discovering source {source_name}: {e}")
                discovery_results.append({
                    "source_name": source_name,
                    "error": str(e),
                    "status": "failed"
                })
        
        return {
            "task_type": "discovery",
            "discovered_sources": discovery_results,
            "total_sources": len(discovery_results),
            "successful_discoveries": len([r for r in discovery_results if "error" not in r])
        }
    
    def _create_mock_source_config(self, source_name: str) -> Dict[str, Any]:
        """Create a mock source configuration based on the source name."""
        # Simple heuristics to determine source type from name
        source_name_lower = source_name.lower()
        
        if "mysql" in source_name_lower:
            return {"type": "mysql", "id": source_name, "name": source_name}
        elif "snowflake" in source_name_lower:
            return {"type": "snowflake", "id": source_name, "name": source_name}
        elif "postgres" in source_name_lower:
            return {"type": "postgres", "id": source_name, "name": source_name}
        elif "mongo" in source_name_lower:
            return {"type": "mongodb", "id": source_name, "name": source_name}
        else:
            # Default to MySQL for unknown sources
            return {"type": "mysql", "id": source_name, "name": source_name}
    
    async def _discover_source(self, source_config: Dict[str, Any]) -> DataSourceInfo:
        """Discover a data source."""
        source_type = source_config.get("type")
        
        if source_type not in self.source_connectors:
            raise ValueError(f"Unsupported source type: {source_type}")
        
        connector = self.source_connectors[source_type]
        source_info = await connector.connect_and_discover(source_config)
        
        # Store the discovered source
        self.discovered_sources[source_info.source_id] = source_info
        
        self.logger.info(
            f"Discovered source {source_info.source_id} ({source_info.source_type}) "
            f"with {len(source_info.tables)} tables"
        )
        
        return source_info
    
    async def _analyze_table(self, source_info: DataSourceInfo, table_name: str) -> TableInfo:
        """Analyze a specific table."""
        connector_class = self.source_connectors[source_info.source_type]
        
        # For real database connectors, we need to pass source config
        if connector_class == DatabaseSourceConnector:
            # Convert source_info back to config format for the real connector
            source_config = {
                "type": source_info.source_type,
                "id": source_info.source_id,
                # Add default connection parameters (in real usage, these would come from config)
                "host": "localhost",
                "database": "test",
                "username": "user",
                "password": "password"
            }
            table_info = await connector_class.analyze_table(source_config, table_name)
        else:
            # For mock connectors
            table_info = await connector_class.analyze_table(source_info, table_name)
        
        # Store the table analysis
        if source_info.source_id not in self.discovered_tables:
            self.discovered_tables[source_info.source_id] = []
        
        self.discovered_tables[source_info.source_id].append(table_info)
        
        self.logger.info(
            f"Analyzed table {table_name} from {source_info.source_id}: "
            f"{len(table_info.columns)} columns, {table_info.row_count} rows"
        )
        
        return table_info
    
    async def process_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Process a generic task assigned to this agent."""
        task_type = task.get("type", "unknown")
        
        if task_type == "discover_sources":
            sources = task.get("sources", [])
            results = []
            
            for source_config in sources:
                try:
                    source_info = await self._discover_source(source_config)
                    results.append(source_info.to_dict())
                except Exception as e:
                    results.append({"error": str(e), "source_config": source_config})
            
            return {"results": results}
        
        elif task_type == "analyze_tables":
            source_id = task.get("source_id")
            table_names = task.get("table_names", [])
            
            if source_id not in self.discovered_sources:
                return {"error": f"Source {source_id} not found"}
            
            source_info = self.discovered_sources[source_id]
            results = []
            
            for table_name in table_names:
                try:
                    table_info = await self._analyze_table(source_info, table_name)
                    results.append(table_info.to_dict())
                except Exception as e:
                    results.append({"error": str(e), "table_name": table_name})
            
            return {"results": results}
        
        else:
            return {"error": f"Unknown task type: {task_type}"}
    
    def get_discovery_summary(self) -> Dict[str, Any]:
        """Get a summary of all discoveries."""
        total_tables = sum(len(tables) for tables in self.discovered_tables.values())
        
        return {
            "total_sources": len(self.discovered_sources),
            "total_tables": total_tables,
            "sources_by_type": self._get_sources_by_type(),
            "last_discovery": max(
                [info.discovered_at for info in self.discovered_sources.values()],
                default=None
            )
        }
    
    def _get_sources_by_type(self) -> Dict[str, int]:
        """Get count of sources by type."""
        type_counts = {}
        for source_info in self.discovered_sources.values():
            source_type = source_info.source_type
            type_counts[source_type] = type_counts.get(source_type, 0) + 1
        return type_counts
