"""Base connector classes for data sources."""

from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Union
import asyncio
import logging
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class ConnectionStatus(Enum):
    """Connection status enumeration."""
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    ERROR = "error"
    CONNECTING = "connecting"


@dataclass
class ConnectionConfig:
    """Configuration for database connections."""
    host: str
    port: int
    database: str
    username: str
    password: str
    additional_params: Optional[Dict[str, Any]] = None
    
    def __post_init__(self):
        if self.additional_params is None:
            self.additional_params = {}


@dataclass
class TableMetadata:
    """Metadata for database tables."""
    name: str
    schema: str
    columns: List[Dict[str, Any]]
    row_count: Optional[int] = None
    size_bytes: Optional[int] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class BaseConnector(ABC):
    """Base class for all data connectors."""
    
    def __init__(self, config: ConnectionConfig):
        self.config = config
        self.connection = None
        self.status = ConnectionStatus.DISCONNECTED
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    @abstractmethod
    async def connect(self) -> bool:
        """Establish connection to the data source."""
        pass
    
    @abstractmethod
    async def disconnect(self) -> bool:
        """Close connection to the data source."""
        pass
    
    @abstractmethod
    async def test_connection(self) -> bool:
        """Test the connection without establishing a persistent connection."""
        pass
    
    @abstractmethod
    async def get_tables(self, schema: Optional[str] = None) -> List[str]:
        """Get list of tables in the database."""
        pass
    
    @abstractmethod
    async def get_table_metadata(self, table_name: str, schema: Optional[str] = None) -> TableMetadata:
        """Get metadata for a specific table."""
        pass
    
    @abstractmethod
    async def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """Execute a query and return results."""
        pass
    
    @abstractmethod
    async def get_sample_data(self, table_name: str, limit: int = 100, schema: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get sample data from a table."""
        pass
    
    async def get_connection_info(self) -> Dict[str, Any]:
        """Get connection information."""
        return {
            "host": self.config.host,
            "port": self.config.port,
            "database": self.config.database,
            "status": self.status.value,
            "type": self.__class__.__name__
        }
    
    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.config.host}:{self.config.port}/{self.config.database})"
