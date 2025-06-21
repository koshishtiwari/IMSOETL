"""Connector factory for creating database connectors."""

from typing import Dict, Type, Optional
from .base import BaseConnector, ConnectionConfig
from .sqlite import SQLiteConnector

# Import connectors with fallback for missing dependencies
try:
    from .postgresql import PostgreSQLConnector
    POSTGRESQL_AVAILABLE = True
except ImportError:
    PostgreSQLConnector = None
    POSTGRESQL_AVAILABLE = False

try:
    from .mysql import MySQLConnector
    MYSQL_AVAILABLE = True
except ImportError:
    MySQLConnector = None
    MYSQL_AVAILABLE = False


class ConnectorFactory:
    """Factory for creating database connectors."""
    
    _connectors: Dict[str, Type[BaseConnector]] = {
        'sqlite': SQLiteConnector,
    }
    
    @classmethod
    def register_connector(cls, name: str, connector_class: Type[BaseConnector]):
        """Register a new connector type."""
        cls._connectors[name] = connector_class
    
    @classmethod
    def create_connector(cls, connector_type: str, config: ConnectionConfig) -> BaseConnector:
        """Create a connector instance."""
        if connector_type not in cls._connectors:
            available_types = list(cls._connectors.keys())
            raise ValueError(f"Unknown connector type: {connector_type}. Available types: {available_types}")
        
        connector_class = cls._connectors[connector_type]
        return connector_class(config)
    
    @classmethod
    def get_available_connectors(cls) -> Dict[str, bool]:
        """Get available connector types and their availability status."""
        return {
            'sqlite': True,
            'postgresql': POSTGRESQL_AVAILABLE,
            'mysql': MYSQL_AVAILABLE
        }
    
    @classmethod
    def create_config(cls, 
                     connector_type: str,
                     host: str = 'localhost',
                     port: Optional[int] = None,
                     database: str = '',
                     username: str = '',
                     password: str = '',
                     **kwargs) -> ConnectionConfig:
        """Create a connection configuration with default ports."""
        
        # Set default ports based on connector type
        if port is None:
            default_ports = {
                'postgresql': 5432,
                'mysql': 3306,
                'sqlite': 0  # Not applicable for SQLite
            }
            port = default_ports.get(connector_type, 0)
        
        return ConnectionConfig(
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            additional_params=kwargs
        )


# Register available connectors
if POSTGRESQL_AVAILABLE and PostgreSQLConnector:
    ConnectorFactory.register_connector('postgresql', PostgreSQLConnector)

if MYSQL_AVAILABLE and MySQLConnector:
    ConnectorFactory.register_connector('mysql', MySQLConnector)


# Convenience functions
def create_sqlite_connector(database_path: str) -> SQLiteConnector:
    """Create a SQLite connector."""
    config = ConnectionConfig(
        host='localhost',
        port=0,
        database=database_path,
        username='',
        password=''
    )
    return SQLiteConnector(config)


def create_postgresql_connector(host: str, port: int, database: str, 
                              username: str, password: str, **kwargs) -> BaseConnector:
    """Create a PostgreSQL connector."""
    if not POSTGRESQL_AVAILABLE:
        raise ImportError("PostgreSQL connector not available. Install with: pip install asyncpg")
    
    config = ConnectorFactory.create_config(
        'postgresql', host, port, database, username, password, **kwargs
    )
    return ConnectorFactory.create_connector('postgresql', config)


def create_mysql_connector(host: str, port: int, database: str,
                          username: str, password: str, **kwargs) -> BaseConnector:
    """Create a MySQL connector."""
    if not MYSQL_AVAILABLE:
        raise ImportError("MySQL connector not available. Install with: pip install aiomysql")
    
    config = ConnectorFactory.create_config(
        'mysql', host, port, database, username, password, **kwargs
    )
    return ConnectorFactory.create_connector('mysql', config)


# Register available connectors
if POSTGRESQL_AVAILABLE and PostgreSQLConnector:
    ConnectorFactory.register_connector('postgresql', PostgreSQLConnector)
    
if MYSQL_AVAILABLE and MySQLConnector:
    ConnectorFactory.register_connector('mysql', MySQLConnector)
