# IMSOETL Enhanced Features Documentation

## Version 0.2.0 - Enhanced Release

This document covers the enhanced features added to IMSOETL, including real database connectors, advanced error handling, configuration management, and improved CLI.

## 🗄️ Database Connectors

### Supported Databases

IMSOETL now supports real database connections through dedicated connectors:

#### SQLite
- **Status**: ✅ Available (no additional dependencies)
- **Use Case**: Development, testing, lightweight applications
- **Features**: File-based or in-memory databases

#### PostgreSQL
- **Status**: ✅ Available (requires `asyncpg`)
- **Use Case**: Production applications, advanced features
- **Features**: Full SQL support, advanced data types, JSON support

#### MySQL
- **Status**: ✅ Available (requires `aiomysql`)
- **Use Case**: Web applications, traditional relational databases
- **Features**: Popular open-source database, wide compatibility

### Installation

Install database connectors:

```bash
# All connectors (included in requirements.txt)
pip install -r requirements.txt

# Individual connectors
pip install asyncpg      # PostgreSQL
pip install aiomysql     # MySQL
pip install aiosqlite    # SQLite (lightweight)
```

### Usage Examples

#### CLI Commands

```bash
# List available connectors
imsoetl connectors

# Test database connection
imsoetl test-connection postgresql \
  --host localhost \
  --port 5432 \
  --database mydb \
  --username user \
  --password

# Discover database schema
imsoetl discover postgresql \
  --host localhost \
  --database mydb \
  --username user \
  --password \
  --output schema.json

# Test SQLite connection
imsoetl test-connection sqlite --database ./data/mydb.sqlite
```

#### Python API

```python
from imsoetl.connectors import (
    create_sqlite_connector,
    create_postgresql_connector,
    create_mysql_connector,
    ConnectorFactory
)

# Create SQLite connector
sqlite_conn = create_sqlite_connector("./data/mydb.sqlite")

# Create PostgreSQL connector
pg_conn = create_postgresql_connector(
    host="localhost",
    port=5432,
    database="mydb",
    username="user",
    password="password"
)

# Test connection
if await pg_conn.test_connection():
    print("Connection successful!")
    
    # Connect and discover
    await pg_conn.connect()
    tables = await pg_conn.get_tables()
    print(f"Found {len(tables)} tables")
    
    # Get table metadata
    for table in tables:
        metadata = await pg_conn.get_table_metadata(table)
        print(f"Table {table}: {len(metadata.columns)} columns")
    
    await pg_conn.disconnect()
```

### Connector Factory

The `ConnectorFactory` provides a unified interface for creating connectors:

```python
from imsoetl.connectors import ConnectorFactory, ConnectionConfig

# Create configuration
config = ConnectorFactory.create_config(
    connector_type="postgresql",
    host="localhost",
    port=5432,
    database="mydb",
    username="user",
    password="password"
)

# Create connector
connector = ConnectorFactory.create_connector("postgresql", config)

# Check available connectors
available = ConnectorFactory.get_available_connectors()
print(available)  # {'sqlite': True, 'postgresql': True, 'mysql': True}
```

## ⚙️ Configuration Management

### Configuration Files

IMSOETL supports YAML and JSON configuration files:

```bash
# Generate template
imsoetl config-template --output my-config.yaml

# Use configuration
imsoetl --config my-config.yaml interactive
```

### Sample Configuration

```yaml
# Environment settings
environment: production
debug: false

# Database connections
databases:
  primary_db:
    type: postgresql
    host: localhost
    port: 5432
    database: production_db
    username: app_user
    password: ${DB_PASSWORD}  # Environment variable
    
  analytics_db:
    type: sqlite
    database: ./data/analytics.db
    
  cache_db:
    type: redis
    host: redis.example.com
    port: 6379

# Agent configurations
agents:
  discovery:
    enabled: true
    max_concurrent_tasks: 10
    timeout_seconds: 300
    
  transformation:
    enabled: true
    max_concurrent_tasks: 5
    custom_params:
      optimization_level: high
      parallel_processing: true

# Logging configuration
logging:
  level: INFO
  structured: true
  file_path: ./logs/imsoetl.log
  max_file_size_mb: 100
  backup_count: 5

# API settings
api_host: 0.0.0.0
api_port: 8000
api_key: ${API_KEY}

# Storage settings
temp_dir: ./tmp
data_dir: ./data

# Custom settings
custom:
  feature_flags:
    experimental_features: false
    advanced_transformations: true
```

### Environment Variables

Configuration values can be overridden with environment variables:

```bash
# Database settings
export IMSOETL_DATABASES_PRIMARY_DB_PASSWORD="secure_password"
export IMSOETL_DATABASES_PRIMARY_DB_HOST="prod-db.example.com"

# Logging
export IMSOETL_LOGGING_LEVEL="DEBUG"
export IMSOETL_LOGGING_FILE_PATH="/var/log/imsoetl.log"

# API settings
export IMSOETL_API_KEY="your-api-key"
export IMSOETL_API_PORT="9000"
```

### Python Configuration API

```python
from imsoetl.core import load_config, get_config

# Load configuration from file
config = load_config("./config/production.yaml")

# Access configuration
print(f"Environment: {config.environment}")
print(f"Database: {config.databases['primary_db'].host}")

# Get global configuration
config = get_config()
```

## 🛡️ Error Handling & Logging

### Custom Exception Classes

```python
from imsoetl.core import (
    IMSOETLError,
    ConnectionError,
    ConfigurationError,
    ValidationError,
    AgentError,
    PipelineError
)

try:
    # Database operation
    result = await connector.connect()
except ConnectionError as e:
    print(f"Connection failed: {e.message}")
    print(f"Details: {e.details}")
    print(f"Error code: {e.error_code}")
```

### Error Handling Decorators

```python
from imsoetl.core import handle_errors, log_performance

@handle_errors(reraise=False, default_return=None)
@log_performance("data_processing")
async def process_data(data):
    # Your data processing logic
    return processed_data

# Usage
result = await process_data(my_data)
```

### Structured Logging

```python
from imsoetl.core import setup_logging

# Setup structured logging
logger = setup_logging(
    level="INFO",
    log_file="./logs/app.log",
    structured=True
)

# Use logger
logger.info("Processing started", extra={
    "user_id": "123",
    "operation": "data_discovery",
    "source_count": 5
})
```

### Log Output Examples

Structured JSON logs:
```json
{
  "timestamp": "2025-06-20T00:04:48.249Z",
  "level": "INFO",
  "logger": "imsoetl.discovery",
  "message": "Discovery completed successfully",
  "module": "discovery",
  "function": "discover_sources",
  "line": 145,
  "user_id": "123",
  "operation": "data_discovery",
  "duration_seconds": 2.34,
  "sources_found": 5
}
```

## 🚀 Enhanced CLI Features

### New Commands

#### Database Management
```bash
# List connectors
imsoetl connectors

# Test connections
imsoetl test-connection postgresql --host db.example.com --database mydb

# Discover schemas
imsoetl discover mysql --host localhost --database prod --output schema.json
```

#### Configuration
```bash
# Generate config template
imsoetl config-template --output my-config.yaml

# Validate configuration
imsoetl validate --config my-config.yaml
```

#### Enhanced Demos
```bash
# Run enhanced demo
imsoetl enhanced-demo

# Run specific agent tests
imsoetl test-agent discovery --verbose
```

### CLI Output Examples

**Connector Status:**
```
IMSOETL Database Connectors
==================================================
┏━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Connector Type ┃ Status       ┃ Description                              ┃
┡━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ Sqlite         │ ✅ Available │ Lightweight file-based database          │
│ Postgresql     │ ✅ Available │ Advanced open-source relational database │
│ Mysql          │ ✅ Available │ Popular open-source relational database  │
└────────────────┴──────────────┴──────────────────────────────────────────┘
```

**Connection Test:**
```
Testing Postgresql Connection
==================================================
Connecting to PostgreSQLConnector(localhost:5432/mydb)...
✅ Connection successful!
📊 Found 15 tables
Tables:
  • users
  • orders
  • products
  • customers
  • transactions
Connection closed.
```

## 🔧 Agent Enhancements

### Discovery Agent
- **Real Database Connections**: Uses actual database connectors
- **Schema Analysis**: Detailed table and column metadata
- **Error Handling**: Graceful failure with detailed error messages
- **Performance Monitoring**: Connection timing and metrics

### Enhanced Capabilities
```python
from imsoetl.agents import DiscoveryAgent

agent = DiscoveryAgent("discovery_main")

# Real database discovery
source_config = {
    "type": "postgresql",
    "host": "localhost",
    "database": "mydb",
    "username": "user",
    "password": "password"
}

# Discover with error handling
try:
    source_info = await agent.discover_source(source_config)
    print(f"Discovered {len(source_info.tables)} tables")
    for table in source_info.tables:
        table_info = await agent.analyze_table(source_info, table)
        print(f"Table {table}: {len(table_info.columns)} columns")
except Exception as e:
    print(f"Discovery failed: {e}")
```

## 📊 Monitoring & Performance

### Performance Decorators
```python
from imsoetl.core import log_performance

@log_performance("database_query")
async def execute_query(query):
    # Your database query logic
    return results
```

### Metrics Collection
- Connection timing
- Query execution time
- Agent processing time
- Error rates
- Resource usage

### Health Checks
```python
from imsoetl.agents import MonitoringAgent

monitor = MonitoringAgent("health_monitor")
health_status = await monitor.check_system_health()
print(health_status)
```

## 🔄 Migration from v0.1.0

### Breaking Changes
1. **Configuration Format**: New YAML-based configuration
2. **Connector Interface**: Updated to use real database connectors
3. **Error Handling**: New exception hierarchy

### Migration Steps

1. **Update Configuration:**
```bash
# Generate new config template
imsoetl config-template --output new-config.yaml

# Migrate your settings
# Edit new-config.yaml with your database details
```

2. **Update Code:**
```python
# Old (v0.1.0)
from imsoetl.agents.discovery import MockDataSourceConnector

# New (v0.2.0)
from imsoetl.connectors import create_postgresql_connector
from imsoetl.agents.discovery import DatabaseSourceConnector
```

3. **Install Dependencies:**
```bash
pip install -r requirements.txt  # Includes new database connectors
```

## 🎯 Best Practices

### Configuration Management
- Use environment variables for sensitive data
- Separate configs for development/staging/production
- Use configuration validation

### Error Handling
- Always handle connection errors gracefully
- Use structured logging for better debugging
- Implement retry mechanisms for transient failures

### Performance
- Use connection pooling for production
- Monitor query performance
- Implement proper timeout handling

### Security
- Store passwords in environment variables
- Use connection encryption (SSL/TLS)
- Implement proper access controls

## 🚀 What's Next

### Planned Features (v0.3.0)
- MongoDB connector support
- Kafka/streaming data support
- Advanced data lineage tracking
- Web-based dashboard
- Docker containerization
- Kubernetes deployment
- Advanced scheduling and orchestration

### Contributing
- Submit issues for bug reports
- Create pull requests for new features
- Add documentation improvements
- Write tests for new connectors

## 📞 Support

For support and questions:
- GitHub Issues: [IMSOETL Issues](https://github.com/your-repo/IMSOETL/issues)
- Documentation: This file and inline code documentation
- Examples: See `demo_enhanced.py` and test files

---

**IMSOETL v0.2.0** - Enhanced Agentic Data Engineering Platform
