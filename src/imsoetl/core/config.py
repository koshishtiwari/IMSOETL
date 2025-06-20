"""Configuration management for IMSOETL."""

import os
import yaml
import json
from pathlib import Path
from typing import Any, Dict, Optional, Union
from dataclasses import dataclass, field
from .errors import ConfigurationError

@dataclass
class DatabaseConfig:
    """Database connection configuration."""
    type: str
    host: str = "localhost"
    port: int = 5432
    database: str = ""
    username: str = ""
    password: str = ""
    additional_params: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Set default ports based on database type."""
        if self.port == 5432 and self.type != "postgresql":  # Only use default if not explicitly set
            port_defaults = {
                "mysql": 3306,
                "postgresql": 5432,
                "sqlite": 0,
                "mongodb": 27017,
                "redis": 6379
            }
            if self.type in port_defaults:
                self.port = port_defaults[self.type]


@dataclass
class AgentConfig:
    """Agent configuration."""
    enabled: bool = True
    max_concurrent_tasks: int = 5
    timeout_seconds: int = 300
    retry_attempts: int = 3
    custom_params: Dict[str, Any] = field(default_factory=dict)


@dataclass
class LoggingConfig:
    """Logging configuration."""
    level: str = "INFO"
    structured: bool = True
    file_path: Optional[str] = None
    max_file_size_mb: int = 100
    backup_count: int = 5


@dataclass
class IMSOETLConfig:
    """Main IMSOETL configuration."""
    # Environment
    environment: str = "development"
    debug: bool = False
    
    # Database connections
    databases: Dict[str, DatabaseConfig] = field(default_factory=dict)
    
    # Agent configurations
    agents: Dict[str, AgentConfig] = field(default_factory=dict)
    
    # Logging
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    
    # API settings
    api_host: str = "localhost"
    api_port: int = 8000
    api_key: Optional[str] = None
    
    # Storage settings  
    temp_dir: str = "/tmp/imsoetl"
    data_dir: str = "./data"
    
    # LLM settings
    openai_api_key: Optional[str] = None
    gemini_api_key: Optional[str] = None
    
    # Custom settings
    custom: Dict[str, Any] = field(default_factory=dict)


class ConfigManager:
    """Configuration manager for IMSOETL."""
    
    def __init__(self, config_path: Optional[Union[str, Path]] = None):
        self._config: Optional[IMSOETLConfig] = None
        self._config_path = Path(config_path) if config_path else None
        
    def load_config(
        self, 
        config_path: Optional[Union[str, Path]] = None,
        env_prefix: str = "IMSOETL_"
    ) -> IMSOETLConfig:
        """Load configuration from file and environment variables."""
        
        # Use provided path or stored path
        if config_path:
            self._config_path = Path(config_path)
        
        # Start with default config
        config_data = {}
        
        # Load from file if exists
        if self._config_path and self._config_path.exists():
            config_data = self._load_config_file(self._config_path)
        
        # Override with environment variables
        env_config = self._load_from_env(env_prefix)
        config_data.update(env_config)
        
        # Create config object
        self._config = self._create_config_object(config_data)
        
        # Validate configuration
        self._validate_config(self._config)
        
        return self._config
    
    def get_config(self) -> IMSOETLConfig:
        """Get current configuration."""
        if self._config is None:
            return self.load_config()
        return self._config
    
    def _load_config_file(self, config_path: Path) -> Dict[str, Any]:
        """Load configuration from YAML or JSON file."""
        try:
            with open(config_path, 'r') as f:
                if config_path.suffix.lower() in ['.yaml', '.yml']:
                    return yaml.safe_load(f) or {}
                elif config_path.suffix.lower() == '.json':
                    return json.load(f)
                else:
                    raise ConfigurationError(
                        f"Unsupported config file format: {config_path.suffix}",
                        config_key="config_file_format"
                    )
        except Exception as e:
            raise ConfigurationError(
                f"Failed to load config file {config_path}: {str(e)}",
                config_key="config_file_load"
            )
    
    def _load_from_env(self, prefix: str) -> Dict[str, Any]:
        """Load configuration from environment variables."""
        config = {}
        
        for key, value in os.environ.items():
            if key.startswith(prefix):
                # Remove prefix and convert to lowercase
                config_key = key[len(prefix):].lower()
                
                # Handle nested keys (e.g., IMSOETL_DATABASE_HOST -> database.host)
                key_parts = config_key.split('_')
                
                # Convert string values to appropriate types
                converted_value = self._convert_env_value(value)
                
                # Set nested dictionary values
                current = config
                for part in key_parts[:-1]:
                    if part not in current:
                        current[part] = {}
                    current = current[part]
                current[key_parts[-1]] = converted_value
        
        return config
    
    def _convert_env_value(self, value: str) -> Union[str, int, float, bool, None]:
        """Convert environment variable string to appropriate type."""
        # Handle None/null values
        if value.lower() in ['none', 'null', '']:
            return None
        
        # Handle boolean values
        if value.lower() in ['true', 'yes', '1']:
            return True
        if value.lower() in ['false', 'no', '0']:
            return False
        
        # Try to convert to number
        try:
            if '.' in value:
                return float(value)
            else:
                return int(value)
        except ValueError:
            pass
        
        # Return as string
        return value
    
    def _create_config_object(self, config_data: Dict[str, Any]) -> IMSOETLConfig:
        """Create IMSOETLConfig object from dictionary."""
        
        # Handle databases
        databases = {}
        if 'databases' in config_data:
            for db_name, db_config in config_data['databases'].items():
                databases[db_name] = DatabaseConfig(**db_config)
        
        # Handle agents
        agents = {}
        if 'agents' in config_data:
            for agent_name, agent_config in config_data['agents'].items():
                agents[agent_name] = AgentConfig(**agent_config)
        
        # Handle logging
        logging_config = LoggingConfig()
        if 'logging' in config_data:
            logging_data = config_data['logging']
            logging_config = LoggingConfig(**logging_data)
        
        # Create main config
        main_config_data = config_data.copy()
        main_config_data['databases'] = databases
        main_config_data['agents'] = agents
        main_config_data['logging'] = logging_config
        
        return IMSOETLConfig(**main_config_data)
    
    def _validate_config(self, config: IMSOETLConfig) -> None:
        """Validate configuration."""
        
        # Validate database configurations
        for db_name, db_config in config.databases.items():
            if not db_config.type:
                raise ConfigurationError(
                    f"Database type not specified for '{db_name}'",
                    config_key=f"databases.{db_name}.type"
                )
            
            if db_config.type in ['postgresql', 'mysql'] and not db_config.host:
                raise ConfigurationError(
                    f"Host not specified for database '{db_name}'",
                    config_key=f"databases.{db_name}.host"
                )
        
        # Validate directories
        for dir_attr in ['temp_dir', 'data_dir']:
            dir_path = getattr(config, dir_attr)
            if dir_path:
                try:
                    Path(dir_path).mkdir(parents=True, exist_ok=True)
                except Exception as e:
                    raise ConfigurationError(
                        f"Cannot create directory {dir_path}: {str(e)}",
                        config_key=dir_attr
                    )
    
    def save_config(self, config_path: Optional[Union[str, Path]] = None) -> None:
        """Save current configuration to file."""
        if not self._config:
            raise ConfigurationError("No configuration to save")
        
        save_path = Path(config_path) if config_path else self._config_path
        if not save_path:
            raise ConfigurationError("No config path specified for saving")
        
        # Convert config to dictionary
        config_dict = self._config_to_dict(self._config)
        
        try:
            with open(save_path, 'w') as f:
                if save_path.suffix.lower() in ['.yaml', '.yml']:
                    yaml.dump(config_dict, f, default_flow_style=False, indent=2)
                elif save_path.suffix.lower() == '.json':
                    json.dump(config_dict, f, indent=2)
                else:
                    raise ConfigurationError(f"Unsupported config file format: {save_path.suffix}")
        except Exception as e:
            raise ConfigurationError(f"Failed to save config file: {str(e)}")
    
    def _config_to_dict(self, config: IMSOETLConfig) -> Dict[str, Any]:
        """Convert config object to dictionary."""
        result = {}
        
        # Basic fields
        for field_name in ['environment', 'debug', 'api_host', 'api_port', 'api_key',
                          'temp_dir', 'data_dir', 'openai_api_key', 'gemini_api_key']:
            value = getattr(config, field_name)
            if value is not None:
                result[field_name] = value
        
        # Databases
        if config.databases:
            result['databases'] = {}
            for db_name, db_config in config.databases.items():
                result['databases'][db_name] = {
                    'type': db_config.type,
                    'host': db_config.host,
                    'port': db_config.port,
                    'database': db_config.database,
                    'username': db_config.username,
                    'password': db_config.password,
                    'additional_params': db_config.additional_params
                }
        
        # Agents
        if config.agents:
            result['agents'] = {}
            for agent_name, agent_config in config.agents.items():
                result['agents'][agent_name] = {
                    'enabled': agent_config.enabled,
                    'max_concurrent_tasks': agent_config.max_concurrent_tasks,
                    'timeout_seconds': agent_config.timeout_seconds,
                    'retry_attempts': agent_config.retry_attempts,
                    'custom_params': agent_config.custom_params
                }
        
        # Logging
        result['logging'] = {
            'level': config.logging.level,
            'structured': config.logging.structured,
            'file_path': config.logging.file_path,
            'max_file_size_mb': config.logging.max_file_size_mb,
            'backup_count': config.logging.backup_count
        }
        
        # Custom settings
        if config.custom:
            result['custom'] = config.custom
        
        return result


# Global config manager instance
_config_manager = ConfigManager()

def get_config() -> IMSOETLConfig:
    """Get the global configuration."""
    return _config_manager.get_config()

def load_config(config_path: Optional[Union[str, Path]] = None) -> IMSOETLConfig:
    """Load configuration from file."""
    return _config_manager.load_config(config_path)

def reload_config() -> IMSOETLConfig:
    """Reload configuration."""
    return _config_manager.load_config()
