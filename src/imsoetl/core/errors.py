"""Enhanced error handling and logging utilities."""

import logging
import sys
import traceback
from functools import wraps
from typing import Any, Callable, Dict, Optional, Union
from datetime import datetime, timezone
import json


class IMSOETLError(Exception):
    """Base exception class for IMSOETL."""
    
    def __init__(self, message: str, error_code: Optional[str] = None, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.error_code = error_code or "GENERAL_ERROR"
        self.details = details or {}
        self.timestamp = datetime.now(timezone.utc)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert error to dictionary for logging/serialization."""
        return {
            "error_type": self.__class__.__name__,
            "error_code": self.error_code,
            "message": self.message,
            "details": self.details,
            "timestamp": self.timestamp.isoformat()
        }


class ConnectionError(IMSOETLError):
    """Raised when database connection fails."""
    
    def __init__(self, message: str, host: Optional[str] = None, port: Optional[int] = None, database: Optional[str] = None):
        super().__init__(message, "CONNECTION_ERROR")
        if host:
            self.details.update({
                "host": host,
                "port": port,
                "database": database
            })


class ConfigurationError(IMSOETLError):
    """Raised when configuration is invalid."""
    
    def __init__(self, message: str, config_key: Optional[str] = None):
        super().__init__(message, "CONFIGURATION_ERROR")
        if config_key:
            self.details["config_key"] = config_key


class ValidationError(IMSOETLError):
    """Raised when data validation fails."""
    
    def __init__(self, message: str, validation_rules: Optional[Dict[str, Any]] = None):
        super().__init__(message, "VALIDATION_ERROR")
        if validation_rules:
            self.details["validation_rules"] = validation_rules


class AgentError(IMSOETLError):
    """Raised when agent operations fail."""
    
    def __init__(self, message: str, agent_id: Optional[str] = None, task_id: Optional[str] = None):
        super().__init__(message, "AGENT_ERROR")
        if agent_id:
            self.details["agent_id"] = agent_id
        if task_id:
            self.details["task_id"] = task_id


class PipelineError(IMSOETLError):
    """Raised when pipeline execution fails."""
    
    def __init__(self, message: str, pipeline_id: Optional[str] = None, step: Optional[str] = None):
        super().__init__(message, "PIPELINE_ERROR")
        if pipeline_id:
            self.details["pipeline_id"] = pipeline_id
        if step:
            self.details["step"] = step


def setup_logging(
    level: str = "INFO",
    log_file: Optional[str] = None,
    structured: bool = True
) -> logging.Logger:
    """Set up enhanced logging configuration."""
    
    # Create logger
    logger = logging.getLogger("imsoetl")
    logger.setLevel(getattr(logging, level.upper()))
    
    # Remove existing handlers
    logger.handlers.clear()
    
    # Create formatter
    if structured:
        formatter = StructuredFormatter()
    else:
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, level.upper()))
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler if specified
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(getattr(logging, level.upper()))
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


class StructuredFormatter(logging.Formatter):
    """Custom formatter for structured logging."""
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as structured JSON."""
        log_data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno
        }
        
        # Add exception info if present
        if record.exc_info and record.exc_info[0]:
            log_data["exception"] = {
                "type": record.exc_info[0].__name__,
                "message": str(record.exc_info[1]),
                "traceback": traceback.format_exception(*record.exc_info)
            }
        
        # Add extra fields from record
        for key, value in record.__dict__.items():
            if key not in {'name', 'msg', 'args', 'levelname', 'levelno', 'pathname', 
                          'filename', 'module', 'exc_info', 'exc_text', 'stack_info',
                          'lineno', 'funcName', 'created', 'msecs', 'relativeCreated',
                          'thread', 'threadName', 'processName', 'process', 'getMessage',
                          'message'}:
                log_data[key] = value
        
        return json.dumps(log_data, default=str)


def handle_errors(
    reraise: bool = True,
    log_errors: bool = True,
    default_return: Any = None
):
    """Decorator for handling and logging errors."""
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                if log_errors:
                    logger = logging.getLogger(func.__module__)
                    logger.error(
                        f"Error in {func.__name__}: {str(e)}",
                        exc_info=True,
                        extra={
                            "function": func.__name__,
                            "args": str(args)[:200],  # Truncate for logging
                            "kwargs": str(kwargs)[:200]
                        }
                    )
                
                if reraise:
                    raise
                return default_return
        
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if log_errors:
                    logger = logging.getLogger(func.__module__)
                    logger.error(
                        f"Error in {func.__name__}: {str(e)}",
                        exc_info=True,
                        extra={
                            "function": func.__name__,
                            "args": str(args)[:200],
                            "kwargs": str(kwargs)[:200]
                        }
                    )
                
                if reraise:
                    raise
                return default_return
        
        # Return appropriate wrapper based on function type
        if hasattr(func, '__code__') and func.__code__.co_flags & 0x80:  # CO_COROUTINE
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator


def log_performance(operation_name: Optional[str] = None):
    """Decorator for logging function performance."""
    
    def decorator(func: Callable) -> Callable:
        op_name = operation_name or f"{func.__module__}.{func.__name__}"
        
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            start_time = datetime.now(timezone.utc)
            logger = logging.getLogger(func.__module__)
            
            logger.info(f"Starting operation: {op_name}")
            
            try:
                result = await func(*args, **kwargs)
                end_time = datetime.now(timezone.utc)
                duration = (end_time - start_time).total_seconds()
                
                logger.info(
                    f"Completed operation: {op_name}",
                    extra={
                        "operation": op_name,
                        "duration_seconds": duration,
                        "status": "success"
                    }
                )
                
                return result
            
            except Exception as e:
                end_time = datetime.now(timezone.utc)
                duration = (end_time - start_time).total_seconds()
                
                logger.error(
                    f"Failed operation: {op_name}",
                    extra={
                        "operation": op_name,
                        "duration_seconds": duration,
                        "status": "failed",
                        "error": str(e)
                    }
                )
                raise
        
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            start_time = datetime.now(timezone.utc)
            logger = logging.getLogger(func.__module__)
            
            logger.info(f"Starting operation: {op_name}")
            
            try:
                result = func(*args, **kwargs)
                end_time = datetime.now(timezone.utc)
                duration = (end_time - start_time).total_seconds()
                
                logger.info(
                    f"Completed operation: {op_name}",
                    extra={
                        "operation": op_name,
                        "duration_seconds": duration,
                        "status": "success"
                    }
                )
                
                return result
            
            except Exception as e:
                end_time = datetime.now(timezone.utc)
                duration = (end_time - start_time).total_seconds()
                
                logger.error(
                    f"Failed operation: {op_name}",
                    extra={
                        "operation": op_name,
                        "duration_seconds": duration,
                        "status": "failed",
                        "error": str(e)
                    }
                )
                raise
        
        # Return appropriate wrapper
        if hasattr(func, '__code__') and func.__code__.co_flags & 0x80:
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator
