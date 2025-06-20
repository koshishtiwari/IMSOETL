"""
IMSOETL: I'm so ETL - An agentic data engineering platform using Generative AI

This package provides a comprehensive platform for autonomous data pipeline
creation, execution, and maintenance through AI agents and natural language interfaces.
"""

__version__ = "0.1.0"
__author__ = "IMSOETL Team"
__email__ = "team@imsoetl.com"

# Import core components with error handling
try:
    from .core.orchestrator import OrchestratorAgent
    from .core.base_agent import BaseAgent
    
    __all__ = [
        "OrchestratorAgent",
        "BaseAgent",
    ]
except ImportError as e:
    # Handle import errors gracefully during development
    import logging
    logging.warning(f"Some imports failed during initialization: {e}")
    
    __all__ = []
