"""
IMSOETL: I'm so ETL - An agentic data engineering platform using Generative AI

This package provides a comprehensive platform for autonomous data pipeline
creation, execution, and maintenance through AI agents and natural language interfaces.

Key Features:
- AI-powered natural language intent parsing
- Multi-engine execution (Pandas, DuckDB, Spark)
- Intelligent agent orchestration
- Local-first AI with cloud fallback
- Production-ready data pipeline automation
"""

from ._version import __version__

__author__ = "IMSOETL Team"
__email__ = "team@imsoetl.com"

# Import core components with error handling
try:
    from .core.orchestrator import OrchestratorAgent
    from .core.base_agent import BaseAgent
    from .agents.execution import ExecutionAgent
    from .agents.discovery import DiscoveryAgent
    from .agents.schema import SchemaAgent
    from .agents.transformation import TransformationAgent
    from .agents.quality import QualityAgent
    from .agents.monitoring import MonitoringAgent
    from .engines.manager import ExecutionEngineManager
    from .llm.manager import LLMManager
    
    __all__ = [
        "__version__",
        "OrchestratorAgent",
        "BaseAgent",
        "ExecutionAgent",
        "DiscoveryAgent", 
        "SchemaAgent",
        "TransformationAgent",
        "QualityAgent",
        "MonitoringAgent",
        "ExecutionEngineManager",
        "LLMManager",
    ]
except ImportError as e:
    # Handle import errors gracefully during development
    import logging
    logging.warning(f"Some imports failed during initialization: {e}")
    
    __all__ = ["__version__"]
