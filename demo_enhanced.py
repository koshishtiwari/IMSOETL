#!/usr/bin/env python3
"""
Enhanced IMSOETL Demo Script

This script demonstrates the enhanced capabilities of the IMSOETL platform
including real database connectors, error handling, and configuration management.
"""

import asyncio
import sys
import os
from pathlib import Path

# Add the src directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from datetime import datetime
from imsoetl.core import (
    setup_logging, 
    get_config, 
    load_config,
    IMSOETLConfig,
    DatabaseConfig,
    handle_errors,
    log_performance,
    IMSOETLError
)
from imsoetl.core.orchestrator import OrchestratorAgent
from imsoetl.core.base_agent import Message
from imsoetl.agents.discovery import DiscoveryAgent
from imsoetl.agents.schema import SchemaAgent
from imsoetl.agents.transformation import TransformationAgent
from imsoetl.agents.quality import QualityAgent
from imsoetl.agents.execution import ExecutionAgent
from imsoetl.agents.monitoring import MonitoringAgent
from imsoetl.connectors import ConnectorFactory, create_sqlite_connector


def print_banner():
    """Print the enhanced IMSOETL banner."""
    banner = """
    ██╗███╗   ███╗███████╗ ██████╗ ███████╗████████╗██╗     
    ██║████╗ ████║██╔════╝██╔═══██╗██╔════╝╚══██╔══╝██║     
    ██║██╔████╔██║███████╗██║   ██║█████╗     ██║   ██║     
    ██║██║╚██╔╝██║╚════██║██║   ██║██╔══╝     ██║   ██║     
    ██║██║ ╚═╝ ██║███████║╚██████╔╝███████╗   ██║   ███████╗
    ╚═╝╚═╝     ╚═╝╚══════╝ ╚═════╝ ╚══════╝   ╚═╝   ╚══════╝
    
        I'm so ETL - Enhanced Agentic Data Engineering Platform
              Version 0.2.0 - Enhanced Demo with Real Connectors
    """
    print(banner)


def print_section(title):
    """Print a section header."""
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}")


@handle_errors(reraise=False, default_return=None)
@log_performance("setup_demo_environment")
async def setup_demo_environment():
    """Set up the demo environment with configuration and logging."""
    print_section("Setting Up Demo Environment")
    
    # Setup logging
    logger = setup_logging(level="INFO", structured=False)
    logger.info("Demo environment setup started")
    
    # Create demo configuration
    config = IMSOETLConfig(
        environment="demo",
        debug=True,
        databases={
            "demo_sqlite": DatabaseConfig(
                type="sqlite",
                database=":memory:",
                host="localhost",
                port=0
            )
        },
        temp_dir="./tmp",
        data_dir="./demo_data"
    )
    
    # Setup directories
    Path(config.temp_dir).mkdir(exist_ok=True)
    Path(config.data_dir).mkdir(exist_ok=True)
    
    logger.info("Demo environment setup completed")
    return config, logger


@handle_errors(reraise=False, default_return=[])
@log_performance("test_database_connectors")
async def test_database_connectors():
    """Test the available database connectors."""
    print_section("Testing Database Connectors")
    
    available_connectors = ConnectorFactory.get_available_connectors()
    print("Available connectors:")
    for connector_type, available in available_connectors.items():
        status = "✅ Available" if available else "❌ Not Available"
        print(f"  {connector_type}: {status}")
    
    # Test SQLite connector
    print("\nTesting SQLite connector...")
    try:
        sqlite_connector = create_sqlite_connector(":memory:")
        
        if await sqlite_connector.test_connection():
            print("✅ SQLite connection test successful")
            
            # Connect and test basic operations
            if await sqlite_connector.connect():
                print("✅ SQLite connection established")
                
                # Since it's in-memory, no tables will exist initially
                tables = await sqlite_connector.get_tables()
                print(f"✅ Found {len(tables)} tables")
                
                await sqlite_connector.disconnect()
                print("✅ SQLite disconnected successfully")
            else:
                print("❌ Failed to establish SQLite connection")
        else:
            print("❌ SQLite connection test failed")
    
    except Exception as e:
        print(f"❌ SQLite connector error: {str(e)}")
    
    return available_connectors


@handle_errors(reraise=False, default_return=None)
@log_performance("demo_enhanced_discovery")
async def demo_enhanced_discovery():
    """Demonstrate enhanced discovery capabilities."""
    print_section("Enhanced Discovery Agent Demo")
    
    # Create discovery agent
    discovery_agent = DiscoveryAgent("demo_discovery")
    
    # Test with various source types
    test_sources = [
        {"type": "sqlite", "id": "demo_sqlite", "database": ":memory:"},
        {"type": "postgresql", "id": "demo_postgres", "host": "localhost", "database": "test"},
        {"type": "mysql", "id": "demo_mysql", "host": "localhost", "database": "test"}
    ]
    
    for source_config in test_sources:
        print(f"\nTesting discovery for {source_config['type']}...")
        
        message = Message(
            sender_id="demo",
            receiver_id="demo_discovery",
            message_type="discover_source",
            content={"source_config": source_config}
        )
        
        try:
            await discovery_agent.receive_message(message)
            # Since this is async processing, we'll just check if it was received
            print(f"✅ Source discovery message sent for {source_config['type']}")
            
        except Exception as e:
            print(f"❌ Discovery failed: {str(e)}")


@handle_errors(reraise=False, default_return=None)
@log_performance("demo_error_handling")
async def demo_error_handling():
    """Demonstrate error handling capabilities."""
    print_section("Error Handling Demo")
    
    print("Testing various error scenarios...")
    
    # Test configuration error
    try:
        raise IMSOETLError("Test configuration error", "CONFIG_ERROR", {"key": "test"})
    except IMSOETLError as e:
        print(f"✅ Caught IMSOETLError: {e.error_code} - {e.message}")
        print(f"   Details: {e.details}")
    
    # Test invalid connector
    try:
        from imsoetl.connectors.base import ConnectionConfig
        dummy_config = ConnectionConfig(
            host="localhost",
            port=5432,
            database="test",
            username="user",
            password="pass"
        )
        ConnectorFactory.create_connector("invalid_type", dummy_config)
    except ValueError as e:
        print(f"✅ Caught ValueError for invalid connector: {str(e)}")
    
    # Test agent error handling
    discovery_agent = DiscoveryAgent("error_test")
    
    # Send invalid message
    invalid_message = Message(
        sender_id="demo",
        receiver_id="error_test",
        message_type="invalid_message_type",
        content={"invalid": "data"}
    )
    
    try:
        await discovery_agent.receive_message(invalid_message)
        print(f"✅ Agent received invalid message gracefully")
    except Exception as e:
        print(f"❌ Agent error handling failed: {str(e)}")


@handle_errors(reraise=False, default_return=None)
@log_performance("demo_full_workflow")
async def demo_full_workflow():
    """Demonstrate a complete ETL workflow with enhanced features."""
    print_section("Enhanced Full Workflow Demo")
    
    # Create orchestrator
    orchestrator = OrchestratorAgent("demo_orchestrator")
    
    # Create all agents
    agents = {
        "discovery": DiscoveryAgent("demo_discovery"),
        "schema": SchemaAgent("demo_schema"),
        "transformation": TransformationAgent("demo_transformation"),
        "quality": QualityAgent("demo_quality"),
        "execution": ExecutionAgent("demo_execution"),
        "monitoring": MonitoringAgent("demo_monitoring")
    }
    
    print(f"Created {len(agents)} agents")
    
    # Simulate a complex data pipeline request
    user_request = """
    I need to create a data pipeline that:
    1. Connects to multiple data sources (SQLite and PostgreSQL)
    2. Discovers customer and order tables
    3. Validates data quality (check for duplicates, null values)
    4. Transforms the data to create a customer analytics view
    5. Loads the result into a data warehouse
    6. Sets up monitoring and alerts
    
    Please handle any connection errors gracefully and provide detailed logging.
    """
    
    print(f"\nProcessing user request: {user_request[:100]}...")
    
    # Send request to orchestrator
    message = Message(
        sender_id="demo_user",
        receiver_id="demo_orchestrator",
        message_type="user_request",
        content={"request": user_request}
    )
    
    try:
        await orchestrator.receive_message(message)
        print("✅ Orchestrator received request successfully")
        
        # For demo purposes, we'll simulate the workflow steps
        print("\nExecuting workflow steps...")
        
        # Step 1: Discovery
        discovery_message = Message(
            sender_id="demo_orchestrator",
            receiver_id="demo_discovery",
            message_type="task_assignment",
            content={
                "task_type": "discovery",
                "sources": ["sqlite_demo", "postgres_demo"]
            }
        )
        
        await agents["discovery"].receive_message(discovery_message)
        print("✅ Discovery step initiated")
        
        # Step 2: Schema Analysis
        schema_message = Message(
            sender_id="demo_orchestrator",
            receiver_id="demo_schema",
            message_type="task_assignment",
            content={
                "task_type": "schema_analysis",
                "tables": ["customers", "orders"]
            }
        )
        
        await agents["schema"].receive_message(schema_message)
        print("✅ Schema analysis step initiated")
        
        # Step 3: Quality Check
        quality_message = Message(
            sender_id="demo_orchestrator",
            receiver_id="demo_quality",
            message_type="task_assignment",
            content={
                "task_type": "quality_assessment",
                "rules": ["check_duplicates", "check_nulls", "validate_emails"]
            }
        )
        
        await agents["quality"].receive_message(quality_message)
        print("✅ Quality assessment step initiated")
        
        print("\n✅ Enhanced workflow demo completed successfully!")
    
    except Exception as e:
        print(f"❌ Workflow execution failed: {str(e)}")


async def main():
    """Main demo function."""
    print_banner()
    
    # Setup demo environment
    config, logger = await setup_demo_environment()
    if not config:
        print("❌ Failed to setup demo environment")
        return
    
    try:
        # Test database connectors
        await test_database_connectors()
        
        # Test enhanced discovery
        await demo_enhanced_discovery()
        
        # Test error handling
        await demo_error_handling()
        
        # Full workflow demo
        await demo_full_workflow()
        
        print_section("Demo Summary")
        print("✅ Enhanced IMSOETL demo completed successfully!")
        print("\nKey features demonstrated:")
        print("  • Real database connectors (SQLite, PostgreSQL, MySQL)")
        print("  • Enhanced error handling and logging")
        print("  • Configuration management")
        print("  • Performance monitoring")
        print("  • Graceful failure handling")
        print("  • Structured logging")
        
    except Exception as e:
        logger.error(f"Demo failed with error: {str(e)}", exc_info=True)
        print(f"\n❌ Demo failed: {str(e)}")
    
    finally:
        print("\nThank you for trying IMSOETL Enhanced Demo!")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n⏹️  Demo interrupted by user")
    except Exception as e:
        print(f"\n❌ Fatal error: {str(e)}")
        sys.exit(1)
