"""
Command Line Interface for IMSOETL.

This module provides a CLI for interacting with the IMSOETL platform.
"""

import asyncio
import logging
import sys
from pathlib import Path
from typing import Optional

try:
    import typer
    from rich.console import Console
    from rich.logging import RichHandler
    from rich.table import Table
    from rich.panel import Panel
    from rich.text import Text
except ImportError:
    print("CLI dependencies not installed. Please install with: pip install imsoetl[dev]")
    sys.exit(1)

from .core.orchestrator import OrchestratorAgent
from .core.base_agent import AgentContext

app = typer.Typer(
    name="imsoetl",
    help="IMSOETL: I'm so ETL - An agentic data engineering platform",
    add_completion=False
)
console = Console()


def setup_logging(verbose: bool = False) -> None:
    """Set up logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    
    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=console, rich_tracebacks=True)]
    )


@app.command()
def interactive(
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose logging")
) -> None:
    """Start interactive mode for natural language ETL requests."""
    setup_logging(verbose)
    
    console.print(Panel.fit(
        "[bold blue]IMSOETL Interactive Mode[/bold blue]\n"
        "Enter your ETL requests in natural language.\n"
        "Type 'quit' or 'exit' to leave.",
        border_style="blue"
    ))
    
    asyncio.run(interactive_session())


async def interactive_session() -> None:
    """Run the interactive session."""
    # Initialize orchestrator
    orchestrator = OrchestratorAgent()
    await orchestrator.start()
    
    console.print("[green]✓[/green] Orchestrator started successfully")
    console.print("[dim]Example: 'Move last 30 days of customer orders from MySQL to Snowflake'[/dim]\n")
    
    try:
        while True:
            try:
                # Get user input
                user_input = console.input("[bold cyan]ETL Request[/bold cyan] > ")
                
                if user_input.lower().strip() in ['quit', 'exit', 'q']:
                    break
                
                if not user_input.strip():
                    continue
                
                # Process the request
                console.print(f"[dim]Processing: {user_input}[/dim]")
                
                # Create a mock message to simulate user input
                from .core.base_agent import Message
                import uuid
                
                user_message = Message(
                    sender_id="cli_user",
                    receiver_id=orchestrator.agent_id,
                    message_type="user_intent",
                    content={
                        "intent": user_input,
                        "session_id": f"cli_session_{uuid.uuid4().hex[:8]}"
                    }
                )
                
                await orchestrator.receive_message(user_message)
                
                # Give the orchestrator time to process
                await asyncio.sleep(2)
                
                # Show session status
                sessions = orchestrator.get_all_sessions()
                if sessions:
                    latest_session = sessions[-1]
                    show_session_status(latest_session)
                
            except KeyboardInterrupt:
                console.print("\n[yellow]Interrupted[/yellow]")
                break
            except EOFError:
                break
            except Exception as e:
                console.print(f"[red]Error: {e}[/red]")
    
    finally:
        console.print("\n[yellow]Shutting down...[/yellow]")
        await orchestrator.stop()
        console.print("[green]✓[/green] Goodbye!")


def show_session_status(session: dict) -> None:
    """Display session status in a nice format."""
    table = Table(title=f"Session Status: {session['session_id']}")
    table.add_column("Property", style="cyan", no_wrap=True)
    table.add_column("Value", style="white")
    
    table.add_row("Current Phase", session.get('current_phase', 'N/A'))
    table.add_row("Progress", f"{session.get('progress', 0):.1f}%")
    table.add_row("User Intent", session.get('user_intent', 'N/A'))
    table.add_row("Created", str(session.get('created_at', 'N/A')))
    
    console.print(table)


@app.command()
def status(
    session_id: Optional[str] = typer.Option(None, "--session", "-s", help="Session ID to check")
) -> None:
    """Check the status of IMSOETL sessions."""
    console.print("[yellow]Status checking not yet implemented in standalone mode[/yellow]")
    console.print("Use interactive mode to see real-time status updates.")


@app.command()
def validate(
    query: str = typer.Argument(..., help="ETL query to validate")
) -> None:
    """Validate an ETL query without executing it."""
    from .core.orchestrator import IntentParser
    
    console.print(f"[dim]Validating query: {query}[/dim]\n")
    
    # Parse the intent
    parser = IntentParser()
    parsed_intent = parser.parse_intent(query)
    
    # Display parsing results
    table = Table(title="Query Analysis")
    table.add_column("Aspect", style="cyan")
    table.add_column("Details", style="white")
    
    table.add_row("Original Query", parsed_intent['original_text'])
    table.add_row("Complexity", parsed_intent['complexity'])
    table.add_row("Operations Found", str(len(parsed_intent['operations'])))
    
    if parsed_intent['operations']:
        ops = [op['type'] for op in parsed_intent['operations']]
        table.add_row("Operation Types", ", ".join(ops))
    
    if parsed_intent['entities']['sources']:
        table.add_row("Sources", ", ".join(parsed_intent['entities']['sources']))
    
    if parsed_intent['entities']['columns']:
        table.add_row("Columns", ", ".join(parsed_intent['entities']['columns']))
    
    console.print(table)
    
    # Show operations detail
    if parsed_intent['operations']:
        console.print("\n[bold]Detected Operations:[/bold]")
        for i, op in enumerate(parsed_intent['operations'], 1):
            console.print(f"  {i}. [cyan]{op['type'].title()}[/cyan]: {op['pattern']}")


@app.command()
def version() -> None:
    """Show IMSOETL version information."""
    from . import __version__, __author__
    
    version_text = Text()
    version_text.append("IMSOETL ", style="bold blue")
    version_text.append(f"v{__version__}", style="bold white")
    version_text.append(f"\nBy {__author__}", style="dim")
    
    console.print(Panel.fit(version_text, border_style="blue"))


@app.command()
def init(
    project_name: str = typer.Argument(..., help="Name of the project to initialize"),
    path: Optional[Path] = typer.Option(None, "--path", "-p", help="Path to initialize project")
) -> None:
    """Initialize a new IMSOETL project."""
    if path is None:
        path = Path.cwd() / project_name
    
    path.mkdir(exist_ok=True)
    
    # Create basic project structure
    config_dir = path / "config"
    config_dir.mkdir(exist_ok=True)
    
    # Create sample configuration
    config_file = config_dir / "imsoetl.yaml"
    config_content = f"""# IMSOETL Project Configuration
project_name: {project_name}
version: "1.0.0"

# Agent Configuration
agents:
  orchestrator:
    enabled: true
  discovery:
    enabled: true
  schema:
    enabled: true
  transformation:
    enabled: true
  quality:
    enabled: true
  execution:
    enabled: true
  monitoring:
    enabled: true

# Data Sources
sources:
  # Example MySQL connection
  mysql_main:
    type: mysql
    host: localhost
    port: 3306
    database: your_database
    # credentials should be in environment variables
    
  # Example Snowflake connection
  snowflake_warehouse:
    type: snowflake
    account: your_account
    warehouse: your_warehouse
    database: your_database
    schema: your_schema

# Logging
logging:
  level: INFO
  format: structured
"""
    
    config_file.write_text(config_content)
    
    # Create sample pipeline
    pipelines_dir = path / "pipelines"
    pipelines_dir.mkdir(exist_ok=True)
    
    sample_pipeline = pipelines_dir / "sample.yaml"
    sample_content = """# Sample IMSOETL Pipeline
pipeline_name: sample_etl
description: "Sample pipeline demonstrating IMSOETL capabilities"

# Natural language description
intent: "Move customer data from MySQL to Snowflake and clean phone numbers"

# Source and target definitions
source:
  connection: mysql_main
  table: customers
  
target:
  connection: snowflake_warehouse
  table: clean_customers

# Transformations (will be auto-generated by agents)
transformations:
  - type: phone_cleanup
    column: phone_number
  - type: deduplication
    key_columns: [email, customer_id]
"""
    
    sample_pipeline.write_text(sample_content)
    
    console.print(f"[green]✓[/green] Initialized IMSOETL project at {path}")
    console.print(f"[dim]  - Created configuration: {config_file}[/dim]")
    console.print(f"[dim]  - Created sample pipeline: {sample_pipeline}[/dim]")
    console.print("\n[yellow]Next steps:[/yellow]")
    console.print("1. Edit the configuration file with your data source details")
    console.print("2. Set up environment variables for credentials")
    console.print("3. Run 'imsoetl interactive' to start using the platform")


@app.command()
def agents(
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose logging")
) -> None:
    """Show available agents and their status."""
    setup_logging(verbose)
    
    console.print("[bold blue]IMSOETL Agents[/bold blue]")
    console.print()
    
    # Create a table of agents
    table = Table(title="Available Agents")
    table.add_column("Agent", style="cyan", no_wrap=True)
    table.add_column("Type", style="magenta")
    table.add_column("Description", style="green")
    table.add_column("Status", style="yellow")
    
    agents_info = [
        ("OrchestratorAgent", "orchestrator", "Master coordinator for all agents", "Available"),
        ("DiscoveryAgent", "discovery", "Data source discovery and analysis", "Available"),
        ("SchemaAgent", "schema", "Schema analysis and mapping", "Available"),
        ("TransformationAgent", "transformation", "Data transformation operations", "Available"),
        ("QualityAgent", "quality", "Data quality assessment and validation", "Available"),
        ("ExecutionAgent", "execution", "Pipeline execution and management", "Available"),
        ("MonitoringAgent", "monitoring", "System monitoring and observability", "Available"),
    ]
    
    for name, agent_type, description, status in agents_info:
        table.add_row(name, agent_type, description, status)
    
    console.print(table)
    console.print()
    console.print("[dim]Use 'imsoetl test-agent <agent_type>' to test individual agents[/dim]")


@app.command()
def test_agent(
    agent_type: str = typer.Argument(..., help="Agent type to test (e.g., discovery, schema, transformation)"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose logging")
) -> None:
    """Test individual agents."""
    setup_logging(verbose)
    
    async def run_agent_test():
        """Run the agent test."""
        agent_type_lower = agent_type.lower()
        
        console.print(f"[bold blue]Testing {agent_type} Agent[/bold blue]")
        console.print()
        
        try:
            if agent_type_lower == "discovery":
                from .agents.discovery import DiscoveryAgent
                agent = DiscoveryAgent()
                await agent.start()
                console.print("✓ Discovery agent started successfully")
                
                # Test discovery with a simple task
                task = {
                    "type": "discover_sources",
                    "source_configs": [{"type": "mysql", "id": "test_mysql"}]
                }
                result = await agent.process_task(task)
                console.print(f"✓ Discovery test completed: {result.get('success', False)}")
                
                await agent.stop()
                console.print("✓ Discovery agent stopped")
                
            elif agent_type_lower == "schema":
                from .agents.schema import SchemaAgent
                agent = SchemaAgent()
                await agent.start()
                console.print("✓ Schema agent started successfully")
                
                # Test schema parsing
                mock_table = {
                    "table_name": "test_table",
                    "columns": [
                        {"name": "id", "type": "INTEGER", "nullable": False},
                        {"name": "name", "type": "VARCHAR(50)", "nullable": True}
                    ]
                }
                schema = await agent._parse_table_schema(mock_table)
                console.print(f"✓ Parsed schema with {len(schema.columns)} columns")
                
                await agent.stop()
                console.print("✓ Schema agent stopped")
                
            elif agent_type_lower == "transformation":
                from .agents.transformation import TransformationAgent
                agent = TransformationAgent()
                await agent.initialize()
                console.print("✓ Transformation agent initialized successfully")
                
                # Test transformation
                task = {
                    "type": "generate_transformation",
                    "requirements": {"operations": [{"type": "select", "source_columns": ["id"]}]},
                    "source_schema": {"columns": {"id": {"type": "INTEGER"}}},
                    "target_schema": {"columns": {"id": {"type": "INTEGER"}}}
                }
                result = await agent.process_task(task)
                console.print(f"✓ Generated transformation: {result.get('success', False)}")
                
            elif agent_type_lower == "quality":
                from .agents.quality import QualityAgent
                agent = QualityAgent()
                await agent.initialize()
                console.print("✓ Quality agent initialized successfully")
                
                # Test quality assessment
                task = {
                    "type": "assess_quality",
                    "dataset": {"id": "test", "schema": {}},
                    "rules": ["default_completeness"]
                }
                result = await agent.process_task(task)
                console.print(f"✓ Quality assessment: {result.get('success', False)}")
                
            elif agent_type_lower == "execution":
                from .agents.execution import ExecutionAgent
                agent = ExecutionAgent()
                await agent.initialize()
                console.print("✓ Execution agent initialized successfully")
                
                # Test execution
                task = {
                    "type": "get_status",
                    "execution_id": None
                }
                result = await agent.process_task(task)
                console.print(f"✓ Status check: {result.get('success', False)}")
                
            elif agent_type_lower == "monitoring":
                from .agents.monitoring import MonitoringAgent
                agent = MonitoringAgent()
                await agent.initialize()
                console.print("✓ Monitoring agent initialized successfully")
                
                # Test monitoring
                task = {
                    "type": "collect_metric",
                    "metric_name": "test_metric",
                    "metric_value": 42.0
                }
                result = await agent.process_task(task)
                console.print(f"✓ Metric collection: {result.get('success', False)}")
                
                await agent.stop_monitoring()
                console.print("✓ Monitoring stopped")
                
            else:
                console.print(f"[red]Unknown agent type: {agent_type}[/red]")
                console.print("Available types: discovery, schema, transformation, quality, execution, monitoring")
                return False
                
            console.print(f"[green]✓ {agent_type} agent test completed successfully[/green]")
            return True
            
        except Exception as e:
            console.print(f"[red]✗ Agent test failed: {e}[/red]")
            return False
    
    success = asyncio.run(run_agent_test())
    if not success:
        raise typer.Exit(1)


@app.command()
def demo(
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose logging"),
    quick: bool = typer.Option(False, "--quick", "-q", help="Run quick demo without detailed output")
) -> None:
    """Run the IMSOETL demo."""
    setup_logging(verbose)
    
    async def run_demo():
        """Run the demo."""
        if not quick:
            console.print("[bold cyan]IMSOETL Demo[/bold cyan]")
            console.print("🚀 Starting comprehensive demo of all agents...")
            console.print()
        
        try:
            # Import all agents
            from .core.orchestrator import OrchestratorAgent, IntentParser, TaskPlanner
            from .agents.discovery import DiscoveryAgent
            from .agents.schema import SchemaAgent
            from .agents.transformation import TransformationAgent
            from .agents.quality import QualityAgent
            from .agents.execution import ExecutionAgent
            from .agents.monitoring import MonitoringAgent
            
            if not quick:
                console.print("1. Testing Intent Parsing...")
            parser = IntentParser()
            intent = parser.parse_intent("Move customer data from MySQL to Snowflake")
            console.print(f"   ✓ Parsed intent with {len(intent['operations'])} operations")
            
            if not quick:
                console.print("2. Testing Task Planning...")
            planner = TaskPlanner()
            plan = planner.create_execution_plan(intent)
            console.print(f"   ✓ Created plan with {len(plan['phases'])} phases")
            
            if not quick:
                console.print("3. Testing Agents...")
            
            # Test each agent quickly
            agents = [
                ("Discovery", DiscoveryAgent()),
                ("Schema", SchemaAgent()),
                ("Transformation", TransformationAgent()),
                ("Quality", QualityAgent()),
                ("Execution", ExecutionAgent()),
                ("Monitoring", MonitoringAgent())
            ]
            
            for name, agent in agents:
                if hasattr(agent, 'start'):
                    await agent.start()
                else:
                    await agent.initialize()
                console.print(f"   ✓ {name} agent initialized")
                
                if hasattr(agent, 'stop'):
                    await agent.stop()
                elif hasattr(agent, 'stop_monitoring'):
                    await agent.stop_monitoring()
            
            console.print()
            console.print("[green]🎉 Demo completed successfully![/green]")
            console.print("[dim]For a full demo, run the demo.py script directly[/dim]")
            return True
            
        except Exception as e:
            console.print(f"[red]✗ Demo failed: {e}[/red]")
            if verbose:
                import traceback
                console.print(traceback.format_exc())
            return False
    
    success = asyncio.run(run_demo())
    if not success:
        raise typer.Exit(1)


@app.command()
def pipeline(
    action: str = typer.Argument(..., help="Action: create, run, status, list"),
    name: Optional[str] = typer.Option(None, "--name", "-n", help="Pipeline name"),
    file: Optional[Path] = typer.Option(None, "--file", "-f", help="Pipeline definition file"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose logging")
) -> None:
    """Manage data pipelines."""
    setup_logging(verbose)
    
    async def run_pipeline_action():
        """Run the pipeline action."""
        try:
            if action == "create":
                if not name:
                    console.print("[red]Pipeline name is required for creation[/red]")
                    return False
                    
                console.print(f"[blue]Creating pipeline: {name}[/blue]")
                
                # Create a sample pipeline definition
                pipeline_def = {
                    "name": name,
                    "description": f"Auto-generated pipeline: {name}",
                    "source": {"type": "placeholder", "config": {}},
                    "target": {"type": "placeholder", "config": {}},
                    "transformations": [],
                    "quality_rules": [],
                    "schedule": "manual"
                }
                
                if file:
                    import json
                    file.write_text(json.dumps(pipeline_def, indent=2))
                    console.print(f"✓ Pipeline definition saved to {file}")
                else:
                    console.print("✓ Pipeline created (use --file to save definition)")
                    
            elif action == "list":
                console.print("[blue]Available Pipelines:[/blue]")
                console.print("(Pipeline management coming soon)")
                
            elif action == "status":
                if name:
                    console.print(f"[blue]Pipeline Status: {name}[/blue]")
                else:
                    console.print("[blue]All Pipeline Status:[/blue]")
                console.print("(Pipeline status monitoring coming soon)")
                
            elif action == "run":
                if not name:
                    console.print("[red]Pipeline name is required for execution[/red]")
                    return False
                    
                console.print(f"[blue]Running pipeline: {name}[/blue]")
                console.print("(Pipeline execution coming soon)")
                
            else:
                console.print(f"[red]Unknown action: {action}[/red]")
                console.print("Available actions: create, run, status, list")
                return False
                
            return True
            
        except Exception as e:
            console.print(f"[red]Pipeline action failed: {e}[/red]")
            return False
    
    success = asyncio.run(run_pipeline_action())
    if not success:
        raise typer.Exit(1)


@app.command()
def connectors(
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose logging")
) -> None:
    """List available database connectors and their status."""
    setup_logging(verbose)
    
    from .connectors import ConnectorFactory
    
    console.print("\n[bold cyan]IMSOETL Database Connectors[/bold cyan]")
    console.print("=" * 50)
    
    available_connectors = ConnectorFactory.get_available_connectors()
    
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Connector Type", style="cyan")
    table.add_column("Status", style="green")
    table.add_column("Description")
    
    descriptions = {
        "sqlite": "Lightweight file-based database",
        "postgresql": "Advanced open-source relational database",
        "mysql": "Popular open-source relational database"
    }
    
    for connector_type, available in available_connectors.items():
        status = "✅ Available" if available else "❌ Not Available"
        description = descriptions.get(connector_type, "Database connector")
        
        if not available:
            description += " (Install required dependencies)"
        
        table.add_row(connector_type.title(), status, description)
    
    console.print(table)
    console.print(f"\n[dim]Total connectors: {len(available_connectors)}[/dim]")


@app.command()
def test_connection(
    connector_type: str = typer.Argument(..., help="Database connector type (sqlite, postgresql, mysql)"),
    host: str = typer.Option("localhost", "--host", "-h", help="Database host"),
    port: int = typer.Option(None, "--port", "-p", help="Database port"),
    database: str = typer.Option("", "--database", "-d", help="Database name"),
    username: str = typer.Option("", "--username", "-u", help="Database username"),
    password: str = typer.Option("", "--password", "-P", help="Database password", hide_input=True),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose logging")
) -> None:
    """Test connection to a database."""
    setup_logging(verbose)
    
    async def _test_connection():
        from .connectors import ConnectorFactory, ConnectionConfig
        from .core.errors import ConnectionError as IMSOConnectionError
        
        console.print(f"\n[bold cyan]Testing {connector_type.title()} Connection[/bold cyan]")
        console.print("=" * 50)
        
        try:
            # Create connection config
            config = ConnectorFactory.create_config(
                connector_type=connector_type,
                host=host,
                port=port,
                database=database,
                username=username,
                password=password
            )
            
            # Create connector
            connector = ConnectorFactory.create_connector(connector_type, config)
            
            console.print(f"[yellow]Connecting to {connector}...[/yellow]")
            
            # Test connection
            if await connector.test_connection():
                console.print("[green]✅ Connection successful![/green]")
                
                # Try to get additional info
                if await connector.connect():
                    try:
                        tables = await connector.get_tables()
                        console.print(f"[green]📊 Found {len(tables)} tables[/green]")
                        
                        if tables and len(tables) <= 10:
                            console.print("[dim]Tables:[/dim]")
                            for table in tables[:10]:
                                console.print(f"  • {table}")
                    
                    except Exception as e:
                        console.print(f"[yellow]⚠️  Could not list tables: {str(e)}[/yellow]")
                    
                    finally:
                        await connector.disconnect()
                        console.print("[dim]Connection closed.[/dim]")
            else:
                console.print("[red]❌ Connection failed![/red]")
        
        except ImportError as e:
            console.print(f"[red]❌ Connector not available: {str(e)}[/red]")
            console.print("[dim]Install required dependencies with: pip install <connector-package>[/dim]")
        
        except Exception as e:
            console.print(f"[red]❌ Connection error: {str(e)}[/red]")
    
    asyncio.run(_test_connection())


@app.command() 
def discover(
    connector_type: str = typer.Argument(..., help="Database connector type"),
    host: str = typer.Option("localhost", "--host", "-h", help="Database host"),
    port: int = typer.Option(None, "--port", "-p", help="Database port"),
    database: str = typer.Option("", "--database", "-d", help="Database name"),
    username: str = typer.Option("", "--username", "-u", help="Database username"),
    password: str = typer.Option("", "--password", "-P", help="Database password", hide_input=True),
    output_file: Optional[str] = typer.Option(None, "--output", "-o", help="Save results to file"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose logging")
) -> None:
    """Discover and analyze database schema."""
    setup_logging(verbose)
    
    async def _discover():
        from .agents.discovery import DiscoveryAgent, DatabaseSourceConnector
        from .connectors import ConnectorFactory
        import json
        
        console.print(f"\n[bold cyan]Discovering {connector_type.title()} Database Schema[/bold cyan]")
        console.print("=" * 60)
        
        try:
            # Create source config
            source_config = {
                "type": connector_type,
                "id": f"{connector_type}_discovery",
                "host": host,
                "port": port or ConnectorFactory.create_config(connector_type).port,
                "database": database,
                "username": username,
                "password": password
            }
            
            console.print(f"[yellow]Connecting to {host}:{source_config['port']}/{database}...[/yellow]")
            
            # Run discovery
            source_info = await DatabaseSourceConnector.connect_and_discover(source_config)
            
            if source_info.status == "connected":
                console.print(f"[green]✅ Discovery successful![/green]")
                console.print(f"[green]📊 Found {len(source_info.tables)} tables[/green]")
                
                # Display tables
                if source_info.tables:
                    table = Table(show_header=True, header_style="bold magenta")
                    table.add_column("Table Name", style="cyan")
                    table.add_column("Type", style="green")
                    
                    for table_name in source_info.tables:
                        table.add_row(table_name, "Table")
                    
                    console.print(table)
                
                # Save to file if requested
                if output_file:
                    output_data = source_info.to_dict()
                    with open(output_file, 'w') as f:
                        json.dump(output_data, f, indent=2, default=str)
                    console.print(f"[green]💾 Results saved to {output_file}[/green]")
            
            else:
                console.print(f"[red]❌ Discovery failed: {source_info.error_message}[/red]")
        
        except Exception as e:
            console.print(f"[red]❌ Discovery error: {str(e)}[/red]")
    
    asyncio.run(_discover())


@app.command()
def config_template(
    output_file: str = typer.Option("imsoetl-config.yaml", "--output", "-o", help="Output file name")
) -> None:
    """Generate a configuration template file."""
    from .core.config import IMSOETLConfig, DatabaseConfig, AgentConfig, LoggingConfig
    import yaml
    
    # Create sample configuration
    sample_config = {
        "environment": "development",
        "debug": True,
        "databases": {
            "primary_db": {
                "type": "postgresql",
                "host": "localhost",
                "port": 5432,
                "database": "your_database",
                "username": "your_username",
                "password": "your_password"
            },
            "analytics_db": {
                "type": "sqlite",
                "database": "./data/analytics.db"
            }
        },
        "agents": {
            "discovery": {
                "enabled": True,
                "max_concurrent_tasks": 5,
                "timeout_seconds": 300
            },
            "transformation": {
                "enabled": True,
                "max_concurrent_tasks": 3,
                "custom_params": {
                    "optimization_level": "high"
                }
            }
        },
        "logging": {
            "level": "INFO",
            "structured": True,
            "file_path": "./logs/imsoetl.log"
        },
        "api_host": "localhost",
        "api_port": 8000,
        "temp_dir": "./tmp",
        "data_dir": "./data"
    }
    
    try:
        with open(output_file, 'w') as f:
            yaml.dump(sample_config, f, default_flow_style=False, indent=2)
        
        console.print(f"[green]✅ Configuration template saved to {output_file}[/green]")
        console.print("\n[bold cyan]Next steps:[/bold cyan]")
        console.print("1. Edit the configuration file with your database details")
        console.print("2. Set environment variables for sensitive data")
        console.print("3. Run: imsoetl --config your-config.yaml")
        
    except Exception as e:
        console.print(f"[red]❌ Failed to create config template: {str(e)}[/red]")


@app.command()
def enhanced_demo() -> None:
    """Run the enhanced demo with real database connectors."""
    import subprocess
    import sys
    from pathlib import Path
    
    # Find the demo script
    demo_path = Path(__file__).parent.parent.parent / "demo_enhanced.py"
    
    if not demo_path.exists():
        console.print("[red]❌ Enhanced demo script not found![/red]")
        console.print(f"Expected location: {demo_path}")
        return
    
    console.print("[bold cyan]Starting Enhanced IMSOETL Demo...[/bold cyan]")
    
    try:
        # Run the demo script
        result = subprocess.run([sys.executable, str(demo_path)], check=True)
    except subprocess.CalledProcessError as e:
        console.print(f"[red]❌ Demo failed with exit code {e.returncode}[/red]")
    except KeyboardInterrupt:
        console.print("\n[yellow]Demo interrupted by user[/yellow]")
    except Exception as e:
        console.print(f"[red]❌ Demo error: {str(e)}[/red]")


def main() -> None:
    """Main CLI entry point."""
    app()


if __name__ == "__main__":
    main()
