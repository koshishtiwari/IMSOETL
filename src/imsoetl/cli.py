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


def main() -> None:
    """Main entry point for the CLI."""
    app()


if __name__ == "__main__":
    main()
