#!/usr/bin/env python3
"""
IMSOETL LLM Integration Demo

This demo showcases the complete LLM integration with:
- Local Ollama (Gemma3:4b) as primary provider
- Gemini API as fallback
- Enhanced intent parsing
- SQL generation and optimization
- Agent-to-agent communication with LLM
"""

import os
import sys
import asyncio
import json
from pathlib import Path
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table
from rich.syntax import Syntax

# Add the src directory to the path
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

from imsoetl.llm.manager import LLMManager
from imsoetl.core.orchestrator import OrchestratorAgent
from imsoetl.agents.transformation import TransformationAgent

console = Console()


async def demo_llm_integration():
    """Main demo function."""
    console.print(Panel.fit(
        "[bold cyan]🤖 IMSOETL LLM Integration Demo[/bold cyan]\n"
        "[green]Local Ollama (Gemma3:4b) + Gemini API Fallback[/green]",
        border_style="cyan"
    ))
    
    # Configuration
    config = {
        "llm": {
            "ollama": {
                "enabled": True,
                "base_url": "http://localhost:11434",
                "model": "gemma3:4b",
                "timeout": 30
            },
            "gemini": {
                "enabled": True,
                "model": "gemini-1.5-flash",
                "api_key": "AIzaSyCXeLTsst3w9hmPybXuCageQERBS6pQqBk"
            }
        }
    }
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console
    ) as progress:
        # Initialize LLM Manager
        task1 = progress.add_task("Initializing LLM Manager...", total=None)
        llm_manager = LLMManager(config)
        await llm_manager.initialize()
        progress.remove_task(task1)
        
        # Show provider status
        providers_table = Table(title="LLM Providers Status")
        providers_table.add_column("Provider", style="cyan")
        providers_table.add_column("Status", style="green")
        providers_table.add_column("Model", style="yellow")
        
        for provider in ["ollama", "gemini"]:
            if provider in llm_manager.get_available_providers():
                status = "✅ Available"
                model = config["llm"][provider]["model"]
            else:
                status = "❌ Unavailable"
                model = "N/A"
            providers_table.add_row(provider.title(), status, model)
        
        console.print(providers_table)
        console.print(f"\n[bold green]Primary Provider:[/bold green] {llm_manager.get_primary_provider()}")
    
    # Demo 1: Intent Parsing
    console.print(Panel("[bold yellow]Demo 1: Enhanced Intent Parsing[/bold yellow]", border_style="yellow"))
    
    intents = [
        "Extract customer data from MySQL and transform phone numbers, then load into PostgreSQL",
        "Monitor the ETL pipeline for failures and send Slack alerts when errors occur",
        "Analyze data quality in the sales table and generate a report with recommendations"
    ]
    
    for i, intent in enumerate(intents, 1):
        console.print(f"\n[bold]Intent {i}:[/bold] {intent}")
        
        with Progress(
            SpinnerColumn(),
            TextColumn("Parsing intent with LLM..."),
            console=console
        ) as progress:
            task = progress.add_task("", total=None)
            result = await llm_manager.parse_intent(intent)
            progress.remove_task(task)
        
        # Display parsed result
        result_json = json.dumps(result, indent=2)
        syntax = Syntax(result_json, "json", theme="monokai", line_numbers=True)
        console.print(syntax)
    
    # Demo 2: SQL Generation
    console.print(Panel("[bold yellow]Demo 2: LLM-Powered SQL Generation[/bold yellow]", border_style="yellow"))
    
    nl_queries = [
        "Get all orders from the last 30 days with customer names",
        "Find the top 10 products by revenue this year",
        "Calculate monthly growth rate for each product category"
    ]
    
    for query in nl_queries:
        console.print(f"\n[bold cyan]Natural Language:[/bold cyan] {query}")
        
        with Progress(
            SpinnerColumn(),
            TextColumn("Generating SQL..."),
            console=console
        ) as progress:
            task = progress.add_task("", total=None)
            sql = await llm_manager.generate(f"""
Convert this to SQL query:
"{query}"

Use these tables: customers (id, name, email), orders (id, customer_id, order_date, total), 
products (id, name, category, price), order_items (order_id, product_id, quantity)

Return only clean SQL without explanations or markdown.
""")
            progress.remove_task(task)
        
        # Clean up the SQL response
        sql_clean = sql.strip()
        if sql_clean.startswith("```sql"):
            sql_clean = sql_clean[6:]
        if sql_clean.endswith("```"):
            sql_clean = sql_clean[:-3]
        sql_clean = sql_clean.strip()
        
        syntax = Syntax(sql_clean, "sql", theme="monokai", line_numbers=True)
        console.print(syntax)
    
    # Demo 3: Orchestrator Integration
    console.print(Panel("[bold yellow]Demo 3: Orchestrator + LLM Integration[/bold yellow]", border_style="yellow"))
    
    with Progress(
        SpinnerColumn(),
        TextColumn("Initializing orchestrator with LLM..."),
        console=console
    ) as progress:
        task = progress.add_task("", total=None)
        orchestrator = OrchestratorAgent()
        await orchestrator.initialize_llm(config)
        progress.remove_task(task)
    
    console.print("✅ Orchestrator enhanced with LLM capabilities")
    
    # Demo 4: Transformation Agent with LLM
    console.print(Panel("[bold yellow]Demo 4: LLM-Enhanced Transformations[/bold yellow]", border_style="yellow"))
    
    with Progress(
        SpinnerColumn(),
        TextColumn("Setting up transformation agent..."),
        console=console
    ) as progress:
        task = progress.add_task("", total=None)
        transform_agent = TransformationAgent()
        await transform_agent.initialize_llm(config)
        progress.remove_task(task)
    
    # Example transformation
    source_schema = {
        "table": "raw_customers",
        "columns": {
            "id": "int",
            "first_name": "varchar(50)",
            "last_name": "varchar(50)", 
            "email_addr": "varchar(100)",
            "phone_num": "varchar(20)"
        }
    }
    
    target_schema = {
        "table": "customers",
        "columns": {
            "customer_id": "serial",
            "full_name": "varchar(100)",
            "email": "varchar(100)",
            "phone": "varchar(15)"
        }
    }
    
    requirements = [
        "Combine first_name and last_name into full_name",
        "Standardize phone number format",
        "Validate email addresses",
        "Handle null values properly"
    ]
    
    console.print("\n[bold cyan]Transformation Scenario:[/bold cyan]")
    console.print(f"Source: {source_schema['table']}")
    console.print(f"Target: {target_schema['table']}")
    console.print("Requirements:")
    for req in requirements:
        console.print(f"  • {req}")
    
    with Progress(
        SpinnerColumn(),
        TextColumn("Generating LLM-powered transformation..."),
        console=console
    ) as progress:
        task = progress.add_task("", total=None)
        transformation = await transform_agent.generate_llm_enhanced_transformation(
            source_schema, target_schema, requirements
        )
        progress.remove_task(task)
    
    if "llm_generated" in transformation:
        console.print("\n[bold green]LLM-Generated SQL:[/bold green]")
        sql_code = transformation["llm_generated"]["sql"]
        if sql_code.startswith("```sql"):
            sql_code = sql_code[6:]
        if sql_code.endswith("```"):
            sql_code = sql_code[:-3]
        syntax = Syntax(sql_code.strip(), "sql", theme="monokai", line_numbers=True)
        console.print(syntax)
    
    # Demo 5: Data Quality Suggestions
    console.print(Panel("[bold yellow]Demo 5: AI-Powered Data Quality Suggestions[/bold yellow]", border_style="yellow"))
    
    sample_schema = {
        "table": "user_transactions",
        "columns": {
            "transaction_id": {"type": "int", "nullable": False},
            "user_id": {"type": "int", "nullable": True},
            "amount": {"type": "decimal(10,2)", "nullable": True},
            "transaction_date": {"type": "timestamp", "nullable": True},
            "status": {"type": "varchar(20)", "nullable": True}
        }
    }
    
    with Progress(
        SpinnerColumn(),
        TextColumn("Analyzing schema for quality recommendations..."),
        console=console
    ) as progress:
        task = progress.add_task("", total=None)
        quality_suggestions = await llm_manager.suggest_data_quality_checks(sample_schema)
        progress.remove_task(task)
    
    console.print("[bold cyan]Recommended Quality Checks:[/bold cyan]")
    for i, suggestion in enumerate(quality_suggestions, 1):
        console.print(f"  {i}. {suggestion}")
    
    # Summary
    console.print(Panel.fit(
        "[bold green]🎉 Demo Complete![/bold green]\n\n"
        "[cyan]IMSOETL now features:[/cyan]\n"
        "✅ Local Ollama (Gemma3:4b) integration\n"
        "✅ Gemini API fallback support\n"
        "✅ Enhanced intent parsing\n"
        "✅ AI-powered SQL generation\n"
        "✅ Intelligent transformations\n"
        "✅ Data quality recommendations\n"
        "✅ Agent-to-agent LLM communication\n\n"
        "[yellow]Ready for production ETL workflows![/yellow]",
        border_style="green"
    ))


if __name__ == "__main__":
    try:
        asyncio.run(demo_llm_integration())
    except KeyboardInterrupt:
        console.print("\n[yellow]Demo interrupted by user[/yellow]")
    except Exception as e:
        console.print(f"\n[red]Demo failed: {e}[/red]")
        import traceback
        console.print(f"[red]{traceback.format_exc()}[/red]")
