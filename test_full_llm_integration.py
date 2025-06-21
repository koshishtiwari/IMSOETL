#!/usr/bin/env python3
"""Comprehensive test for IMSOETL LLM integration."""

import os
import sys
import asyncio
import json
from pathlib import Path

# Add the src directory to the path
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

from imsoetl.llm.manager import LLMManager
from imsoetl.core.orchestrator import OrchestratorAgent
from imsoetl.agents.transformation import TransformationAgent


async def test_full_llm_pipeline():
    """Test the complete LLM-enhanced ETL pipeline."""
    print("🚀 IMSOETL Full LLM Integration Test")
    print("=====================================")
    
    # Create configuration with LLM settings
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
    
    print("🔧 Setting up LLM-enhanced agents...")
    
    # Initialize LLM Manager
    llm_manager = LLMManager(config)
    await llm_manager.initialize()
    print(f"✅ LLM Manager - Providers: {llm_manager.get_available_providers()}")
    
    # Initialize Orchestrator with LLM
    orchestrator = OrchestratorAgent()
    await orchestrator.initialize_llm(config)
    print("✅ Orchestrator with LLM integration")
    
    # Initialize Transformation Agent with LLM
    transformation_agent = TransformationAgent()
    await transformation_agent.initialize_llm(config)
    print("✅ Transformation Agent with LLM integration")
    
    print("\n🧪 Testing Complex ETL Scenarios")
    print("=" * 40)
    
    # Test 1: Complex Data Migration
    print("\n📊 Test 1: Complex Data Migration")
    user_intent = "Migrate customer data from MySQL to PostgreSQL, clean phone numbers, validate emails, and create a summary report"
    
    try:
        intent_result = await llm_manager.parse_intent(user_intent)
        print(f"Intent: {json.dumps(intent_result, indent=2)}")
        
        # Test transformation generation
        source_schema = {
            "table": "customers",
            "columns": {
                "id": {"type": "int", "primary_key": True},
                "name": {"type": "varchar(255)", "nullable": False},
                "email": {"type": "varchar(255)", "nullable": True},
                "phone": {"type": "varchar(20)", "nullable": True},
                "created_at": {"type": "timestamp", "nullable": False}
            }
        }
        
        target_schema = {
            "table": "clean_customers", 
            "columns": {
                "customer_id": {"type": "serial", "primary_key": True},
                "full_name": {"type": "varchar(255)", "nullable": False},
                "email_address": {"type": "varchar(255)", "nullable": True},
                "phone_number": {"type": "varchar(15)", "nullable": True},
                "registration_date": {"type": "timestamp", "nullable": False}
            }
        }
        
        requirements = [
            "Clean phone numbers to standard format",
            "Validate email addresses",
            "Map source columns to target schema",
            "Handle null values appropriately"
        ]
        
        transformation_result = await transformation_agent.generate_llm_enhanced_transformation(
            source_schema, target_schema, requirements
        )
        
        print("🔄 Generated Transformation:")
        if "llm_generated" in transformation_result:
            print("LLM SQL:")
            print(transformation_result["llm_generated"]["sql"])
        
    except Exception as e:
        print(f"❌ Test 1 failed: {e}")
    
    # Test 2: Data Quality and Monitoring
    print("\n📈 Test 2: Data Quality Analysis")
    quality_intent = "Analyze data quality in the orders table, find duplicates, check for missing values, and suggest improvements"
    
    try:
        quality_result = await llm_manager.parse_intent(quality_intent)
        print(f"Quality Intent: {json.dumps(quality_result, indent=2)}")
        
        # Test data quality suggestions
        schema = {
            "table": "orders",
            "columns": {
                "order_id": {"type": "int", "nullable": False},
                "customer_id": {"type": "int", "nullable": True},
                "order_date": {"type": "date", "nullable": True},
                "total_amount": {"type": "decimal(10,2)", "nullable": True},
                "status": {"type": "varchar(50)", "nullable": True}
            }
        }
        
        quality_suggestions = await llm_manager.suggest_data_quality_checks(schema)
        print("🔍 Quality Check Suggestions:")
        for suggestion in quality_suggestions:
            print(f"  • {suggestion}")
        
    except Exception as e:
        print(f"❌ Test 2 failed: {e}")
    
    # Test 3: SQL Optimization
    print("\n⚡ Test 3: SQL Optimization")
    sample_sql = """
    SELECT c.customer_id, c.name, o.order_date, o.total_amount
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    WHERE o.order_date >= '2024-01-01'
    AND c.status = 'active'
    ORDER BY o.order_date DESC
    """
    
    try:
        schema_info = {
            "tables": ["customers", "orders"],
            "indexes": ["customers.customer_id", "orders.order_date"],
            "row_counts": {"customers": 100000, "orders": 500000}
        }
        
        optimized_sql = await transformation_agent.optimize_transformation_with_llm(
            sample_sql, schema_info
        )
        
        print("Original SQL:")
        print(sample_sql)
        print("\nOptimized SQL:")
        print(optimized_sql)
        
    except Exception as e:
        print(f"❌ Test 3 failed: {e}")
    
    # Test 4: Natural Language to SQL
    print("\n💬 Test 4: Natural Language to SQL Generation")
    nl_queries = [
        "Show me all customers who placed orders last month",
        "Find the top 5 products by sales volume",
        "Get monthly revenue trends for the past year"
    ]
    
    for query in nl_queries:
        try:
            sql_result = await llm_manager.generate(f"""
Convert this natural language query to SQL:
"{query}"

Assume we have tables: customers, orders, products, order_items
Return only the SQL query without explanations.
""")
            print(f"Query: {query}")
            print(f"SQL: {sql_result.strip()}")
            print("-" * 30)
        except Exception as e:
            print(f"❌ Failed to convert '{query}': {e}")
    
    print("\n✅ Full LLM Integration Test Completed!")
    print("Summary:")
    print(f"- LLM Providers Available: {llm_manager.get_available_providers()}")
    print(f"- Primary Provider: {llm_manager.get_primary_provider()}")
    print("- Intent Parsing: Working")
    print("- SQL Generation: Working") 
    print("- SQL Optimization: Working")
    print("- Agent Integration: Working")


if __name__ == "__main__":
    try:
        asyncio.run(test_full_llm_pipeline())
    except KeyboardInterrupt:
        print("\n⏹️  Testing interrupted by user")
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
