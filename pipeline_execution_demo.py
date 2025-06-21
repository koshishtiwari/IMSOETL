#!/usr/bin/env python3
"""
Pipeline Execution Demo for IMSOETL

This script demonstrates the new pipeline execution capabilities
with real data processing engines (Pandas, DuckDB, Spark).
"""

import asyncio
import json
import pandas as pd
import tempfile
import os
from pathlib import Path

# Sample data for demonstration
SAMPLE_DATA = [
    {"id": 1, "name": "Alice", "age": 30, "city": "New York", "salary": 75000},
    {"id": 2, "name": "Bob", "age": 25, "city": "Los Angeles", "salary": 65000},
    {"id": 3, "name": "Charlie", "age": 35, "city": "Chicago", "salary": 80000},
    {"id": 4, "name": "Diana", "age": 28, "city": "Houston", "salary": 72000},
    {"id": 5, "name": "Eve", "age": 32, "city": "Phoenix", "salary": 68000},
]

LARGE_SAMPLE_DATA = []
for i in range(10000):
    LARGE_SAMPLE_DATA.append({
        "id": i + 1,
        "name": f"Person_{i+1}",
        "age": 20 + (i % 50),
        "city": ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"][i % 5],
        "salary": 50000 + (i % 100) * 1000,
        "department": ["Engineering", "Sales", "Marketing", "HR", "Finance"][i % 5]
    })


async def demo_sql_execution():
    """Demo SQL query execution with different engines."""
    print("🔍 SQL Execution Demo")
    print("=" * 50)
    
    try:
        from src.imsoetl.core.orchestrator import OrchestratorAgent
        
        # Create temporary CSV file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            df = pd.DataFrame(SAMPLE_DATA)
            df.to_csv(f.name, index=False)
            csv_path = f.name
        
        try:
            # Initialize orchestrator
            orchestrator = OrchestratorAgent()
            await orchestrator.start()
            
            # Test SQL queries with different engines
            queries = [
                {
                    "name": "Simple SELECT",
                    "sql": "SELECT * FROM data WHERE age > 30",
                    "engine": "pandas"
                },
                {
                    "name": "Aggregation Query",
                    "sql": "SELECT city, AVG(salary) as avg_salary, COUNT(*) as count FROM data GROUP BY city",
                    "engine": "duckdb"
                },
                {
                    "name": "Complex Filter",
                    "sql": "SELECT name, age, salary FROM data WHERE salary > 70000 ORDER BY salary DESC",
                    "engine": "pandas"
                }
            ]
            
            for query_info in queries:
                print(f"\n📊 Testing: {query_info['name']} (Engine: {query_info['engine']})")
                print(f"Query: {query_info['sql']}")
                
                task_config = {
                    "type": "sql_query",
                    "query": query_info['sql'],
                    "source": {"path": csv_path, "type": "csv"},
                    "engine_hint": query_info['engine']
                }
                
                result = await orchestrator.process_task({
                    "type": "execute_task",
                    "task_config": task_config
                })
                
                if result.get("success"):
                    data_result = result.get("data_result", {})
                    print(f"✅ Success! Execution time: {data_result.get('execution_time', 0):.3f}s")
                    
                    data = data_result.get("data")
                    if data:
                        print(f"Results: {len(data)} rows")
                        if len(data) <= 5:
                            for row in data:
                                print(f"  {row}")
                        else:
                            print(f"  First 3 rows: {data[:3]}")
                else:
                    print(f"❌ Failed: {result.get('error', 'Unknown error')}")
                    
        finally:
            # Clean up temporary file
            os.unlink(csv_path)
            
    except Exception as e:
        print(f"❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()


async def demo_data_transformation():
    """Demo data transformation with different engines."""
    print("\n🔧 Data Transformation Demo")
    print("=" * 50)
    
    try:
        from src.imsoetl.core.orchestrator import OrchestratorAgent
        
        # Create temporary CSV file with larger dataset
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            df = pd.DataFrame(LARGE_SAMPLE_DATA)
            df.to_csv(f.name, index=False)
            csv_path = f.name
        
        try:
            # Initialize orchestrator
            orchestrator = OrchestratorAgent()
            await orchestrator.start()
            
            # Test different transformations
            transformations = [
                {
                    "name": "Add Bonus Column",
                    "engine": "pandas",
                    "spec": {
                        "type": "add_column",
                        "column_name": "bonus",
                        "expression": "salary * 0.1"
                    }
                },
                {
                    "name": "Department Statistics",
                    "engine": "duckdb",
                    "spec": {
                        "type": "aggregate",
                        "group_by": ["department"],
                        "aggregations": {
                            "avg_salary": "AVG(salary)",
                            "max_age": "MAX(age)",
                            "employee_count": "COUNT(*)"
                        }
                    }
                }
            ]
            
            for trans_info in transformations:
                print(f"\n🔄 Testing: {trans_info['name']} (Engine: {trans_info['engine']})")
                
                task_config = {
                    "type": "data_transformation",
                    "source_data": csv_path,
                    "transformation": trans_info['spec'],
                    "engine_hint": trans_info['engine']
                }
                
                result = await orchestrator.process_task({
                    "type": "execute_task",
                    "task_config": task_config
                })
                
                if result.get("success"):
                    data_result = result.get("data_result", {})
                    print(f"✅ Success! Execution time: {data_result.get('execution_time', 0):.3f}s")
                    print(f"Rows processed: {data_result.get('rows_processed', 0)}")
                else:
                    print(f"❌ Failed: {result.get('error', 'Unknown error')}")
                    
        finally:
            # Clean up temporary file
            os.unlink(csv_path)
            
    except Exception as e:
        print(f"❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()


async def demo_pipeline_execution():
    """Demo full pipeline execution."""
    print("\n🚀 Pipeline Execution Demo")
    print("=" * 50)
    
    try:
        from src.imsoetl.core.orchestrator import OrchestratorAgent
        
        # Create sample pipeline configuration
        pipeline_config = {
            "name": "Employee Data Processing Pipeline",
            "description": "Process employee data with multiple steps",
            "tasks": [
                {
                    "task_id": "load_data",
                    "task_name": "Load Employee Data",
                    "task_type": "data_load",
                    "source": {"type": "generate", "count": 1000},
                    "engine_hint": "pandas"
                },
                {
                    "task_id": "calculate_metrics",
                    "task_name": "Calculate Employee Metrics",
                    "task_type": "sql_query",
                    "query": "SELECT department, AVG(salary) as avg_salary, COUNT(*) as count FROM data GROUP BY department",
                    "dependencies": ["load_data"],
                    "engine_hint": "duckdb"
                },
                {
                    "task_id": "filter_high_earners",
                    "task_name": "Filter High Earners",
                    "task_type": "sql_query",
                    "query": "SELECT * FROM data WHERE salary > 75000 ORDER BY salary DESC",
                    "dependencies": ["load_data"],
                    "engine_hint": "pandas"
                }
            ],
            "execution_mode": "dag"  # Execute based on dependencies
        }
        
        # Initialize orchestrator
        orchestrator = OrchestratorAgent()
        await orchestrator.start()
        
        print(f"📋 Pipeline: {pipeline_config['name']}")
        print(f"Tasks: {len(pipeline_config['tasks'])}")
        
        # Execute pipeline
        result = await orchestrator.process_task({
            "type": "execute_pipeline",
            "pipeline_config": pipeline_config
        })
        
        if result.get("success"):
            pipeline_result = result.get("pipeline_result", {})
            print(f"✅ Pipeline completed successfully!")
            print(f"Status: {pipeline_result.get('status', 'Unknown')}")
            print(f"Tasks completed: {pipeline_result.get('tasks_completed', 0)}")
            print(f"Total duration: {pipeline_result.get('total_duration', 0):.2f}s")
        else:
            print(f"❌ Pipeline failed: {result.get('error', 'Unknown error')}")
            
    except Exception as e:
        print(f"❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()


async def demo_engine_selection():
    """Demo automatic engine selection based on data size."""
    print("\n⚙️ Engine Selection Demo")
    print("=" * 50)
    
    try:
        from src.imsoetl.engines.manager import ExecutionEngineManager
        from src.imsoetl.core.config import load_config
        
        # Load configuration
        config = load_config()
        config_dict = {
            "execution_engines": {
                "selection_strategy": "auto",
                "small_data_threshold": 1000,
                "medium_data_threshold": 10000,
                "pandas": {"enabled": True},
                "duckdb": {"enabled": True},
                "spark": {"enabled": False}
            }
        }
        
        # Initialize engine manager
        engine_manager = ExecutionEngineManager(config_dict)
        await engine_manager.initialize()
        
        print("🔍 Available engines:")
        capabilities = engine_manager.get_engine_capabilities()
        for engine_type, info in capabilities.items():
            status = "✅" if info.get("available") else "❌"
            print(f"  {status} {engine_type.title()}: {', '.join(info.get('capabilities', []))}")
        
        # Test engine selection with different data sizes
        test_cases = [
            {"size": 100, "description": "Small dataset"},
            {"size": 5000, "description": "Medium dataset"},
            {"size": 50000, "description": "Large dataset"}
        ]
        
        for case in test_cases:
            print(f"\n📊 Testing {case['description']} ({case['size']} rows)")
            
            # Select engine based on data characteristics
            engine = engine_manager._select_engine(
                operation_type="sql",
                data_size=case['size']
            )
            
            if engine:
                print(f"Selected engine: {engine.engine_type.value}")
                
                # Test simple query
                try:
                    # Generate test data
                    test_data = [{"id": i, "value": i * 2} for i in range(case['size'])]
                    
                    result = await engine.execute_sql(
                        "SELECT COUNT(*) as total FROM data", 
                        {"data": test_data}
                    )
                    
                    if result.success:
                        print(f"✅ Query executed in {result.execution_time:.3f}s")
                    else:
                        print(f"❌ Query failed: {result.error}")
                        
                except Exception as e:
                    print(f"❌ Engine test failed: {e}")
            else:
                print("❌ No suitable engine found")
                
    except Exception as e:
        print(f"❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()


async def main():
    """Run all pipeline execution demos."""
    print("🎯 IMSOETL Pipeline Execution Demo")
    print("=" * 60)
    print("This demo showcases the new pipeline execution capabilities")
    print("with real data processing engines (Pandas, DuckDB, Spark).")
    print("=" * 60)
    
    # Run all demos
    await demo_sql_execution()
    await demo_data_transformation() 
    await demo_pipeline_execution()
    await demo_engine_selection()
    
    print("\n🎉 All demos completed!")
    print("=" * 60)
    print("Key features demonstrated:")
    print("✅ SQL query execution with multiple engines")
    print("✅ Data transformation with engine selection")
    print("✅ Multi-step pipeline execution")
    print("✅ Automatic engine selection based on data size")
    print("✅ Engine capabilities and performance comparison")


if __name__ == "__main__":
    asyncio.run(main())
