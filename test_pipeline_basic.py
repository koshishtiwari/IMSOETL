#!/usr/bin/env python3
"""
Simple test to verify pipeline execution functionality
"""

import asyncio
import pandas as pd
import tempfile
import os

async def test_basic_functionality():
    """Test basic engine functionality"""
    print("🧪 Testing Basic Pipeline Execution Functionality")
    print("=" * 60)
    
    try:
        from src.imsoetl.engines.manager import ExecutionEngineManager
        
        # Test configuration
        config = {
            "execution_engines": {
                "selection_strategy": "auto",
                "small_data_threshold": 1000,
                "pandas": {"enabled": True},
                "duckdb": {"enabled": True}
            }
        }
        
        # Initialize engine manager
        print("🚀 Initializing engine manager...")
        manager = ExecutionEngineManager(config)
        await manager.initialize()
        print("✅ Engine manager initialized successfully")
        
        # Test simple SQL execution
        print("\n📊 Testing SQL execution...")
        
        # Simple test data
        test_data = [
            {"id": 1, "name": "Alice", "age": 30},
            {"id": 2, "name": "Bob", "age": 25},
            {"id": 3, "name": "Charlie", "age": 35}
        ]
        
        # Test with pandas engine
        from src.imsoetl.engines import ExecutionEngineType
        
        result = await manager.execute_sql(
            "SELECT COUNT(*) as total FROM data",
            params={"data": test_data},
            engine_hint=ExecutionEngineType.PANDAS
        )
        
        if result.success:
            print(f"✅ Pandas SQL execution successful: {result.data}")
        else:
            print(f"❌ Pandas SQL execution failed: {result.error}")
        
        # Test with DuckDB engine (if available)
        result2 = await manager.execute_sql(
            "SELECT COUNT(*) as total FROM data",
            params={"data": test_data},
            engine_hint=ExecutionEngineType.DUCKDB
        )
        
        if result2.success:
            print(f"✅ DuckDB SQL execution successful: {result2.data}")
        else:
            print(f"❌ DuckDB SQL execution failed: {result2.error}")
        
        # Test data loading
        print("\n📁 Testing data loading...")
        
        # Create temporary CSV file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            df = pd.DataFrame(test_data)
            df.to_csv(f.name, index=False)
            csv_path = f.name
        
        try:
            result3 = await manager.load_data(
                source=csv_path,
                format_type="csv"
            )
            
            if result3.success:
                # Fix DataFrame evaluation issue
                data_len = len(result3.data) if result3.data is not None else 0
                print(f"✅ Data loading successful: {data_len} rows")
            else:
                print(f"❌ Data loading failed: {result3.error}")
                
        finally:
            os.unlink(csv_path)
        
        print("\n🎯 Testing execution agent...")
        
        # Test execution agent directly
        from src.imsoetl.agents.execution import ExecutionAgent
        
        agent = ExecutionAgent()
        # Set config directly using dict format
        agent.config = config
        await agent.initialize()
        
        # Test data task execution
        task_config = {
            "type": "sql_query",
            "query": "SELECT COUNT(*) as total FROM data",
            "parameters": {"data": test_data},
            "engine_hint": "pandas"
        }
        
        result4 = await agent.execute_data_task(task_config)
        
        if result4.get("success"):
            print("✅ Execution agent data task successful")
            print(f"   Data: {result4.get('data')}")
            print(f"   Execution time: {result4.get('execution_time', 0):.3f}s")
        else:
            print(f"❌ Execution agent data task failed: {result4.get('error')}")
        
        print("\n🎉 Basic functionality test completed!")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(test_basic_functionality())
