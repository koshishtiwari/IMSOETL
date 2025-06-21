#!/usr/bin/env python3
"""
Quick test of just the SQL functionality
"""

import asyncio
import tempfile
import pandas as pd

async def quick_sql_test():
    """Quick test of SQL execution"""
    try:
        from src.imsoetl.engines.manager import ExecutionEngineManager
        from src.imsoetl.engines import ExecutionEngineType
        
        print("🧪 Quick SQL Test")
        
        # Simple config
        config = {
            "execution_engines": {
                "pandas": {"enabled": True}
            }
        }
        
        # Initialize
        manager = ExecutionEngineManager(config)
        await manager.initialize()
        print("✅ Manager initialized")
        
        # Test data
        data = [
            {"id": 1, "name": "Alice", "age": 30},
            {"id": 2, "name": "Bob", "age": 25}
        ]
        
        # Test simple COUNT query
        result = await manager.execute_sql(
            "SELECT COUNT(*) as total FROM data",
            params={"data": data},
            engine_hint=ExecutionEngineType.PANDAS
        )
        
        print(f"SQL Result: {result.success}")
        if result.success:
            print(f"  Data: {result.data}")
            print(f"  Rows: {result.rows_processed}")
            print(f"  Time: {result.execution_time:.3f}s")
        else:
            print(f"  Error: {result.error}")
            
        print("✅ Test completed")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(quick_sql_test())
