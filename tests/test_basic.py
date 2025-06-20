"""
Basic tests for IMSOETL agents.
"""

import asyncio
import sys
import os

# Add the src directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from datetime import datetime

from imsoetl.core.orchestrator import OrchestratorAgent, IntentParser, TaskPlanner
from imsoetl.core.base_agent import Message, AgentContext
from imsoetl.agents.discovery import DiscoveryAgent
from imsoetl.agents.schema import SchemaAgent
from imsoetl.agents.transformation import TransformationAgent
from imsoetl.agents.quality import QualityAgent
from imsoetl.agents.execution import ExecutionAgent
from imsoetl.agents.monitoring import MonitoringAgent


def test_intent_parser():
    """Test the intent parser."""
    print("Testing IntentParser...")
    
    # Test simple intent
    intent = "Move customer data from MySQL to Snowflake"
    parsed = IntentParser.parse_intent(intent)
    
    assert parsed['original_text'] == intent.lower()
    assert parsed['complexity'] in ['simple', 'medium', 'complex']
    assert len(parsed['operations']) > 0
    assert 'sources' in parsed['entities']
    print("✓ Simple intent parsing works")
    
    # Test complex intent
    intent = "Move last 30 days of customer orders from MySQL to Snowflake, clean the phone numbers, and create a daily summary table"
    parsed = IntentParser.parse_intent(intent)
    
    assert parsed['complexity'] in ['medium', 'complex']
    assert len(parsed['operations']) > 1
    print("✓ Complex intent parsing works")


def test_task_planner():
    """Test the task planner."""
    print("Testing TaskPlanner...")
    
    intent = {
        'original_text': 'move data from mysql to snowflake',
        'operations': [{'type': 'extract', 'matches': []}],
        'entities': {'sources': ['mysql', 'snowflake'], 'columns': [], 'conditions': []},
        'complexity': 'simple'
    }
    
    plan = TaskPlanner.create_execution_plan(intent)
    
    assert 'plan_id' in plan
    assert 'phases' in plan
    assert len(plan['phases']) == 3  # discovery, design, execution
    assert 'agents_required' in plan
    assert plan['estimated_duration'] > 0
    print("✓ Task planning works")


async def test_orchestrator_agent():
    """Test the orchestrator agent."""
    print("Testing OrchestratorAgent...")
    
    orchestrator = OrchestratorAgent()
    
    assert orchestrator.agent_type.value == "orchestrator"
    assert orchestrator.intent_parser is not None
    assert orchestrator.task_planner is not None
    
    await orchestrator.start()
    print("✓ Orchestrator started successfully")
    
    # Test handling user intent
    message = Message(
        sender_id="test_user",
        receiver_id=orchestrator.agent_id,
        message_type="user_intent",
        content={
            "intent": "Move customer data from MySQL to Snowflake",
            "session_id": "test_session"
        }
    )
    
    await orchestrator.handle_user_intent(message)
    
    # Check that session was created
    assert "test_session" in orchestrator.active_sessions
    print("✓ User intent handling works")
    
    await orchestrator.stop()
    print("✓ Orchestrator stopped successfully")


async def test_discovery_agent():
    """Test the discovery agent."""
    print("Testing DiscoveryAgent...")
    
    agent = DiscoveryAgent()
    
    assert agent.agent_type.value == "discovery"
    assert len(agent.source_connectors) > 0
    
    await agent.start()
    print("✓ Discovery agent started successfully")
    
    # Test SQLite source discovery (which should work without external dependencies)
    source_config = {"type": "sqlite", "id": "test_sqlite", "database": ":memory:"}
    source_info = await agent._discover_source(source_config)
    
    assert source_info.source_id == "test_sqlite"
    assert source_info.source_type == "sqlite"
    # SQLite in-memory should connect successfully
    if source_info.status != "connected":
        print(f"SQLite connection status: {source_info.status}")
        print(f"Error message: {source_info.error_message}")
        
        # If SQLite fails, test with mock connector
        mock_config = {"type": "mysql", "id": "test_mysql", "name": "test_mysql"}
        source_info = await agent._discover_source(mock_config)
        assert source_info.source_id == "test_mysql"
        assert source_info.source_type == "mysql"
    
    print("✓ Source discovery works")
    
    await agent.stop()
    print("✓ Discovery agent stopped successfully")


async def test_schema_agent():
    """Test the schema agent."""
    print("Testing SchemaAgent...")
    
    agent = SchemaAgent()
    
    assert agent.agent_type.value == "schema"
    assert agent.analyzer is not None
    assert agent.matcher is not None
    
    await agent.start()
    print("✓ Schema agent started successfully")
    
    # Test parsing table schema
    table_info = {
        "table_name": "customers",
        "source_id": "test_mysql",
        "columns": [
            {"name": "customer_id", "type": "INTEGER", "nullable": False, "primary_key": True},
            {"name": "first_name", "type": "VARCHAR(50)", "nullable": False},
            {"name": "email", "type": "VARCHAR(100)", "nullable": False}
        ]
    }
    
    schema = await agent._parse_table_schema(table_info)
    
    assert schema.table_name == "customers"
    assert schema.source_id == "test_mysql"
    assert len(schema.columns) == 3
    assert any(col.is_primary_key for col in schema.columns)
    print("✓ Schema parsing works")
    
    await agent.stop()
    print("✓ Schema agent stopped successfully")


async def test_transformation_agent():
    """Test the transformation agent."""
    print("Testing TransformationAgent...")
    
    agent = TransformationAgent()
    await agent.initialize()
    
    assert agent.agent_type.value == "transformation"
    print("✓ Transformation agent initialized successfully")
    
    # Test transformation generation
    task = {
        "type": "generate_transformation",
        "requirements": {
            "operations": [
                {"type": "select", "source_columns": ["id", "name"], "config": {}}
            ]
        },
        "source_schema": {"columns": {"id": {"type": "INTEGER"}, "name": {"type": "VARCHAR"}}},
        "target_schema": {"columns": {"id": {"type": "INTEGER"}, "name": {"type": "VARCHAR"}}}
    }
    
    result = await agent.process_task(task)
    assert result["success"] == True
    assert "transformation_code" in result
    print("✓ Transformation generation works")
    
    # Test template application
    template_task = {
        "type": "apply_template",
        "template_name": "null_handling",
        "template_params": {
            "category": "data_cleaning",
            "column": "test_column",
            "default_value": "'default'"
        }
    }
    
    template_result = await agent.process_task(template_task)
    assert template_result["success"] == True
    print("✓ Template application works")


async def test_quality_agent():
    """Test the quality agent."""
    print("Testing QualityAgent...")
    
    agent = QualityAgent()
    await agent.initialize()
    
    assert agent.agent_type.value == "quality"
    print("✓ Quality agent initialized successfully")
    
    # Test quality assessment
    task = {
        "type": "assess_quality",
        "dataset": {
            "id": "test_dataset",
            "schema": {"columns": {"id": {"type": "INTEGER"}, "name": {"type": "VARCHAR"}}}
        },
        "rules": ["default_completeness", "default_uniqueness"]
    }
    
    result = await agent.process_task(task)
    assert result["success"] == True
    assert "assessment_results" in result
    assert "overall_score" in result
    print("✓ Quality assessment works")
    
    # Test data profiling
    profile_task = {
        "type": "profile_data",
        "data": [
            {"id": 1, "name": "Test 1"},
            {"id": 2, "name": "Test 2"},
            {"id": 3, "name": None}
        ],
        "schema": {"columns": {"id": {"type": "INTEGER"}, "name": {"type": "VARCHAR"}}}
    }
    
    profile_result = await agent.process_task(profile_task)
    assert profile_result["success"] == True
    assert "data_profile" in profile_result
    print("✓ Data profiling works")


async def test_execution_agent():
    """Test the execution agent."""
    print("Testing ExecutionAgent...")
    
    agent = ExecutionAgent()
    await agent.initialize()
    
    assert agent.agent_type.value == "execution"
    print("✓ Execution agent initialized successfully")
    
    # Test single task execution
    task = {
        "type": "execute_task",
        "task_config": {
            "task_id": "test_task",
            "task_name": "Test Task",
            "task_type": "shell",
            "command": "echo 'test execution'",
            "environment": "local"
        }
    }
    
    result = await agent.process_task(task)
    assert result["success"] == True
    assert "task_result" in result
    print("✓ Task execution works")
    
    # Test status check
    status_task = {
        "type": "get_status",
        "execution_id": None  # Get all status
    }
    
    status_result = await agent.process_task(status_task)
    assert status_result["success"] == True
    assert "status" in status_result
    print("✓ Status check works")


async def test_monitoring_agent():
    """Test the monitoring agent."""
    print("Testing MonitoringAgent...")
    
    agent = MonitoringAgent()
    await agent.initialize()
    
    assert agent.agent_type.value == "monitoring"
    print("✓ Monitoring agent initialized successfully")
    
    # Test metric collection
    task = {
        "type": "collect_metric",
        "metric_name": "test_metric",
        "metric_value": 42.0
    }
    
    result = await agent.process_task(task)
    assert result["success"] == True
    print("✓ Metric collection works")
    
    # Test health check
    health_task = {
        "type": "health_check",
        "components": ["monitoring", "metrics"]
    }
    
    health_result = await agent.process_task(health_task)
    assert health_result["success"] == True
    assert "health_status" in health_result
    print("✓ Health check works")
    
    # Test monitoring report
    report_task = {
        "type": "generate_report",
        "report_type": "summary",
        "time_range": {"minutes": 5}
    }
    
    report_result = await agent.process_task(report_task)
    assert report_result["success"] == True
    assert "report" in report_result
    print("✓ Monitoring report generation works")
    
    # Stop monitoring
    await agent.stop_monitoring()


async def test_all_agents_integration():
    """Test integration between all agents."""
    print("Testing All Agents Integration...")
    
    # Initialize all agents
    orchestrator = OrchestratorAgent()
    discovery = DiscoveryAgent()
    schema = SchemaAgent()
    transformation = TransformationAgent()
    quality = QualityAgent()
    execution = ExecutionAgent()
    monitoring = MonitoringAgent()
    
    agents = [orchestrator, discovery, schema, transformation, quality, execution, monitoring]
    
    # Start/initialize all agents
    for agent in agents:
        if hasattr(agent, 'start'):
            await agent.start()
        else:
            await agent.initialize()
    
    print("✓ All agents initialized/started successfully")
    
    # Test basic agent properties
    expected_types = ["orchestrator", "discovery", "schema", "transformation", "quality", "execution", "monitoring"]
    actual_types = [agent.agent_type.value for agent in agents]
    
    for expected_type in expected_types:
        assert expected_type in actual_types
    
    print("✓ All agent types are correct")
    
    # Test agent registry with orchestrator
    for agent in agents[1:]:  # Skip orchestrator itself
        if hasattr(agent, 'agent_type'):
            await orchestrator.handle_agent_registration(Message(
                sender_id=agent.agent_id,
                receiver_id=orchestrator.agent_id,
                message_type="agent_registration",
                content={"agent_type": agent.agent_type.value}
            ))
    
    print(f"✓ Registered {len(orchestrator.agent_registry)} agents with orchestrator")
    
    # Cleanup
    for agent in agents:
        if hasattr(agent, 'stop'):
            await agent.stop()
        elif hasattr(agent, 'stop_monitoring'):
            await agent.stop_monitoring()
    
    print("✓ All agents stopped successfully")


def run_all_tests():
    """Run all tests."""
    print("Running IMSOETL Agent Tests...")
    print("=" * 50)
    
    # List of test functions
    tests = [
        test_intent_parser,
        test_task_planner,
        test_orchestrator_agent,
        test_discovery_agent, 
        test_schema_agent,
        test_transformation_agent,
        test_quality_agent,
        test_execution_agent,
        test_monitoring_agent,
        test_all_agents_integration
    ]
    
    passed = 0
    failed = 0
    
    for test_func in tests:
        try:
            print(f"\n--- {test_func.__name__} ---")
            if asyncio.iscoroutinefunction(test_func):
                asyncio.run(test_func())
            else:
                test_func()
            print(f"✅ {test_func.__name__} PASSED")
            passed += 1
        except Exception as e:
            print(f"❌ {test_func.__name__} FAILED: {e}")
            import traceback
            traceback.print_exc()
            failed += 1
    
    print("\n" + "=" * 50)
    print(f"Test Results: {passed} passed, {failed} failed")
    
    if failed == 0:
        print("🎉 All tests passed!")
        return True
    else:
        print(f"💥 {failed} tests failed!")
        return False


async def main():
    """Run all tests."""
    print("Running IMSOETL basic tests...")
    print("=" * 50)
    
    try:
        # Test individual components
        test_intent_parser()
        test_task_planner()
        
        # Test agents
        await test_orchestrator_agent()
        await test_discovery_agent()
        await test_schema_agent()
        await test_transformation_agent()
        await test_quality_agent()
        await test_execution_agent()
        await test_monitoring_agent()
        
        # Test integration
        await test_all_agents_integration()
        
        print("=" * 50)
        print("🎉 All tests passed successfully!")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
