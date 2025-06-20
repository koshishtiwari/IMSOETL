"""Tests for IMSOETL."""

import pytest
import asyncio
from typing import Any, Dict

from imsoetl.core.base_agent import BaseAgent, AgentType, AgentStatus, Message, AgentContext
from imsoetl.core.orchestrator import OrchestratorAgent, IntentParser, TaskPlanner


class TestBaseAgent:
    """Test cases for BaseAgent."""
    
    def test_agent_creation(self):
        """Test agent creation and initialization."""
        class TestAgent(BaseAgent):
            async def process_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
                return {"status": "completed", "result": "test"}
        
        agent = TestAgent(
            agent_id="test_agent_1",
            agent_type=AgentType.DISCOVERY,
            name="TestAgent",
            config={"test_param": "test_value"}
        )
        
        assert agent.agent_id == "test_agent_1"
        assert agent.agent_type == AgentType.DISCOVERY
        assert agent.name == "TestAgent"
        assert agent.config["test_param"] == "test_value"
        assert agent.status == AgentStatus.IDLE
    
    @pytest.mark.asyncio
    async def test_agent_lifecycle(self):
        """Test agent start and stop lifecycle."""
        class TestAgent(BaseAgent):
            async def process_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
                return {"status": "completed"}
        
        agent = TestAgent("test_agent_2", AgentType.DISCOVERY)
        
        # Test start
        await agent.start()
        assert agent.status == AgentStatus.IDLE
        assert agent.current_task is not None
        
        # Test stop
        await agent.stop()
        assert agent.status == AgentStatus.STOPPED
    
    def test_message_creation(self):
        """Test message creation."""
        message = Message(
            sender_id="sender1",
            receiver_id="receiver1",
            message_type="test_message",
            content={"key": "value"}
        )
        
        assert message.sender_id == "sender1"
        assert message.receiver_id == "receiver1"
        assert message.message_type == "test_message"
        assert message.content["key"] == "value"
        assert message.id is not None
        assert message.timestamp is not None


class TestOrchestratorAgent:
    """Test cases for OrchestratorAgent."""
    
    def test_orchestrator_creation(self):
        """Test orchestrator creation."""
        orchestrator = OrchestratorAgent()
        
        assert orchestrator.agent_type == AgentType.ORCHESTRATOR
        assert orchestrator.name == "OrchestratorAgent"
        assert hasattr(orchestrator, 'intent_parser')
        assert hasattr(orchestrator, 'task_planner')
    
    @pytest.mark.asyncio
    async def test_orchestrator_lifecycle(self):
        """Test orchestrator start and stop."""
        orchestrator = OrchestratorAgent("test_orchestrator")
        
        await orchestrator.start()
        assert orchestrator.status == AgentStatus.IDLE
        
        await orchestrator.stop()
        assert orchestrator.status == AgentStatus.STOPPED


class TestIntentParser:
    """Test cases for IntentParser."""
    
    def test_simple_extract_intent(self):
        """Test parsing simple extract intent."""
        intent = IntentParser.parse_intent("extract customers from mysql_db")
        
        assert intent['original_text'] == "extract customers from mysql_db"
        assert len(intent['operations']) > 0
        assert any(op['type'] == 'extract' for op in intent['operations'])
        assert 'mysql_db' in intent['entities']['sources']
    
    def test_complex_etl_intent(self):
        """Test parsing complex ETL intent."""
        intent = IntentParser.parse_intent(
            "Move last 30 days of customer orders from MySQL to Snowflake, "
            "clean the phone numbers, and create a daily summary table"
        )
        
        assert intent['complexity'] in ['medium', 'complex']
        assert len(intent['operations']) > 1
        assert any(op['type'] == 'extract' for op in intent['operations'])
        assert any(op['type'] == 'transform' for op in intent['operations'])
    
    def test_transform_intent(self):
        """Test parsing transform intent."""
        intent = IntentParser.parse_intent("clean phone_numbers and normalize addresses")
        
        assert any(op['type'] == 'transform' for op in intent['operations'])
        assert 'phone_numbers' in intent['entities']['columns'] or 'addresses' in intent['entities']['columns']
    
    def test_schedule_intent(self):
        """Test parsing schedule-related intent."""
        intent = IntentParser.parse_intent("run daily customer report")
        
        assert any(op['type'] == 'schedule' for op in intent['operations'])


class TestTaskPlanner:
    """Test cases for TaskPlanner."""
    
    def test_simple_plan_creation(self):
        """Test creating execution plan for simple intent."""
        intent = {
            'original_text': 'extract customers from mysql',
            'operations': [{'type': 'extract', 'matches': [('extract', 'customers', 'mysql')]}],
            'entities': {'sources': ['mysql'], 'targets': [], 'columns': [], 'conditions': []},
            'complexity': 'simple'
        }
        
        plan = TaskPlanner.create_execution_plan(intent)
        
        assert 'plan_id' in plan
        assert len(plan['phases']) > 0
        assert 'discovery' in [phase['phase_id'] for phase in plan['phases']]
        assert 'agents_required' in plan
        assert len(plan['agents_required']) > 0
    
    def test_complex_plan_creation(self):
        """Test creating execution plan for complex intent."""
        intent = {
            'original_text': 'move data from mysql to snowflake and clean phone numbers',
            'operations': [
                {'type': 'extract', 'matches': []},
                {'type': 'transform', 'matches': []},
                {'type': 'load', 'matches': []}
            ],
            'entities': {
                'sources': ['mysql', 'snowflake'],
                'targets': ['snowflake'],
                'columns': ['phone_numbers'],
                'conditions': []
            },
            'complexity': 'complex'
        }
        
        plan = TaskPlanner.create_execution_plan(intent)
        
        assert len(plan['phases']) >= 3  # discovery, design, execution
        assert plan['estimated_duration'] > 0
        assert 'transformation' in plan['agents_required']


class TestAgentContext:
    """Test cases for AgentContext."""
    
    def test_context_creation(self):
        """Test context creation."""
        context = AgentContext(
            session_id="test_session",
            user_intent="test intent",
            current_phase="discovery"
        )
        
        assert context.session_id == "test_session"
        assert context.user_intent == "test intent"
        assert context.current_phase == "discovery"
        assert isinstance(context.shared_data, dict)
        assert context.created_at is not None
        assert context.updated_at is not None


if __name__ == "__main__":
    pytest.main([__file__])
