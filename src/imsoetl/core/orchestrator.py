"""
Orchestrator Agent - The master coordinator for all IMSOETL agents.

This agent is responsible for:
- Parsing user intents and creating execution plans
- Breaking down complex requests into atomic tasks
- Assigning tasks to specialized agents
- Coordinating the overall workflow
- Managing agent lifecycle and communication
"""

import asyncio
import re
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime

from .base_agent import BaseAgent, AgentType, AgentContext, Message


class IntentParser:
    """Parses natural language intents into structured tasks."""
    
    # Pattern matching for common ETL operations
    PATTERNS = {
        'extract': [
            r'(extract|get|fetch|pull|retrieve)\s+(.+?)\s+from\s+(.+)',
            r'move\s+(.+?)\s+from\s+(.+?)\s+to\s+(.+)',
            r'copy\s+(.+?)\s+from\s+(.+?)\s+to\s+(.+)',
        ],
        'transform': [
            r'(clean|cleanse|normalize|format)\s+(.+)',
            r'(aggregate|group|sum|count|average)\s+(.+)',
            r'(join|merge|combine)\s+(.+?)\s+with\s+(.+)',
        ],
        'load': [
            r'(load|insert|save|write|store)\s+(.+?)\s+(to|into)\s+(.+)',
            r'create\s+(table|view|summary)\s+(.+)',
        ],
        'schedule': [
            r'(daily|weekly|monthly|hourly)\s+(.+)',
            r'every\s+(\d+)\s+(minutes?|hours?|days?)\s+(.+)',
            r'at\s+(\d{1,2}):(\d{2})\s+(.+)',
        ],
        'filter': [
            r'(where|filter|only|with)\s+(.+)',
            r'last\s+(\d+)\s+(days?|weeks?|months?)\s+(.+)',
        ]
    }
    
    @classmethod
    def parse_intent(cls, user_input: str) -> Dict[str, Any]:
        """Parse user intent into structured format."""
        user_input = user_input.lower().strip()
        
        intent = {
            'original_text': user_input,
            'operations': [],
            'entities': {
                'sources': [],
                'targets': [],
                'columns': [],
                'conditions': [],
                'schedule': None
            },
            'complexity': 'simple'
        }
        
        # Extract operations
        for operation_type, patterns in cls.PATTERNS.items():
            for pattern in patterns:
                matches = re.findall(pattern, user_input, re.IGNORECASE)
                if matches:
                    intent['operations'].append({
                        'type': operation_type,
                        'matches': matches,
                        'pattern': pattern
                    })
        
        # Extract entities
        cls._extract_entities(user_input, intent)
        
        # Determine complexity
        intent['complexity'] = cls._determine_complexity(intent)
        
        return intent
    
    @classmethod
    def _extract_entities(cls, text: str, intent: Dict[str, Any]) -> None:
        """Extract entities like table names, column names, etc."""
        # Extract table/source names (simple heuristic)
        table_patterns = [
            r'(?:from|to|into|table|view)\s+([a-zA-Z_][a-zA-Z0-9_]*)',
            r'([a-zA-Z_][a-zA-Z0-9_]*)\s+(?:table|view)',
        ]
        
        for pattern in table_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            intent['entities']['sources'].extend(matches)
        
        # Extract column names
        column_patterns = [
            r'(?:clean|format|normalize)\s+([a-zA-Z_][a-zA-Z0-9_]*)',
            r'([a-zA-Z_][a-zA-Z0-9_]*)\s+(?:column|field)',
        ]
        
        for pattern in column_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            intent['entities']['columns'].extend(matches)
        
        # Extract time-based conditions
        time_patterns = [
            r'last\s+(\d+)\s+(days?|weeks?|months?)',
            r'(\d+)\s+(days?|weeks?|months?)\s+ago',
        ]
        
        for pattern in time_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            intent['entities']['conditions'].extend(matches)
    
    @classmethod
    def _determine_complexity(cls, intent: Dict[str, Any]) -> str:
        """Determine the complexity of the intent."""
        num_operations = len(intent['operations'])
        num_sources = len(set(intent['entities']['sources']))
        num_transformations = len([op for op in intent['operations'] if op['type'] == 'transform'])
        
        if num_operations <= 1 and num_sources <= 1:
            return 'simple'
        elif num_operations <= 3 and num_sources <= 2 and num_transformations <= 1:
            return 'medium'
        else:
            return 'complex'


class TaskPlanner:
    """Creates execution plans from parsed intents."""
    
    @classmethod
    def create_execution_plan(cls, intent: Dict[str, Any]) -> Dict[str, Any]:
        """Create an execution plan from parsed intent."""
        plan = {
            'plan_id': f"plan_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
            'intent': intent,
            'phases': [],
            'estimated_duration': 0,
            'dependencies': [],
            'agents_required': set()
        }
        
        # Phase 1: Discovery
        discovery_phase = {
            'phase_id': 'discovery',
            'phase_name': 'Discovery & Analysis',
            'tasks': cls._create_discovery_tasks(intent),
            'agents': ['discovery', 'schema', 'quality'],
            'estimated_duration': 30  # seconds
        }
        plan['phases'].append(discovery_phase)
        plan['agents_required'].update(discovery_phase['agents'])
        
        # Phase 2: Design
        design_phase = {
            'phase_id': 'design',
            'phase_name': 'Pipeline Design',
            'tasks': cls._create_design_tasks(intent),
            'agents': ['transformation', 'execution', 'quality'],
            'estimated_duration': 60
        }
        plan['phases'].append(design_phase)
        plan['agents_required'].update(design_phase['agents'])
        
        # Phase 3: Execution
        execution_phase = {
            'phase_id': 'execution',
            'phase_name': 'Execution & Monitoring',
            'tasks': cls._create_execution_tasks(intent),
            'agents': ['execution', 'monitoring', 'quality'],
            'estimated_duration': 120
        }
        plan['phases'].append(execution_phase)
        plan['agents_required'].update(execution_phase['agents'])
        
        plan['estimated_duration'] = sum(phase['estimated_duration'] for phase in plan['phases'])
        plan['agents_required'] = list(plan['agents_required'])
        
        return plan
    
    @classmethod
    def _create_discovery_tasks(cls, intent: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Create discovery phase tasks."""
        tasks = []
        
        # Data source discovery
        if intent['entities']['sources']:
            tasks.append({
                'task_id': 'discover_sources',
                'task_type': 'discovery',
                'description': 'Discover and analyze data sources',
                'parameters': {
                    'sources': intent['entities']['sources']
                },
                'agent': 'discovery'
            })
        
        # Schema analysis
        tasks.append({
            'task_id': 'analyze_schemas',
            'task_type': 'schema_analysis',
            'description': 'Analyze source and target schemas',
            'parameters': {
                'sources': intent['entities']['sources']
            },
            'agent': 'schema'
        })
        
        # Data quality assessment
        tasks.append({
            'task_id': 'assess_quality',
            'task_type': 'quality_assessment',
            'description': 'Assess data quality and identify issues',
            'parameters': {
                'sources': intent['entities']['sources'],
                'columns': intent['entities']['columns']
            },
            'agent': 'quality'
        })
        
        return tasks
    
    @classmethod
    def _create_design_tasks(cls, intent: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Create design phase tasks."""
        tasks = []
        
        # Transformation design
        transform_ops = [op for op in intent['operations'] if op['type'] == 'transform']
        if transform_ops:
            tasks.append({
                'task_id': 'design_transformations',
                'task_type': 'transformation_design',
                'description': 'Design transformation logic',
                'parameters': {
                    'operations': transform_ops,
                    'columns': intent['entities']['columns']
                },
                'agent': 'transformation'
            })
        
        # Execution plan design
        tasks.append({
            'task_id': 'design_execution',
            'task_type': 'execution_design',
            'description': 'Create optimal execution plan',
            'parameters': {
                'operations': intent['operations'],
                'complexity': intent['complexity']
            },
            'agent': 'execution'
        })
        
        return tasks
    
    @classmethod
    def _create_execution_tasks(cls, intent: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Create execution phase tasks."""
        tasks = []
        
        # Pipeline execution
        tasks.append({
            'task_id': 'execute_pipeline',
            'task_type': 'pipeline_execution',
            'description': 'Execute the designed pipeline',
            'parameters': {
                'operations': intent['operations']
            },
            'agent': 'execution'
        })
        
        # Monitoring setup
        tasks.append({
            'task_id': 'setup_monitoring',
            'task_type': 'monitoring_setup',
            'description': 'Set up monitoring and alerts',
            'parameters': {
                'pipeline_id': 'TBD'  # Will be filled during execution
            },
            'agent': 'monitoring'
        })
        
        return tasks


class OrchestratorAgent(BaseAgent):
    """
    Master coordinator agent that orchestrates all other agents.
    
    Responsibilities:
    - Parse user intents
    - Create execution plans
    - Coordinate agent communication
    - Monitor overall progress
    - Handle error recovery
    """
    
    def __init__(self, agent_id: str = "orchestrator_main", config: Optional[Dict[str, Any]] = None):
        super().__init__(
            agent_id=agent_id,
            agent_type=AgentType.ORCHESTRATOR,
            name="OrchestratorAgent",
            config=config
        )
        
        # Orchestrator-specific components
        self.intent_parser = IntentParser()
        self.task_planner = TaskPlanner()
        
        # Active sessions and plans
        self.active_sessions: Dict[str, AgentContext] = {}
        self.execution_plans: Dict[str, Dict[str, Any]] = {}
        self.agent_registry: Dict[str, str] = {}  # agent_type -> agent_id
        
        # Register message handlers
        self.register_message_handlers()
        
        self.logger.info("Orchestrator Agent initialized")
    
    def register_message_handlers(self) -> None:
        """Register handlers for different message types."""
        self.register_message_handler("user_intent", self.handle_user_intent)
        self.register_message_handler("agent_response", self.handle_agent_response)
        self.register_message_handler("task_complete", self.handle_task_complete)
        self.register_message_handler("task_error", self.handle_task_error)
        self.register_message_handler("agent_registration", self.handle_agent_registration)
        self.register_message_handler("status_request", self.handle_status_request)
    
    async def handle_user_intent(self, message: Message) -> None:
        """Handle user intent messages."""
        user_input = message.content.get("intent", "")
        session_id = message.content.get("session_id", f"session_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}")
        
        self.logger.info(f"Processing user intent for session {session_id}: {user_input}")
        
        try:
            # Parse intent
            parsed_intent = self.intent_parser.parse_intent(user_input)
            self.logger.info(f"Parsed intent: {parsed_intent}")
            
            # Create execution plan
            execution_plan = self.task_planner.create_execution_plan(parsed_intent)
            self.logger.info(f"Created execution plan: {execution_plan['plan_id']}")
            
            # Create session context
            context = AgentContext(
                session_id=session_id,
                user_intent=user_input,
                current_phase="discovery"
            )
            context.shared_data.update({
                "parsed_intent": parsed_intent,
                "execution_plan": execution_plan,
                "current_phase_index": 0,
                "completed_tasks": [],
                "failed_tasks": []
            })
            
            # Store session and plan
            self.active_sessions[session_id] = context
            self.execution_plans[execution_plan['plan_id']] = execution_plan
            
            # Start execution
            await self._start_plan_execution(session_id, execution_plan)
            
        except Exception as e:
            self.logger.error(f"Error processing user intent: {e}")
            await self._send_error_response(message.sender_id, str(e))
    
    async def handle_agent_response(self, message: Message) -> None:
        """Handle responses from other agents."""
        session_id = message.content.get("session_id")
        task_id = message.content.get("task_id")
        result = message.content.get("result")
        
        self.logger.info(f"Received agent response for session {session_id}, task {task_id}")
        
        if session_id in self.active_sessions:
            context = self.active_sessions[session_id]
            context.shared_data["completed_tasks"].append({
                "task_id": task_id,
                "agent_id": message.sender_id,
                "result": result,
                "completed_at": datetime.utcnow()
            })
            
            await self._check_phase_completion(session_id)
    
    async def handle_task_complete(self, message: Message) -> None:
        """Handle task completion notifications."""
        await self.handle_agent_response(message)
    
    async def handle_task_error(self, message: Message) -> None:
        """Handle task error notifications."""
        session_id = message.content.get("session_id")
        task_id = message.content.get("task_id")
        error = message.content.get("error")
        
        if not session_id or not task_id or not error:
            self.logger.warning("Invalid task error message received")
            return
        
        self.logger.error(f"Task error in session {session_id}, task {task_id}: {error}")
        
        if session_id in self.active_sessions:
            context = self.active_sessions[session_id]
            context.shared_data["failed_tasks"].append({
                "task_id": task_id,
                "agent_id": message.sender_id,
                "error": error,
                "failed_at": datetime.utcnow()
            })
            
            # Implement error recovery logic here
            await self._handle_task_failure(session_id, str(task_id), str(error))
    
    async def handle_agent_registration(self, message: Message) -> None:
        """Handle agent registration messages."""
        agent_type = message.content.get("agent_type")
        agent_id = message.sender_id
        
        if not agent_type:
            self.logger.warning("Agent registration without agent_type")
            return
        
        self.agent_registry[str(agent_type)] = agent_id
        self.logger.info(f"Registered agent {agent_id} for type {agent_type}")
        
        # Send acknowledgment
        await self.send_message(
            receiver_id=agent_id,
            message_type="registration_ack",
            content={"status": "registered"}
        )
    
    async def handle_status_request(self, message: Message) -> None:
        """Handle status request messages."""
        session_id = message.content.get("session_id")
        
        if session_id and session_id in self.active_sessions:
            context = self.active_sessions[session_id]
            status = {
                "session_id": session_id,
                "current_phase": context.current_phase,
                "progress": self._calculate_progress(context),
                "completed_tasks": len(context.shared_data.get("completed_tasks", [])),
                "failed_tasks": len(context.shared_data.get("failed_tasks", []))
            }
        else:
            status = {
                "active_sessions": len(self.active_sessions),
                "registered_agents": len(self.agent_registry)
            }
        
        await self.send_message(
            receiver_id=message.sender_id,
            message_type="status_response",
            content=status
        )
    
    async def _start_plan_execution(self, session_id: str, execution_plan: Dict[str, Any]) -> None:
        """Start executing an execution plan."""
        context = self.active_sessions[session_id]
        
        # Start with the first phase
        if execution_plan['phases']:
            first_phase = execution_plan['phases'][0]
            context.current_phase = first_phase['phase_id']
            
            await self._execute_phase_tasks(session_id, first_phase)
    
    async def _execute_phase_tasks(self, session_id: str, phase: Dict[str, Any]) -> None:
        """Execute all tasks in a phase."""
        context = self.active_sessions[session_id]
        
        self.logger.info(f"Starting phase {phase['phase_id']} for session {session_id}")
        
        # Send tasks to appropriate agents
        for task in phase['tasks']:
            agent_type = task['agent']
            if agent_type in self.agent_registry:
                agent_id = self.agent_registry[agent_type]
                
                await self.send_message(
                    receiver_id=agent_id,
                    message_type="task_assignment",
                    content={
                        "session_id": session_id,
                        "task": task,
                        "context": context.shared_data
                    }
                )
            else:
                self.logger.warning(f"Agent type {agent_type} not registered")
    
    async def _check_phase_completion(self, session_id: str) -> None:
        """Check if current phase is complete and move to next phase."""
        context = self.active_sessions[session_id]
        execution_plan = context.shared_data["execution_plan"]
        current_phase_index = context.shared_data["current_phase_index"]
        
        if current_phase_index < len(execution_plan['phases']):
            current_phase = execution_plan['phases'][current_phase_index]
            completed_tasks = context.shared_data.get("completed_tasks", [])
            
            # Check if all tasks in current phase are complete
            phase_task_ids = {task['task_id'] for task in current_phase['tasks']}
            completed_task_ids = {task['task_id'] for task in completed_tasks}
            
            if phase_task_ids.issubset(completed_task_ids):
                self.logger.info(f"Phase {current_phase['phase_id']} completed for session {session_id}")
                
                # Move to next phase
                next_phase_index = current_phase_index + 1
                if next_phase_index < len(execution_plan['phases']):
                    next_phase = execution_plan['phases'][next_phase_index]
                    context.current_phase = next_phase['phase_id']
                    context.shared_data["current_phase_index"] = next_phase_index
                    
                    await self._execute_phase_tasks(session_id, next_phase)
                else:
                    # All phases complete
                    await self._complete_session(session_id)
    
    async def _complete_session(self, session_id: str) -> None:
        """Complete a session."""
        self.logger.info(f"Session {session_id} completed successfully")
        
        context = self.active_sessions[session_id]
        # TODO: Send completion notification to user
        # TODO: Clean up resources
        
        # Mark session as completed but keep for history
        context.shared_data["status"] = "completed"
        context.shared_data["completed_at"] = datetime.utcnow()
    
    async def _handle_task_failure(self, session_id: str, task_id: str, error: str) -> None:
        """Handle task failure and implement recovery."""
        self.logger.error(f"Implementing error recovery for task {task_id} in session {session_id}")
        
        # TODO: Implement sophisticated error recovery
        # For now, just log the error
        # Could implement retry logic, alternative task routing, etc.
    
    def _calculate_progress(self, context: AgentContext) -> float:
        """Calculate session progress as percentage."""
        execution_plan = context.shared_data.get("execution_plan", {})
        total_tasks = sum(len(phase['tasks']) for phase in execution_plan.get('phases', []))
        
        if total_tasks == 0:
            return 0.0
        
        completed_tasks = len(context.shared_data.get("completed_tasks", []))
        return (completed_tasks / total_tasks) * 100.0
    
    async def _send_error_response(self, recipient_id: str, error_message: str) -> None:
        """Send error response to user or agent."""
        await self.send_message(
            receiver_id=recipient_id,
            message_type="error_response",
            content={"error": error_message}
        )
    
    async def process_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Process a task assigned to the orchestrator."""
        # The orchestrator mainly coordinates rather than processing tasks directly
        return {"status": "delegated", "message": "Task delegated to appropriate agent"}
    
    def get_session_status(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a specific session."""
        if session_id in self.active_sessions:
            context = self.active_sessions[session_id]
            return {
                "session_id": session_id,
                "current_phase": context.current_phase,
                "progress": self._calculate_progress(context),
                "user_intent": context.user_intent,
                "created_at": context.created_at,
                "updated_at": context.updated_at
            }
        return None
    
    def get_all_sessions(self) -> List[Dict[str, Any]]:
        """Get status of all active sessions."""
        sessions = []
        for session_id in self.active_sessions.keys():
            status = self.get_session_status(session_id)
            if status:
                sessions.append(status)
        return sessions
