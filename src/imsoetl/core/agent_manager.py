"""
Agent Manager - Handles automatic startup and registration of required agents.

This manager is responsible for:
- Starting required agents based on execution plan needs
- Managing agent lifecycle
- Ensuring agents are properly registered with the orchestrator
- Handling agent health monitoring and restart
"""

import asyncio
import logging
from typing import Dict, List, Set, Any, Optional, Type
from datetime import datetime, timezone

from .base_agent import BaseAgent, AgentType, Message
from ..agents.discovery import DiscoveryAgent
from ..agents.schema import SchemaAgent
from ..agents.quality import QualityAgent
from ..agents.transformation import TransformationAgent
from ..agents.execution import ExecutionAgent
from ..agents.monitoring import MonitoringAgent


class AgentManager:
    """Manages the lifecycle of all IMSOETL agents."""
    
    # Registry of available agent classes
    AGENT_CLASSES: Dict[AgentType, Type[BaseAgent]] = {
        AgentType.DISCOVERY: DiscoveryAgent,
        AgentType.SCHEMA: SchemaAgent,
        AgentType.QUALITY: QualityAgent,
        AgentType.TRANSFORMATION: TransformationAgent,
        AgentType.EXECUTION: ExecutionAgent,
        AgentType.MONITORING: MonitoringAgent,
    }
    
    def __init__(self, orchestrator_id: str, config: Optional[Dict[str, Any]] = None):
        self.orchestrator_id = orchestrator_id
        self.config = config or {}
        self.logger = logging.getLogger(f"imsoetl.core.agent_manager")
        
        # Reference to orchestrator for message passing
        self.orchestrator: Optional[Any] = None
        
        # Active agents registry
        self.active_agents: Dict[AgentType, BaseAgent] = {}
        self.agent_tasks: Dict[AgentType, asyncio.Task] = {}
        self.registration_status: Dict[AgentType, bool] = {}
        
        # Agent startup configuration
        self.startup_timeout = self.config.get("agent_startup_timeout", 30.0)
        self.registration_timeout = self.config.get("agent_registration_timeout", 10.0)
        
        self.logger.info("Agent Manager initialized")
    
    def set_orchestrator(self, orchestrator: Any) -> None:
        """Set the orchestrator reference for message passing."""
        self.orchestrator = orchestrator
    
    async def ensure_agents_available(self, required_agents: List[str], orchestrator: Optional[Any] = None) -> Dict[str, bool]:
        """
        Ensure all required agents are started and registered.
        
        Args:
            required_agents: List of agent type strings that need to be available
            orchestrator: Optional orchestrator instance to register agents with
            
        Returns:
            Dict mapping agent types to their availability status
        """
        agent_types = []
        for agent_str in required_agents:
            try:
                agent_type = AgentType(agent_str)
                agent_types.append(agent_type)
            except ValueError:
                self.logger.warning(f"Unknown agent type: {agent_str}")
                continue
        
        self.logger.info(f"Ensuring agents are available: {[at.value for at in agent_types]}")
        
        # Start missing agents
        startup_results = await self._start_missing_agents(agent_types)
        
        # Directly register agents with orchestrator if provided
        if orchestrator:
            for agent_type in agent_types:
                if startup_results.get(agent_type, False) and agent_type in self.active_agents:
                    agent = self.active_agents[agent_type]
                    orchestrator.agent_registry[agent_type.value] = agent.agent_id
                    self.mark_agent_registered(agent_type)
                    self.logger.info(f"Registered agent {agent.agent_id} with orchestrator")
        
        # For compatibility, mark all started agents as registered
        registration_results = {}
        for agent_type in agent_types:
            registration_results[agent_type] = startup_results.get(agent_type, False)
        
        # Combine results
        results = {}
        for agent_type in agent_types:
            agent_str = agent_type.value
            started = startup_results.get(agent_type, False)
            registered = registration_results.get(agent_type, False)
            results[agent_str] = started and registered
            
            if results[agent_str]:
                self.logger.info(f"Agent {agent_str} is ready")
            else:
                self.logger.error(f"Agent {agent_str} failed to start or register")
        
        return results
    
    async def start_agent(self, agent_type: AgentType) -> bool:
        """
        Start a specific agent.
        
        Args:
            agent_type: The type of agent to start
            
        Returns:
            True if agent started successfully, False otherwise
        """
        if agent_type in self.active_agents:
            self.logger.info(f"Agent {agent_type.value} is already running")
            return True
        
        if agent_type not in self.AGENT_CLASSES:
            self.logger.error(f"No agent class registered for type: {agent_type.value}")
            return False
        
        try:
            self.logger.info(f"Starting agent: {agent_type.value}")
            
            # Create agent instance
            agent_class = self.AGENT_CLASSES[agent_type]
            agent_config = self.config.get("agents", {}).get(agent_type.value, {})
            
            # Try to create agent with config first, fallback to no config
            try:
                agent = agent_class(  # type: ignore
                    agent_id=f"{agent_type.value}_agent_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                    config=agent_config
                )
            except TypeError:
                # Agent doesn't accept config parameter
                agent = agent_class(  # type: ignore
                    agent_id=f"{agent_type.value}_agent_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                )
            
            # Start the agent
            await asyncio.wait_for(agent.start(), timeout=self.startup_timeout)
            
            # Set message dispatcher if orchestrator is available
            if self.orchestrator:
                agent.message_dispatcher = self.orchestrator._dispatch_message
            
            # Store agent and create monitoring task
            self.active_agents[agent_type] = agent
            self.agent_tasks[agent_type] = asyncio.create_task(
                self._monitor_agent(agent_type, agent)
            )
            
            # Trigger agent registration with orchestrator
            await self._register_agent_with_orchestrator(agent)
            
            self.logger.info(f"Agent {agent_type.value} started successfully")
            return True
            
        except asyncio.TimeoutError:
            self.logger.error(f"Timeout starting agent {agent_type.value}")
            return False
        except Exception as e:
            self.logger.error(f"Error starting agent {agent_type.value}: {e}")
            return False
    
    async def stop_agent(self, agent_type: AgentType) -> bool:
        """
        Stop a specific agent.
        
        Args:
            agent_type: The type of agent to stop
            
        Returns:
            True if agent stopped successfully, False otherwise
        """
        if agent_type not in self.active_agents:
            self.logger.warning(f"Agent {agent_type.value} is not running")
            return True
        
        try:
            self.logger.info(f"Stopping agent: {agent_type.value}")
            
            # Stop the agent
            agent = self.active_agents[agent_type]
            await agent.stop()
            
            # Cancel monitoring task
            if agent_type in self.agent_tasks:
                task = self.agent_tasks[agent_type]
                if not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                del self.agent_tasks[agent_type]
            
            # Clean up
            del self.active_agents[agent_type]
            self.registration_status.pop(agent_type, None)
            
            self.logger.info(f"Agent {agent_type.value} stopped successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error stopping agent {agent_type.value}: {e}")
            return False
    
    async def stop_all_agents(self) -> None:
        """Stop all active agents."""
        self.logger.info("Stopping all agents")
        
        # Stop all agents concurrently
        stop_tasks = []
        for agent_type in list(self.active_agents.keys()):
            task = asyncio.create_task(self.stop_agent(agent_type))
            stop_tasks.append(task)
        
        if stop_tasks:
            await asyncio.gather(*stop_tasks, return_exceptions=True)
        
        self.logger.info("All agents stopped")
    
    def get_agent_status(self) -> Dict[str, Dict[str, Any]]:
        """Get status of all managed agents."""
        status = {}
        
        for agent_type, agent in self.active_agents.items():
            status[agent_type.value] = {
                "running": True,
                "registered": self.registration_status.get(agent_type, False),
                "agent_id": agent.agent_id,
                "status": agent.status.value,
                "stats": agent.get_stats()
            }
        
        # Add stopped agents
        for agent_type in AgentType:
            if agent_type not in self.active_agents and agent_type != AgentType.ORCHESTRATOR:
                status[agent_type.value] = {
                    "running": False,
                    "registered": False,
                    "agent_id": None,
                    "status": "stopped",
                    "stats": None
                }
        
        return status
    
    def mark_agent_registered(self, agent_type: AgentType) -> None:
        """Mark an agent as successfully registered with the orchestrator."""
        self.registration_status[agent_type] = True
        self.logger.info(f"Agent {agent_type.value} marked as registered")
    
    async def _start_missing_agents(self, required_agents: List[AgentType]) -> Dict[AgentType, bool]:
        """Start any agents that are not currently running."""
        missing_agents = [
            agent_type for agent_type in required_agents 
            if agent_type not in self.active_agents
        ]
        
        if not missing_agents:
            return {agent_type: True for agent_type in required_agents}
        
        self.logger.info(f"Starting missing agents: {[at.value for at in missing_agents]}")
        
        # Start missing agents concurrently
        startup_tasks = []
        for agent_type in missing_agents:
            task = asyncio.create_task(self.start_agent(agent_type))
            startup_tasks.append((agent_type, task))
        
        # Wait for startup results
        results = {}
        for agent_type, task in startup_tasks:
            try:
                success = await task
                results[agent_type] = success
            except Exception as e:
                self.logger.error(f"Failed to start agent {agent_type.value}: {e}")
                results[agent_type] = False
        
        # Add already running agents
        for agent_type in required_agents:
            if agent_type not in results:
                results[agent_type] = True
        
        return results
    
    async def _wait_for_registrations(self, required_agents: List[AgentType]) -> Dict[AgentType, bool]:
        """Wait for agents to register with the orchestrator."""
        results = {}
        
        for agent_type in required_agents:
            if agent_type not in self.active_agents:
                results[agent_type] = False
                continue
            
            # Wait for registration or timeout
            try:
                timeout_time = asyncio.get_event_loop().time() + self.registration_timeout
                
                while asyncio.get_event_loop().time() < timeout_time:
                    if self.registration_status.get(agent_type, False):
                        results[agent_type] = True
                        break
                    await asyncio.sleep(0.1)
                else:
                    # Timeout
                    self.logger.warning(f"Timeout waiting for {agent_type.value} registration")
                    results[agent_type] = False
                    
            except Exception as e:
                self.logger.error(f"Error waiting for {agent_type.value} registration: {e}")
                results[agent_type] = False
        
        return results
    
    async def _register_agent_with_orchestrator(self, agent: BaseAgent) -> None:
        """Register agent directly with orchestrator."""
        try:
            # Direct registration instead of message passing
            # Since we don't have a full message bus implementation yet
            self.logger.debug(f"Registering agent {agent.agent_id} of type {agent.agent_type.value}")
            
            # We'll mark it as registered immediately since we're managing it directly
            # In a full implementation, this would send a message through the message bus
            
        except Exception as e:
            self.logger.error(f"Failed to register {agent.agent_id}: {e}")
    
    async def _monitor_agent(self, agent_type: AgentType, agent: BaseAgent) -> None:
        """Monitor an agent and handle restarts if needed."""
        self.logger.debug(f"Starting monitoring for agent {agent_type.value}")
        
        try:
            while agent_type in self.active_agents:
                # Check agent health
                if hasattr(agent, 'health_check'):
                    try:
                        healthy = await agent.health_check()  # type: ignore
                        if not healthy:
                            self.logger.warning(f"Agent {agent_type.value} failed health check")
                            # Could implement restart logic here
                    except Exception as e:
                        self.logger.error(f"Health check failed for {agent_type.value}: {e}")
                
                await asyncio.sleep(30)  # Check every 30 seconds
                
        except asyncio.CancelledError:
            self.logger.debug(f"Monitoring cancelled for agent {agent_type.value}")
        except Exception as e:
            self.logger.error(f"Error monitoring agent {agent_type.value}: {e}")
