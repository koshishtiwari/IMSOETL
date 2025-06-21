"""
Base Agent class providing common functionality for all IMSOETL agents.
"""

import asyncio
import logging
import uuid
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Union, Callable, Awaitable


class AgentStatus(str, Enum):
    """Agent status enumeration."""
    IDLE = "idle"
    BUSY = "busy"
    ERROR = "error"
    STOPPED = "stopped"


class AgentType(str, Enum):
    """Agent type enumeration."""
    ORCHESTRATOR = "orchestrator"
    DISCOVERY = "discovery"
    SCHEMA = "schema"
    TRANSFORMATION = "transformation"
    QUALITY = "quality"
    EXECUTION = "execution"
    MONITORING = "monitoring"


class Message:
    """Inter-agent communication message."""
    
    def __init__(
        self,
        sender_id: str,
        receiver_id: str,
        message_type: str,
        content: Dict[str, Any],
        correlation_id: Optional[str] = None,
        reply_to: Optional[str] = None
    ):
        self.id = str(uuid.uuid4())
        self.sender_id = sender_id
        self.receiver_id = receiver_id
        self.message_type = message_type
        self.content = content
        self.timestamp = datetime.now(timezone.utc)
        self.correlation_id = correlation_id
        self.reply_to = reply_to


class AgentContext:
    """Shared context between agents."""
    
    def __init__(
        self,
        session_id: str,
        user_intent: str,
        current_phase: str,
        shared_data: Optional[Dict[str, Any]] = None
    ):
        self.session_id = session_id
        self.user_intent = user_intent
        self.current_phase = current_phase
        self.shared_data = shared_data or {}
        self.created_at = datetime.now(timezone.utc)
        self.updated_at = datetime.now(timezone.utc)


class BaseAgent(ABC):
    """
    Base class for all IMSOETL agents.
    
    Provides common functionality including:
    - Message handling and communication
    - State management
    - Logging and monitoring
    - Error handling
    - Context management
    """
    
    def __init__(
        self,
        agent_id: str,
        agent_type: AgentType,
        name: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None
    ):
        self.agent_id = agent_id
        self.agent_type = agent_type
        self.name = name or f"{agent_type.value}_{agent_id}"
        self.config = config or {}
        self.status = AgentStatus.IDLE
        self.context: Optional[AgentContext] = None
        
        # Message dispatcher (can be set externally)
        self.message_dispatcher: Optional[Callable] = None
        
        # Set up logging
        self.logger = logging.getLogger(f"imsoetl.{self.agent_type.value}.{self.agent_id}")
        
        # Message handling
        self.message_queue: asyncio.Queue = asyncio.Queue()
        self.message_handlers: Dict[str, Callable] = {}
        self.response_handlers: Dict[str, asyncio.Future] = {}
        
        # Task management
        self.current_task: Optional[asyncio.Task] = None
        self.background_tasks: List[asyncio.Task] = []
        
        # Statistics
        self.stats = {
            "messages_sent": 0,
            "messages_received": 0,
            "tasks_completed": 0,
            "errors": 0,
            "started_at": datetime.now(timezone.utc)
        }
        
        self.logger.info(f"Agent {self.name} initialized with config: {self.config}")
    
    async def start(self) -> None:
        """Start the agent and begin processing messages."""
        self.logger.info(f"Starting agent {self.name}")
        self.status = AgentStatus.IDLE
        
        # Start message processing loop
        self.current_task = asyncio.create_task(self._message_loop())
        
        # Start any background tasks
        await self._start_background_tasks()
        
        self.logger.info(f"Agent {self.name} started successfully")
    
    async def stop(self) -> None:
        """Stop the agent and clean up resources."""
        self.logger.info(f"Stopping agent {self.name}")
        self.status = AgentStatus.STOPPED
        
        # Cancel current task
        if self.current_task and not self.current_task.done():
            self.current_task.cancel()
            try:
                await self.current_task
            except asyncio.CancelledError:
                pass
        
        # Cancel background tasks
        for task in self.background_tasks:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        
        self.logger.info(f"Agent {self.name} stopped")
    
    async def send_message(
        self,
        receiver_id: str,
        message_type: str,
        content: Dict[str, Any],
        correlation_id: Optional[str] = None,
        reply_to: Optional[str] = None
    ) -> str:
        """Send a message to another agent."""
        message = Message(
            sender_id=self.agent_id,
            receiver_id=receiver_id,
            message_type=message_type,
            content=content,
            correlation_id=correlation_id,
            reply_to=reply_to
        )
        
        self.logger.debug(
            f"Sending message {message.id} to {receiver_id}: {message_type}"
        )
        
        await self._dispatch_message(message)
        self.stats["messages_sent"] += 1
        
        return message.id
    
    async def send_request(
        self,
        receiver_id: str,
        message_type: str,
        content: Dict[str, Any],
        timeout: float = 30.0
    ) -> Dict[str, Any]:
        """Send a request message and wait for response."""
        correlation_id = str(uuid.uuid4())
        
        # Create future for response
        response_future = asyncio.Future()
        self.response_handlers[correlation_id] = response_future
        
        try:
            # Send request
            await self.send_message(
                receiver_id=receiver_id,
                message_type=message_type,
                content=content,
                correlation_id=correlation_id
            )
            
            # Wait for response
            response = await asyncio.wait_for(response_future, timeout=timeout)
            return response
            
        except asyncio.TimeoutError:
            self.logger.error(
                f"Request timeout to {receiver_id} for {message_type} "
                f"(correlation_id: {correlation_id})"
            )
            raise
        finally:
            # Clean up response handler
            self.response_handlers.pop(correlation_id, None)
    
    async def receive_message(self, message: Message) -> None:
        """Receive a message from another agent."""
        self.logger.debug(
            f"Received message {message.id} from {message.sender_id}: {message.message_type}"
        )
        
        await self.message_queue.put(message)
        self.stats["messages_received"] += 1
    
    def register_message_handler(self, message_type: str, handler: Callable) -> None:
        """Register a handler for a specific message type."""
        self.message_handlers[message_type] = handler
        self.logger.debug(f"Registered message handler for {message_type}")
    
    def update_context(self, context: AgentContext) -> None:
        """Update the agent's context."""
        self.context = context
        context.updated_at = datetime.now(timezone.utc)
        self.logger.debug(f"Context updated for session {context.session_id}")
    
    async def _message_loop(self) -> None:
        """Main message processing loop."""
        while self.status != AgentStatus.STOPPED:
            try:
                # Get next message with timeout
                message = await asyncio.wait_for(
                    self.message_queue.get(),
                    timeout=1.0
                )
                
                await self._process_message(message)
                
            except asyncio.TimeoutError:
                # Timeout is normal, continue processing
                continue
            except Exception as e:
                self.logger.error(f"Error in message loop: {e}")
                self.stats["errors"] += 1
                self.status = AgentStatus.ERROR
                await asyncio.sleep(1)  # Brief pause before retrying
    
    async def _process_message(self, message: Message) -> None:
        """Process a single message."""
        try:
            self.status = AgentStatus.BUSY
            
            # Handle response messages
            if message.correlation_id and message.correlation_id in self.response_handlers:
                response_future = self.response_handlers[message.correlation_id]
                response_future.set_result(message.content)
                return
            
            # Handle regular messages
            handler = self.message_handlers.get(message.message_type)
            if handler:
                await handler(message)
            else:
                await self._handle_unknown_message(message)
            
            # Send reply if requested
            if message.reply_to:
                await self._send_reply(message)
            
        except Exception as e:
            self.logger.error(
                f"Error processing message {message.id} ({message.message_type}): {e}"
            )
            self.stats["errors"] += 1
        finally:
            self.status = AgentStatus.IDLE
    
    async def _handle_unknown_message(self, message: Message) -> None:
        """Handle unknown message types."""
        self.logger.warning(
            f"Unknown message type {message.message_type} from {message.sender_id}"
        )
    
    async def _send_reply(self, original_message: Message) -> None:
        """Send a reply to a message."""
        # Default implementation - can be overridden by subclasses
        pass
    
    async def _dispatch_message(self, message: Message) -> None:
        """Dispatch a message to its intended recipient."""
        if self.message_dispatcher:
            # Use custom dispatcher if available
            await self.message_dispatcher(message)
        else:
            # Default implementation - just log the message
            self.logger.debug(f"Message {message.id} dispatched (no dispatcher configured)")
    
    async def _start_background_tasks(self) -> None:
        """Start any background tasks specific to this agent."""
        # Override in subclasses if needed
        pass
    
    @abstractmethod
    async def process_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Process a task assigned to this agent."""
        pass
    
    def get_stats(self) -> Dict[str, Any]:
        """Get agent statistics."""
        return {
            **self.stats,
            "status": self.status.value,
            "uptime": (datetime.now(timezone.utc) - self.stats["started_at"]).total_seconds()
        }
    
    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}(id={self.agent_id}, type={self.agent_type.value})>"
