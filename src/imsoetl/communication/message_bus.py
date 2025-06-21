"""
Agent-to-Agent Communication Layer for IMSOETL.

This module provides the infrastructure for agents to communicate with each other
through messages, events, and shared state management.
"""

import asyncio
import json
import logging
from typing import Dict, List, Optional, Any, Callable, Awaitable
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from enum import Enum
import uuid

from ..core.base_agent import Message, BaseAgent

logger = logging.getLogger(__name__)


class MessagePriority(Enum):
    """Message priority levels."""
    LOW = 1
    NORMAL = 2
    HIGH = 3
    URGENT = 4


class MessageStatus(Enum):
    """Message status."""
    PENDING = "pending"
    DELIVERED = "delivered"
    FAILED = "failed"
    ACKNOWLEDGED = "acknowledged"


@dataclass
class MessageMetadata:
    """Metadata for messages."""
    priority: MessagePriority = MessagePriority.NORMAL
    timeout: Optional[float] = None
    retries: int = 0
    max_retries: int = 3
    created_at: Optional[datetime] = None
    delivery_attempts: Optional[List[datetime]] = None
    status: MessageStatus = MessageStatus.PENDING
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now(timezone.utc)
        if self.delivery_attempts is None:
            self.delivery_attempts = []


class MessageBus:
    """Central message bus for agent communication."""
    
    def __init__(self):
        self.agents: Dict[str, BaseAgent] = {}
        self.message_queue: asyncio.Queue = asyncio.Queue()
        self.subscriptions: Dict[str, List[Callable]] = {}
        self.message_history: List[Dict] = []
        self.running = False
        self._worker_task: Optional[asyncio.Task] = None
        
    async def start(self):
        """Start the message bus."""
        if self.running:
            return
            
        self.running = True
        self._worker_task = asyncio.create_task(self._message_worker())
        logger.info("Message bus started")
        
    async def stop(self):
        """Stop the message bus."""
        if not self.running:
            return
            
        self.running = False
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
        logger.info("Message bus stopped")
        
    def register_agent(self, agent: BaseAgent):
        """Register an agent with the message bus."""
        self.agents[agent.agent_id] = agent
        logger.info(f"Agent {agent.agent_id} registered with message bus")
        
    def unregister_agent(self, agent_id: str):
        """Unregister an agent from the message bus."""
        if agent_id in self.agents:
            del self.agents[agent_id]
            logger.info(f"Agent {agent_id} unregistered from message bus")
            
    async def send_message(self, message: Message, metadata: Optional[MessageMetadata] = None):
        """Send a message through the bus."""
        if metadata is None:
            metadata = MessageMetadata()
            
        message_envelope = {
            "message": message,
            "metadata": metadata,
            "id": str(uuid.uuid4())
        }
        
        await self.message_queue.put(message_envelope)
        logger.debug(f"Message queued: {message.message_type} from {message.sender_id} to {message.receiver_id}")
        
    async def _message_worker(self):
        """Worker task to process messages."""
        while self.running:
            try:
                # Get message from queue with timeout
                message_envelope = await asyncio.wait_for(
                    self.message_queue.get(), 
                    timeout=1.0
                )
                
                await self._deliver_message(message_envelope)
                
            except asyncio.TimeoutError:
                # Continue loop if no messages
                continue
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                
    async def _deliver_message(self, message_envelope: Dict):
        """Deliver a message to the target agent."""
        message = message_envelope["message"]
        metadata = message_envelope["metadata"]
        
        # Update delivery attempts
        metadata.delivery_attempts.append(datetime.now(timezone.utc))
        
        try:
            target_agent = self.agents.get(message.receiver_id)
            if not target_agent:
                logger.warning(f"Target agent {message.receiver_id} not found")
                metadata.status = MessageStatus.FAILED
                return
                
            # Deliver message to agent
            if hasattr(target_agent, 'receive_message'):
                await target_agent.receive_message(message)
                metadata.status = MessageStatus.DELIVERED
                logger.debug(f"Message delivered to {message.receiver_id}")
            else:
                logger.warning(f"Agent {message.receiver_id} cannot receive messages")
                metadata.status = MessageStatus.FAILED
                
        except Exception as e:
            logger.error(f"Failed to deliver message: {e}")
            metadata.status = MessageStatus.FAILED
            
            # Retry logic
            if metadata.retries < metadata.max_retries:
                metadata.retries += 1
                metadata.status = MessageStatus.PENDING
                await asyncio.sleep(1)  # Wait before retry
                await self.message_queue.put(message_envelope)
                logger.debug(f"Retrying message delivery (attempt {metadata.retries})")
                
        # Store in history
        self.message_history.append({
            "message_id": message_envelope["id"],
            "sender": message.sender_id,
            "receiver": message.receiver_id,
            "type": message.message_type,
            "status": metadata.status.value,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "retries": metadata.retries
        })
        
    def subscribe(self, event_type: str, callback: Callable):
        """Subscribe to event types."""
        if event_type not in self.subscriptions:
            self.subscriptions[event_type] = []
        self.subscriptions[event_type].append(callback)
        
    async def publish_event(self, event_type: str, data: Dict[str, Any]):
        """Publish an event to subscribers."""
        if event_type in self.subscriptions:
            for callback in self.subscriptions[event_type]:
                try:
                    if asyncio.iscoroutinefunction(callback):
                        await callback(data)
                    else:
                        callback(data)
                except Exception as e:
                    logger.error(f"Error in event callback: {e}")
                    
    def get_message_history(self, limit: int = 100) -> List[Dict]:
        """Get recent message history."""
        return self.message_history[-limit:]
        
    def get_agent_stats(self) -> Dict[str, Any]:
        """Get statistics about registered agents."""
        return {
            "total_agents": len(self.agents),
            "agent_ids": list(self.agents.keys()),
            "message_queue_size": self.message_queue.qsize(),
            "running": self.running
        }


class CommunicationManager:
    """Manages communication between agents."""
    
    def __init__(self):
        self.message_bus = MessageBus()
        self.shared_state: Dict[str, Any] = {}
        self.locks: Dict[str, asyncio.Lock] = {}
        
    async def start(self):
        """Start the communication manager."""
        await self.message_bus.start()
        
    async def stop(self):
        """Stop the communication manager."""
        await self.message_bus.stop()
        
    def register_agent(self, agent: BaseAgent):
        """Register an agent for communication."""
        self.message_bus.register_agent(agent)
        
    def unregister_agent(self, agent_id: str):
        """Unregister an agent."""
        self.message_bus.unregister_agent(agent_id)
        
    async def send_message(self, message: Message, priority: MessagePriority = MessagePriority.NORMAL):
        """Send a message between agents."""
        metadata = MessageMetadata(priority=priority)
        await self.message_bus.send_message(message, metadata)
        
    async def broadcast_message(self, sender_id: str, message_type: str, content: Dict[str, Any]):
        """Broadcast a message to all agents."""
        for agent_id in self.message_bus.agents.keys():
            if agent_id != sender_id:
                message = Message(
                    sender_id=sender_id,
                    receiver_id=agent_id,
                    message_type=message_type,
                    content=content
                )
                await self.send_message(message)
                
    async def set_shared_state(self, key: str, value: Any):
        """Set a value in shared state."""
        lock = self.locks.get(key)
        if not lock:
            lock = asyncio.Lock()
            self.locks[key] = lock
            
        async with lock:
            self.shared_state[key] = value
            
        # Notify agents of state change
        await self.message_bus.publish_event("state_changed", {
            "key": key,
            "value": value,
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
        
    async def get_shared_state(self, key: str, default: Any = None) -> Any:
        """Get a value from shared state."""
        lock = self.locks.get(key)
        if not lock:
            return self.shared_state.get(key, default)
            
        async with lock:
            return self.shared_state.get(key, default)
            
    def subscribe_to_events(self, event_type: str, callback: Callable):
        """Subscribe to communication events."""
        self.message_bus.subscribe(event_type, callback)
        
    def get_communication_stats(self) -> Dict[str, Any]:
        """Get communication statistics."""
        stats = self.message_bus.get_agent_stats()
        stats.update({
            "shared_state_keys": len(self.shared_state),
            "active_locks": len(self.locks),
            "recent_messages": len(self.message_bus.get_message_history(10))
        })
        return stats


# Global communication manager instance
_communication_manager = None


def get_communication_manager() -> CommunicationManager:
    """Get the global communication manager instance."""
    global _communication_manager
    if _communication_manager is None:
        _communication_manager = CommunicationManager()
    return _communication_manager


__all__ = [
    'MessagePriority',
    'MessageStatus',
    'MessageMetadata',
    'MessageBus',
    'CommunicationManager',
    'get_communication_manager'
]
