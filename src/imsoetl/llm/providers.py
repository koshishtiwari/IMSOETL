"""LLM provider implementations for IMSOETL."""

import json
import logging
import requests
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any
import google.generativeai as genai
from ..core.errors import IMSOETLError


logger = logging.getLogger(__name__)


class LLMError(IMSOETLError):
    """LLM-related errors."""
    pass


class LLMProvider(ABC):
    """Abstract base class for LLM providers."""
    
    def __init__(self, name: str, config: Dict[str, Any]):
        self.name = name
        self.config = config
        self.is_available = False
        
    @abstractmethod
    async def initialize(self) -> bool:
        """Initialize the provider. Returns True if successful."""
        pass
        
    @abstractmethod
    async def generate(self, prompt: str, **kwargs) -> str:
        """Generate text from a prompt."""
        pass
        
    @abstractmethod
    async def chat(self, messages: List[Dict[str, str]], **kwargs) -> str:
        """Generate response from a conversation."""
        pass
        
    def is_ready(self) -> bool:
        """Check if provider is ready to use."""
        return self.is_available


class OllamaProvider(LLMProvider):
    """Ollama LLM provider for local inference."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__("ollama", config)
        self.base_url = config.get("base_url", "http://localhost:11434")
        self.model = config.get("model", "gemma3:4b")
        self.timeout = config.get("timeout", 30)
        
    async def initialize(self) -> bool:
        """Check if Ollama is available and the model exists."""
        try:
            # Check if Ollama is running
            response = requests.get(f"{self.base_url}/api/tags", timeout=5)
            if response.status_code != 200:
                logger.warning(f"Ollama not available at {self.base_url}")
                return False
                
            # Check if the model exists
            models = response.json().get("models", [])
            model_names = [model["name"] for model in models]
            
            if self.model not in model_names:
                logger.warning(f"Model {self.model} not found in Ollama. Available: {model_names}")
                return False
                
            self.is_available = True
            logger.info(f"Ollama provider initialized with model {self.model}")
            return True
            
        except Exception as e:
            logger.warning(f"Failed to initialize Ollama provider: {e}")
            return False
            
    async def generate(self, prompt: str, **kwargs) -> str:
        """Generate text using Ollama."""
        if not self.is_available:
            raise LLMError("Ollama provider not available")
            
        try:
            data = {
                "model": self.model,
                "prompt": prompt,
                "stream": False,
                **kwargs
            }
            
            response = requests.post(
                f"{self.base_url}/api/generate",
                json=data,
                timeout=self.timeout
            )
            response.raise_for_status()
            
            result = response.json()
            return result.get("response", "")
            
        except Exception as e:
            logger.error(f"Ollama generation failed: {e}")
            raise LLMError(f"Ollama generation failed: {e}")
            
    async def chat(self, messages: List[Dict[str, str]], **kwargs) -> str:
        """Convert chat messages to a single prompt and generate."""
        # Simple conversion of messages to prompt
        prompt_parts = []
        for msg in messages:
            role = msg.get("role", "user")
            content = msg.get("content", "")
            if role == "user":
                prompt_parts.append(f"User: {content}")
            elif role == "assistant":
                prompt_parts.append(f"Assistant: {content}")
            elif role == "system":
                prompt_parts.append(f"System: {content}")
                
        prompt = "\n".join(prompt_parts) + "\nAssistant:"
        return await self.generate(prompt, **kwargs)


class GeminiProvider(LLMProvider):
    """Google Gemini API provider."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__("gemini", config)
        self.api_key = config.get("api_key")
        self.model_name = config.get("model", "gemini-1.5-flash")
        self.client = None
        
    async def initialize(self) -> bool:
        """Initialize Gemini API client."""
        if not self.api_key:
            logger.warning("Gemini API key not provided")
            return False
            
        try:
            genai.configure(api_key=self.api_key)
            self.client = genai.GenerativeModel(self.model_name)
            
            # Test the connection with a simple query
            response = self.client.generate_content("Test")
            if response.text:
                self.is_available = True
                logger.info(f"Gemini provider initialized with model {self.model_name}")
                return True
            else:
                logger.warning("Gemini test query failed")
                return False
                
        except Exception as e:
            logger.warning(f"Failed to initialize Gemini provider: {e}")
            return False
            
    async def generate(self, prompt: str, **kwargs) -> str:
        """Generate text using Gemini."""
        if not self.is_available or not self.client:
            raise LLMError("Gemini provider not available")
            
        try:
            response = self.client.generate_content(prompt)
            return response.text
            
        except Exception as e:
            logger.error(f"Gemini generation failed: {e}")
            raise LLMError(f"Gemini generation failed: {e}")
            
    async def chat(self, messages: List[Dict[str, str]], **kwargs) -> str:
        """Generate response using Gemini chat."""
        if not self.is_available or not self.client:
            raise LLMError("Gemini provider not available")
            
        try:
            # Convert messages to Gemini format
            chat = self.client.start_chat()
            
            for msg in messages[:-1]:  # All but the last message
                role = msg.get("role", "user")
                content = msg.get("content", "")
                
                if role == "user":
                    chat.send_message(content)
                    
            # Send the last message and get response
            last_msg = messages[-1] if messages else {"content": ""}
            response = chat.send_message(last_msg.get("content", ""))
            return response.text
            
        except Exception as e:
            logger.error(f"Gemini chat failed: {e}")
            raise LLMError(f"Gemini chat failed: {e}")
