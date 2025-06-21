"""LLM Manager for handling multiple LLM providers with fallback."""

import json
import logging
from typing import Dict, List, Optional, Any, Union
from .providers import LLMProvider, OllamaProvider, GeminiProvider
from ..core.errors import IMSOETLError


logger = logging.getLogger(__name__)


class LLMManager:
    """Manages multiple LLM providers with automatic fallback."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.providers: List[LLMProvider] = []
        self.primary_provider: Optional[LLMProvider] = None
        self.fallback_providers: List[LLMProvider] = []
        
    async def initialize(self) -> bool:
        """Initialize all configured LLM providers."""
        providers_config = self.config.get("llm", {})
        
        # Initialize Ollama provider if configured
        ollama_config = providers_config.get("ollama", {})
        if ollama_config.get("enabled", True):
            ollama = OllamaProvider(ollama_config)
            if await ollama.initialize():
                self.providers.append(ollama)
                if not self.primary_provider:
                    self.primary_provider = ollama
                    logger.info("Using Ollama as primary LLM provider")
                    
        # Initialize Gemini provider if configured
        gemini_config = providers_config.get("gemini", {})
        if gemini_config.get("enabled", True) and gemini_config.get("api_key"):
            gemini = GeminiProvider(gemini_config)
            if await gemini.initialize():
                self.providers.append(gemini)
                if not self.primary_provider:
                    self.primary_provider = gemini
                    logger.info("Using Gemini as primary LLM provider")
                else:
                    self.fallback_providers.append(gemini)
                    logger.info("Added Gemini as fallback LLM provider")
                    
        if not self.providers:
            logger.warning("No LLM providers available")
            return False
            
        logger.info(f"Initialized {len(self.providers)} LLM provider(s)")
        return True
        
    async def generate(self, prompt: str, **kwargs) -> str:
        """Generate text using the best available provider."""
        if not self.providers:
            raise IMSOETLError("No LLM providers available")
            
        # Try primary provider first
        if self.primary_provider and self.primary_provider.is_ready():
            try:
                result = await self.primary_provider.generate(prompt, **kwargs)
                logger.debug(f"Generated text using {self.primary_provider.name}")
                return result
            except Exception as e:
                logger.warning(f"Primary provider {self.primary_provider.name} failed: {e}")
                
        # Try fallback providers
        for provider in self.fallback_providers:
            if provider.is_ready():
                try:
                    result = await provider.generate(prompt, **kwargs)
                    logger.info(f"Generated text using fallback provider {provider.name}")
                    return result
                except Exception as e:
                    logger.warning(f"Fallback provider {provider.name} failed: {e}")
                    
        raise IMSOETLError("All LLM providers failed")
        
    async def chat(self, messages: List[Dict[str, str]], **kwargs) -> str:
        """Generate chat response using the best available provider."""
        if not self.providers:
            raise IMSOETLError("No LLM providers available")
            
        # Try primary provider first
        if self.primary_provider and self.primary_provider.is_ready():
            try:
                result = await self.primary_provider.chat(messages, **kwargs)
                logger.debug(f"Generated chat response using {self.primary_provider.name}")
                return result
            except Exception as e:
                logger.warning(f"Primary provider {self.primary_provider.name} failed: {e}")
                
        # Try fallback providers
        for provider in self.fallback_providers:
            if provider.is_ready():
                try:
                    result = await provider.chat(messages, **kwargs)
                    logger.info(f"Generated chat response using fallback provider {provider.name}")
                    return result
                except Exception as e:
                    logger.warning(f"Fallback provider {provider.name} failed: {e}")
                    
        raise IMSOETLError("All LLM providers failed")
        
    def get_available_providers(self) -> List[str]:
        """Get list of available provider names."""
        return [provider.name for provider in self.providers if provider.is_ready()]
        
    def get_primary_provider(self) -> Optional[str]:
        """Get the name of the primary provider."""
        return self.primary_provider.name if self.primary_provider else None
        
    async def parse_intent(self, user_input: str) -> Dict[str, Any]:
        """Parse user intent from natural language input."""
        prompt = f"""
Analyze the following user input and extract the intent for an ETL (Extract, Transform, Load) system.

User Input: "{user_input}"

Please identify:
1. The primary action/intent (discover, transform, validate, execute, monitor)
2. Source database/table information if mentioned
3. Target database/table information if mentioned
4. Any specific transformations or conditions
5. Data quality requirements if mentioned

Respond in JSON format with the following structure:
{{
    "intent": "primary_action",
    "source": {{
        "type": "database_type",
        "connection": "connection_details",
        "table": "table_name"
    }},
    "target": {{
        "type": "database_type", 
        "connection": "connection_details",
        "table": "table_name"
    }},
    "transformations": ["list", "of", "transformations"],
    "conditions": ["list", "of", "conditions"],
    "quality_checks": ["list", "of", "quality_requirements"]
}}

If any information is not available, use null for that field.
"""
        
        response = ""
        try:
            response = await self.generate(prompt)
            
            # Extract JSON from markdown code blocks if present
            json_text = response.strip()
            if json_text.startswith("```json"):
                json_text = json_text[7:]  # Remove ```json
            if json_text.endswith("```"):
                json_text = json_text[:-3]  # Remove ```
            json_text = json_text.strip()
            
            # Try to parse as JSON
            return json.loads(json_text)
        except json.JSONDecodeError:
            logger.warning("Failed to parse LLM response as JSON")
            return {
                "intent": "unknown",
                "raw_response": response,
                "source": None,
                "target": None,
                "transformations": [],
                "conditions": [],
                "quality_checks": []
            }
        except Exception as e:
            logger.error(f"Intent parsing failed: {e}")
            return {
                "intent": "error",
                "error": str(e),
                "source": None,
                "target": None,
                "transformations": [],
                "conditions": [],
                "quality_checks": []
            }
            
    async def generate_sql_transformation(self, source_schema: Dict, target_schema: Dict, requirements: List[str]) -> str:
        """Generate SQL transformation code based on schemas and requirements."""
        prompt = f"""
Generate SQL transformation code based on the following:

Source Schema:
{source_schema}

Target Schema:
{target_schema}

Requirements:
{requirements}

Please generate SQL code that:
1. Extracts data from the source schema
2. Applies necessary transformations
3. Maps to the target schema format
4. Handles data type conversions
5. Includes error handling where appropriate

Provide only the SQL code without explanations.
"""
        
        return await self.generate(prompt)
        
    async def suggest_data_quality_checks(self, schema: Dict, data_sample: Optional[Dict] = None) -> List[str]:
        """Suggest data quality checks based on schema and sample data."""
        prompt = f"""
Based on the following database schema{' and data sample' if data_sample else ''}:

Schema:
{schema}

{f'Data Sample:\n{data_sample}' if data_sample else ''}

Suggest specific data quality checks that should be performed. Consider:
1. Null value checks for required fields
2. Data type validation
3. Range checks for numeric fields
4. Format validation for dates, emails, etc.
5. Referential integrity checks
6. Duplicate detection
7. Business logic validation

Provide the suggestions as a simple list, one per line, without numbering.
"""
        
        response = await self.generate(prompt)
        # Parse response into list
        suggestions = [line.strip() for line in response.split('\n') if line.strip()]
        return suggestions
