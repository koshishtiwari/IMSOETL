#!/usr/bin/env python3
"""Test script for LLM integration in IMSOETL."""

import os
import sys
import asyncio
import json
from pathlib import Path

# Add the src directory to the path
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

from imsoetl.llm.manager import LLMManager


async def test_llm_basic():
    """Test basic LLM functionality."""
    print("🤖 Testing LLM Integration")
    print("=" * 50)
    
    # Create configuration
    config = {
        "llm": {
            "ollama": {
                "enabled": True,
                "base_url": "http://localhost:11434",
                "model": "gemma3:4b",
                "timeout": 30
            },
            "gemini": {
                "enabled": True,
                "model": "gemini-1.5-flash",
                "api_key": "AIzaSyCXeLTsst3w9hmPybXuCageQERBS6pQqBk"
            }
        }
    }
    
    # Initialize LLM manager
    print("🔧 Initializing LLM Manager...")
    llm_manager = LLMManager(config)
    success = await llm_manager.initialize()
    
    if not success:
        print("❌ Failed to initialize LLM manager")
        return
    
    # Show available providers
    available = llm_manager.get_available_providers()
    primary = llm_manager.get_primary_provider()
    
    print(f"✅ Available providers: {', '.join(available)}")
    print(f"✅ Primary provider: {primary}")
    print()
    
    # Test basic generation
    print("🔤 Testing text generation...")
    prompt = "Explain what an ETL pipeline is in 2 sentences."
    
    try:
        response = await llm_manager.generate(prompt)
        print(f"Prompt: {prompt}")
        print(f"Response: {response}")
        print()
    except Exception as e:
        print(f"❌ Generation failed: {e}")
        return
    
    # Test intent parsing
    print("🧠 Testing intent parsing...")
    user_inputs = [
        "Extract data from users table in MySQL and load it into PostgreSQL",
        "Transform customer data by cleaning phone numbers and validate emails",
        "Monitor the daily ETL job for errors and send alerts if failures occur",
        "Discover the schema of products table and check data quality"
    ]
    
    for user_input in user_inputs:
        try:
            intent = await llm_manager.parse_intent(user_input)
            print(f"Input: {user_input}")
            print(f"Intent: {json.dumps(intent, indent=2)}")
            print("-" * 40)
        except Exception as e:
            print(f"❌ Intent parsing failed for '{user_input}': {e}")
    
    print("✅ LLM testing completed!")


async def test_orchestrator_integration():
    """Test LLM integration with orchestrator."""
    print("\n🎭 Testing Orchestrator + LLM Integration")
    print("=" * 50)
    
    from imsoetl.core.orchestrator import OrchestratorAgent
    
    # Create configuration with LLM settings
    config = {
        "llm": {
            "ollama": {
                "enabled": True,
                "base_url": "http://localhost:11434", 
                "model": "gemma3:4b",
                "timeout": 30
            },
            "gemini": {
                "enabled": True,
                "model": "gemini-1.5-flash",
                "api_key": "AIzaSyCXeLTsst3w9hmPybXuCageQERBS6pQqBk"
            }
        }
    }
    
    # Create orchestrator
    orchestrator = OrchestratorAgent()
    
    # Initialize LLM
    print("🔧 Initializing Orchestrator LLM...")
    llm_success = await orchestrator.initialize_llm(config)
    
    if llm_success:
        print("✅ Orchestrator LLM initialized successfully")
        if orchestrator.llm_manager:
            print(f"Available providers: {orchestrator.llm_manager.get_available_providers()}")
            print(f"Primary provider: {orchestrator.llm_manager.get_primary_provider()}")
    else:
        print("❌ Orchestrator LLM initialization failed")
        return
    
    # Test enhanced intent parsing
    print("\n🧠 Testing enhanced intent parsing...")
    test_intents = [
        "Copy all customer data from MySQL to PostgreSQL and clean phone numbers",
        "Find duplicate records in the orders table and create a quality report",
        "Set up monitoring for the nightly ETL job with email alerts"
    ]
    
    from imsoetl.core.base_agent import Message
    
    for intent_text in test_intents:
        print(f"\nTesting: {intent_text}")
        
        # Create a message for the orchestrator
        message = Message(
            sender_id="test",
            receiver_id="orchestrator_main",
            message_type="user_intent",
            content={
                "intent": intent_text,
                "session_id": f"test_session_{hash(intent_text) % 1000}"
            }
        )
        
        # Handle the intent
        try:
            await orchestrator.handle_user_intent(message)
            print("✅ Intent processed successfully")
        except Exception as e:
            print(f"❌ Intent processing failed: {e}")
            import traceback
            traceback.print_exc()
    
    print("\n✅ Orchestrator integration testing completed!")


if __name__ == "__main__":
    print("🚀 IMSOETL LLM Integration Test")
    print("================================")
    
    try:
        # Test basic LLM functionality
        asyncio.run(test_llm_basic())
        
        # Test orchestrator integration
        asyncio.run(test_orchestrator_integration())
        
    except KeyboardInterrupt:
        print("\n⏹️  Testing interrupted by user")
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
