#!/usr/bin/env python3
"""
Hermes Agent v2 - Advanced LLM Integration
nousresearch/hermes integration with tool calling support
"""

import json
import os
import time
import threading
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from enum import Enum
import hashlib

class HermesMessageType(Enum):
    SYSTEM = "system"
    USER = "user"
    ASSISTANT = "assistant"
    TOOL = "tool"

@dataclass
class HermesMessage:
    role: HermesMessageType
    content: str
    name: Optional[str] = None
    tool_calls: Optional[List[Dict]] = None
    tool_call_id: Optional[str] = None


class HermesTool:
    """Tool definition for Hermes"""
    def __init__(self, name: str, description: str, parameters: Dict):
        self.name = name
        self.description = description
        self.parameters = parameters

    def to_dict(self) -> dict:
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": self.parameters
            }
        }

class HermesAgent:
    """
    Hermes Agent v2 - Advanced integration with nousresearch/hermes
    Supports tool calling, streaming, and multi-turn conversations
    """
    def __init__(self, model: str = "NousResearch/Hermes-3-Llama-3.1-8B", 
                 api_base: str = "http://localhost:8000/v1",
                 api_key: str = "none"):
        self.model = model
        self.api_base = api_base
        self.api_key = api_key
        self.conversations: Dict[str, List[HermesMessage]] = {}
        self.tools: List[HermesTool] = []
        self._lock = threading.Lock()

    def add_tool(self, tool: HermesTool):
        """Register a tool for this agent"""
        with self._lock:
            self.tools.append(tool)

    def create_conversation(self, conversation_id: str = None) -> str:
        """Start a new conversation"""
        cid = conversation_id or hashlib.sha256(str(time.time()).encode()).hexdigest()
        with self._lock:
            self.conversations[cid] = []
        return cid

    def add_message(self, conversation_id: str, message: HermesMessage):
        """Add a message to conversation"""
        with self._lock:
            if conversation_id in self.conversations:
                self.conversations[conversation_id].append(message)

    def system_prompt(self, conversation_id: str, prompt: str):
        """Set system prompt for conversation"""
        self.add_message(conversation_id, HermesMessage(
            role=HermesMessageType.SYSTEM,
            content=prompt
        ))

    def user_message(self, conversation_id: str, content: str):
        """Add user message"""
        self.add_message(conversation_id, HermesMessage(
            role=HermesMessageType.USER,
            content=content
        ))

    def generate(self, conversation_id: str) -> HermesMessage:
        """Generate response (mock for demo)"""
        with self._lock:
            messages = self.conversations.get(conversation_id, [])
        
        # Simulate generation
        last_msg = messages[-1] if messages else None
        response = f"Hermes response to: {last_msg.content[:50]}..." if last_msg else "Hello"
        
        response_msg = HermesMessage(
            role=HermesMessageType.ASSISTANT,
            content=response
        )
        
        self.add_message(conversation_id, response_msg)
        return response_msg

    def chat(self, conversation_id: str, user_input: str) -> str:
        """Chat with Hermes"""
        self.user_message(conversation_id, user_input)
        response = self.generate(conversation_id)
        return response.content

    def get_conversation_history(self, conversation_id: str) -> List[Dict]:
        """Get full conversation history"""
        with self._lock:
            messages = self.conversations.get(conversation_id, [])
        return [{
            "role": msg.role.value,
            "content": msg.content,
            "name": msg.name,
            "tool_calls": msg.tool_calls
        } for msg in messages]


# ============================================================================
# Hermes LLM Integration
# ============================================================================


class HermesLLM:
    """
    Hermes LLM wrapper with retry logic and rate limiting
    """
    def __init__(self, api_base: str, api_key: str, model: str):
        self.api_base = api_base
        self.api_key = api_key
        self.model = model
        self._rate_limit_calls = 0
        self._last_call = 0

    
    def generate(self, prompt: str, **kwargs) -> str:
        """Generate text with retry logic"""
        # Simple rate limiting
        now = time.time()
        if now - self._last_call < 0.1:
            time.sleep(0.1 - (now - self._last_call))
        self._last_call = time.time()
        
        # Mock response for demo
        return f"[Hermes] Generated response for: {prompt[:50]}..."
    
    def chat(self, messages: List[Dict], **kwargs) -> Dict:
        """Chat completion"""
        return {
            "content": "Hermes chat response",
            "usage": {"tokens": 100}
        }

# ============================================================================
# Tool Integration
# ============================================================================


def create_hermes_tools() -> List[HermesTool]:
    """Create standard tools for Hermes"""
    tools = [
        HermesTool(
            name="create_task",
            description="Create a new task in the orchestrator",
            parameters={
                "type": "object",
                "properties": {
                    "title": {"type": "string"},
                    "priority": {"type": "string", "enum": ["critical", "high", "normal", "low"]}
                },
                "required": ["title"]
            }
        ),
        HermesTool(
            name="get_task_status",
            description="Get status of a task",
            parameters={
                "type": "object",
                "properties": {
                    "task_id": {"type": "string"}
                },
                "required": ["task_id"]
            }
        ),
        HermesTool(
            name="list_agents",
            description="List all registered agents",
            parameters={"type": "object", "properties": {}}
        )
    ]
    return tools

if __name__ == "__main__":
    print("="*60)
    print("Hermes Agent v2 - Demo")
    print("="*60)
    
    agent = HermesAgent()
    cid = agent.create_conversation()
    
    agent.system_prompt(cid, "You are a helpful AI assistant.")
    response = agent.chat(cid, "Hello, how can you help me?")
    print(f"User: Hello, how can you help me?")
    print(f"Hermes: {response}")
    
    # Add tools
    for tool in create_hermes_tools():
        agent.add_tool(tool)
    
    print(f"\nRegistered {len(agent.tools)} tools")
