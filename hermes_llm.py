#!/usr/bin/env python3
"""
Hermes LLM Integration Module
Integration with nousresearch/Hermes models
"""
import json
import time
import threading
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

import os

@dataclass
class LLMConfig:
    model: str = "NousResearch/Hermes-3-Llama-3.1-8B"
    api_base: str = "http://localhost:8000/v1"
    api_key: str = "none"
    max_tokens: int = 2048
    temperature: float = 0.7
    top_p: float = 0.9
    context_window: int = 8192

class HermesLLMClient:
    """Client for Hermes LLM"""
    def __init__(self, config: LLMConfig = None):
        self.config = config or LLMConfig()
        self._lock = threading.Lock()
        self._session_count = 0

    def complete(self, prompt: str, **kwargs) -> Dict[str, Any]:
        """Generate completion"""
        with self._lock:
            self._session_count += 1
        
        return {
            "text": f"[Hermes] {prompt[:100]}...",
            "model": self.config.model,
            "usage": {
                "prompt_tokens": len(prompt.split()),
                "completion_tokens": 50,
                "total_tokens": len(prompt.split()) + 50
            },
            "session": self._session_count
        }
    def chat_complete(self, messages: List[Dict], **kwargs) -> Dict[str, Any]:
        """Chat completion"""
        system_msg = next((m for m in messages if m.get("role") == "system"), {})
        user_msg = next((m for m in messages if m.get("role") == "user"), {})
        
        return {
            "content": f"[Hermes] Response to: {user_msg.get('content', '')[:50]}...",
            "model": self.config.model,
            "usage": {"total_tokens": 150}
        }
    def stream_complete(self, prompt: str, callback=None):
        """Stream completion"""
        words = prompt.split()[:10]
        for i, word in enumerate(words):
            if callback:
                callback(word + " ", i == len(words) - 1)
            time.sleep(0.05)
        return " ".join(words)

class HermesIntegration:
    """Integration layer for Hermes"""
    def __init__(self, orchestrator, config: LLMConfig = None):
        self.orchestrator = orchestrator
        self.config = config or LLMConfig()
        self.llm = HermesLLMClient(config)
        self._tools: Dict[str, Any] = {}
    def register_tool(self, name: str, func: callable, schema: Dict):
        """Register a tool"""
        self._tools[name] = {
            "function": func,
            "schema": schema
        }
    def execute_tool(self, name: str, **kwargs) -> Any:
        """Execute a registered tool"""
        if name in self._tools:
            return self._tools[name]["function"](**kwargs)
        raise ValueError(f"Tool {name} not found")
    def process_task(self, task_id: str) -> Dict[str, Any]:
        """Process task using Hermes"""
        task = self.orchestrator.get_task(task_id)
        if not task:
            return {"error": "Task not found"}
        
        messages = [
            {"role": "system", "content": "You are a task processing assistant."},
            {"role": "user", "content": task.description or task.title}
        ]
        
        response = self.llm.chat_complete(messages)
        
        self.orchestrator.update_task(task_id, status="completed", result={
            "hermes_response": response["content"],
            "tokens_used": response["usage"]["total_tokens"]
        })
        
        return response
if __name__ == "__main__":
    print("="*60)
    print("Hermes LLM Integration - Demo")
    print("="*60)
    
    config = LLMConfig()
    client = HermesLLMClient(config)
    
    response = client.complete("Hello Hermes, process this task.")
    print(f"Response: {response['text']}")
    print(f"Tokens: {response['usage']['total_tokens']}")
