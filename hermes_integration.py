#!/usr/bin/env python3
"""
Hermes Integration Module
Bridge between Hermes agent and orchestrator
"""
import json
import time
import threading
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from enum import Enum

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent))


try:
    from hermes_agent_v2 import HermesAgent, HermesTool, HermesMessage, HermesMessageType
    from hermes_llm import HermesLLMClient, HermesIntegration
    HERMES_AVAILABLE = True
except ImportError:
    HERMES_AVAILABLE = False
    print("Hermes modules not available - using mock mode")

class HermesBridge:
    """Bridge between Hermes and the orchestrator"""
    def __init__(self, orchestrator):
        self.orchestrator = orchestrator
        self.hermes_client = None
        self._lock = threading.Lock()
        
        if HERMES_AVAILABLE:
            self.hermes_client = HermesAgent()
    def process_with_hermes(self, task_id: str) -> Dict[str, Any]:
        """Process task using Hermes"""
        task = self.orchestrator.get_task(task_id)
        if not task:
            return {"error": "Task not found"}
        
        cid = self.hermes_client.create_conversation() if HERMES_AVAILABLE else None
        
        if HERMES_AVAILABLE:
            response = self.hermes_client.chat(cid, task.description or task.title)
        else:
            response = f"[Mock Hermes] Processed: {task.title}"
        
        return {
            "task_id": task_id,
            "response": response,
            "hermes_used": HERMES_AVAILABLE
        }
    def get_hermes_capabilities(self) -> Dict[str, Any]:
        """Get Hermes capabilities"""
        return {
            "available": HERMES_AVAILABLE,
            "model": "NousResearch/Hermes-3-Llama-3.1-8B",
            "features": [
                "tool_calling",
                "streaming",
                "multi_turn",
                "function_execution"
            ] if HERMES_AVAILABLE else ["mock_mode"]
        }
if __name__ == "__main__":
    print("="*60)
    print("Hermes Bridge - Demo")
    print("="*60)
    
    from orchestrator_v5 import HybridOrchestrator
    
    orchestrator = HybridOrchestrator(state_dir="state/hermes_demo")
    bridge = HermesBridge(orchestrator)
    
    task_id = orchestrator.create_task(
        title="Test Hermes Integration",
        description="Process this task with Hermes AI"
    )
    print(f"Created task: {task_id}")
    
    result = bridge.process_with_hermes(task_id)
    print(f"Result: {result}")
