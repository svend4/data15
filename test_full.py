#!/usr/bin/env python3
"""
Full Test Suite
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from orchestrator import Orchestrator
from hybrid_orchestrator import HybridOrchestrator

def test_orchestrator():
    print("Testing Orchestrator...")
    orch = Orchestrator()
    task = orch.create_task("Test Task")
    print(f"  Created: {task.id}")
    print("  PASSED")

def test_hybrid():
    print("Testing HybridOrchestrator...")
    orch = HybridOrchestrator()
    task_id = orch.create_task("Hybrid Test")
    print(f"  Created: {task_id}")
    print("  PASSED")

if __name__ == "__main__":
    test_orchestrator()
    test_hybrid()
    print("\nAll tests passed!")
