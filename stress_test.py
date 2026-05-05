#!/usr/bin/env python3
"""
Stress Test Suite
"""
import time
import threading
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent))
from orchestrator_v5 import HybridOrchestrator

def stress_test():
    orch = HybridOrchestrator(state_dir="state/stress")
    results = {"tasks": 0, "errors": 0, "time": 0}
    
    def worker(n):
        for i in range(50):
            try:
                orch.create_task(f"Stress-{n}-{i}")
                results["tasks"] += 1
            except Exception:
                results["errors"] += 1
    
    start = time.time()
    threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    
    elapsed = time.time() - start
    results["time"] = elapsed
    
    print(f"Stress Test Results:")
    print(f"  Tasks: {results['tasks']}")
    print(f"  Errors: {results['errors']}")
    print(f"  Time: {elapsed:.2f}s")
    print(f"  Rate: {results['tasks']/elapsed:.1f} tasks/sec")

if __name__ == "__main__":
    stress_test()
