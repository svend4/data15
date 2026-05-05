#!/usr/bin/env python3
"""
Performance Test Suite
"""
import time
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from orchestrator_v5 import HybridOrchestrator, Priority

def test_throughput():
    orch = HybridOrchestrator(state_dir="state/perf_test")
    start = time.time()
    
    for i in range(100):
        orch.create_task(f"Task {i}", priority=Priority.NORMAL)
    
    elapsed = time.time() - start
    print(f"Created 100 tasks in {elapsed:.3f}s ({100/elapsed:.1f} tasks/sec)")
    return elapsed < 1.0

def test_concurrent():
    import threading
    orch = HybridOrchestrator(state_dir="state/perf_concurrent")
    
    def worker(n):
        for i in range(20):
            orch.create_task(f"Task-{n}-{i}")
    
    start = time.time()
    threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    
    elapsed = time.time() - start
    print(f"5 threads x 20 tasks in {elapsed:.3f}s")
    return True

if __name__ == "__main__":
    print("Performance Tests")
    test_throughput()
    test_concurrent()
