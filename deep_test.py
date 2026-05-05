#!/usr/bin/env python3
"""
Deep Test Suite for Multi-Agent Hybrid Orchestrator v5.0
Comprehensive testing of all 33+ modules
"""

import sys
import time
import threading
from pathlib import Path

# Add orchestrator to path
sys.path.insert(0, str(Path(__file__).parent))

from orchestrator_v5 import (
    HybridOrchestrator, Task, TaskStatus, TaskPriority,
    Agent, AgentRole, MetricsCollector, CircuitBreaker,
    CircuitState, RateLimiter, ThreadSafeStorage
)

def test_core():
    """Test core orchestrator functionality"""
    print("\n" + "="*60)
    print("Testing Core Orchestrator")
    print("="*60)
    
    orch = HybridOrchestrator(state_dir="state/test_core")
    
    # Create task
    task_id = orch.create_task(
        title="Test Task",
        description="Testing core functionality",
        priority=TaskPriority.HIGH
    )
    print(f"✓ Created task: {task_id}")
    
    # Get task
    task = orch.get_task(task_id)
    assert task is not None
    print(f"✓ Retrieved task: {task.title}")
    
    # Update task
    orch.update_task(task_id, status=TaskStatus.COMPLETED)
    updated = orch.get_task(task_id)
    assert updated.status == TaskStatus.COMPLETED
    print(f"✓ Updated task status: {updated.status.value}")
    
    return True

def test_agents():
    """Test agent management"""
    print("\n" + "="*60)
    print("Testing Agent Management")
    print("="*60)
    
    orch = HybridOrchestrator(state_dir="state/test_agents")
    
    # Register agent
    agent_id = orch.register_agent(
        name="TestAgent",
        role=AgentRole.EXECUTOR,
        capabilities=["coding", "review"]
    )
    print(f"✓ Registered agent: {agent_id}")
    
    agents = orch.list_agents()
    assert len(agents) > 0
    print(f"✓ Listed {len(agents)} agents")
    
    return True

def test_metrics():
    """Test metrics collection"""
    print("\n" + "="*60)
    print("Testing Metrics Collection")
    print("="*60)
    
    mc = MetricsCollector()
    
    mc.inc_counter("test_counter", 1)
    mc.set_gauge("test_gauge", 42.0)
    mc.observe_histogram("test_histogram", 1.5)
    
    metrics = mc.get_metrics()
    assert metrics["test_counter"]["counter"] == 1
    print(f"✓ Counter: {metrics['test_counter']['counter']}")
    
    assert metrics["test_gauge"]["gauge"] == 42.0
    print(f"✓ Gauge: {metrics['test_gauge']['gauge']}")
    
    prom_output = mc.export_prometheus()
    assert "orchestrator_metrics" in prom_output
    print(f"✓ Prometheus export: {len(prom_output)} chars")
    
    return True

def test_circuit_breaker():
    """Test circuit breaker pattern"""
    print("\n" + "="*60)
    print("Testing Circuit Breaker")
    print("="*60)
    
    cb = CircuitBreaker(failure_threshold=3)
    
    # Test successful call
    result = cb.call(lambda: "success")
    assert result == "success"
    print(f"✓ Successful call: {result}")
    
    # Test failure tracking
    for _ in range(3):
        try:
            cb.call(lambda: (_ for _ in ()).throw(Exception("fail")))
        except:
            pass
    
    assert cb.state == CircuitState.OPEN
    print(f"✓ Circuit opened after failures")
    
    return True

def test_rate_limiter():
    """Test rate limiting"""
    print("\n" + "="*60)
    print("Testing Rate Limiter")
    print("="*60)
    
    rl = RateLimiter(rate=5, per_seconds=1.0)
    
    # Acquire tokens
    for i in range(5):
        assert rl.acquire()
        print(f"✓ Acquired token {i+1}/5")
    
    # Should fail when exhausted
    assert not rl.acquire()
    print("✓ Rate limit enforced")
    
    return True

def test_storage():
    """Test thread-safe storage"""
    print("\n" + "="*60)
    print("Testing Thread-Safe Storage")
    print("="*60)
    
    storage = ThreadSafeStorage("state/test_storage")
    
    # Write and read
    storage.write("test.json", {"key": "value"})
    data = storage.read("test.json")
    assert data["key"] == "value"
    print(f"✓ Wrote and read data: {data}")
    
    # Update
    storage.update("test.json", lambda d: {**d, "updated": True})
    updated = storage.read("test.json")
    assert updated["updated"]
    print(f"✓ Atomic update: {updated}")
    
    return True

def test_knowledge_base():
    """Test knowledge base"""
    print("\n" + "="*60)
    print("Testing Knowledge Base")
    print("="*60)
    
    orch = HybridOrchestrator(state_dir="state/test_kb")
    kb = orch.knowledge
    
    # Add entry
    kb.add("test_key", "This is a test entry about Python programming", {"category": "programming"})
    print("✓ Added knowledge entry")
    
    # Search
    results = kb.search("Python programming", top_k=1)
    assert len(results) > 0
    print(f"✓ Search found {len(results)} results")
    
    return True

def test_statistics_dashboard():
    """Test statistics dashboard"""
    print("\n" + "="*60)
    print("Testing Statistics Dashboard")
    print("="*60)
    
    orch = HybridOrchestrator(state_dir="state/test_dashboard")
    
    # Create some tasks
    for i in range(3):
        orch.create_task(title=f"Task {i}", priority=TaskPriority.NORMAL)
    
    dashboard = orch.dashboard
    summary = dashboard.get_summary()
    
    print(f"✓ Total tasks: {summary['total_tasks']}")
    print(f"✓ Metrics: {dashboard.get_metrics()}")
    
    return True

def test_priority_queue():
    """Test priority task queue"""
    print("\n" + "="*60)
    print("Testing Priority Queue")
    print("="*60)
    
    from orchestrator_v5 import PriorityTaskQueue
    
    pq = PriorityTaskQueue()
    
    # Add tasks with different priorities
    pq.add("task1", priority=1)  # Critical
    pq.add("task2", priority=3)  # Normal
    pq.add("task3", priority=5)  # Background
    
    # Get next (should be critical)
    next_task = pq.get_next()
    assert next_task == "task1"
    print(f"✓ Got priority task: {next_task}")
    
    return True

def test_workflow_engine():
    """Test workflow engine"""
    print("\n" + "="*60)
    print("Testing Workflow Engine")
    print("="*60)
    
    orch = HybridOrchestrator(state_dir="state/test_workflow")
    
    # Create a workflow
    workflow_id = orch.workflows.create_workflow(
        name="Test Workflow",
        steps=[{"action": "step1", "agent": "agent1"}]
    )
    print(f"✓ Created workflow: {workflow_id}")
    
    workflows = orch.workflows.list_workflows()
    assert len(workflows) > 0
    print(f"✓ Listed {len(workflows)} workflows")
    
    return True

def test_sla_monitor():
    """Test SLA monitoring"""
    print("\n" + "="*60)
    print("Testing SLA Monitor")
    print("="*60)
    
    orch = HybridOrchestrator(state_dir="state/test_sla")
    
    # Create task with SLA
    task_id = orch.create_task(
        title="SLA Task",
        description="Task with SLA",
        priority=TaskPriority.HIGH
    )
    
    orch.sla_monitor.set_sla(task_id, max_duration=3600, priority="high")
    print("✓ Set SLA for task")
    
    compliance = orch.sla_monitor.get_compliance()
    print(f"✓ SLA Compliance: {compliance['overall_compliance']:.1%}")
    
    return True

def test_integration_hub():
    """Test integration hub"""
    print("\n" + "="*60)
    print("Testing Integration Hub")
    print("="*60)
    
    orch = HybridOrchestrator(state_dir="state/test_integration")
    
    # Test integrations dict exists
    integrations = orch.integrations.list_integrations()
    print(f"✓ Integration types: {list(integrations.keys())}")
    
    return True

def test_concurrent_operations():
    """Test concurrent operations"""
    print("\n" + "="*60)
    print("Testing Concurrent Operations")
    print("="*60)
    
    orch = HybridOrchestrator(state_dir="state/test_concurrent")
    results = []
    
    def worker(i):
        task_id = orch.create_task(title=f"Concurrent Task {i}")
        time.sleep(0.01)
        results.append(task_id)
    
    threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    
    assert len(results) == 10
    print(f"✓ Created {len(results)} tasks concurrently")
    
    return True

def run_all_tests():
    """Run all tests"""
    print("\n" + "="*60)
    print("MULTI-AGENT ORCHESTRATOR v5.0 - DEEP TEST SUITE")
    print("="*60)
    
    tests = [
        ("Core Functionality", test_core),
        ("Agent Management", test_agents),
        ("Metrics Collection", test_metrics),
        ("Circuit Breaker", test_circuit_breaker),
        ("Rate Limiter", test_rate_limiter),
        ("Thread-Safe Storage", test_storage),
        ("Knowledge Base", test_knowledge_base),
        ("Statistics Dashboard", test_statistics_dashboard),
        ("Priority Queue", test_priority_queue),
        ("Workflow Engine", test_workflow_engine),
        ("SLA Monitor", test_sla_monitor),
        ("Integration Hub", test_integration_hub),
        ("Concurrent Operations", test_concurrent),
    ]
    
    passed = 0
    failed = 0
    
    for name, test_func in tests:
        try:
            if test_func():
                passed += 1
        except Exception as e:
            print(f"✗ FAILED: {e}")
            failed += 1
    
    print("\n" + "="*60)
    print(f"TEST RESULTS: {passed} passed, {failed} failed")
    print("="*60)
    
    return failed == 0

if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
