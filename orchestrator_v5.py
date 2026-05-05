#!/usr/bin/env python3
"""
Hybrid Orchestrator v5.0 - PRODUCTION
=====================================
Part I Improvements (Medium-term):

1. WebSocket Monitoring (Flask-SocketIO) - Real-time events
2. Rate Limiting + Retry Logic - Reliability
3. Role-Based Access Control (RBAC) - Multi-user support
4. PostgreSQL Ready (optional) - Scalable storage
5. Message Queue Architecture - Async execution ready

Part II Integrations:
1. Hermes Agent - nousresearch/hermes ready
2. MCP Server Bridge - Tool standardization
"""

import json
import os
import sys
import argparse
import subprocess
import hashlib
import time
import threading
import fcntl
import shutil
from pathlib import Path
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, asdict, field
from enum import Enum
from functools import lru_cache, wraps
from collections import defaultdict
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor, Future
import uuid

# ============================================================================
# IMPROVEMENT: Prometheus Metrics
# ============================================================================

class MetricsCollector:
    """
    Prometheus-compatible metrics collector.
    Usage: prometheus_port = 9090, endpoint = /metrics
    """

    def __init__(self):
        self._metrics = defaultdict(lambda: {
            "counter": 0,
            "gauge": 0,
            "histogram": [],
            "labels": {}
        })
        self._lock = threading.Lock()

    def inc_counter(self, name: str, value: float = 1, labels: dict = None):
        with self._lock:
            self._metrics[name]["counter"] += value
            if labels:
                key = f"{name}:{hash(frozenset(labels.items()))}"
                self._metrics[key]["counter"] += value

    def set_gauge(self, name: str, value: float, labels: dict = None):
        with self._lock:
            self._metrics[name]["gauge"] = value

    def observe_histogram(self, name: str, value: float):
        with self._lock:
            self._metrics[name]["histogram"].append(value)
            if len(self._metrics[name]["histogram"]) > 1000:
                self._metrics[name]["histogram"] = self._metrics[name]["histogram"][-1000:]

    def get_metrics(self) -> dict:
        with self._lock:
            result = {}
            for name, data in self._metrics.items():
                result[name] = {
                    "counter": data["counter"],
                    "gauge": data["gauge"],
                    "histogram_avg": sum(data["histogram"]) / len(data["histogram"]) if data["histogram"] else 0,
                    "histogram_count": len(data["histogram"])
                }
            return result

    def export_prometheus(self) -> str:
        """Export metrics in Prometheus text format"""
        lines = ["# HELP orchestrator_metrics Prometheus metrics for orchestrator"]
        lines.append("# TYPE orchestrator_metrics gauge")
        for name, data in self.get_metrics().items():
            lines.append(f'orchester_metrics{{name="{name}"}} {data["gauge"]}')
            if data["counter"] > 0:
                lines.append(f'orchester_metrics_total{{name="{name}"}} {data["counter"]}')
        return "\n".join(lines)

# Global metrics collector
metrics = MetricsCollector()

# ============================================================================
# IMPROVEMENT: Circuit Breaker
# ============================================================================

class CircuitState(Enum):
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failing, reject requests
    HALF_OPEN = "half_open"  # Testing recovery

class CircuitBreaker:
    """
    Circuit Breaker pattern implementation.
    Prevents cascade failures by stopping requests to failing services.
    """

    def __init__(self, failure_threshold: int = 5, timeout: float = 60.0, recovery_timeout: float = 30.0):
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time: Optional[float] = None
        self.state = CircuitState.CLOSED
        self._lock = threading.Lock()

    def call(self, func: Callable, *args, **kwargs):
        with self._lock:
            if self.state == CircuitState.OPEN:
                if time.time() - self.last_failure_time >= self.recovery_timeout:
                    self.state = CircuitState.HALF_OPEN
                    metrics.inc_counter("circuit_breaker_half_open")
                else:
                    raise Exception("Circuit breaker is OPEN")

        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise e

    def _on_success(self):
        with self._lock:
            self.failure_count = 0
            if self.state == CircuitState.HALF_OPEN:
                self.state = CircuitState.CLOSED
                metrics.inc_counter("circuit_breaker_closed")

    def _on_failure(self):
        with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            metrics.inc_counter("circuit_breaker_failure")

            if self.failure_count >= self.failure_threshold:
                self.state = CircuitState.OPEN
                metrics.inc_counter("circuit_breaker_open")

# ============================================================================
# IMPROVEMENT: Retry Logic with Exponential Backoff
# ============================================================================

def retry_with_backoff(max_retries: int = 3, base_delay: float = 1.0, max_delay: float = 60.0):
    """Decorator for retry with exponential backoff"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise
                    delay = min(base_delay * (2 ** attempt), max_delay)
                    time.sleep(delay)
                    metrics.inc_counter("retry_attempt", labels={"attempt": str(attempt + 1)})
            return None
        return wrapper
    return decorator

# ============================================================================
# ENHANCED: Thread-Safe Storage with File Locking
# ============================================================================

class ThreadSafeStorage:
    """Thread-safe JSON storage with file locking"""

    def __init__(self, base_path: str = "state"):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._file_locks: Dict[str, threading.Lock] = defaultdict(threading.Lock)

    def _get_file_lock(self, filename: str) -> threading.Lock:
        with self._lock:
            if filename not in self._file_locks:
                self._file_locks[filename] = threading.Lock()
            return self._file_locks[filename]

    def _atomic_write(self, filepath: Path, data: dict):
        """Write atomically using temp file + rename"""
        temp_file = filepath.parent / f".{filepath.name}.tmp"
        with open(temp_file, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        os.replace(str(temp_file), str(filepath))

    def read(self, filename: str, default: Any = None) -> Any:
        filepath = self.base_path / filename
        file_lock = self._get_file_lock(filename)

        with file_lock:
            try:
                with open(filepath, 'r') as f:
                    fcntl.flock(f.fileno(), fcntl.LOCK_SH)
                    try:
                        return json.load(f)
                    finally:
                        fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            except FileNotFoundError:
                return default

    def write(self, filename: str, data: Any):
        filepath = self.base_path / filename
        file_lock = self._get_file_lock(filename)

        with file_lock:
            with open(filepath, 'w') as f:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                try:
                    self._atomic_write(filepath, data)
                finally:
                    fcntl.flock(f.fileno(), fcntl.LOCK_UN)

    def update(self, filename: str, update_func: Callable[[Any], Any]):
        """Atomic read-modify-write"""
        current = self.read(filename, default=None)
        updated = update_func(current or {})
        self.write(filename, updated)

# ============================================================================
# IMPROVEMENT: Rate Limiter
# ============================================================================

class RateLimiter:
    """Token bucket rate limiter"""

    def __init__(self, rate: int = 10, per_seconds: float = 1.0):
        self.rate = rate
        self.per_seconds = per_seconds
        self.tokens = rate
        self.last_update = time.time()
        self._lock = threading.Lock()

    def acquire(self, tokens: int = 1) -> bool:
        with self._lock:
            now = time.time()
            elapsed = now - self.last_update
            self.tokens = min(self.rate, self.tokens + elapsed * (self.rate / self.per_seconds))
            self.last_update = now

            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            return False

    def wait_and_acquire(self, tokens: int = 1, timeout: float = 30.0):
        start = time.time()
        while time.time() - start < timeout:
            if self.acquire(tokens):
                return True
            time.sleep(0.1)
        return False

# ============================================================================
# IMPROVEMENT: Task Priority Queue
# ============================================================================

class PriorityQueue:
    """Thread-safe priority queue with multiple priority levels"""

    def __init__(self):
        self._queues: Dict[int, Queue] = {
            1: Queue(),  # Critical
            2: Queue(),  # High
            3: Queue(),  # Normal
            4: Queue(),  # Low
            5: Queue(),  # Background
        }
        self._lock = threading.Lock()
        self._counts = defaultdict(int)

    def enqueue(self, item: Any, priority: int = 3):
        priority = max(1, min(5, priority))
        with self._lock:
            self._queues[priority].put(item)
            self._counts[priority] += 1
            metrics.inc_counter("priority_queue_enqueue", labels={"priority": str(priority)})

    def dequeue(self, timeout: float = 1.0) -> Optional[Any]:
        for priority in range(1, 6):
            try:
                item = self._queues[priority].get(timeout=timeout)
                with self._lock:
                    self._counts[priority] = max(0, self._counts[priority] - 1)
                metrics.inc_counter("priority_queue_dequeue", labels={"priority": str(priority)})
                return item
            except Empty:
                continue
        return None

    def get_stats(self) -> dict:
        with self._lock:
            return dict(self._counts)

# ============================================================================
# Data Classes and Enums
# ============================================================================

class TaskStatus(Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    BLOCKED = "blocked"

class TaskPriority(Enum):
    CRITICAL = 1
    HIGH = 2
    NORMAL = 3
    LOW = 4
    BACKGROUND = 5

class AgentRole(Enum):
    COORDINATOR = "coordinator"
    EXECUTOR = "executor"
    REVIEWER = "reviewer"
    PLANNER = "planner"

@dataclass
class Task:
    id: str
    title: str
    description: str = ""
    status: TaskStatus = TaskStatus.PENDING
    priority: TaskPriority = TaskPriority.NORMAL
    assigned_agent: Optional[str] = None
    created: float = field(default_factory=time.time)
    updated: float = field(default_factory=time.time)
    dependencies: List[str] = field(default_factory=list)
    result: Optional[Dict] = None
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "title": self.title,
            "description": self.description,
            "status": self.status.value,
            "priority": self.priority.value,
            "assigned_agent": self.assigned_agent,
            "created": self.created,
            "updated": self.updated,
            "dependencies": self.dependencies,
            "result": self.result,
            "tags": self.tags,
            "metadata": self.metadata
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'Task':
        return cls(
            id=data["id"],
            title=data["title"],
            description=data.get("description", ""),
            status=TaskStatus(data.get("status", "pending")),
            priority=TaskPriority(data.get("priority", 3)),
            assigned_agent=data.get("assigned_agent"),
            created=data.get("created", time.time()),
            updated=data.get("updated", time.time()),
            dependencies=data.get("dependencies", []),
            result=data.get("result"),
            tags=data.get("tags", []),
            metadata=data.get("metadata", {})
        )

@dataclass
class Agent:
    id: str
    name: str
    role: AgentRole
    capabilities: List[str] = field(default_factory=list)
    active: bool = True
    current_task: Optional[str] = None
    workload: int = 0

# ============================================================================
# IMPROVEMENT: Knowledge Base System
# ============================================================================

class KnowledgeBase:
    """
    Vector-based knowledge base for context retrieval.
    Uses simple embedding + cosine similarity for demonstration.
    """

    def __init__(self, storage: ThreadSafeStorage):
        self.storage = storage
        self._embeddings: Dict[str, List[float]] = {}
        self._lock = threading.Lock()

    def _simple_embed(self, text: str) -> List[float]:
        """Simple hashing-based embedding for demonstration"""
        import hashlib
        hash_val = hashlib.sha256(text.encode()).digest()
        return [b / 255.0 for b in hash_val[:32]]

    def add(self, key: str, content: str, metadata: dict = None):
        with self._lock:
            entry = {
                "content": content,
                "metadata": metadata or {},
                "embedding": self._simple_embed(content),
                "timestamp": time.time()
            }
            self.storage.update("knowledge.json", lambda d: {**d, key: entry})
            self._embeddings[key] = entry["embedding"]

    def search(self, query: str, top_k: int = 5) -> List[Dict]:
        query_emb = self._simple_embed(query)
        results = []
        data = self.storage.read("knowledge.json", default={})

        for key, entry in data.items():
            emb = entry.get("embedding", self._simple_embed(entry["content"]))
            similarity = self._cosine_similarity(query_emb, emb)
            results.append({
                "key": key,
                "content": entry["content"],
                "metadata": entry.get("metadata", {}),
                "similarity": similarity
            })

        return sorted(results, key=lambda x: x["similarity"], reverse=True)[:top_k]

    def _cosine_similarity(self, a: List[float], b: List[float]) -> float:
        dot = sum(x * y for x, y in zip(a, b))
        norm_a = sum(x * x for x in a) ** 0.5
        norm_b = sum(x * x for x in b) ** 0.5
        return dot / (norm_a * norm_b + 1e-8)

# ============================================================================
# IMPROVEMENT: Analytics Dashboard
# ============================================================================

class StatisticsDashboard:
    """Real-time statistics and analytics"""

    def __init__(self, orchestrator: 'HybridOrchestrator'):
        self.orchestrator = orchestrator

    def get_summary(self) -> dict:
        tasks = self.orchestrator.list_tasks()
        return {
            "total_tasks": len(tasks),
            "by_status": {
                status.value: sum(1 for t in tasks if t.status == status)
                for status in TaskStatus
            },
            "by_priority": {
                priority.value: sum(1 for t in tasks if t.priority == priority)
                for priority in TaskPriority
            },
            "active_agents": sum(1 for a in self.orchestrator.list_agents() if a.active),
            "timestamp": time.time()
        }

    def get_metrics(self) -> dict:
        return {
            "queue_depth": self.orchestrator.task_queue.qsize(),
            "processing_time_avg": metrics.get_metrics().get("processing_time", {}).get("histogram_avg", 0),
            "success_rate": self._calculate_success_rate()
        }

    def _calculate_success_rate(self) -> float:
        tasks = self.orchestrator.list_tasks()
        completed = sum(1 for t in tasks if t.status == TaskStatus.COMPLETED)
        return completed / len(tasks) if tasks else 0.0

# ============================================================================
# IMPROVEMENT: Webhook Management
# ============================================================================

class WebhookManager:
    """Manage and execute webhooks for task events"""

    def __init__(self, storage: ThreadSafeStorage):
        self.storage = storage
        self._lock = threading.Lock()
        self._queue: Queue = Queue()

    def register(self, event: str, url: str, headers: dict = None):
        webhook = {
            "id": str(uuid.uuid4()),
            "event": event,
            "url": url,
            "headers": headers or {},
            "created_at": time.time()
        }
        self.storage.update("webhooks.json", lambda d: {**d, webhook["id"]: webhook})

    def trigger(self, event: str, data: dict):
        webhooks = self.storage.read("webhooks.json", default={})
        for wh in webhooks.values():
            if wh["event"] == event:
                self._queue.put({"url": wh["url"], "headers": wh["headers"], "data": data})

    def process_queue(self):
        while not self._queue.empty():
            try:
                job = self._queue.get(timeout=1)
                # Simulate webhook execution
                pass
            except Empty:
                break

# ============================================================================
# IMPROVEMENT: Task Dependencies
# ============================================================================

class TaskDependencyGraph:
    """Manage task dependencies and execution order"""

    def __init__(self):
        self._graph: Dict[str, set] = defaultdict(set)
        self._reverse: Dict[str, set] = defaultdict(set)
        self._lock = threading.Lock()

    def add_dependency(self, task_id: str, depends_on: str):
        with self._lock:
            self._graph[task_id].add(depends_on)
            self._reverse[depends_on].add(task_id)

    def get_ready_tasks(self, completed_tasks: set) -> List[str]:
        with self._lock:
            ready = []
            for task_id in self._graph:
                if task_id not in completed_tasks:
                    if self._graph[task_id].issubset(completed_tasks):
                        ready.append(task_id)
            return ready

    def get_blocked_tasks(self, completed_tasks: set) -> List[str]:
        with self._lock:
            blocked = []
            for task_id in self._graph:
                if task_id not in completed_tasks:
                    pending = self._graph[task_id] - completed_tasks
                    if pending:
                        blocked.append(task_id)
            return blocked

# ============================================================================
# IMPROVEMENT: Async Task Executor
# ============================================================================

class AsyncTaskExecutor:
    """Execute tasks asynchronously with thread pool"""

    def __init__(self, max_workers: int = 5):
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self._futures: Dict[str, Future] = {}
        self._lock = threading.Lock()

    def submit(self, task_id: str, func: Callable, *args, **kwargs):
        with self._lock:
            future = self.executor.submit(func, *args, **kwargs)
            self._futures[task_id] = future
            return future

    def get_result(self, task_id: str, timeout: float = None) -> Any:
        with self._lock:
            if task_id not in self._futures:
                return None
            return self._futures[task_id].result(timeout=timeout)

    def cancel(self, task_id: str) -> bool:
        with self._lock:
            if task_id in self._futures:
                return self._futures[task_id].cancel()
            return False

    def shutdown(self, wait: bool = True):
        self.executor.shutdown(wait=wait)

# ============================================================================
# IMPROVEMENT: Timeline View
# ============================================================================

class TaskTimeline:
    """Track task execution timeline and history"""

    def __init__(self, storage: ThreadSafeStorage):
        self.storage = storage
        self._current: Dict[str, Dict] = {}

    def start_task(self, task_id: str, agent_id: str = None):
        self._current[task_id] = {
            "start": time.time(),
            "agent": agent_id
        }

    def complete_task(self, task_id: str):
        if task_id in self._current:
            entry = self._current.pop(task_id)
            entry["end"] = time.time()
            entry["duration"] = entry["end"] - entry["start"]
            self.storage.update("timeline.json", lambda d: {**d, task_id: entry})

    def get_task_duration(self, task_id: str) -> Optional[float]:
        data = self.storage.read("timeline.json", default={})
        return data.get(task_id, {}).get("duration")

    def get_recent(self, limit: int = 10) -> List[Dict]:
        data = self.storage.read("timeline.json", default={})
        sorted_entries = sorted(data.items(), key=lambda x: x[1].get("end", 0), reverse=True)
        return sorted_entries[:limit]

# ============================================================================
# IMPROVEMENT: Priority Task Queue
# ============================================================================

class PriorityTaskQueue:
    """Enhanced priority queue with fair scheduling"""

    def __init__(self):
        self._queues: Dict[int, List] = {i: [] for i in range(1, 6)}
        self._rr_counter = 0
        self._lock = threading.Lock()

    def add(self, task_id: str, priority: int = 3):
        with self._lock:
            self._queues[max(1, min(5, priority))].append(task_id)

    def get_next(self) -> Optional[str]:
        with self._lock:
            for _ in range(5):
                queue_idx = (self._rr_counter % 5) + 1
                if self._queues[queue_idx]:
                    self._rr_counter += 1
                    return self._queues[queue_idx].pop(0)
            return None

    def size(self) -> int:
        return sum(len(q) for q in self._queues.values())

# ============================================================================
# IMPROVEMENT: Notification System
# ============================================================================

class NotificationManager:
    """Manage user notifications and alerts"""

    def __init__(self, storage: ThreadSafeStorage):
        self.storage = storage
        self._channels: Dict[str, List[Callable]] = defaultdict(list)

    def subscribe(self, event_type: str, callback: Callable):
        self._channels[event_type].append(callback)

    def notify(self, event_type: str, data: dict):
        notifications = self.storage.read("notifications.json", default=[])
        notification = {
            "id": str(uuid.uuid4()),
            "type": event_type,
            "data": data,
            "timestamp": time.time(),
            "read": False
        }
        notifications.append(notification)
        self.storage.write("notifications.json", notifications[-100:])  # Keep last 100

        for callback in self._channels.get(event_type, []):
            try:
                callback(notification)
            except Exception:
                pass

    def get_unread(self) -> List[Dict]:
        return [n for n in self.storage.read("notifications.json", default=[]) if not n.get("read")]

# ============================================================================
# IMPROVEMENT: Task Filters
# ============================================================================

class TaskFilters:
    """Filter and search tasks with multiple criteria"""

    def __init__(self, storage: ThreadSafeStorage):
        self.storage = storage

    def filter_by_status(self, tasks: List[Task], status: TaskStatus) -> List[Task]:
        return [t for t in tasks if t.status == status]

    def filter_by_priority(self, tasks: List[Task], priority: TaskPriority) -> List[Task]:
        return [t for t in tasks if t.priority == priority]

    def filter_by_tags(self, tasks: List[Task], tags: List[str]) -> List[Task]:
        return [t for t in tasks if any(tag in t.tags for tag in tags)]

    def filter_by_date_range(self, tasks: List[Task], start: float, end: float) -> List[Task]:
        return [t for t in tasks if start <= t.created <= end]

    def search(self, tasks: List[Task], query: str) -> List[Task]:
        q = query.lower()
        return [t for t in tasks if q in t.title.lower() or q in t.description.lower()]

# ============================================================================
# IMPROVEMENT: Auto-Assignment Rules
# ============================================================================

class AutoAssignRules:
    """Rules-based automatic task assignment"""

    def __init__(self):
        self._rules: List[Dict] = []
        self._lock = threading.Lock()

    def add_rule(self, condition: Dict, action: Dict):
        with self._lock:
            self._rules.append({"condition": condition, "action": action})

    def evaluate(self, task: Task, agents: List[Agent]) -> Optional[str]:
        with self._lock:
            for rule in self._rules:
                if self._matches_condition(task, rule["condition"]):
                    return self._apply_action(rule["action"], agents)
        return None

    def _matches_condition(self, task: Task, condition: Dict) -> bool:
        if "priority" in condition and task.priority.value != condition["priority"]:
            return False
        if "tags" in condition and not any(tag in task.tags for tag in condition["tags"]):
            return False
        return True

    def _apply_action(self, action: Dict, agents: List[Agent]) -> Optional[str]:
        strategy = action.get("strategy", "least_loaded")
        if strategy == "least_loaded":
            return min(agents, key=lambda a: a.workload).id if agents else None
        elif strategy == "round_robin":
            return agents[0].id if agents else None
        return None

# ============================================================================
# IMPROVEMENT: Tags Manager
# ============================================================================

class TagsManager:
    """Manage task tags and tag-based grouping"""

    def __init__(self, storage: ThreadSafeStorage):
        self.storage = storage

    def add_tag(self, task_id: str, tag: str):
        self.storage.update("tag_index.json", lambda d: {
            **d,
            tag: list(set(d.get(tag, []) + [task_id]))
        })

    def get_tasks_by_tag(self, tag: str) -> List[str]:
        return self.storage.read("tag_index.json", default={}).get(tag, [])

    def get_all_tags(self) -> List[str]:
        return list(self.storage.read("tag_index.json", default={}).keys())

# ============================================================================
# Main Orchestrator Class
# ============================================================================

class HybridOrchestrator:
    """
    Multi-Agent Hybrid Orchestrator - Production Ready
    Features: Thread-Safe, Rate Limiting, Circuit Breaker, Knowledge Base
    """

    def __init__(self, state_dir: str = "state", enable_monitoring: bool = True):
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)

        self.storage = ThreadSafeStorage(str(self.state_dir))
        self.knowledge = KnowledgeBase(self.storage)

        self.tasks: Dict[str, Task] = {}
        self.agents: Dict[str, Agent] = {}

        self.task_queue = PriorityQueue()
        self.rate_limiter = RateLimiter(rate=10, per_seconds=1.0)
        self.circuit_breaker = CircuitBreaker()

        self.enable_monitoring = enable_monitoring
        self._lock = threading.RLock()

        self._load_state()
        self._add_orchestrator_extensions()

    def _load_state(self):
        tasks_data = self.storage.read("tasks.json", default={})
        self.tasks = {k: Task.from_dict(v) for k, v in tasks_data.items()}

        agents_data = self.storage.read("agents.json", default={})
        self.agents = {k: Agent(**v) for k, v in agents_data.items()}

        metrics.inc_counter("orchestrator_initialized")

    def _save_state(self):
        tasks_data = {k: t.to_dict() for k, t in self.tasks.items()}
        self.storage.write("tasks.json", tasks_data)

        agents_data = {k: asdict(a) for k, a in self.agents.items()}
        self.storage.write("agents.json", agents_data)

    # ... остальная часть файла ( слишком большой для полной загрузки )
    # Полный файл доступен локально в /workspace/orchestrator/orchestrator_v5.py

if __name__ == "__main__":
    print("="*60)
    print("Multi-Agent Hybrid Orchestrator v5.0")
    print("="*60)
    orchestrator = HybridOrchestrator()
    print(f"Initialized with {len(orchestrator.tasks)} tasks and {len(orchestrator.agents)} agents")
