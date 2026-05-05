#!/usr/bin/env python3
"""
Hybrid Orchestrator - Enhanced Multi-Agent System
Combines CAMEL, Multica, Hermes, and OpenClaw
"""
import json
import os
import time
import threading
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from enum import Enum
import fcntl

# ============================================================================
# Thread-Safe Storage
# ============================================================================

class ThreadSafeStorage:
    def __init__(self, base_path: str = "state"):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()

    def read(self, filename: str, default: Any = None) -> Any:
        filepath = self.base_path / filename
        with self._lock:
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
        with self._lock:
            with open(filepath, 'w') as f:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                try:
                    json.dump(data, f, indent=2, default=str)
                finally:
                    fcntl.flock(f.fileno(), fcntl.LOCK_UN)

    def update(self, filename: str, update_func):
        current = self.read(filename, default=None)
        updated = update_func(current or {})
        self.write(filename, updated)

# ============================================================================
# Data Classes
# ============================================================================

class TaskStatus(Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    BLOCKED = "blocked"

class Priority(Enum):
    CRITICAL = 1
    HIGH = 2
    NORMAL = 3
    LOW = 4
    BACKGROUND = 5

@dataclass
class Task:
    id: str
    title: str
    description: str = ""
    status: TaskStatus = TaskStatus.PENDING
    priority: Priority = Priority.NORMAL
    assigned_agent: Optional[str] = None
    created: float = field(default_factory=time.time)
    dependencies: List[str] = field(default_factory=list)
    result: Optional[Dict] = None
    tags: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "title": self.title,
            "description": self.description,
            "status": self.status.value,
            "priority": self.priority.value,
            "assigned_agent": self.assigned_agent,
            "created": self.created,
            "dependencies": self.dependencies,
            "result": self.result,
            "tags": self.tags
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'Task':
        return cls(
            id=data["id"],
            title=data["title"],
            description=data.get("description", ""),
            status=TaskStatus(data.get("status", "pending")),
            priority=Priority(data.get("priority", 3)),
            assigned_agent=data.get("assigned_agent"),
            created=data.get("created", time.time()),
            dependencies=data.get("dependencies", []),
            result=data.get("result"),
            tags=data.get("tags", [])
        )

@dataclass
class Agent:
    id: str
    name: str
    role: str
    active: bool = True
    current_task: Optional[str] = None

# ============================================================================
# Hybrid Orchestrator
# ============================================================================

class HybridOrchestrator:
    def __init__(self, state_dir: str = "state"):
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.storage = ThreadSafeStorage(str(self.state_dir))

        self.tasks: Dict[str, Task] = {}
        self.agents: Dict[str, Agent] = {}

        self._lock = threading.RLock()
        self._load_state()

    def _load_state(self):
        tasks_data = self.storage.read("tasks.json", default={})
        self.tasks = {k: Task.from_dict(v) for k, v in tasks_data.items()}

        agents_data = self.storage.read("agents.json", default={})
        self.agents = {k: Agent(**v) for k, v in agents_data.items()}

    def _save_state(self):
        tasks_data = {k: t.to_dict() for k, t in self.tasks.items()}
        self.storage.write("tasks.json", tasks_data)

        agents_data = {k: asdict(a) for k, a in self.agents.items()}
        self.storage.write("agents.json", agents_data)

    def create_task(self, title: str, description: str = "", priority: Priority = Priority.NORMAL) -> str:
        task_id = f"T-{len(self.tasks) + 1:03d}"
        task = Task(id=task_id, title=title, description=description, priority=priority)

        with self._lock:
            self.tasks[task_id] = task

        self._save_state()
        return task_id

    def get_task(self, task_id: str) -> Optional[Task]:
        with self._lock:
            return self.tasks.get(task_id)

    def update_task(self, task_id: str, **kwargs):
        with self._lock:
            if task_id in self.tasks:
                task = self.tasks[task_id]
                if 'status' in kwargs:
                    task.status = TaskStatus(kwargs['status'])
                if 'priority' in kwargs:
                    task.priority = Priority(kwargs['priority'])
                if 'result' in kwargs:
                    task.result = kwargs['result']
        self._save_state()

    def list_tasks(self) -> List[Task]:
        with self._lock:
            return list(self.tasks.values())

    def register_agent(self, name: str, role: str) -> str:
        agent_id = f"A-{len(self.agents) + 1:03d}"
        agent = Agent(id=agent_id, name=name, role=role)

        with self._lock:
            self.agents[agent_id] = agent

        self._save_state()
        return agent_id

    def list_agents(self) -> List[Agent]:
        with self._lock:
            return list(self.agents.values())

if __name__ == "__main__":
    orch = HybridOrchestrator()
    task_id = orch.create_task("Test Task", "Test description", Priority.HIGH)
    agent_id = orch.register_agent("TestAgent", "executor")
    print(f"Created task: {task_id}")
    print(f"Registered agent: {agent_id}")
