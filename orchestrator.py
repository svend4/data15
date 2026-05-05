#!/usr/bin/env python3
"""
MiniMax Multi-Agent Orchestrator v2.1
=====================================
A complete multi-agent orchestration system with:
- CAMEL-style strategic layer (task decomposition)
- Multica-style task management (board, queue, status)
- Hermes/OpenClaw execution layer (internal/external agents)
- Real OpenClaw integration for external tasks
- File-based persistence
- Scheduled tasks support
"""

import json
import os
import sys
import argparse
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any
from dataclasses import dataclass, asdict
from enum import Enum
import uuid

# ============================================================================
# Configuration
# ============================================================================

ORCHESTRATOR_DIR = Path("/workspace/orchestrator")
TASKS_DIR = ORCHESTRATOR_DIR / "tasks"
LOGS_DIR = ORCHESTRATOR_DIR / "logs"
STATE_DIR = ORCHESTRATOR_DIR / "state"
SKILLS_DIR = ORCHESTRATOR_DIR / "skills"
MEMORY_DIR = ORCHESTRATOR_DIR / "memory"

BOARD_FILE = TASKS_DIR / "board.json"
STATE_FILE = STATE_DIR / "orchestrator_state.json"
CONFIG_FILE = ORCHESTRATOR_DIR / "config.json"

# Create directories
for d in [TASKS_DIR, LOGS_DIR, STATE_DIR, SKILLS_DIR, MEMORY_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# ============================================================================
# Enums
# ============================================================================

class TaskStatus(Enum):
    QUEUED = "queued"
    DISPATCHED = "dispatched"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    BLOCKED = "blocked"
    CANCELLED = "cancelled"
    NEEDS_REVIEW = "needs_review"

class Priority(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

class AgentType(Enum):
    HERMES = "Hermes"
    OPENCLAW = "OpenClaw"
    RESEARCHER = "Researcher"
    REPORT_WRITER = "ReportWriter"
    DOCCX_PROCESSOR = "DocxProcessor"
    PDF_PROCESSOR = "PdfProcessor"

class Layer(Enum):
    CAMEL = "camel"
    MULTICA = "multica"
    EXECUTION = "execution"

# ============================================================================
# Data Classes
# ============================================================================

@dataclass
class Task:
    id: str
    title: str
    description: str
    agent: str
    status: str
    priority: str
    created: str
    updated: str
    progress: int
    dependencies: List[str]
    blocked_by: List[str]
    subtasks: List[str]
    comments: List[Dict]
    runs: List[Dict]
    layer: str
    complexity: int
    tags: List[str]

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> 'Task':
        return cls(**data)

@dataclass
class OrchestratorState:
    version: str
    created: str
    updated: str
    active_layer: str
    current_mode: str
    stats: Dict[str, int]
    active_tasks: List[str]
    completed_tasks: List[str]
    failed_tasks: List[str]
    agents_online: List[str]

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> 'Task':
        return cls(**data)

# ============================================================================
# Board Manager
# ============================================================================

class BoardManager:
    """Multica-style task board management"""

    def __init__(self, board_file: Path = BOARD_FILE):
        self.board_file = board_file
        self._ensure_board_exists()

    def _ensure_board_exists(self):
        if not self.board_file.exists():
            self._create_empty_board()

    def _create_empty_board(self):
        data = {
            "version": "1.0",
            "created": self._timestamp(),
            "updated": self._timestamp(),
            "stats": {
                "total": 0,
                "queued": 0,
                "dispatched": 0,
                "running": 0,
                "completed": 0,
                "failed": 0,
                "blocked": 0
            },
            "tasks": []
        }
        self._save(data)

    def _load(self) -> dict:
        with open(self.board_file, 'r') as f:
            return json.load(f)

    def _save(self, data: dict):
        with open(self.board_file, 'w') as f:
            json.dump(data, f, indent=2)

    def _timestamp(self) -> str:
        return datetime.utcnow().isoformat() + 'Z'

    def _generate_id(self) -> str:
        data = self._load()
        count = data['stats']['total']
        return f"T-{count + 1:03d}"

    def add_task(self, title: str, description: str = "", agent: str = "Hermes",
                 priority: str = "medium", layer: str = "execution",
                 complexity: int = 5, tags: List[str] = None) -> Task:
        data = self._load()
        task_id = self._generate_id()
        now = self._timestamp()

        task = Task(
            id=task_id,
            title=title,
            description=description,
            agent=agent,
            status=TaskStatus.QUEUED.value,
            priority=priority,
            created=now,
            updated=now,
            progress=0,
            dependencies=[],
            blocked_by=[],
            subtasks=[],
            comments=[],
            runs=[],
            layer=layer,
            complexity=complexity,
            tags=tags or []
        )
        data['tasks'].append(task.to_dict())
        data['stats']['total'] += 1
        data['stats']['queued'] += 1
        data['updated'] = now
        self._save(data)
        return task

    def get_task(self, task_id: str) -> Optional[Task]:
        data = self._load()
        for task_data in data['tasks']:
            if task_data['id'] == task_id:
                return Task.from_dict(task_data)
        return None

    def update_task_status(self, task_id: str, status: str, progress: int = None) -> bool:
        data = self._load()
        now = self._timestamp()
        for task_data in data['tasks']:
            if task_data['id'] == task_id:
                old_status = task_data['status']
                data['stats'][status] = data['stats'].get(status, 0) + 1
                task_data['status'] = status
                task_data['updated'] = now
                if progress is not None:
                    task_data['progress'] = progress
                self._save(data)
                return True
        return False

    def assign_task(self, task_id: str, agent: str) -> bool:
        data = self._load()
        now = self._timestamp()
        for task_data in data['tasks']:
            if task_data['id'] == task_id:
                task_data['agent'] = agent
                task_data['status'] = TaskStatus.DISPATCHED.value
                task_data['updated'] = now
                data['stats']['queued'] -= 1
                data['stats']['dispatched'] += 1
                self._save(data)
                return True
        return False

    def list_tasks(self, status: str = None, agent: str = None) -> List[Task]:
        data = self._load()
        tasks = [Task.from_dict(t) for t in data['tasks']]
        if status:
            tasks = [t for t in tasks if t.status == status]
        if agent:
            tasks = [t for t in tasks if t.agent == agent]
        return tasks

    def get_stats(self) -> dict:
        return self._load()['stats']

# ============================================================================
# Orchestrator Core
# ============================================================================

class Orchestrator:
    """Main Orchestrator class with CAMEL + Multica + Hermes/OpenClaw layers"""

    def __init__(self):
        self.board = BoardManager()
        self.state = self._load_state()

    def _load_state(self) -> OrchestratorState:
        if STATE_FILE.exists():
            with open(STATE_FILE, 'r') as f:
                return OrchestratorState(**json.load(f))
        else:
            state = OrchestratorState(
                version="1.0",
                created=self._timestamp(),
                updated=self._timestamp(),
                active_layer="execution",
                current_mode="combined",
                stats={"total_tasks": 0, "completed": 0},
                active_tasks=[],
                completed_tasks=[],
                failed_tasks=[],
                agents_online=["Hermes", "OpenClaw"]
            )
            self._save_state(state)
            return state

    def _save_state(self, state: OrchestratorState):
        with open(STATE_FILE, 'w') as f:
            json.dump(state.to_dict(), f, indent=2)

    def _timestamp(self) -> str:
        return datetime.utcnow().isoformat() + 'Z'

    def decompose_goal(self, goal: str, depth: int = 3) -> List[Dict]:
        tasks = []
        if depth >= 3:
            tasks.append({"title": f"Исследовать: {goal}", "agent": "Researcher", "priority": "high", "layer": "camel", "type": "research"})
        if depth >= 2:
            tasks.append({"title": f"Проанализировать: {goal}", "agent": "Hermes", "priority": "high", "layer": "execution", "type": "analysis"})
        tasks.append({"title": f"Выполнить: {goal}", "agent": "Hermes", "priority": "medium", "layer": "execution", "type": "creation"})
        if depth >= 1:
            tasks.append({"title": f"Проверить результаты: {goal}", "agent": "Hermes", "priority": "medium", "layer": "execution", "type": "review"})
        return tasks

    def plan_workflow(self, goal: str) -> List[Task]:
        subtasks = self.decompose_goal(goal)
        created_tasks = []
        for i, subtask in enumerate(subtasks):
            task = self.board.add_task(
                title=subtask['title'],
                description=f"Подзадача {i+1} из плана для: {goal}",
                agent=subtask['agent'],
                priority=subtask['priority'],
                layer=subtask['layer'],
                complexity=subtask.get('complexity', 5),
                tags=[subtask['type'], 'camel-plan']
            )
            created_tasks.append(task)
        return created_tasks

    def show_board(self) -> str:
        stats = self.board.get_stats()
        lines = []
        lines.append("=" * 60)
        lines.append("MULTI-AGENT ORCHESTRATOR BOARD")
        lines.append("=" * 60)
        lines.append(f"Stats: Total={stats['total']} | Queued={stats['queued']} | Running={stats['running']} | Done={stats['completed']} | Failed={stats['failed']}")
        lines.append("QUEUED:")
        for task in self.board.list_tasks(status="queued"):
            lines.append(f"  [{task.id}] {task.title} -> {task.agent}")
        lines.append("RUNNING:")
        for task in self.board.list_tasks(status="running"):
            lines.append(f"  [{task.id}] {task.title} -> {task.agent} [{task.progress}%]")
        lines.append("COMPLETED:")
        for task in self.board.list_tasks(status="completed"):
            lines.append(f"  [{task.id}] {task.title} -> {task.agent}")
        lines.append("=" * 60)
        return "\n".join(lines)

    def create_task(self, title: str, **kwargs) -> Task:
        return self.board.add_task(title, **kwargs)

    def start_task(self, task_id: str) -> bool:
        return self.board.update_task_status(task_id, TaskStatus.RUNNING.value)

    def complete_task(self, task_id: str) -> bool:
        return self.board.update_task_status(task_id, TaskStatus.COMPLETED.value, 100)

if __name__ == "__main__":
    orch = Orchestrator()
    print(orch.show_board())
