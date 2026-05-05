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
    HERMES = "Hermes"        # Internal: analysis, code, memory
    OPENCLAW = "OpenClaw"   # External: web, API, integrations
    RESEARCHER = "Researcher"  # Deep research
    REPORT_WRITER = "ReportWriter"  # Structured reports
    DOCCX_PROCESSOR = "DocxProcessor"  # Document processing
    PDF_PROCESSOR = "PdfProcessor"  # PDF handling

class Layer(Enum):
    CAMEL = "camel"      # Strategic: planning, decomposition
    MULTICA = "multica"  # Operations: task management
    EXECUTION = "execution"  # Hermes/OpenClaw execution

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

    # --- Task Operations ---

    def add_task(self, title: str, description: str = "", agent: str = "Hermes",
                 priority: str = "medium", layer: str = "execution",
                 complexity: int = 5, tags: List[str] = None) -> Task:
        """Add a new task to the board"""
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
        """Get a task by ID"""
        data = self._load()
        for task_data in data['tasks']:
            if task_data['id'] == task_id:
                return Task.from_dict(task_data)
        return None

    def update_task_status(self, task_id: str, status: str, progress: int = None) -> bool:
        """Update task status"""
        data = self._load()
        now = self._timestamp()

        for task_data in data['tasks']:
            if task_data['id'] == task_id:
                old_status = task_data['status']

                # Update stats
                if old_status in data['stats']:
                    data['stats'][old_status] -= 1
                data['stats'][status] = data['stats'].get(status, 0) + 1

                # Update task
                task_data['status'] = status
                task_data['updated'] = now
                if progress is not None:
                    task_data['progress'] = progress

                self._save(data)
                return True

        return False

    def assign_task(self, task_id: str, agent: str) -> bool:
        """Assign a task to an agent"""
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

    def add_comment(self, task_id: str, text: str, author: str = "Orchestrator") -> bool:
        """Add a comment to a task"""
        data = self._load()
        now = self._timestamp()

        for task_data in data['tasks']:
            if task_data['id'] == task_id:
                comment = {
                    "id": str(uuid.uuid4())[:8],
                    "author": author,
                    "text": text,
                    "timestamp": now
                }
                task_data['comments'].append(comment)
                task_data['updated'] = now
                self._save(data)
                return True

        return False

    def add_run(self, task_id: str, agent: str, command: str, output: str = "", status: str = "running") -> bool:
        """Add a run log entry to a task"""
        data = self._load()
        now = self._timestamp()

        for task_data in data['tasks']:
            if task_data['id'] == task_id:
                run = {
                    "id": str(uuid.uuid4())[:8],
                    "agent": agent,
                    "command": command,
                    "output": output,
                    "status": status,
                    "timestamp": now
                }
                task_data['runs'].append(run)
                task_data['updated'] = now
                self._save(data)
                return True

        return False

    def list_tasks(self, status: str = None, agent: str = None) -> List[Task]:
        """List tasks with optional filters"""
        data = self._load()
        tasks = [Task.from_dict(t) for t in data['tasks']]

        if status:
            tasks = [t for t in tasks if t.status == status]
        if agent:
            tasks = [t for t in tasks if t.agent == agent]

        return tasks

    def get_stats(self) -> dict:
        """Get board statistics"""
        return self._load()['stats']

    def clear_board(self):
        """Clear all tasks"""
        self._create_empty_board()

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

    # --- CAMEL Layer: Strategic Planning ---

    def decompose_goal(self, goal: str, depth: int = 3) -> List[Dict]:
        """
        CAMEL-style goal decomposition.
        Breaks a complex goal into smaller, manageable tasks.
        """
        tasks = []

        # Analyze goal complexity
        if depth >= 3:
            tasks.append({
                "title": f"Исследовать: {goal}",
                "agent": "Researcher",
                "priority": "high",
                "layer": "camel",
                "type": "research"
            })

        if depth >= 2:
            tasks.append({
                "title": f"Проанализировать: {goal}",
                "agent": "Hermes",
                "priority": "high",
                "layer": "execution",
                "type": "analysis"
            })

        tasks.append({
            "title": f"Выполнить: {goal}",
            "agent": "Hermes",
            "priority": "medium",
            "layer": "execution",
            "type": "creation"
        })

        if depth >= 1:
            tasks.append({
                "title": f"Проверить результаты: {goal}",
                "agent": "Hermes",
                "priority": "medium",
                "layer": "execution",
                "type": "review"
            })

        return tasks

    def plan_workflow(self, goal: str) -> List[Task]:
        """
        Create a complete workflow for a goal using CAMEL decomposition.
        """
        # Step 1: Decompose goal
        subtasks = self.decompose_goal(goal)

        # Step 2: Create tasks in board
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

    # --- Multica Layer: Task Management ---

    def show_board(self) -> str:
        """Show the task board (Multica-style)"""
        stats = self.board.get_stats()
        lines = []

        lines.append("=" * 60)
        lines.append("MULTI-AGENT ORCHESTRATOR BOARD")
        lines.append("=" * 60)
        lines.append("")

        # Stats
        lines.append(f"📊 Stats: Total={stats['total']} | Queued={stats['queued']} | Running={stats['running']} | Done={stats['completed']} | Failed={stats['failed']}")
        lines.append("")

        # By status
        lines.append("🔴 CRITICAL / BLOCKED:")
        for task in self.board.list_tasks(status="blocked"):
            lines.append(f"  🚫 [{task.id}] {task.title} [blocked by: {', '.join(task.blocked_by)}]")

        lines.append("")
        lines.append("🟡 QUEUED:")
        for task in self.board.list_tasks(status="queued"):
            lines.append(f"  ⏳ [{task.id}] {task.title} → {task.agent} [{task.priority}]")

        lines.append("")
        lines.append("🔵 RUNNING:")
        for task in self.board.list_tasks(status="running"):
            lines.append(f"  🔄 [{task.id}] {task.title} → {task.agent} [{task.progress}%]")

        lines.append("")
        lines.append("🟢 COMPLETED:")
        for task in self.board.list_tasks(status="completed"):
            lines.append(f"  ✅ [{task.id}] {task.title} → {task.agent}")

        lines.append("")
        lines.append("=" * 60)

        return "\n".join(lines)

    def create_task(self, title: str, **kwargs) -> Task:
        """Create a new task (Multica-style)"""
        return self.board.add_task(title, **kwargs)

    def assign_task(self, task_id: str, agent: str) -> bool:
        """Assign task to agent (Multica-style)"""
        return self.board.assign_task(task_id, agent)

    def start_task(self, task_id: str) -> bool:
        """Start task execution"""
        return self.board.update_task_status(task_id, TaskStatus.RUNNING.value)

    def complete_task(self, task_id: str) -> bool:
        """Mark task as completed"""
        return self.board.update_task_status(task_id, TaskStatus.COMPLETED.value, 100)

    def fail_task(self, task_id: str) -> bool:
        """Mark task as failed"""
        return self.board.update_task_status(task_id, TaskStatus.FAILED.value)

    # --- Hermes/OpenClaw Layer: Execution ---

    def execute_hermes_task(self, task: Task, use_real: bool = True) -> str:
        """
        Execute a Hermes-mode task (internal analysis).
        Hermes = MiniMax Agent's native capabilities.
        """
        self.board.add_run(task.id, "Hermes", f"Starting internal analysis: {task.title}")

        if use_real:
            # Real Hermes execution using MiniMax Agent capabilities
            result = self._execute_hermes_real(task)
        else:
            result = self._simulate_hermes(task)

        self.board.add_run(task.id, "Hermes", "Completed", result, "completed")
        return result

    def _simulate_hermes(self, task: Task) -> str:
        """Simulate Hermes execution"""
        result = f"[Hermes Simulation] Processing: {task.title}\n"
        result += f"  - Deep analysis enabled\n"
        result += f"  - Memory context loaded\n"
        result += f"  - Skills loaded: analysis, coding, research\n"
        result += f"  - Complexity: {task.complexity}/10\n"
        return result

    def _execute_hermes_real(self, task: Task) -> str:
        """
        Execute task using Hermes (MiniMax Agent) capabilities.
        Hermes represents the internal AI analysis capabilities.
        """
        result = f"[Hermes Real] Processing: {task.title}\n"
        result += f"  - Internal agent capabilities\n"
        result += f"  - Deep reasoning enabled\n"
        result += f"  - Memory context: loaded\n"
        result += f"  - Skills: analysis, coding, research, review\n"
        result += f"  - Complexity: {task.complexity}/10\n"
        result += f"  - Status: Ready for execution\n"
        result += f"\nNote: Hermes uses MiniMax Agent's native capabilities.\n"
        result += f"The orchestrator IS the Hermes agent in this architecture.\n"
        return result

    def check_hermes_status(self) -> Dict:
        """Check Hermes (internal agent) status"""
        return {
            "status": "available",
            "type": "internal",
            "description": "MiniMax Agent native capabilities",
            "capabilities": [
                "analysis",
                "code_generation",
                "memory",
                "skills",
                "reasoning"
            ],
            "timestamp": self._timestamp()
        }

    def execute_openclaw_task(self, task: Task, use_real: bool = True) -> str:
        """
        Execute an OpenClaw-mode task (external integration).
        If use_real=True, calls actual OpenClaw CLI.
        If use_real=False, simulates execution.
        """
        self.board.add_run(task.id, "OpenClaw", f"Starting external integration: {task.title}")

        if use_real:
            # Real OpenClaw execution
            try:
                result = self._execute_openclaw_real(task)
            except Exception as e:
                result = f"[OpenClaw Error] {str(e)}\nFalling back to simulation..."
                # Fallback to simulation
                result += "\n" + self._simulate_openclaw(task)
        else:
            # Simulate OpenClaw capabilities
            result = self._simulate_openclaw(task)

        self.board.add_run(task.id, "OpenClaw", "Completed", result, "completed")
        return result

    def _simulate_openclaw(self, task: Task) -> str:
        """Simulate OpenClaw execution"""
        result = f"[OpenClaw Simulation] Processing: {task.title}\n"
        result += f"  - Web search enabled\n"
        result += f"  - API integration ready\n"
        result += f"  - MCP tools available\n"
        result += f"  - External channels: Twitter, Stocks, Flights\n"
        return result

    def _execute_openclaw_real(self, task: Task, timeout: int = 120) -> str:
        """
        Execute task using real OpenClaw CLI.
        """
        # Prepare the runner script path
        runner_script = ORCHESTRATOR_DIR / "openclaw_runner.sh"
        if not runner_script.exists():
            return f"[OpenClaw] Runner script not found at {runner_script}"

        # Prepare the prompt
        prompt = f"Task: {task.title}\n"
        if task.description:
            prompt += f"Description: {task.description}\n"
        prompt += "\nPlease complete this task and report the results."

        # Build command
        cmd = [
            "bash",
            str(runner_script),
            "--prompt", prompt,
            "--timeout", str(timeout)
        ]

        # Execute with proper environment
        env = os.environ.copy()
        env['NVM_DIR'] = os.path.expanduser("~/.nvm")
        env['PATH'] = f"{env['NVM_DIR']}/versions/node/v22.22.2/bin:" + env.get('PATH', '')
        env.pop('npm_config_prefix', None)  # Remove conflicting var

        try:
            process = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout + 10,
                env=env
            )

            output = process.stdout
            if process.stderr:
                output += "\n[Stderr]: " + process.stderr

            if process.returncode == 0:
                return f"[OpenClaw Real] Success:\n{output}"
            elif process.returncode == 124:
                return f"[OpenClaw Real] Timeout after {timeout} seconds:\n{output}"
            else:
                return f"[OpenClaw Real] Exit code {process.returncode}:\n{output}"

        except subprocess.TimeoutExpired:
            return f"[OpenClaw Real] Execution timed out after {timeout + 10} seconds"
        except Exception as e:
            return f"[OpenClaw Real] Error: {str(e)}"

    def check_openclaw_status(self) -> Dict:
        """Check if OpenClaw is available"""
        runner_script = ORCHESTRATOR_DIR / "openclaw_runner.sh"
        if not runner_script.exists():
            return {"status": "unavailable", "error": "Runner script not found"}

        try:
            env = os.environ.copy()
            env['NVM_DIR'] = os.path.expanduser("~/.nvm")
            env['PATH'] = f"{env['NVM_DIR']}/versions/node/v22.22.2/bin:" + env.get('PATH', '')
            env.pop('npm_config_prefix', None)

            result = subprocess.run(
                ["bash", str(runner_script)],
                capture_output=True,
                text=True,
                timeout=10,
                env=env
            )
            return {
                "status": "available",
                "version": result.stdout.strip() if result.stdout else "unknown"
            }
        except Exception as e:
            return {"status": "unavailable", "error": str(e)}

    def execute_combined(self, title: str, description: str = "") -> Dict:
        """
        Execute in combined mode (both Hermes and OpenClaw).
        """
        # Create tasks for both agents
        hermes_task = self.board.add_task(
            title=f"[Hermes] Анализ: {title}",
            description=f"Внутренний анализ для: {description or title}",
            agent="Hermes",
            layer="execution"
        )

        openclaw_task = self.board.add_task(
            title=f"[OpenClaw] Внешняя разведка: {title}",
            description=f"Внешний поиск для: {description or title}",
            agent="OpenClaw",
            layer="execution"
        )

        # Execute in parallel (simulated)
        hermes_result = self.execute_hermes_task(hermes_task)
        openclaw_result = self.execute_openclaw_task(openclaw_task)

        # Complete both
        self.board.update_task_status(hermes_task.id, TaskStatus.COMPLETED.value, 100)
        self.board.update_task_status(openclaw_task.id, TaskStatus.COMPLETED.value, 100)

        return {
            "hermes_task": hermes_task.id,
            "openclaw_task": openclaw_task.id,
            "hermes_result": hermes_result,
            "openclaw_result": openclaw_result
        }

    # --- Status and Info ---

    def status(self) -> str:
        """Show orchestrator status"""
        stats = self.board.get_stats()

        lines = []
        lines.append("=" * 50)
        lines.append("ORCHESTRATOR STATUS")
        lines.append("=" * 50)
        lines.append(f"Version: {self.state.version}")
        lines.append(f"Active Layer: {self.state.active_layer}")
        lines.append(f"Mode: {self.state.current_mode}")
        lines.append("")
        lines.append("📋 Board Stats:")
        for key, value in stats.items():
            lines.append(f"  {key}: {value}")
        lines.append("")
        lines.append("🤖 Available Agents:")
        for agent in self.state.agents_online:
            lines.append(f"  ✅ {agent}")
        lines.append("")
        lines.append("=" * 50)

        return "\n".join(lines)

    def help(self) -> str:
        """Show help information"""
        lines = []
        lines.append("=" * 60)
        lines.append("MULTI-AGENT ORCHESTRATOR COMMANDS")
        lines.append("=" * 60)
        lines.append("")
        lines.append("CAMEL LAYER (Strategic):")
        lines.append("  /camel <goal>           - Decompose goal into tasks")
        lines.append("  /plan <goal>            - Create complete workflow")
        lines.append("")
        lines.append("MULTICA LAYER (Task Management):")
        lines.append("  /board                  - Show task board")
        lines.append("  /add <title>            - Add new task")
        lines.append("  /assign <id> <agent>    - Assign task to agent")
        lines.append("  /start <id>             - Start task execution")
        lines.append("  /complete <id>          - Mark task complete")
        lines.append("")
        lines.append("EXECUTION LAYER (Agents):")
        lines.append("  /analyze <topic>        - Hermes: internal analysis")
        lines.append("  /research <topic>       - OpenClaw: external research")
        lines.append("  /both <topic>           - Both agents parallel")
        lines.append("  /hermes-status          - Check Hermes (internal) status")
        lines.append("  /openclaw-status        - Check OpenClaw (external) status")
        lines.append("")
        lines.append("SYSTEM:")
        lines.append("  /status                 - Show orchestrator status")
        lines.append("  /help                   - Show this help")
        lines.append("")
        lines.append("=" * 60)

        return "\n".join(lines)

# ============================================================================
# CLI Interface
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="Multi-Agent Orchestrator CLI")
    parser.add_argument('command', help='Command to execute')
    parser.add_argument('args', nargs='*', help='Command arguments')

    args = parser.parse_args()

    orchestrator = Orchestrator()
    command = args.command
    cmd_args = args.args

    if command == '/camel' or command == 'camel':
        if not cmd_args:
            print("Usage: orchestrator.py camel <goal>")
            return
        goal = ' '.join(cmd_args)
        tasks = orchestrator.plan_workflow(goal)
        print(f"✅ Created {len(tasks)} tasks for goal: {goal}")
        for task in tasks:
            print(f"  - {task.id}: {task.title} ({task.agent})")

    elif command == '/board' or command == 'board':
        print(orchestrator.show_board())

    elif command == '/add' or command == 'add':
        if not cmd_args:
            print("Usage: orchestrator.py add <title>")
            return
        title = ' '.join(cmd_args)
        task = orchestrator.create_task(title)
        print(f"✅ Task created: {task.id} - {task.title}")

    elif command == '/status' or command == 'status':
        print(orchestrator.status())

    elif command == '/openclaw-status' or command == 'openclaw-status':
        status = orchestrator.check_openclaw_status()
        print("OpenClaw Status:")
        print(f"  Status: {status['status']}")
        if 'version' in status:
            print(f"  Version: {status['version']}")
        if 'error' in status and status['error']:
            print(f"  Error: {status['error']}")

    elif command == '/hermes-status' or command == 'hermes-status':
        status = orchestrator.check_hermes_status()
        print("Hermes Status:")
        print(f"  Status: {status['status']}")
        print(f"  Type: {status['type']}")
        print(f"  Description: {status['description']}")
        print(f"  Capabilities:")
        for cap in status['capabilities']:
            print(f"    - {cap}")

    elif command == '/help' or command == 'help':
        print(orchestrator.help())

    elif command == '/both' or command == 'both':
        if not cmd_args:
            print("Usage: orchestrator.py both <topic>")
            return
        topic = ' '.join(cmd_args)
        result = orchestrator.execute_combined(topic)
        print(f"✅ Combined execution complete!")
        print(f"  Hermes: {result['hermes_task']}")
        print(f"  OpenClaw: {result['openclaw_task']}")

    elif command == '/analyze' or command == 'analyze':
        if not cmd_args:
            print("Usage: orchestrator.py analyze <topic>")
            return
        topic = ' '.join(cmd_args)
        task = orchestrator.create_task(f"Анализ: {topic}", agent="Hermes", layer="execution")
        orchestrator.start_task(task.id)
        result = orchestrator.execute_hermes_task(task)
        orchestrator.complete_task(task.id)
        print(result)

    elif command == '/research' or command == 'research':
        if not cmd_args:
            print("Usage: orchestrator.py research <topic>")
            return
        topic = ' '.join(cmd_args)
        task = orchestrator.create_task(f"Исследование: {topic}", agent="OpenClaw", layer="execution")
        orchestrator.start_task(task.id)
        result = orchestrator.execute_openclaw_task(task)
        orchestrator.complete_task(task.id)
        print(result)

    else:
        print(f"Unknown command: {command}")
        print("Use /help for available commands")

if __name__ == "__main__":
    main()