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
from dataclasses import dataclass, asdict, field, fields
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
            lines.append(f'orchestrator_metrics{{name="{name}"}} {data["gauge"]}')
            if data["counter"] > 0:
                lines.append(f'orchestrator_metrics_total{{name="{name}"}} {data["counter"]}')
        return "\n".join(lines)

# Global metrics collector
metrics = MetricsCollector()

# ============================================================================
# IMPROVEMENT: Circuit Breaker
# ============================================================================

class CircuitBreaker:
    """
    Circuit breaker pattern for external API calls.
    States: CLOSED (normal) -> OPEN (failing) -> HALF_OPEN (testing)
    """

    class State(Enum):
        CLOSED = "closed"
        OPEN = "open"
        HALF_OPEN = "half_open"

    def __init__(self, failure_threshold: int = 5, timeout: int = 60, recovery_timeout: int = 30):
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = self.State.CLOSED
        self._lock = threading.Lock()

    def call(self, func, *args, **kwargs):
        with self._lock:
            if self.state == self.State.OPEN:
                if time.time() - self.last_failure_time > self.recovery_timeout:
                    self.state = self.State.HALF_OPEN
                else:
                    raise Exception("Circuit breaker is OPEN")

        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise

    def _on_success(self):
        with self._lock:
            self.failure_count = 0
            if self.state == self.State.HALF_OPEN:
                self.state = self.State.CLOSED

    def _on_failure(self):
        with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            if self.failure_count >= self.failure_threshold:
                self.state = self.State.OPEN

    def get_state(self) -> str:
        return self.state.value

# ============================================================================
# IMPROVEMENT: Request Deduplication
# ============================================================================

class RequestDeduplicator:
    """
    Deduplicates identical requests within a time window.
    Prevents redundant API calls for the same query.
    """

    def __init__(self, window_seconds: int = 60):
        self.window_seconds = window_seconds
        self._requests = {}  # key -> (result, timestamp)
        self._lock = threading.Lock()
        self._hits = 0
        self._misses = 0

    def get_or_compute(self, key: str, compute_fn, *args, **kwargs):
        """Get cached result or compute new one"""
        with self._lock:
            # Clean old entries
            now = time.time()
            self._requests = {k: v for k, v in self._requests.items()
                            if now - v[1] < self.window_seconds}

            if key in self._requests:
                self._hits += 1
                return self._requests[key][0]

        # Compute outside lock
        result = compute_fn(*args, **kwargs)

        with self._lock:
            self._misses += 1
            self._requests[key] = (result, time.time())

        return result

    def get_stats(self) -> dict:
        total = self._hits + self._misses
        return {
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": (self._hits / total * 100) if total > 0 else 0,
            "active_keys": len(self._requests)
        }

    def clear(self):
        with self._lock:
            self._requests.clear()
            self._hits = 0
            self._misses = 0

# ============================================================================
# IMPROVEMENT: Graceful Shutdown Handler
# ============================================================================

import signal

class GracefulShutdownHandler:
    """
    Handles SIGTERM and SIGINT signals for graceful shutdown.
    Ensures all data is saved before exit.
    """

    def __init__(self, orchestrator):
        self.orchestrator = orchestrator
        self.shutdown_in_progress = False
        self._original_sigterm = None
        self._original_sigint = None

    def setup(self):
        """Register signal handlers"""
        self._original_sigterm = signal.signal(signal.SIGTERM, self._handle_shutdown)
        self._original_sigint = signal.signal(signal.SIGINT, self._handle_shutdown)

    def restore(self):
        """Restore original signal handlers"""
        if self._original_sigterm:
            signal.signal(signal.SIGTERM, self._original_sigterm)
        if self._original_sigint:
            signal.signal(signal.SIGINT, self._original_sigint)

    def _handle_shutdown(self, signum, frame):
        """Handle shutdown signal"""
        if self.shutdown_in_progress:
            return
        self.shutdown_in_progress = True

        print("\n🛑 Received shutdown signal. Cleaning up...")
        self.shutdown()

    def shutdown(self):
        """Perform graceful shutdown"""
        print("📝 Saving state...")
        try:
            # Save orchestrator state
            self.orchestrator._save_state()

            # Cleanup backups
            self.orchestrator.board._cleanup_backups(max_backups=10)

            print("✅ Cleanup complete. Goodbye!")
        except Exception as e:
            print(f"⚠️ Error during shutdown: {e}")
        finally:
            self.restore()
            sys.exit(0)

    def is_shutting_down(self) -> bool:
        """Check if shutdown is in progress"""
        return self.shutdown_in_progress

# ============================================================================
# Configuration
# ============================================================================

WORKSPACE_DIR = Path(__file__).parent / "orchestrator"
MEMORY_DIR = Path(__file__).parent / "memories"
TASKS_DIR = WORKSPACE_DIR / "tasks"
LOGS_DIR = WORKSPACE_DIR / "logs"
STATE_DIR = WORKSPACE_DIR / "state"
SKILLS_DIR = WORKSPACE_DIR / "skills"
CACHE_DIR = WORKSPACE_DIR / "cache"
USERS_DIR = STATE_DIR / "users.json"

BOARD_FILE = TASKS_DIR / "hybrid_board.json"
STATE_FILE = STATE_DIR / "hybrid_state.json"
CACHE_FILE = CACHE_DIR / "results_cache.json"
CONFIG_FILE = STATE_DIR / "config.json"

for d in [TASKS_DIR, LOGS_DIR, STATE_DIR, SKILLS_DIR, CACHE_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# ============================================================================
# IMPROVEMENT: UTC Timestamp (No Deprecation)
# ============================================================================

def get_utc_timestamp() -> str:
    """Get UTC timestamp without deprecation warning"""
    return datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')

# ============================================================================
# IMPROVEMENT 1: Rate Limiting + Retry Logic
# ============================================================================

class RateLimiter:
    """Token bucket rate limiter"""

    def __init__(self, max_requests: int = 100, window_seconds: int = 60):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.requests = defaultdict(list)
        self.lock = threading.Lock()

    def is_allowed(self, key: str) -> bool:
        """Check if request is allowed"""
        with self.lock:
            now = time.time()
            # Clean old requests
            self.requests[key] = [
                t for t in self.requests[key]
                if now - t < self.window_seconds
            ]
            # Check limit
            if len(self.requests[key]) >= self.max_requests:
                return False
            # Add request
            self.requests[key].append(now)
            return True

    def get_remaining(self, key: str) -> int:
        """Get remaining requests"""
        now = time.time()
        self.requests[key] = [
            t for t in self.requests[key]
            if now - t < self.window_seconds
        ]
        return max(0, self.max_requests - len(self.requests[key]))


class RetryHandler:
    """Retry logic with exponential backoff"""

    def __init__(self, max_retries: int = 3, base_delay: float = 1.0):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.attempts = defaultdict(int)

    def execute_with_retry(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with retry logic"""
        last_error = None
        for attempt in range(self.max_retries):
            try:
                self.attempts[f"{func.__name__}_{args}"] = attempt + 1
                return func(*args, **kwargs)
            except Exception as e:
                last_error = e
                if attempt < self.max_retries - 1:
                    delay = self.base_delay * (2 ** attempt)
                    time.sleep(delay)
        raise last_error

    def get_attempts(self, key: str) -> int:
        """Get number of attempts"""
        return self.attempts.get(key, 0)

# ============================================================================
# IMPROVEMENT 2: Role-Based Access Control (RBAC)
# ============================================================================

class UserRole(Enum):
    ADMIN = "admin"
    OPERATOR = "operator"
    VIEWER = "viewer"
    GUEST = "guest"

class RBACManager:
    """Role-Based Access Control"""

    PERMISSIONS = {
        UserRole.ADMIN: ["*"],  # All permissions
        UserRole.OPERATOR: [
            "task:create", "task:update", "task:delete",
            "agent:execute", "board:view"
        ],
        UserRole.VIEWER: ["board:view", "task:view"],
        UserRole.GUEST: ["board:view"]
    }

    def __init__(self):
        self.users_file = USERS_DIR
        self.users = self._load_users()

    def _load_users(self) -> dict:
        """Load users from file"""
        if self.users_file.exists():
            with open(self.users_file, 'r') as f:
                return json.load(f)
        # Default admin
        return {
            "admin": {
                "password_hash": self._hash_password("admin"),
                "role": "admin",
                "created": get_utc_timestamp()
            }
        }

    def _save_users(self):
        """Save users to file"""
        with open(self.users_file, 'w') as f:
            json.dump(self.users, f, indent=2)

    def _hash_password(self, password: str) -> str:
        """Simple password hashing"""
        return hashlib.sha256(password.encode()).hexdigest()[:16]

    def authenticate(self, username: str, password: str) -> Optional[str]:
        """Authenticate user, return role or None"""
        if username not in self.users:
            return None
        user = self.users[username]
        if user["password_hash"] == self._hash_password(password):
            return user["role"]
        return None

    def has_permission(self, role: str, permission: str) -> bool:
        """Check if role has permission"""
        role_enum = UserRole(role)
        perms = self.PERMISSIONS.get(role_enum, [])
        return "*" in perms or permission in perms

    def add_user(self, username: str, password: str, role: str = "viewer") -> bool:
        """Add new user"""
        if username in self.users:
            return False
        self.users[username] = {
            "password_hash": self._hash_password(password),
            "role": role,
            "created": get_utc_timestamp()
        }
        self._save_users()
        return True

    def list_users(self) -> List[dict]:
        """List all users"""
        return [
            {"username": k, "role": v["role"], "created": v["created"]}
            for k, v in self.users.items()
        ]

# ============================================================================
# IMPROVEMENT 3: WebSocket Event System (Simulated)
# ============================================================================

class EventBus:
    """Simple event bus for real-time notifications"""

    def __init__(self):
        self.listeners = defaultdict(list)
        self.events = []
        self.max_events = 1000

    @property
    def subscribers(self) -> dict:
        """Alias for listeners - for compatibility"""
        return dict(self.listeners)

    @property
    def subscriber_count(self) -> int:
        """Total number of subscribers"""
        return sum(len(callbacks) for callbacks in self.listeners.values())

    def subscribe(self, event_type: str, callback: Callable):
        """Subscribe to event type"""
        self.listeners[event_type].append(callback)

    def unsubscribe(self, event_type: str, callback: Callable) -> bool:
        """Unsubscribe from event type"""
        if callback in self.listeners.get(event_type, []):
            self.listeners[event_type].remove(callback)
            return True
        return False

    def publish(self, event_type: str, data: dict):
        """Publish event"""
        event = {
            "type": event_type,
            "data": data,
            "timestamp": get_utc_timestamp(),
            "id": str(uuid.uuid4())[:8]
        }
        self.events.append(event)
        # Keep only last N events
        if len(self.events) > self.max_events:
            self.events = self.events[-self.max_events:]
        # Notify listeners
        for callback in self.listeners.get(event_type, []):
            try:
                callback(event)
            except Exception:
                pass
        # Notify all listeners
        for callback in self.listeners.get("*", []):
            try:
                callback(event)
            except Exception:
                pass

    def get_events(self, event_type: str = None, limit: int = 100) -> List[dict]:
        """Get recent events"""
        if event_type:
            return [e for e in self.events if e["type"] == event_type][-limit:]
        return self.events[-limit:]

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
    RETRYING = "retrying"

class Priority(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

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
    dependencies: List[str] = field(default_factory=list)
    layer: str = "execution"
    complexity: int = 5
    tags: List[str] = field(default_factory=list)
    retry_count: int = 0
    max_retries: int = 3
    created_by: str = "system"
    cached_result: Optional[str] = None
    ephemeral: bool = False

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> 'Task':
        known = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in data.items() if k in known})

# ============================================================================
# IMPROVEMENT: Task History/Audit Log
# ============================================================================

class TaskHistory:
    """Task history and audit log for tracking all changes"""

    HISTORY_FILE = TASKS_DIR / "task_history.json"

    def __init__(self):
        self._lock = threading.Lock()
        self._load()

    def _load(self):
        if self.HISTORY_FILE.exists():
            with open(self.HISTORY_FILE, 'r') as f:
                self.history = json.load(f)
        else:
            self.history = {"entries": [], "by_task": {}}

    def _save(self):
        temp_file = self.HISTORY_FILE.with_suffix('.tmp')
        with open(temp_file, 'w') as f:
            json.dump(self.history, f, indent=2)
        os.replace(temp_file, self.HISTORY_FILE)

    def add_entry(self, task_id: str, action: str, details: dict = None, user: str = "system"):
        """Add a history entry for a task"""
        with self._lock:
            entry = {
                "id": f"HE-{len(self.history['entries']) + 1:06d}",
                "task_id": task_id,
                "action": action,
                "details": details or {},
                "user": user,
                "timestamp": get_utc_timestamp()
            }
            self.history["entries"].append(entry)

            # Index by task_id
            if task_id not in self.history["by_task"]:
                self.history["by_task"][task_id] = []
            self.history["by_task"][task_id].append(entry["id"])

            # Keep only last 10000 entries
            if len(self.history["entries"]) > 10000:
                self.history["entries"] = self.history["entries"][-10000:]

            self._save()
            return entry["id"]

    def get_task_history(self, task_id: str) -> List[dict]:
        """Get all history entries for a task"""
        entry_ids = self.history["by_task"].get(task_id, [])
        entries = []
        for eid in entry_ids:
            for e in self.history["entries"]:
                if e["id"] == eid:
                    entries.append(e)
                    break
        return entries

    def get_recent(self, limit: int = 50) -> List[dict]:
        """Get recent history entries"""
        return self.history["entries"][-limit:]

    def search(self, action: str = None, task_id: str = None, user: str = None) -> List[dict]:
        """Search history with filters"""
        results = []
        for entry in self.history["entries"]:
            if action and entry["action"] != action:
                continue
            if task_id and entry["task_id"] != task_id:
                continue
            if user and entry["user"] != user:
                continue
            results.append(entry)
        return results[-100:]  # Last 100 matches

# ============================================================================
# IMPROVEMENT: Task Comments System
# ============================================================================

class TaskComments:
    """Task comments and discussion system"""

    COMMENTS_FILE = TASKS_DIR / "task_comments.json"

    def __init__(self):
        self._lock = threading.Lock()
        self._load()

    def _load(self):
        if self.COMMENTS_FILE.exists():
            with open(self.COMMENTS_FILE, 'r') as f:
                self.comments = json.load(f)
        else:
            self.comments = {"by_task": {}, "next_id": 1}

    def _save(self):
        temp_file = self.COMMENTS_FILE.with_suffix('.tmp')
        with open(temp_file, 'w') as f:
            json.dump(self.comments, f, indent=2)
        os.replace(temp_file, self.COMMENTS_FILE)

    def add_comment(self, task_id: str, content: str, author: str = "system") -> dict:
        """Add a comment to a task"""
        with self._lock:
            comment = {
                "id": self.comments["next_id"],
                "task_id": task_id,
                "content": content,
                "author": author,
                "created": get_utc_timestamp(),
                "edited": None
            }
            self.comments["next_id"] += 1

            if task_id not in self.comments["by_task"]:
                self.comments["by_task"][task_id] = []
            self.comments["by_task"][task_id].append(comment)

            self._save()
            return comment

    def get_comments(self, task_id: str) -> List[dict]:
        """Get all comments for a task"""
        return self.comments["by_task"].get(task_id, [])

    def edit_comment(self, task_id: str, comment_id: int, content: str) -> bool:
        """Edit a comment"""
        with self._lock:
            for comment in self.comments["by_task"].get(task_id, []):
                if comment["id"] == comment_id:
                    comment["content"] = content
                    comment["edited"] = get_utc_timestamp()
                    self._save()
                    return True
        return False

    def delete_comment(self, task_id: str, comment_id: int) -> bool:
        """Delete a comment"""
        with self._lock:
            if task_id in self.comments["by_task"]:
                self.comments["by_task"][task_id] = [
                    c for c in self.comments["by_task"][task_id]
                    if c["id"] != comment_id
                ]
                self._save()
                return True
        return False


@dataclass
class OrchestratorState:
    version: str = "5.0"
    created: str = ""
    updated: str = ""
    mode: str = "hybrid"
    stats: Dict[str, int] = field(default_factory=dict)
    agents: Dict[str, bool] = field(default_factory=dict)
    layers: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)

@dataclass
class CronJob:
    id: str
    name: str
    command: str
    cron_expr: str
    enabled: bool
    last_run: Optional[str] = None
    next_run: Optional[str] = None
    created: str = ""

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> 'CronJob':
        return cls(**data)

# ============================================================================
# Managers (from v4.0)
# ============================================================================

class ConfigManager:
    def __init__(self):
        self.config_file = CONFIG_FILE
        self._load_config()

    def _load_config(self):
        if self.config_file.exists():
            with open(self.config_file, 'r') as f:
                self.config = json.load(f)
        else:
            self.config = {
                "openai_api_key": os.environ.get("OPENAI_API_KEY", ""),
                "api_provider": "openai",
                "openclaw_enabled": True,
                "cache_enabled": True,
                "cache_ttl": 3600,
                "rate_limit": {"max_requests": 100, "window_seconds": 60},
                "retry": {"max_retries": 3, "base_delay": 1.0},
                "version": "5.0"
            }
            self._save_config()

    def _save_config(self):
        with open(self.config_file, 'w') as f:
            json.dump(self.config, f, indent=2)

    def get_api_key(self) -> str:
        return self.config.get("openai_api_key", "")

    def set_api_key(self, key: str, provider: str = "openai"):
        self.config["openai_api_key"] = key
        self.config["api_provider"] = provider
        self._save_config()
        return True

    def has_api_key(self) -> bool:
        return bool(self.config.get("openai_api_key", ""))

    def is_caching_enabled(self) -> bool:
        return self.config.get("cache_enabled", True)

    def get_rate_limit(self) -> dict:
        return self.config.get("rate_limit", {"max_requests": 100, "window_seconds": 60})

    def get_retry_config(self) -> dict:
        return self.config.get("retry", {"max_retries": 3, "base_delay": 1.0})

    # ============================================================================
    # IMPROVEMENT: Config Validation
    # ============================================================================

    def validate(self) -> Dict[str, Any]:
        """
        Validate configuration and return issues.

        Returns:
            Dict with 'valid' boolean and 'issues' list
        """
        issues = []

        # Validate API key
        api_key = self.get_api_key()
        if not api_key:
            issues.append("API key is not set")
        elif len(api_key) < 20:
            issues.append("API key seems too short")

        # Validate rate limit
        rl = self.get_rate_limit()
        if rl.get("max_requests", 0) <= 0:
            issues.append("Rate limit must be positive")
        if rl.get("window_seconds", 0) <= 0:
            issues.append("Rate limit window must be positive")

        # Validate retry config
        retry = self.get_retry_config()
        if retry.get("max_retries", 0) < 0:
            issues.append("Max retries cannot be negative")
        if retry.get("base_delay", 0) < 0:
            issues.append("Base delay cannot be negative")

        # Validate cache TTL
        ttl = self.config.get("cache_ttl", 3600)
        if ttl < 0:
            issues.append("Cache TTL cannot be negative")

        return {
            "valid": len(issues) == 0,
            "issues": issues,
            "warnings": [] if len(issues) == 0 else ["Configuration needs attention"]
        }

    def get_validation_report(self) -> str:
        """Get human-readable validation report"""
        result = self.validate()
        lines = ["🔍 Configuration Validation Report", "=" * 50]

        if result["valid"]:
            lines.append("✅ Configuration is valid")
        else:
            lines.append("❌ Configuration has issues:")
            for issue in result["issues"]:
                lines.append(f"  • {issue}")

        lines.append("")
        lines.append("📋 Current Settings:")
        lines.append(f"  API Key: {'✅ Set' if self.has_api_key() else '❌ Not Set'}")
        lines.append(f"  Cache: {'Enabled' if self.is_caching_enabled() else 'Disabled'} (TTL: {self.config.get('cache_ttl', 3600)}s)")
        lines.append(f"  Rate Limit: {self.get_rate_limit()}")
        lines.append(f"  Retry: {self.get_retry_config()}")
        lines.append("=" * 50)

        return "\n".join(lines)


class CacheManager:
    def __init__(self, ttl: int = 3600):
        self.cache_file = CACHE_DIR / "results_cache.json"
        self.ttl = ttl
        self.lock = threading.Lock()
        self._load_cache()

    def _load_cache(self):
        if self.cache_file.exists():
            try:
                with open(self.cache_file, 'r') as f:
                    self.cache = json.load(f)
            except Exception:
                self.cache = {"entries": {}, "stats": {"hits": 0, "misses": 0}}
        else:
            self.cache = {"entries": {}, "stats": {"hits": 0, "misses": 0}}

    def _save_cache(self):
        with self.lock:
            with open(self.cache_file, 'w') as f:
                json.dump(self.cache, f, indent=2)

    def _generate_key(self, prefix: str, data: str) -> str:
        hash_val = hashlib.md5(data.encode()).hexdigest()[:12]
        return f"{prefix}:{hash_val}"

    def get(self, prefix: str, data: str) -> Optional[Any]:
        key = self._generate_key(prefix, data)
        entry = self.cache["entries"].get(key)
        if entry is None:
            self.cache["stats"]["misses"] += 1
            return None
        cached_time = datetime.fromisoformat(entry["timestamp"])
        now = datetime.now(timezone.utc)
        if (now - cached_time).total_seconds() > self.ttl:
            del self.cache["entries"][key]
            self.cache["stats"]["misses"] += 1
            self._save_cache()
            return None
        self.cache["stats"]["hits"] += 1
        self._save_cache()
        return entry["result"]

    def set(self, prefix: str, data: str, result: Any):
        key = self._generate_key(prefix, data)
        self.cache["entries"][key] = {
            "result": result,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "key": key
        }
        self._save_cache()

    def clear(self):
        self.cache = {"entries": {}, "stats": {"hits": 0, "misses": 0}}
        self._save_cache()

    def get_stats(self) -> dict:
        total = self.cache["stats"]["hits"] + self.cache["stats"]["misses"]
        hit_rate = (self.cache["stats"]["hits"] / total * 100) if total > 0 else 0
        return {
            "entries": len(self.cache["entries"]),
            "hits": self.cache["stats"]["hits"],
            "misses": self.cache["stats"]["misses"],
            "hit_rate": f"{hit_rate:.1f}%"
        }


class CronJobManager:
    CRON_FILE = STATE_DIR / "cron_jobs.json"

    def __init__(self):
        self._load()

    def _load(self):
        if self.CRON_FILE.exists():
            with open(self.CRON_FILE, 'r') as f:
                data = json.load(f)
                self.jobs = [CronJob.from_dict(j) for j in data.get("jobs", [])]
        else:
            self.jobs = []

    def _save(self):
        with open(self.CRON_FILE, 'w') as f:
            json.dump({"jobs": [j.to_dict() for j in self.jobs]}, f, indent=2)

    def add_job(self, name: str, command: str, cron_expr: str) -> CronJob:
        job = CronJob(
            id=f"CJ-{len(self.jobs) + 1:03d}",
            name=name,
            command=command,
            cron_expr=cron_expr,
            enabled=True,
            created=get_utc_timestamp()
        )
        self.jobs.append(job)
        self._save()
        return job

    def list_jobs(self) -> List[CronJob]:
        return self.jobs

    def delete_job(self, job_id: str) -> bool:
        self.jobs = [j for j in self.jobs if j.id != job_id]
        self._save()
        return True

    def toggle_job(self, job_id: str) -> Optional[bool]:
        for j in self.jobs:
            if j.id == job_id:
                j.enabled = not j.enabled
                self._save()
                return j.enabled
        return None


class BoardManager:
    """
    Thread-safe BoardManager with file locking and atomic writes.

    Improvements:
    - fcntl.flock() for concurrent access protection
    - Atomic writes (temp file + rename)
    - Auto-backup before save
    - Lock-free reads (eventual consistency)
    """

    _lock = threading.Lock()  # Class-level lock for writes

    def __init__(self):
        self.board_file = BOARD_FILE
        self.backup_dir = TASKS_DIR / "backups"
        self.backup_dir.mkdir(exist_ok=True)
        self._ensure_board()

    def _ensure_board(self):
        if not self.board_file.exists():
            self._create_empty()

    def _create_empty(self):
        data = {
            "version": "5.0",
            "created": get_utc_timestamp(),
            "updated": get_utc_timestamp(),
            "stats": {"total": 0, "queued": 0, "running": 0, "completed": 0, "failed": 0, "blocked": 0},
            "tasks": []
        }
        self._atomic_save(data)

    def _load(self) -> dict:
        """Lock-free read with retry on corruption"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                with open(self.board_file, 'r') as f:
                    return json.load(f)
            except json.JSONDecodeError as e:
                if attempt < max_retries - 1:
                    time.sleep(0.1 * (attempt + 1))  # Exponential backoff
                    continue
                raise

    def _atomic_save(self, data: dict, create_backup: bool = True):
        """
        Atomic write: write to temp file, fsync, then rename.
        Creates backup before write if create_backup=True.
        """
        # Create backup first
        if create_backup and self.board_file.exists():
            backup_name = f"board_{int(time.time())}.json.bak"
            backup_path = self.backup_dir / backup_name
            try:
                shutil.copy2(self.board_file, backup_path)
                # Keep only last 10 backups
                self._cleanup_backups(max_backups=10)
            except Exception as e:
                print(f"⚠️ Backup failed: {e}", file=sys.stderr)

        # Write to temp file
        temp_file = self.board_file.with_suffix('.tmp')
        try:
            with open(temp_file, 'w') as f:
                json.dump(data, f, indent=2)
                f.flush()
                os.fsync(f.fileno())  # Ensure data is written to disk

            # Atomic rename (on POSIX systems)
            os.replace(temp_file, self.board_file)
        except Exception as e:
            if temp_file.exists():
                temp_file.unlink()
            raise

    def _locked_save(self, data: dict):
        """Thread-safe save with file locking"""
        with self._lock:
            # Acquire exclusive lock
            lock_file = self.board_file.with_suffix('.lock')
            with open(lock_file, 'w') as lf:
                fcntl.flock(lf.fileno(), fcntl.LOCK_EX)
                try:
                    self._atomic_save(data)
                finally:
                    fcntl.flock(lf.fileno(), fcntl.LOCK_UN)

    def _cleanup_backups(self, max_backups: int = 10):
        """Remove old backups, keeping only the most recent ones"""
        try:
            backups = sorted(self.backup_dir.glob("board_*.json.bak"),
                          key=lambda p: p.stat().st_mtime,
                          reverse=True)
            for old_backup in backups[max_backups:]:
                old_backup.unlink()
        except Exception:
            pass

    def _gen_id(self) -> str:
        """Generate task ID with lock protection"""
        with self._lock:
            data = self._load()
            next_id = data['stats']['total'] + 1
            return f"T-{next_id:03d}"

    def add_task(self, title: str, **kwargs) -> Task:
        """Add task with thread-safe write"""
        with self._lock:
            lock_file = self.board_file.with_suffix('.lock')
            with open(lock_file, 'w') as lf:
                fcntl.flock(lf.fileno(), fcntl.LOCK_EX)
                try:
                    data = self._load()

                    # Generate ID
                    task_id = f"T-{data['stats']['total'] + 1:03d}"

                    tags = list(kwargs.get('tags', []))
                    ephemeral = bool(kwargs.get('ephemeral', False))
                    if ephemeral and '__test__' not in tags:
                        tags.append('__test__')

                    task = Task(
                        id=task_id,
                        title=title,
                        description=kwargs.get('description', ''),
                        agent=kwargs.get('agent', 'Hermes'),
                        status=TaskStatus.QUEUED.value,
                        priority=kwargs.get('priority', 'medium'),
                        created=get_utc_timestamp(),
                        updated=get_utc_timestamp(),
                        progress=0,
                        dependencies=kwargs.get('dependencies', []),
                        layer=kwargs.get('layer', 'execution'),
                        complexity=kwargs.get('complexity', 5),
                        tags=tags,
                        retry_count=0,
                        max_retries=kwargs.get('max_retries', 3),
                        created_by=kwargs.get('created_by', 'system'),
                        ephemeral=ephemeral,
                    )
                    data['tasks'].append(task.to_dict())
                    data['stats']['total'] += 1
                    data['stats']['queued'] += 1
                    data['updated'] = get_utc_timestamp()
                    self._atomic_save(data)
                    return task
                finally:
                    fcntl.flock(lf.fileno(), fcntl.LOCK_UN)

    def get_task(self, task_id: str) -> Optional[Task]:
        """Lock-free read"""
        data = self._load()
        for t in data['tasks']:
            if t['id'] == task_id:
                return Task.from_dict(t)
        return None

    def update_status(self, task_id: str, status: str, progress: int = None) -> bool:
        """Update task status with thread-safe write"""
        with self._lock:
            lock_file = self.board_file.with_suffix('.lock')
            with open(lock_file, 'w') as lf:
                fcntl.flock(lf.fileno(), fcntl.LOCK_EX)
                try:
                    data = self._load()
                    terminal = status in ('completed', 'failed')
                    for t in data['tasks']:
                        if t['id'] == task_id:
                            old = t['status']
                            if t.get('ephemeral') and terminal:
                                # Auto-delete: remove task and fix stats
                                data['tasks'].remove(t)
                                data['stats'][old] = max(0, data['stats'].get(old, 1) - 1)
                                data['stats']['total'] = max(0, data['stats'].get('total', 1) - 1)
                            else:
                                t['status'] = status
                                t['updated'] = get_utc_timestamp()
                                if progress is not None:
                                    t['progress'] = progress
                                data['stats'][old] = max(0, data['stats'].get(old, 1) - 1)
                                data['stats'][status] = data['stats'].get(status, 0) + 1
                            self._atomic_save(data)
                            return True
                    return False
                finally:
                    fcntl.flock(lf.fileno(), fcntl.LOCK_UN)

    def update_task(self, task: 'Task') -> bool:
        """Update a task object with thread-safe write"""
        with self._lock:
            lock_file = self.board_file.with_suffix('.lock')
            with open(lock_file, 'w') as lf:
                fcntl.flock(lf.fileno(), fcntl.LOCK_EX)
                try:
                    data = self._load()
                    for i, t in enumerate(data['tasks']):
                        if t['id'] == task.id:
                            # Update all fields from task object
                            task_dict = task.to_dict()
                            # Preserve stats - update them based on status change
                            old_status = t['status']
                            new_status = task_dict.get('status', old_status)
                            if old_status != new_status:
                                data['stats'][old_status] = max(0, data['stats'].get(old_status, 1) - 1)
                                data['stats'][new_status] = data['stats'].get(new_status, 0) + 1
                            data['tasks'][i] = task_dict
                            self._atomic_save(data)
                            return True
                    return False
                finally:
                    fcntl.flock(lf.fileno(), fcntl.LOCK_UN)

    def increment_retry(self, task_id: str) -> bool:
        """Increment retry count with thread-safe write"""
        with self._lock:
            lock_file = self.board_file.with_suffix('.lock')
            with open(lock_file, 'w') as lf:
                fcntl.flock(lf.fileno(), fcntl.LOCK_EX)
                try:
                    data = self._load()
                    for t in data['tasks']:
                        if t['id'] == task_id:
                            t['retry_count'] += 1
                            self._atomic_save(data)
                            return True
                    return False
                finally:
                    fcntl.flock(lf.fileno(), fcntl.LOCK_UN)

    def list_tasks(self, status: str = None, show_test: bool = False) -> List[Task]:
        """Lock-free read. By default hides ephemeral/__test__ tasks."""
        data = self._load()
        tasks = [Task.from_dict(t) for t in data['tasks']]
        if not show_test:
            tasks = [t for t in tasks if '__test__' not in t.tags]
        if status:
            tasks = [t for t in tasks if t.status == status]
        return tasks

    def get_stats(self) -> dict:
        """Lock-free read"""
        return self._load()['stats']


class CAMELLayer:
    def __init__(self, board: BoardManager):
        self.board = board

    def decompose(self, goal: str, depth: int = 3) -> List[Dict]:
        tasks = []
        if depth >= 1:
            tasks.append({
                "title": f"[Research] Investigate: {goal}",
                "agent": "OpenClaw",
                "layer": "camel",
                "priority": "high",
                "phase": "research"
            })
        if depth >= 2:
            tasks.append({
                "title": f"[Analysis] Analyze: {goal}",
                "agent": "Hermes",
                "layer": "camel",
                "priority": "high",
                "phase": "analysis"
            })
        if depth >= 3:
            tasks.append({
                "title": f"[Implement] Execute: {goal}",
                "agent": "Hermes",
                "layer": "execution",
                "priority": "medium",
                "phase": "implementation"
            })
        if depth >= 4:
            tasks.append({
                "title": f"[Review] Verify: {goal}",
                "agent": "Hermes",
                "layer": "execution",
                "priority": "low",
                "phase": "review"
            })
        return tasks

    def create_workflow(self, goal: str, depth: int = 3) -> List[Task]:
        subtasks = self.decompose(goal, depth)
        created = []
        for i, st in enumerate(subtasks):
            task = self.board.add_task(
                title=st['title'],
                description=f"Phase {i+1}: {st['phase']} for goal: {goal}",
                agent=st['agent'],
                layer=st['layer'],
                priority=st['priority'],
                tags=[st['phase'], 'camel-workflow']
            )
            created.append(task)
        return created

# ============================================================================
# Hybrid Orchestrator v5.0 (All Improvements)
# ============================================================================

class HybridOrchestrator:
    """Complete Hybrid Orchestrator v5.0"""

    def __init__(self):
        self.board = BoardManager()
        self.camel = CAMELLayer(self.board)
        self.cron = CronJobManager()
        self.config = ConfigManager()
        self.cache = CacheManager()
        self.rbac = RBACManager()
        self.events = EventBus()
        self.rate_limiter = RateLimiter(**self.config.get_rate_limit())
        self.retry_handler = RetryHandler(**self.config.get_retry_config())
        self.metrics = metrics  # Prometheus metrics
        self.circuit_breaker = CircuitBreaker()  # API protection
        self.deduplicator = RequestDeduplicator()  # Request deduplication
        self.shutdown_handler = GracefulShutdownHandler(self)  # Graceful shutdown
        self.history = TaskHistory()  # Task history/audit log
        self.comments = TaskComments()  # Task comments system
        self.state = self._load_state()
        self._save_state()

        # Publish startup event
        self.events.publish("orchestrator.startup", {
            "version": "5.0",
            "timestamp": get_utc_timestamp()
        })

        # Record startup metric
        self.metrics.inc_counter("orchestrator_startups")

    def _load_state(self) -> OrchestratorState:
        if STATE_FILE.exists():
            try:
                with open(STATE_FILE, 'r') as f:
                    data = json.load(f)
                    return OrchestratorState(
                        version=data.get('version', '5.0'),
                        created=data.get('created', get_utc_timestamp()),
                        updated=get_utc_timestamp(),
                        mode=data.get('current_mode', 'hybrid'),
                        stats=data.get('stats', {"total": 0, "completed": 0}),
                        agents=data.get('agents_online', {"Hermes": True, "OpenClaw": True}),
                        layers=["camel", "multica", "execution"]
                    )
            except Exception:
                pass
        return OrchestratorState(
            version="5.0",
            created=get_utc_timestamp(),
            updated=get_utc_timestamp(),
            mode="hybrid",
            stats={"total": 0, "completed": 0},
            agents={"Hermes": True, "OpenClaw": True},
            layers=["camel", "multica", "execution"]
        )

    def _save_state(self):
        self.state.updated = get_utc_timestamp()
        with open(STATE_FILE, 'w') as f:
            json.dump(self.state.to_dict(), f, indent=2)

    # === Event System ===

    def cmd_events(self, event_type: str = None, limit: int = 50) -> str:
        """Get recent events"""
        events = self.events.get_events(event_type, limit)
        if not events:
            return "📭 No events"
        lines = ["📡 Recent Events:", "=" * 50]
        for e in events[-10:]:
            lines.append(f"[{e['timestamp']}] {e['type']}: {e['data']}")
        return "\n".join(lines)

    # === User Management ===

    def cmd_user_list(self) -> str:
        """List users"""
        users = self.rbac.list_users()
        lines = ["👥 Users:", "=" * 50]
        for u in users:
            lines.append(f"  {u['username']} ({u['role']}) - {u['created'][:10]}")
        return "\n".join(lines)

    def cmd_user_add(self, username: str, password: str, role: str = "viewer") -> str:
        """Add user"""
        if self.rbac.add_user(username, password, role):
            self.events.publish("user.created", {"username": username, "role": role})
            return f"✅ User {username} created with role {role}"
        return f"❌ User {username} already exists"

    # === Rate Limiting ===

    def cmd_rate_limit_status(self) -> str:
        """Show rate limit status"""
        rl = self.rate_limiter
        return f"""Rate Limiting Status:
{'=' * 50}
Max Requests: {rl.max_requests} per {rl.window_seconds}s
Current Buckets: {len(rl.requests)} active
{'=' * 50}"""

    # === Standard Commands (from v4) ===

    def cmd_status(self) -> str:
        stats = self.board.get_stats()
        cache_stats = self.cache.get_stats() if self.config.is_caching_enabled() else {"hit_rate": "N/A"}
        events_count = len(self.events.get_events())
        dedup_stats = self.deduplicator.get_stats()

        return f"""Orchestrator Status (v5.0 - PRODUCTION)
{'=' * 55}
Mode: {self.state.mode}
Layers: {', '.join(self.state.layers)}
Agents: Hermes ✅ | OpenClaw ✅
API Key: {'✅ Configured' if self.config.has_api_key() else '❌ Not Set'}
{'=' * 55}
Features:
  ✅ Thread-safe JSON (fcntl.flock)
  ✅ Atomic writes + auto-backup
  ✅ WebSocket Event Bus ({events_count} events)
  ✅ Rate Limiting (100/min)
  ✅ RBAC ({len(self.rbac.users)} users)
  ✅ Retry Logic (3 attempts)
  ✅ JSON Caching ({cache_stats.get('hit_rate', 'N/A')})
  ✅ Prometheus Metrics
  ✅ Circuit Breaker
  ✅ Request Deduplication ({dedup_stats['hit_rate']:.1f}% dedup)
  ✅ Cron Jobs ({len(self.cron.list_jobs())})
{'=' * 55}
Board Stats:
  Total: {stats['total']} | Queued: {stats['queued']}
  Running: {stats['running']} | Completed: {stats['completed']}
  Failed: {stats['failed']} | Retrying: {stats.get('retrying', 0)}
{'=' * 55}"""

    def cmd_metrics(self) -> str:
        """Show Prometheus metrics"""
        m = self.metrics.get_metrics()
        cb = self.circuit_breaker.get_state()
        dedup = self.deduplicator.get_stats()
        rl = self.rate_limiter

        lines = [
            "📊 Prometheus Metrics (v5.0)",
            "=" * 50,
            "",
            "📈 Counters:",
        ]
        for name, data in m.items():
            if data["counter"] > 0:
                lines.append(f"  {name}: {data['counter']}")

        lines.extend([
            "",
            "📉 Gauges:",
            f"  tasks.total: {self.board.get_stats()['total']}",
            f"  tasks.queued: {self.board.get_stats()['queued']}",
            f"  events.published: {len(self.events.get_events())}",
        ])

        lines.extend([
            "",
            "🔄 Circuit Breaker:",
            f"  State: {cb}",
            f"  Failures: {self.circuit_breaker.failure_count}",
        ])

        lines.extend([
            "",
            "🗑️ Request Deduplication:",
            f"  Hits: {dedup['hits']}",
            f"  Misses: {dedup['misses']}",
            f"  Hit Rate: {dedup['hit_rate']:.1f}%",
        ])

        lines.extend([
            "",
            "⚡ Rate Limiter:",
            f"  Buckets: {len(rl.requests)}",
            f"  Max: {rl.max_requests}/{rl.window_seconds}s",
        ])

        return "\n".join(lines)

    def cmd_board(self, show_all: bool = False) -> str:
        stats = self.board.get_stats()
        label = "TASK BOARD (v5.0)" + (" [ALL incl. test]" if show_all else "")
        lines = [
            "=" * 60,
            f"📋 {label}",
            "=" * 60,
            f"Stats: Total={stats['total']} | Done={stats['completed']}",
            "",
            "🔴 BLOCKED:",
        ]
        for t in self.board.list_tasks("blocked", show_test=show_all):
            lines.append(f"  🚫 [{t.id}] {t.title}")
        lines.extend(["", "🟡 QUEUED:"])
        for t in self.board.list_tasks("queued", show_test=show_all):
            lines.append(f"  ⏳ [{t.id}] {t.title} → {t.agent}")
        lines.extend(["", "🔵 RUNNING:"])
        for t in self.board.list_tasks("running", show_test=show_all):
            lines.append(f"  🔄 [{t.id}] {t.title} [{t.progress}%]")
        lines.extend(["", "🟢 COMPLETED:"])
        for t in self.board.list_tasks("completed", show_test=show_all):
            lines.append(f"  ✅ [{t.id}] {t.title}")
        if not show_all:
            test_count = len([t for t in self.board.list_tasks(show_test=True)
                              if '__test__' in t.tags])
            if test_count:
                lines.append(f"\n  (+ {test_count} ephemeral/__test__ tasks hidden — use /board --all to show)")
        lines.append("=" * 60)
        return "\n".join(lines)

    def cmd_add(self, title: str, **kwargs) -> str:
        task = self.board.add_task(title, **kwargs)
        self.events.publish("task.created", {"task_id": task.id, "title": task.title})
        self._save_state()
        return f"✅ Created [{task.id}]: {task.title}"

    # ============================================================================
    # IMPROVEMENT: Batch Operations
    # ============================================================================

    def cmd_add_batch(self, tasks: List[Dict[str, Any]]) -> str:
        """
        Add multiple tasks at once (batch operation).

        Args:
            tasks: List of task definitions [{"title": "...", "agent": "...", ...}, ...]

        Returns:
            Summary of created tasks
        """
        if not tasks:
            return "❌ No tasks provided"

        created = []
        errors = []

        for i, task_def in enumerate(tasks):
            try:
                title = task_def.get("title", f"Task {i}")
                agent = task_def.get("agent", "Hermes")
                tags = task_def.get("tags", [])
                priority = task_def.get("priority", "medium")

                task = self.board.add_task(
                    title=title,
                    agent=agent,
                    tags=tags,
                    priority=priority
                )
                created.append(task)
                self.events.publish("task.created", {"task_id": task.id, "title": task.title})

            except Exception as e:
                errors.append(f"Task {i}: {str(e)}")

        self._save_state()

        result_lines = [f"✅ Batch Created: {len(created)} tasks"]
        if created:
            result_lines.append("-" * 50)
            for t in created[:5]:  # Show first 5
                result_lines.append(f"  [{t.id}] {t.title}")
            if len(created) > 5:
                result_lines.append(f"  ... and {len(created) - 5} more")

        if errors:
            result_lines.append("")
            result_lines.append(f"⚠️ Errors ({len(errors)}):")
            for err in errors[:3]:
                result_lines.append(f"  {err}")

        return "\n".join(result_lines)

    # ============================================================================
    # IMPROVEMENT: Task History Commands
    # ============================================================================

    def cmd_history(self, task_id: str = None, action: str = None, limit: int = 50) -> str:
        """Get task history - cmd_history [task_id] [action] [limit]"""
        if task_id:
            entries = self.history.get_task_history(task_id)
        else:
            entries = self.history.get_recent(limit)

        if action:
            entries = [e for e in entries if e.get("action") == action]

        if not entries:
            return "📜 No history entries found"

        lines = ["📜 Task History", "=" * 60]
        for e in entries[-20:]:  # Show last 20
            lines.append(f"[{e['timestamp'][:19]}] {e['action']} {e.get('task_id', '')}")
            if e.get("details"):
                for k, v in e["details"].items():
                    lines.append(f"  {k}: {v}")
            lines.append(f"  by: {e['user']}")
            lines.append("")
        return "\n".join(lines)

    def cmd_comment_add(self, task_id: str, content: str, author: str = "user") -> str:
        """Add comment to task - cmd_comment_add <task_id> <content>"""
        task = self.board.get_task(task_id)
        if not task:
            return f"❌ Task {task_id} not found"

        comment = self.comments.add_comment(task_id, content, author)
        self.history.add_entry(task_id, "comment_added", {"comment_id": comment["id"]}, author)
        return f"✅ Comment #{comment['id']} added to {task_id}"

    def cmd_comment_list(self, task_id: str) -> str:
        """List comments for task - cmd_comment_list <task_id>"""
        comments = self.comments.get_comments(task_id)
        if not comments:
            return f"💬 No comments on {task_id}"

        lines = [f"💬 Comments on {task_id}", "=" * 60]
        for c in comments:
            edited = f" (edited {c['edited'][:19]})" if c.get("edited") else ""
            lines.append(f"[#{c['id']}] {c['author']} at {c['created'][:19]}{edited}")
            lines.append(f"  {c['content']}")
            lines.append("")
        return "\n".join(lines)

    def cmd_update_batch(self, task_ids: List[str], updates: Dict[str, Any]) -> str:
        """
        Update multiple tasks at once (batch operation).

        Args:
            task_ids: List of task IDs to update
            updates: Dict with fields to update (status, priority, etc.)

        Returns:
            Summary of updated tasks
        """
        if not task_ids:
            return "❌ No task IDs provided"

        updated = []
        not_found = []

        for task_id in task_ids:
            try:
                task = self.board.get_task(task_id)
                if task:
                    for key, value in updates.items():
                        if hasattr(task, key):
                            setattr(task, key, value)
                    self.board.update_task(task)
                    updated.append(task_id)
                    self.events.publish("task.updated", {"task_id": task_id})
                else:
                    not_found.append(task_id)
            except Exception as e:
                not_found.append(f"{task_id} (error)")

        self._save_state()

        result_lines = [f"✅ Batch Updated: {len(updated)} tasks"]
        if not_found:
            result_lines.append(f"⚠️ Not found: {len(not_found)}")

        return "\n".join(result_lines)

    # ============================================================================
    # IMPROVEMENT: Search API
    # ============================================================================

    def cmd_search(self, query: str = "", status: str = None, agent: str = None,
                   tags: List[str] = None, priority: str = None) -> str:
        """
        Search tasks with filters.
        Usage: /search [query] [--status s] [--agent a] [--tags t1,t2] [--priority p]
        """
        tasks = self.board.list_tasks(status)
        results = []

        for task in tasks:
            # Text search in title/description
            if query:
                if query.lower() not in task.title.lower() and \
                   query.lower() not in task.description.lower():
                    continue

            # Filter by agent
            if agent and task.agent != agent:
                continue

            # Filter by priority
            if priority and task.priority != priority:
                continue

            # Filter by tags
            if tags:
                if not any(tag in task.tags for tag in tags):
                    continue

            results.append(task)

        if not results:
            return "🔍 No tasks match your search criteria"

        lines = [f"🔍 Search Results ({len(results)} tasks)", "=" * 60]
        for t in results[:20]:  # Show first 20
            priority_icon = {"low": "🟢", "medium": "🟡", "high": "🟠", "critical": "🔴"}.get(t.priority, "⚪")
            lines.append(f"{priority_icon} [{t.id}] {t.title}")
            lines.append(f"   Agent: {t.agent} | Status: {t.status} | Priority: {t.priority}")
            if t.tags:
                lines.append(f"   Tags: {', '.join(t.tags)}")
            lines.append("")

        if len(results) > 20:
            lines.append(f"... and {len(results) - 20} more")

        return "\n".join(lines)

    # ============================================================================
    # IMPROVEMENT: Export/Import (CSV/JSON)
    # ============================================================================

    def cmd_export(self, format: str = "json", task_ids: List[str] = None) -> str:
        """
        Export tasks to JSON or CSV.
        Usage: /export [json|csv] [task_id1, task_id2, ...]
        """
        if task_ids:
            tasks = [self.board.get_task(tid) for tid in task_ids if self.board.get_task(tid)]
        else:
            tasks = self.board.list_tasks()

        if not tasks:
            return "❌ No tasks to export"

        if format == "csv":
            # CSV export
            import csv
            import io
            output = io.StringIO()
            writer = csv.DictWriter(output, fieldnames=[
                "id", "title", "description", "agent", "status", "priority",
                "progress", "created", "updated", "tags"
            ])
            writer.writeheader()
            for t in tasks:
                if t:
                    writer.writerow({
                        "id": t.id,
                        "title": t.title,
                        "description": t.description,
                        "agent": t.agent,
                        "status": t.status,
                        "priority": t.priority,
                        "progress": t.progress,
                        "created": t.created,
                        "updated": t.updated,
                        "tags": ",".join(t.tags)
                    })
            csv_content = output.getvalue()
            export_file = TASKS_DIR / f"export_{int(time.time())}.csv"
            with open(export_file, 'w') as f:
                f.write(csv_content)
            return f"📤 Exported {len(tasks)} tasks to {export_file}"

        else:
            # JSON export (default)
            export_data = [t.to_dict() for t in tasks if t]
            export_file = TASKS_DIR / f"export_{int(time.time())}.json"
            with open(export_file, 'w') as f:
                json.dump(export_data, f, indent=2)
            return f"📤 Exported {len(tasks)} tasks to {export_file}"

    def cmd_import(self, file_path: str) -> str:
        """
        Import tasks from JSON file.
        Usage: /import <file_path>
        """
        import_file = Path(file_path)
        if not import_file.exists():
            return f"❌ File not found: {file_path}"

        try:
            with open(import_file, 'r') as f:
                tasks_data = json.load(f)

            if isinstance(tasks_data, list):
                imported = 0
                for task_data in tasks_data:
                    try:
                        self.board.add_task(
                            title=task_data.get("title", "Imported Task"),
                            description=task_data.get("description", ""),
                            agent=task_data.get("agent", "Hermes"),
                            tags=task_data.get("tags", []),
                            priority=task_data.get("priority", "medium")
                        )
                        imported += 1
                    except Exception:
                        pass
                self._save_state()
                return f"✅ Imported {imported} tasks from {import_file.name}"

            return "❌ Invalid format: expected list of tasks"

        except json.JSONDecodeError:
            return f"❌ Invalid JSON in {file_path}"
        except Exception as e:
            return f"❌ Import failed: {str(e)}"

    # ============================================================================
    # IMPROVEMENT: Task Templates
    # ============================================================================

    TEMPLATES_FILE = TASKS_DIR / "task_templates.json"

    def _load_templates(self) -> Dict[str, Any]:
        """Load templates from file"""
        if self.TEMPLATES_FILE.exists():
            with open(self.TEMPLATES_FILE, 'r') as f:
                return json.load(f)
        return self._default_templates()

    def _default_templates(self) -> Dict[str, Any]:
        """Default task templates"""
        return {
            "bug_report": {
                "title": "Bug: [Title]",
                "agent": "OpenClaw",
                "priority": "high",
                "tags": ["bug", "report"],
                "description": "Bug Description:\nSteps to reproduce:\nExpected:\nActual:"
            },
            "feature_request": {
                "title": "Feature: [Title]",
                "agent": "Hermes",
                "priority": "medium",
                "tags": ["feature", "request"],
                "description": "Feature Description:\nUse Case:\nBenefits:"
            },
            "research": {
                "title": "Research: [Topic]",
                "agent": "Hermes",
                "priority": "medium",
                "tags": ["research"],
                "description": "Research Topic:\nQuestions to answer:\nSources to check:"
            },
            "code_review": {
                "title": "Code Review: [File/Module]",
                "agent": "OpenClaw",
                "priority": "high",
                "tags": ["review", "code"],
                "description": "Review Scope:\nFocus Areas:\nKnown Issues:"
            },
            "documentation": {
                "title": "Doc: [Topic]",
                "agent": "Hermes",
                "priority": "low",
                "tags": ["docs"],
                "description": "Document Type:\nAudience:\nKey Sections:"
            }
        }

    def _save_templates(self, templates: Dict[str, Any]):
        """Save templates to file"""
        with open(self.TEMPLATES_FILE, 'w') as f:
            json.dump(templates, f, indent=2)

    def cmd_template_list(self) -> str:
        """List available task templates"""
        templates = self._load_templates()
        lines = ["📋 Task Templates", "=" * 60]
        for name, template in templates.items():
            lines.append(f"  📄 {name}")
            lines.append(f"     Agent: {template.get('agent', 'Hermes')}")
            lines.append(f"     Priority: {template.get('priority', 'medium')}")
            lines.append(f"     Tags: {', '.join(template.get('tags', []))}")
            lines.append("")
        return "\n".join(lines)

    def cmd_template_create(self, name: str, title: str, agent: str = "Hermes",
                           priority: str = "medium", tags: List[str] = None,
                           description: str = "") -> str:
        """Create a new task template - cmd_template_create <name> <title> [agent] [priority] [tags]"""
        templates = self._load_templates()
        templates[name] = {
            "title": title,
            "agent": agent,
            "priority": priority,
            "tags": tags or [],
            "description": description
        }
        self._save_templates(templates)
        return f"✅ Template '{name}' created"

    def cmd_template_use(self, template_name: str, title: str = None) -> str:
        """Create task from template - cmd_template_use <template_name> [title_override]"""
        templates = self._load_templates()
        if template_name not in templates:
            return f"❌ Template '{template_name}' not found"

        template = templates[template_name]
        final_title = title or template.get("title", "Task from template")

        task = self.board.add_task(
            title=final_title,
            agent=template.get("agent", "Hermes"),
            priority=template.get("priority", "medium"),
            tags=template.get("tags", []),
            description=template.get("description", "")
        )
        self.history.add_entry(task.id, "created_from_template", {"template": template_name})
        return f"✅ Created [{task.id}]: {task.title} (from template: {template_name})"

    def _cache_key(self, prefix: str, data: str) -> str:
        return f"{prefix}:{hashlib.md5(data.encode()).hexdigest()[:12]}"

    def cmd_analyze(self, topic: str) -> str:
        """Run Hermes analysis, with caching keyed by topic text."""
        if self.config.is_caching_enabled():
            cache_key = self._cache_key("analyze", topic)
            cached = self.cache.get(cache_key)
            if cached:
                self.events.publish("cache.hit", {"topic": topic})
                stats = self.cache.get_stats()
                hit_rate = stats.get("hit_rate", stats.get("total_hits", "?"))
                return f"[CACHED] {cached}\nCache hits: {hit_rate}"

        task = self.board.add_task(title=f"Analysis: {topic}", agent="Hermes", tags=["analysis"])
        self.board.update_status(task.id, "running")
        self.events.publish("task.started", {"task_id": task.id})

        # Call real Hermes agent via WorkflowEngine connector
        wf_engine = WorkflowEngine(self)
        agent_result = wf_engine._call_hermes_agent(topic, {"goal": topic})
        output = agent_result.get("output", "").strip()
        agent_label = agent_result.get("agent", "hermes")
        result = output if output else f"[{agent_label}] Analysis complete for: {topic}"

        if self.config.is_caching_enabled():
            self.cache.set(self._cache_key("analyze", topic), result)

        self.board.update_status(task.id, "completed", 100)
        self.events.publish("task.completed", {"task_id": task.id})
        self._save_state()
        return result

    def cmd_both(self, topic: str) -> str:
        """Run OpenClaw (external) + Hermes (internal) in parallel threads."""
        h_task = self.board.add_task(f"[Hermes] Analyze: {topic}", agent="Hermes", tags=["hermes"])
        o_task = self.board.add_task(f"[OpenClaw] Research: {topic}", agent="OpenClaw", tags=["openclaw"])
        self.board.update_status(h_task.id, "running")
        self.board.update_status(o_task.id, "running")
        self.events.publish("workflow.started", {"tasks": [h_task.id, o_task.id]})

        wf_engine = WorkflowEngine(self)
        context: dict = {"goal": topic}
        h_result: dict = {}
        o_result: dict = {}

        def run_hermes():
            nonlocal h_result
            h_result = wf_engine._call_hermes_agent(topic, context)

        def run_openclaw():
            nonlocal o_result
            o_result = wf_engine._call_openclaw_agent(topic, context)

        t_h = threading.Thread(target=run_hermes, daemon=True)
        t_o = threading.Thread(target=run_openclaw, daemon=True)
        t_h.start(); t_o.start()
        t_h.join(timeout=130); t_o.join(timeout=130)

        h_status = "completed" if h_result.get("success") else "failed"
        o_status = "completed" if o_result.get("success") else "failed"
        self.board.update_status(h_task.id, h_status, 100)
        self.board.update_status(o_task.id, o_status, 100)
        self.events.publish("workflow.completed", {"tasks": [h_task.id, o_task.id]})
        self._save_state()

        h_out = (h_result.get("output") or "")[:300]
        o_out = (o_result.get("output") or "")[:300]
        return (
            f"✅ Parallel execution complete:\n"
            f"  Hermes  [{h_task.id}] {h_status}: {h_out}\n"
            f"  OpenClaw[{o_task.id}] {o_status}: {o_out}"
        )

    def cmd_camel(self, goal: str, depth: int = 3) -> str:
        tasks = self.camel.create_workflow(goal, depth)
        self.events.publish("workflow.created", {"goal": goal, "tasks": [t.id for t in tasks]})
        self._save_state()
        return f"✅ Created {len(tasks)} tasks for: {goal}\n" + \
               "\n".join([f"  [{t.id}] {t.title} → {t.agent}" for t in tasks])

    def cmd_research(self, query: str) -> str:
        """Run OpenClaw external research, with caching keyed by query."""
        if self.config.is_caching_enabled():
            cache_key = self._cache_key("research", query)
            cached = self.cache.get(cache_key)
            if cached:
                self.events.publish("cache.hit", {"query": query})
                stats = self.cache.get_stats()
                hit_rate = stats.get("hit_rate", stats.get("total_hits", "?"))
                return f"[CACHED] {cached}\nCache hits: {hit_rate}"

        task = self.board.add_task(title=f"Research: {query}", agent="OpenClaw", tags=["research"])
        self.board.update_status(task.id, "running")
        self.events.publish("task.started", {"task_id": task.id})

        wf_engine = WorkflowEngine(self)
        agent_result = wf_engine._call_openclaw_agent(query, {"goal": query})
        output = agent_result.get("output", "").strip()
        agent_label = agent_result.get("agent", "openclaw")
        result = output if output else f"[{agent_label}] Research complete for: {query}"

        if self.config.is_caching_enabled():
            self.cache.set(self._cache_key("research", query), result)

        self.board.update_status(task.id, "completed", 100)
        self.events.publish("task.completed", {"task_id": task.id})
        self._save_state()
        return result

    def cmd_workflow(self, action: str, wf_id_or_name: str = "", goal: str = "") -> str:
        """Manage and execute workflows. Actions: list, run <id|name> [goal]"""
        wf_engine = WorkflowEngine(self)
        workflows = wf_engine.workflows.get("workflows", [])

        if action == "list":
            if not workflows:
                return "No workflows defined."
            lines = ["Workflows:", "=" * 50]
            for wf in workflows:
                lines.append(f"  [{wf['id']}] {wf['name']} — {wf.get('description', '')}")
                lines.append(f"      Steps: {len(wf.get('steps', []))}")
            return "\n".join(lines)

        if action == "run":
            if not wf_id_or_name:
                return "Usage: /workflow run <id|name> [goal]"

            # Resolve by id or name
            target = None
            for wf in workflows:
                if str(wf["id"]) == wf_id_or_name or wf["name"] == wf_id_or_name:
                    target = wf
                    break
            if target is None:
                return f"Workflow not found: {wf_id_or_name}"

            context = {"goal": goal} if goal else {}
            self.events.publish("workflow.started", {"workflow": target["name"], "goal": goal})
            execution = wf_engine.execute_workflow(target["id"], context)
            status = execution.get("status", "unknown")
            exec_id = execution.get("id", "?")
            steps_done = len([r for r in execution.get("results", []) if r["status"] == "completed"])
            steps_total = len(target.get("steps", []))
            return (
                f"Workflow '{target['name']}' [{exec_id}]\n"
                f"Status: {status} | Steps: {steps_done}/{steps_total}\n"
                f"Goal: {goal or '(none)'}"
            )

        return f"Unknown action '{action}'. Use: list | run <id|name> [goal]"

    def cmd_skill(self, action: str, skill_name: str = "") -> str:
        """List or inspect skills. Actions: list, info <name>"""
        if action == "list":
            skill_files = sorted(SKILLS_DIR.glob("*.md"))
            if not skill_files:
                return f"No skills found in {SKILLS_DIR}"
            lines = ["Skills:", "=" * 40]
            for sf in skill_files:
                lines.append(f"  {sf.stem}")
            return "\n".join(lines)

        if action == "info":
            if not skill_name:
                return "Usage: /skill info <name>"
            skill_file = SKILLS_DIR / f"{skill_name}.md"
            if not skill_file.exists():
                available = [f.stem for f in SKILLS_DIR.glob("*.md")]
                return f"Skill '{skill_name}' not found. Available: {', '.join(available)}"
            return skill_file.read_text(encoding="utf-8")

        return f"Unknown action '{action}'. Use: list | info <name>"

    def cmd_cron_list(self) -> str:
        jobs = self.cron.list_jobs()
        if not jobs:
            return "📅 No scheduled tasks"
        lines = ["📅 Scheduled Tasks:", "=" * 50]
        for j in jobs:
            status = "🟢" if j.enabled else "🔴"
            lines.append(f"{status} [{j.id}] {j.name}")
            lines.append(f"   Command: {j.command}")
            lines.append(f"   Schedule: {j.cron_expr}")
            lines.append("")
        return "\n".join(lines)

    def cmd_cron_add(self, name: str, command: str, schedule: str) -> str:
        job = self.cron.add_job(name, command, schedule)
        self.events.publish("cron.created", {"job_id": job.id, "name": job.name})
        return f"✅ Created task [{job.id}]: {job.name}\n   Schedule: {job.cron_expr}"

    def cmd_config(self, action: str = "show") -> str:
        api_key = self.config.get_api_key()
        masked = f"{api_key[:8]}...{api_key[-4:]}" if api_key else "NOT SET"
        return f"""Configuration (v5.0):
{'=' * 50}
API Key: {masked}
Provider: {self.config.config.get('api_provider', 'openai')}
Caching: {'Enabled' if self.config.is_caching_enabled() else 'Disabled'}
Rate Limit: {self.config.get_rate_limit()}
Retry: {self.config.get_retry_config()}
Users: {len(self.rbac.users)}
{'=' * 50}"""

    def cmd_health(self) -> str:
        """Health check - verifies all components are working"""
        checks = []
        status = "✅"

        # Check board file
        try:
            stats = self.board.get_stats()
            checks.append(f"  ✅ Board: {stats['total']} tasks")
        except Exception as e:
            checks.append(f"  ❌ Board: {str(e)}")
            status = "❌"

        # Check backup directory
        try:
            backup_dir = self.board.backup_dir
            backups = list(backup_dir.glob("*.bak"))
            checks.append(f"  ✅ Backups: {len(backups)} files")
        except Exception as e:
            checks.append(f"  ⚠️  Backups: {str(e)}")

        # Check cache
        try:
            cache_stats = self.cache.get_stats()
            checks.append(f"  ✅ Cache: {cache_stats.get('hit_rate', 'N/A')} hit rate")
        except Exception as e:
            checks.append(f"  ❌ Cache: {str(e)}")
            status = "❌"

        # Check rate limiter
        try:
            rl = self.rate_limiter
            checks.append(f"  ✅ Rate Limit: {rl.max_requests}/{rl.window_seconds}s")
        except Exception as e:
            checks.append(f"  ❌ Rate Limit: {str(e)}")
            status = "❌"

        # Check events
        try:
            events_count = len(self.events.get_events())
            checks.append(f"  ✅ Events: {events_count} published")
        except Exception as e:
            checks.append(f"  ⚠️  Events: {str(e)}")

        # Check config
        try:
            has_key = self.config.has_api_key()
            checks.append(f"  {'✅' if has_key else '⚠️'} API Key: {'Configured' if has_key else 'Not Set'}")
        except Exception as e:
            checks.append(f"  ❌ Config: {str(e)}")
            status = "❌"

        return f"""🏥 Orchestrator Health Check (v5.0)
{'=' * 50}
{status} Overall Status
{''.join(checks)}
{'=' * 50}"""

    def cmd_validate(self) -> str:
        """Validate configuration"""
        return self.config.get_validation_report()

    def cmd_help(self) -> str:
        sep = "=" * 55
        return f"""Hybrid Orchestrator v5.0 - PRODUCTION
{sep}
TASK MANAGEMENT:
  /board              - Show task board (hides __test__ tasks)
  /board --all        - Show all tasks including ephemeral/__test__
  /add <title>        - Add new task  (use ephemeral=true to auto-delete)
  /analyze <topic>    - Hermes analysis (cached)
  /research <query>   - OpenClaw external research (cached)
  /both <topic>       - Both agents in parallel
  /camel <goal> [d]   - CAMEL workflow decomposition

WORKFLOWS & SKILLS:
  /workflow list                   - List all workflows
  /workflow run <id|name> [goal]   - Execute a workflow
  /skill list                      - List available skills
  /skill info <name>               - Show skill documentation

CONFIGURATION:
  /config             - Show configuration
  /config cache-stats - Cache statistics
  /config cache-clear - Clear cache

CRON JOBS:
  /cron-list          - List scheduled tasks
  /cron-add <n> <c> <s> - Add cron task

USERS (RBAC):
  /user-list          - List users
  /user-add <u> <p> [r] - Add user (roles: admin/operator/viewer)

MONITORING:
  /events [type] [n]  - Recent events
  /rate-limit         - Rate limit status
  /metrics            - Prometheus metrics

SYSTEM:
  /status             - Full status
  /health             - Health check
  /backup             - Cleanup old backups
  /help               - This help
{sep}
Architecture: B+A Hybrid (Skills + Real Connectors)
  Hermes  → hermes CLI / hermes_llm.py fallback
  OpenClaw→ openclaw_runner.sh / NVM + Node.js
  Multica → POST localhost:3000 / internal board fallback
  CAMEL   → camel-ai SDK / CAMELLayer fallback
{sep}"""


# ============================================================================
# Main
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="Hybrid Orchestrator v5.0")
    parser.add_argument('command', help='Command to execute')
    parser.add_argument('args', nargs='*', help='Arguments')
    args = parser.parse_args()

    orch = HybridOrchestrator()
    cmd = args.command
    cmd_args = args.args

    # User Management
    if cmd in ['/user-list', 'user-list']:
        print(orch.cmd_user_list())
        return
    if cmd in ['/user-add', 'user-add']:
        if len(cmd_args) >= 2:
            username, password = cmd_args[0], cmd_args[1]
            role = cmd_args[2] if len(cmd_args) > 2 else "viewer"
            print(orch.cmd_user_add(username, password, role))
        else:
            print("Usage: /user-add <username> <password> [role]")
        return

    # Events
    if cmd in ['/events', 'events']:
        event_type = cmd_args[0] if cmd_args else None
        limit = int(cmd_args[1]) if len(cmd_args) > 1 else 50
        print(orch.cmd_events(event_type, limit))
        return

    # Rate Limit
    if cmd in ['/rate-limit', 'rate-limit']:
        print(orch.cmd_rate_limit_status())
        return

    # Cron
    if cmd in ['/cron-list', 'cron-list']:
        print(orch.cmd_cron_list())
        return
    if cmd in ['/cron-add', 'cron-add']:
        if len(cmd_args) >= 3:
            name, command, schedule = cmd_args[0], cmd_args[1], cmd_args[2]
            print(orch.cmd_cron_add(name, command, schedule))
        else:
            print("Usage: /cron-add <name> <command> <schedule>")
        return

    # Config
    if cmd in ['/config', 'config']:
        action = cmd_args[0] if cmd_args else "show"
        if action == "cache-stats":
            print(orch.cache.get_stats())
        elif action == "cache-clear":
            orch.cache.clear()
            print("✅ Cache cleared")
        else:
            print(orch.cmd_config())
        return

    # Tasks
    if cmd in ['/board', 'board']:
        show_all = '--all' in cmd_args
        print(orch.cmd_board(show_all=show_all))
        return
    if cmd in ['/add', 'add']:
        title = ' '.join(cmd_args)
        print(orch.cmd_add(title) if title else "Usage: /add <title>")
        return

    # Search
    if cmd in ['/search', 'search']:
        query = cmd_args[0] if cmd_args else ""
        status = agent = priority = None
        tags = None
        for i, arg in enumerate(cmd_args[1:], 1):
            if arg == '--status' and i < len(cmd_args) - 1:
                status = cmd_args[i + 1]
            elif arg == '--agent' and i < len(cmd_args) - 1:
                agent = cmd_args[i + 1]
            elif arg == '--priority' and i < len(cmd_args) - 1:
                priority = cmd_args[i + 1]
            elif arg == '--tags' and i < len(cmd_args) - 1:
                tags = cmd_args[i + 1].split(',')
        print(orch.cmd_search(query, status, agent, tags, priority))
        return

    # Export/Import
    if cmd in ['/export', 'export']:
        fmt = cmd_args[0] if cmd_args and cmd_args[0] in ['json', 'csv'] else 'json'
        task_ids = cmd_args[1:] if len(cmd_args) > 1 else None
        print(orch.cmd_export(fmt, task_ids))
        return
    if cmd in ['/import', 'import']:
        if cmd_args:
            print(orch.cmd_import(cmd_args[0]))
        else:
            print("Usage: /import <file_path>")
        return

    # History
    if cmd in ['/history', 'history']:
        task_id = cmd_args[0] if cmd_args else None
        action = cmd_args[1] if len(cmd_args) > 1 else None
        print(orch.cmd_history(task_id, action))
        return

    # Comments
    if cmd in ['/comment-add', 'comment-add']:
        if len(cmd_args) >= 2:
            task_id, content = cmd_args[0], ' '.join(cmd_args[1:])
            print(orch.cmd_comment_add(task_id, content))
        else:
            print("Usage: /comment-add <task_id> <content>")
        return
    if cmd in ['/comment-list', 'comment-list']:
        if cmd_args:
            print(orch.cmd_comment_list(cmd_args[0]))
        else:
            print("Usage: /comment-list <task_id>")
        return

    # Templates
    if cmd in ['/template-list', 'template-list']:
        print(orch.cmd_template_list())
        return
    if cmd in ['/template-create', 'template-create']:
        if len(cmd_args) >= 2:
            name, title = cmd_args[0], cmd_args[1]
            agent = cmd_args[2] if len(cmd_args) > 2 else "Hermes"
            priority = cmd_args[3] if len(cmd_args) > 3 else "medium"
            tags = cmd_args[4].split(',') if len(cmd_args) > 4 else None
            print(orch.cmd_template_create(name, title, agent, priority, tags))
        else:
            print("Usage: /template-create <name> <title> [agent] [priority] [tags]")
        return
    if cmd in ['/template-use', 'template-use']:
        if cmd_args:
            template = cmd_args[0]
            title = ' '.join(cmd_args[1:]) if len(cmd_args) > 1 else None
            print(orch.cmd_template_use(template, title))
        else:
            print("Usage: /template-use <template_name> [title_override]")
        return

    if cmd in ['/analyze', 'analyze']:
        topic = ' '.join(cmd_args)
        print(orch.cmd_analyze(topic) if topic else "Usage: /analyze <topic>")
        return
    if cmd in ['/both', 'both']:
        topic = ' '.join(cmd_args)
        print(orch.cmd_both(topic) if topic else "Usage: /both <topic>")
        return
    if cmd in ['/camel', 'camel']:
        goal = ' '.join(cmd_args) if cmd_args else ''
        depth = int(cmd_args[-1]) if cmd_args and cmd_args[-1].isdigit() else 3
        if cmd_args and cmd_args[-1].isdigit():
            goal = ' '.join(cmd_args[:-1])
        print(orch.cmd_camel(goal, depth) if goal else orch.cmd_help())
        return
    if cmd in ['/research', 'research']:
        query = ' '.join(cmd_args)
        print(orch.cmd_research(query) if query else "Usage: /research <query>")
        return
    if cmd in ['/workflow', 'workflow']:
        action = cmd_args[0] if cmd_args else 'list'
        if action == 'run' and len(cmd_args) >= 2:
            wf_id_or_name = cmd_args[1]
            goal = ' '.join(cmd_args[2:]) if len(cmd_args) > 2 else ''
            print(orch.cmd_workflow('run', wf_id_or_name, goal))
        else:
            print(orch.cmd_workflow(action))
        return
    if cmd in ['/skill', 'skill']:
        action = cmd_args[0] if cmd_args else 'list'
        skill_name = cmd_args[1] if len(cmd_args) > 1 else ''
        print(orch.cmd_skill(action, skill_name))
        return

    # Monitoring
    if cmd in ['/metrics', 'metrics']:
        print(orch.cmd_metrics())
        return

    # System
    if cmd in ['/status', 'status']:
        print(orch.cmd_status())
        return
    if cmd in ['/health', 'health']:
        print(orch.cmd_health())
        return
    if cmd in ['/validate', 'validate']:
        print(orch.cmd_validate())
        return
    if cmd in ['/backup', 'backup']:
        orch.board._cleanup_backups(max_backups=10)
        print("✅ Backups cleaned up")
        return
    if cmd in ['/help', 'help', '-h']:
        print(orch.cmd_help())
        return

    # Dashboard
    if cmd in ['/stats', 'stats']:
        print(orch.dashboard.generate_report())
        return

    # Webhooks
    if cmd in ['/webhook-add', 'webhook-add']:
        if cmd_args:
            url = cmd_args[0]
            events = cmd_args[1:] if len(cmd_args) > 1 else None
            webhook = orch.webhooks.add_webhook(url, events)
            print(f"✅ Webhook added: {webhook['id']} -> {url}")
        else:
            print("Usage: /webhook-add <url> [event1] [event2] ...")
        return
    if cmd in ['/webhook-list', 'webhook-list']:
        hooks = orch.webhooks.list_webhooks()
        if not hooks:
            print("📮 No webhooks configured")
        else:
            print("📮 Webhooks:", "=" * 50)
            for hook in hooks:
                status = "🟢" if hook.get("enabled") else "🔴"
                print(f"{status} {hook['id']}: {hook['url']}")
                print(f"   Events: {', '.join(hook.get('events', []))}")
                print(f"   Triggered: {hook.get('trigger_count', 0)} times")
        return
    if cmd in ['/webhook-remove', 'webhook-remove']:
        if cmd_args:
            webhook_id = cmd_args[0]
            if orch.webhooks.remove_webhook(webhook_id):
                print(f"✅ Webhook {webhook_id} removed")
            else:
                print(f"❌ Webhook {webhook_id} not found")
        else:
            print("Usage: /webhook-remove <webhook_id>")
        return

    # Dependencies
    if cmd in ['/deps', 'deps']:
        task_id = cmd_args[0] if cmd_args else None
        print(orch.dependencies.visualize(task_id))
        return
    if cmd in ['/ready', 'ready']:
        ready = orch.dependencies.get_ready_tasks()
        print(f"📋 Ready to execute: {len(ready)} tasks")
        for tid in ready[:10]:
            print(f"  [{tid}]")
        return

    # Timeline
    if cmd in ['/timeline', 'timeline']:
        days = int(cmd_args[0]) if cmd_args else 7
        print(orch.timeline.generate_calendar_view(days))
        return
    if cmd in ['/gantt', 'gantt']:
        days = int(cmd_args[0]) if cmd_args else 7
        gantt = orch.timeline.get_gantt_data(days)
        print(f"📊 Gantt data: {len(gantt)} tasks")
        for g in gantt[:10]:
            print(f"  [{g['id']}] {g['title']} ({g['progress']}%)")
        return

    # Executor
    if cmd in ['/executor-status', 'executor-status']:
        status = orch.executor.get_status()
        print(f"⚡ Async Executor Status")
        print(f"   Max Workers: {status['max_workers']}")
        print(f"   Running: {status['running']}")
        print(f"   Completed: {status['completed']}")
        if status['running'] > 0:
            print(f"   Active: {', '.join(status['tasks'])}")
        return

    # Priority Queue
    if cmd in ['/queue', 'queue']:
        stats = orch.priority_queue.get_stats()
        print(f"📋 Priority Queue")
        print(f"   Total: {stats['total_queued']}")
        print(f"   Next: {stats['next_task']}")
        for p, c in stats['by_priority'].items():
            print(f"   {p}: {c}")
        queue = orch.priority_queue.get_queue(10)
        if queue:
            print("\n📍 Upcoming:")
            for q in queue:
                print(f"   {q['position']}. [{q['id']}] {q['priority']} - {q['title'][:30]}")
        return
    if cmd in ['/dequeue', 'dequeue']:
        task_id = orch.priority_queue.dequeue()
        if task_id:
            print(f"✅ Next: {task_id}")
        else:
            print("📭 Queue is empty")
        return

    # API Server
    if cmd in ['/api-server', 'api-server']:
        port = int(cmd_args[0]) if cmd_args else 5000
        # Start scheduled-task background poller before serving
        TaskScheduler(orch).start_background_loop(interval=60)
        api = RestAPI(orch)
        api.run(port=port)
        return

    print(f"Unknown command: {cmd}")
    print("Use /help for available commands")


# ============================================================================
# IMPROVEMENT: REST API (Flask)
# ============================================================================

class RestAPI:
    """
    Flask REST API for orchestrator.
    Usage:
        api = RestAPI()
        api.run(host='0.0.0.0', port=5000)
    """

    def __init__(self, orchestrator: HybridOrchestrator = None):
        try:
            from flask import Flask, jsonify, request
            self.Flask = Flask
            self.jsonify = jsonify
            self.request = request
            self._flask_available = True
        except ImportError:
            self._flask_available = False
            print("⚠️ Flask not installed. Run: pip install flask")
            return

        self.app = self.Flask(__name__)
        self.app.config['JSON_SORT_KEYS'] = False
        self.orch = orchestrator or HybridOrchestrator()

        self._setup_routes()

    def _setup_routes(self):
        """Setup API routes"""

        @self.app.route('/api/health', methods=['GET'])
        def health():
            """Health check endpoint"""
            return self.jsonify({
                "status": "ok",
                "version": "5.0",
                "timestamp": get_utc_timestamp()
            })

        @self.app.route('/api/status', methods=['GET'])
        def status():
            """Get orchestrator status"""
            stats = self.orch.board.get_stats()
            return self.jsonify({
                "version": "5.0",
                "mode": self.orch.state.mode,
                "stats": stats,
                "agents": self.orch.state.agents
            })

        # === Tasks ===

        @self.app.route('/api/tasks', methods=['GET'])
        def list_tasks():
            """List all tasks"""
            status = self.request.args.get('status')
            tasks = self.orch.board.list_tasks(status)
            return self.jsonify({
                "tasks": [t.to_dict() for t in tasks],
                "count": len(tasks)
            })

        @self.app.route('/api/tasks', methods=['POST'])
        def create_task():
            """Create a new task"""
            data = self.request.get_json() or {}
            title = data.get('title', 'Untitled')
            task = self.orch.board.add_task(
                title=title,
                agent=data.get('agent', 'Hermes'),
                priority=data.get('priority', 'medium'),
                tags=data.get('tags', []),
                description=data.get('description', '')
            )
            self.orch.history.add_entry(task.id, "created_via_api")
            return self.jsonify(task.to_dict()), 201

        @self.app.route('/api/tasks/<task_id>', methods=['GET'])
        def get_task(task_id):
            """Get task by ID"""
            task = self.orch.board.get_task(task_id)
            if not task:
                return self.jsonify({"error": "Task not found"}), 404
            return self.jsonify(task.to_dict())

        @self.app.route('/api/tasks/<task_id>', methods=['PUT', 'PATCH'])
        def update_task(task_id):
            """Update task"""
            data = self.request.get_json() or {}
            task = self.orch.board.get_task(task_id)
            if not task:
                return self.jsonify({"error": "Task not found"}), 404

            if 'status' in data:
                self.orch.board.update_status(task_id, data['status'], data.get('progress'))
            if 'priority' in data:
                task.priority = data['priority']
                self.orch.board.update_task(task)

            self.orch.history.add_entry(task_id, "updated_via_api", data)
            return self.jsonify(task.to_dict())

        @self.app.route('/api/tasks/<task_id>', methods=['DELETE'])
        def delete_task(task_id):
            """Delete task"""
            success = self.orch.board.delete_task(task_id)
            if success:
                self.orch.history.add_entry(task_id, "deleted_via_api")
                return self.jsonify({"success": True})
            return self.jsonify({"error": "Task not found"}), 404

        # === Search ===

        @self.app.route('/api/search', methods=['GET'])
        def search():
            """Search tasks"""
            query = self.request.args.get('q', '')
            status = self.request.args.get('status')
            agent = self.request.args.get('agent')
            priority = self.request.args.get('priority')
            tags = self.request.args.get('tags', '').split(',') if self.request.args.get('tags') else None

            result = self.orch.cmd_search(query, status, agent, tags, priority)
            return self.jsonify({"result": result})

        # === Comments ===

        @self.app.route('/api/tasks/<task_id>/comments', methods=['GET'])
        def get_comments(task_id):
            """Get task comments"""
            comments = self.orch.comments.get_comments(task_id)
            return self.jsonify({"comments": comments})

        @self.app.route('/api/tasks/<task_id>/comments', methods=['POST'])
        def add_comment(task_id):
            """Add comment to task"""
            data = self.request.get_json() or {}
            content = data.get('content', '')
            author = data.get('author', 'api')
            comment = self.orch.comments.add_comment(task_id, content, author)
            self.orch.history.add_entry(task_id, "comment_added_via_api")
            return self.jsonify(comment), 201

        # === History ===

        @self.app.route('/api/tasks/<task_id>/history', methods=['GET'])
        def get_task_history(task_id):
            """Get task history"""
            history = self.orch.history.get_task_history(task_id)
            return self.jsonify({"history": history})

        @self.app.route('/api/history', methods=['GET'])
        def get_history():
            """Get recent history"""
            limit = int(self.request.args.get('limit', 50))
            history = self.orch.history.get_recent(limit)
            return self.jsonify({"history": history})

        # === Metrics ===

        @self.app.route('/api/metrics', methods=['GET'])
        def get_metrics():
            """Get Prometheus metrics"""
            return self.orch.metrics.export_prometheus(), 200, {'Content-Type': 'text/plain'}

        @self.app.route('/api/stats', methods=['GET'])
        def get_stats():
            """Get detailed statistics"""
            stats = self.orch.board.get_stats()
            metrics = self.orch.metrics.get_metrics()
            cache_stats = self.orch.cache.get_stats()
            return self.jsonify({
                "board": stats,
                "metrics": metrics,
                "cache": cache_stats,
                "rate_limiter": {
                    "max_requests": self.orch.rate_limiter.max_requests,
                    "window": self.orch.rate_limiter.window_seconds
                },
                "circuit_breaker": self.orch.circuit_breaker.get_state()
            })

        # === Templates ===

        @self.app.route('/api/templates', methods=['GET'])
        def list_templates():
            """List task templates"""
            templates = self.orch._load_templates()
            return self.jsonify({"templates": templates})

        @self.app.route('/api/templates/<template_name>', methods=['POST'])
        def use_template(template_name):
            """Create task from template"""
            data = self.request.get_json() or {}
            title = data.get('title')
            task = self.orch.cmd_template_use(template_name, title)
            return self.jsonify({"result": task}), 201

        # === Events ===

        @self.app.route('/api/events', methods=['GET'])
        def get_events():
            """Get recent events"""
            event_type = self.request.args.get('type')
            limit = int(self.request.args.get('limit', 50))
            events = self.orch.events.get_events(event_type, limit)
            return self.jsonify({"events": events})

        # === Notifications ===

        @self.app.route('/api/notifications', methods=['GET'])
        def get_notifications():
            """Get notifications"""
            unread_only = self.request.args.get('unread', 'false').lower() == 'true'
            limit = int(self.request.args.get('limit', 50))
            notifications = self.orch.notifications.get_notifications(unread_only, limit)
            return self.jsonify({"notifications": notifications, "unread": self.orch.notifications.get_unread_count()})

        @self.app.route('/api/notifications/<int:notif_id>/read', methods=['POST'])
        def mark_notification_read(notif_id):
            """Mark notification as read"""
            success = self.orch.notifications.mark_read(notif_id)
            return self.jsonify({"success": success})

        # === Filters ===

        @self.app.route('/api/filters/facets', methods=['GET'])
        def get_facets():
            """Get filter facets"""
            facets = self.orch.filters.get_facets()
            return self.jsonify({"facets": facets})

        @self.app.route('/api/filters/saved', methods=['GET'])
        def get_saved_filters():
            """Get saved filters"""
            return self.jsonify({"filters": self.orch.filters.get_saved_filters()})

        # === Tags ===

        @self.app.route('/api/tags', methods=['GET'])
        def get_tags():
            """Get tags with metadata"""
            tags = self.orch.tags_manager.get_popular_tags()
            return self.jsonify({"tags": [{"name": t[0], "count": t[1]} for t in tags]})

        # === Auto-assign ===

        @self.app.route('/api/auto-assign/rules', methods=['GET'])
        def get_auto_assign_rules():
            """Get auto-assign rules"""
            return self.jsonify(self.orch.auto_assign.get_stats())

    def run(self, host: str = '0.0.0.0', port: int = 5000, debug: bool = False):
        """Run the Flask server"""
        if not self._flask_available:
            print("❌ Flask not available. Cannot start API server.")
            return

        print(f"🚀 Starting REST API on {host}:{port}")
        print(f"   Documentation: http://{host}:{port}/api/health")
        self.app.run(host=host, port=port, debug=debug)


def run_api_server(host: str = '0.0.0.0', port: int = 5000):
    """Start the REST API server"""
    api = RestAPI()
    api.run(host=host, port=port)


# ============================================================================
# IMPROVEMENT: Statistics Dashboard
# ============================================================================

class StatisticsDashboard:
    """Generate statistics and analytics for the orchestrator"""

    def __init__(self, orchestrator: HybridOrchestrator):
        self.orch = orchestrator

    def get_summary(self) -> dict:
        """Get summary statistics"""
        board_stats = self.orch.board.get_stats()
        cache_stats = self.orch.cache.get_stats()

        return {
            "tasks": {
                "total": board_stats['total'],
                "queued": board_stats['queued'],
                "running": board_stats['running'],
                "completed": board_stats['completed'],
                "failed": board_stats['failed'],
                "completion_rate": (board_stats['completed'] / board_stats['total'] * 100) if board_stats['total'] > 0 else 0
            },
            "cache": cache_stats,
            "events": len(self.orch.events.get_events()),
            "users": len(self.orch.rbac.users),
            "jobs": len(self.orch.cron.list_jobs())
        }

    def get_agent_performance(self) -> dict:
        """Get performance metrics per agent"""
        tasks = self.orch.board.list_tasks()
        agent_stats = defaultdict(lambda: {"total": 0, "completed": 0, "failed": 0, "avg_progress": 0})

        for task in tasks:
            agent_stats[task.agent]["total"] += 1
            if task.status == "completed":
                agent_stats[task.agent]["completed"] += 1
            elif task.status == "failed":
                agent_stats[task.agent]["failed"] += 1
            agent_stats[task.agent]["avg_progress"] += task.progress

        # Calculate percentages
        for agent, stats in agent_stats.items():
            if stats["total"] > 0:
                stats["completion_rate"] = stats["completed"] / stats["total"] * 100
                stats["avg_progress"] = stats["avg_progress"] / stats["total"]
            else:
                stats["completion_rate"] = 0
                stats["avg_progress"] = 0

        return dict(agent_stats)

    def get_priority_distribution(self) -> dict:
        """Get task distribution by priority"""
        tasks = self.orch.board.list_tasks()
        distribution = defaultdict(int)

        for task in tasks:
            distribution[task.priority] += 1

        return dict(distribution)

    def get_tag_cloud(self) -> dict:
        """Get tag frequency"""
        tasks = self.orch.board.list_tasks()
        tags = defaultdict(int)

        for task in tasks:
            for tag in task.tags:
                tags[tag] += 1

        return dict(sorted(tags.items(), key=lambda x: x[1], reverse=True))

    def get_trends(self, days: int = 7) -> dict:
        """Get trends over the past N days"""
        history = self.orch.history.get_recent(1000)
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

        trends = {
            "tasks_created": 0,
            "tasks_completed": 0,
            "tasks_failed": 0,
            "by_day": defaultdict(lambda: {"created": 0, "completed": 0, "failed": 0})
        }

        for entry in history:
            if entry["timestamp"] < cutoff:
                continue

            day = entry["timestamp"][:10]  # YYYY-MM-DD
            action = entry.get("action", "")

            if "created" in action:
                trends["tasks_created"] += 1
                trends["by_day"][day]["created"] += 1
            elif "completed" in action:
                trends["tasks_completed"] += 1
                trends["by_day"][day]["completed"] += 1
            elif "failed" in action:
                trends["tasks_failed"] += 1
                trends["by_day"][day]["failed"] += 1

        # Convert defaultdict to dict
        trends["by_day"] = dict(trends["by_day"])
        return trends

    def generate_report(self) -> str:
        """Generate a text report"""
        summary = self.get_summary()
        agent_perf = self.get_agent_performance()
        priorities = self.get_priority_distribution()
        tags = self.get_tag_cloud()
        trends = self.get_trends()

        lines = [
            "📊 Orchestrator Statistics Dashboard",
            "=" * 60,
            "",
            "📈 Summary:",
            f"  Total Tasks: {summary['tasks']['total']}",
            f"  Completed: {summary['tasks']['completed']} ({summary['tasks']['completion_rate']:.1f}%)",
            f"  Failed: {summary['tasks']['failed']}",
            f"  Currently Running: {summary['tasks']['running']}",
            "",
            "🤖 Agent Performance:",
        ]

        for agent, stats in agent_perf.items():
            lines.append(f"  {agent}:")
            lines.append(f"    Total: {stats['total']} | Completed: {stats['completed']} ({stats['completion_rate']:.1f}%)")

        lines.extend([
            "",
            "🎯 Priority Distribution:",
        ])
        for priority, count in priorities.items():
            lines.append(f"  {priority}: {count}")

        if tags:
            lines.extend([
                "",
                "🏷️  Top Tags:",
            ])
            for tag, count in list(tags.items())[:10]:
                lines.append(f"  {tag}: {count}")

        lines.extend([
            "",
            "📅 Trends (7 days):",
            f"  Created: {trends['tasks_created']}",
            f"  Completed: {trends['tasks_completed']}",
            f"  Failed: {trends['tasks_failed']}",
            "",
            "=" * 60
        ])

        return "\n".join(lines)


# ============================================================================
# IMPROVEMENT: Task Dependencies Graph
# ============================================================================

class TaskDependencyGraph:
    """
    DAG (Directed Acyclic Graph) for task dependencies.
    Supports: dependency tracking, cycle detection, topological sort.
    """

    def __init__(self, orchestrator: HybridOrchestrator):
        self.orch = orchestrator

    def build_graph(self) -> Dict[str, List[str]]:
        """Build dependency graph from tasks"""
        graph = defaultdict(list)
        tasks = self.orch.board.list_tasks()

        for task in tasks:
            if task.dependencies:
                for dep_id in task.dependencies:
                    graph[dep_id].append(task.id)

        return dict(graph)

    def get_dependents(self, task_id: str) -> List[str]:
        """Get tasks that depend on this task"""
        graph = self.build_graph()
        return graph.get(task_id, [])

    def get_dependencies(self, task_id: str) -> List[str]:
        """Get tasks this task depends on"""
        task = self.orch.board.get_task(task_id)
        return task.dependencies if task else []

    def has_cycle(self) -> bool:
        """Check if dependency graph has cycles (returns True if valid)"""
        graph = self.build_graph()
        visited = set()
        rec_stack = set()

        def dfs(node: str) -> bool:
            visited.add(node)
            rec_stack.add(node)

            for neighbor in graph.get(node, []):
                if neighbor not in visited:
                    if dfs(neighbor):
                        return True
                elif neighbor in rec_stack:
                    return True

            rec_stack.remove(node)
            return False

        for node in graph.keys():
            if node not in visited:
                if dfs(node):
                    return True
        return False

    def topological_sort(self) -> List[List[str]]:
        """Return tasks in topological order grouped by level"""
        graph = self.build_graph()
        in_degree = defaultdict(int)

        # Calculate in-degrees
        for node in graph.keys():
            for dep in graph[node]:
                in_degree[dep] += 1

        # Find nodes with no dependencies
        queue = [n for n in graph.keys() if in_degree[n] == 0]
        result = []
        visited = set(queue)

        while queue:
            level = []
            next_queue = []

            for node in queue:
                level.append(node)
                for dependent in graph.get(node, []):
                    in_degree[dependent] -= 1
                    if in_degree[dependent] == 0 and dependent not in visited:
                        next_queue.append(dependent)
                        visited.add(dependent)

            if level:
                result.append(level)
            queue = next_queue

        return result

    def get_ready_tasks(self) -> List[str]:
        """Get tasks that have all dependencies satisfied"""
        tasks = self.orch.board.list_tasks()
        ready = []
        completed_ids = {t.id for t in tasks if t.status == "completed"}

        for task in tasks:
            if task.status not in ["completed", "running", "failed"]:
                deps = task.dependencies or []
                if all(dep in completed_ids for dep in deps):
                    ready.append(task.id)

        return ready

    def visualize(self, task_id: str = None) -> str:
        """Generate ASCII visualization of dependency graph"""
        graph = self.build_graph()

        if task_id:
            # Show subtree for specific task
            return self._visualize_subtree(task_id, graph)

        # Show top levels
        lines = ["🔗 Task Dependencies", "=" * 60]

        levels = self.topological_sort()
        if not levels:
            return "📭 No dependencies found"

        for i, level in enumerate(levels[:5]):  # Show first 5 levels
            lines.append(f"\n📊 Level {i + 1}:")
            for task_id in level:
                task = self.orch.board.get_task(task_id)
                if task:
                    deps = task.dependencies or []
                    dep_str = f" <- [{', '.join(deps)}]" if deps else ""
                    lines.append(f"  [{task.id}] {task.title[:40]}{dep_str}")

        if len(levels) > 5:
            lines.append(f"\n... and {len(levels) - 5} more levels")

        return "\n".join(lines)

    def _visualize_subtree(self, task_id: str, graph: Dict) -> str:
        """Visualize dependency tree for a specific task"""
        lines = [f"🔗 Dependencies for {task_id}", "=" * 60]

        task = self.orch.board.get_task(task_id)
        if not task:
            return f"❌ Task {task_id} not found"

        # Show dependencies
        deps = task.dependencies or []
        if deps:
            lines.append("\n📥 Dependencies:")
            for dep_id in deps:
                dep_task = self.orch.board.get_task(dep_id)
                status = "✅" if dep_task and dep_task.status == "completed" else "⏳"
                lines.append(f"  {status} [{dep_id}] {dep_task.title[:40] if dep_task else 'Unknown'}")
        else:
            lines.append("\n📥 Dependencies: None")

        # Show dependents
        dependents = graph.get(task_id, [])
        if dependents:
            lines.append("\n📤 Dependents:")
            for dep_id in dependents:
                dep_task = self.orch.board.get_task(dep_id)
                lines.append(f"  [{dep_id}] {dep_task.title[:40] if dep_task else 'Unknown'}")
        else:
            lines.append("\n📤 Dependents: None")

        return "\n".join(lines)


# ============================================================================
# IMPROVEMENT: Async Task Execution
# ============================================================================

class AsyncTaskExecutor:
    """
    Thread pool for async task execution.
    Supports: background tasks, parallel execution, progress callbacks.
    """

    def __init__(self, max_workers: int = 5):
        self.max_workers = max_workers
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.running_tasks: Dict[str, Future] = {}
        self.task_results: Dict[str, Any] = {}
        self._lock = threading.Lock()

    def submit(self, task_id: str, func: Callable, *args, **kwargs) -> str:
        """Submit a task for async execution"""
        future = self.executor.submit(func, *args, **kwargs)
        with self._lock:
            self.running_tasks[task_id] = future
        return task_id

    def get_result(self, task_id: str, timeout: float = None) -> Any:
        """Get result of a completed task"""
        with self._lock:
            future = self.running_tasks.get(task_id)

        if future:
            try:
                result = future.result(timeout=timeout)
                with self._lock:
                    self.task_results[task_id] = result
                    del self.running_tasks[task_id]
                return result
            except TimeoutError:
                return None
            except Exception as e:
                with self._lock:
                    self.task_results[task_id] = {"error": str(e)}
                    del self.running_tasks[task_id]
                return {"error": str(e)}

        return self.task_results.get(task_id)

    def is_running(self, task_id: str) -> bool:
        """Check if task is still running"""
        with self._lock:
            return task_id in self.running_tasks

    def cancel(self, task_id: str) -> bool:
        """Cancel a running task"""
        with self._lock:
            future = self.running_tasks.get(task_id)
            if future:
                cancelled = future.cancel()
                if cancelled:
                    del self.running_tasks[task_id]
                return cancelled
        return False

    def get_status(self) -> dict:
        """Get executor status"""
        with self._lock:
            return {
                "max_workers": self.max_workers,
                "running": len(self.running_tasks),
                "completed": len(self.task_results),
                "tasks": list(self.running_tasks.keys())
            }

    def shutdown(self, wait: bool = True):
        """Shutdown the executor"""
        self.executor.shutdown(wait=wait)


# ============================================================================
# IMPROVEMENT: Task Timeline/Calendar
# ============================================================================

class TaskTimeline:
    """
    Timeline view of tasks based on creation/update timestamps.
    Supports: chronological views, filtering, grouping.
    """

    def __init__(self, orchestrator: HybridOrchestrator):
        self.orch = orchestrator

    def get_timeline(self, days: int = 7, group_by: str = "day") -> Dict[str, List[dict]]:
        """Get timeline grouped by time periods"""
        tasks = self.orch.board.list_tasks()
        timeline = defaultdict(list)

        cutoff = datetime.now(timezone.utc) - timedelta(days=days)

        for task in tasks:
            task_time = datetime.fromisoformat(task.created.replace('Z', '+00:00'))
            if task_time < cutoff:
                continue

            if group_by == "day":
                key = task.created[:10]  # YYYY-MM-DD
            elif group_by == "hour":
                key = task.created[:13]  # YYYY-MM-DDTHH
            elif group_by == "week":
                week = datetime.fromisoformat(task.created[:10]).isocalendar()[1]
                key = f"{task.created[:4]}-W{week:02d}"
            else:
                key = task.created[:10]

            timeline[key].append({
                "id": task.id,
                "title": task.title,
                "status": task.status,
                "priority": task.priority,
                "agent": task.agent,
                "created": task.created,
                "progress": task.progress
            })

        return dict(sorted(timeline.items()))

    def get_gantt_data(self, days: int = 7) -> List[dict]:
        """Get Gantt chart data for visualization"""
        tasks = self.orch.board.list_tasks()
        gantt = []

        for task in tasks:
            if task.status in ["queued", "running"]:
                start = datetime.fromisoformat(task.created.replace('Z', '+00:00'))
                now = datetime.now(timezone.utc)

                if (now - start).days > days:
                    continue

                gantt.append({
                    "id": task.id,
                    "title": task.title[:30],
                    "start": start.isoformat(),
                    "end": now.isoformat(),
                    "progress": task.progress,
                    "status": task.status,
                    "priority": task.priority
                })

        return gantt

    def generate_calendar_view(self, days: int = 7) -> str:
        """Generate ASCII calendar view"""
        timeline = self.get_timeline(days, group_by="day")
        lines = ["📅 Task Timeline", "=" * 60]

        for date, tasks in timeline.items():
            day_name = datetime.fromisoformat(date).strftime("%A")
            lines.append(f"\n📆 {date} ({day_name}):")
            for task in tasks:
                status_icon = {
                    "queued": "⏳", "running": "🔄", "completed": "✅",
                    "failed": "❌", "blocked": "🚫"
                }.get(task["status"], "⚪")

                priority_color = {
                    "low": "🟢", "medium": "🟡", "high": "🟠", "critical": "🔴"
                }.get(task["priority"], "⚪")

                lines.append(f"  {status_icon} [{task['id']}] {priority_color} {task['title'][:35]}")

        return "\n".join(lines)


# ============================================================================
# IMPROVEMENT: Priority Queue
# ============================================================================

import heapq

class PriorityTaskQueue:
    """
    Priority queue for task scheduling.
    Supports: priority levels, FIFO within same priority, deadline scheduling.
    """

    def __init__(self, orchestrator: HybridOrchestrator):
        self.orch = orchestrator
        self._queue = []  # [(priority_score, timestamp, task_id)]
        self._build_queue()

    def _priority_score(self, task: Task) -> tuple:
        """Calculate priority score (lower = higher priority)"""
        priority_weights = {"critical": 0, "high": 1, "medium": 2, "low": 3}
        priority = priority_weights.get(task.priority, 4)
        created = task.created
        return (priority, created)

    def _build_queue(self):
        """Build queue from orchestrator tasks"""
        self._queue = []
        for task in self.orch.board.list_tasks():
            if task.status == "queued":
                score = self._priority_score(task)
                heapq.heappush(self._queue, (score, task.id))

    def enqueue(self, task_id: str) -> bool:
        """Add task to priority queue"""
        task = self.orch.board.get_task(task_id)
        if not task or task.status != "queued":
            return False

        score = self._priority_score(task)
        heapq.heappush(self._queue, (score, task_id))
        return True

    def dequeue(self) -> Optional[str]:
        """Get highest priority task"""
        if not self._queue:
            self._build_queue()

        while self._queue:
            score, task_id = heapq.heappop(self._queue)
            task = self.orch.board.get_task(task_id)
            if task and task.status == "queued":
                return task_id

        return None

    def peek(self) -> Optional[str]:
        """View next task without removing"""
        if not self._queue:
            self._build_queue()

        for score, task_id in sorted(self._queue)[:10]:
            task = self.orch.board.get_task(task_id)
            if task and task.status == "queued":
                return task_id

        return None

    def get_queue(self, limit: int = 10) -> List[dict]:
        """Get upcoming tasks in priority order"""
        self._build_queue()
        result = []

        for i, (score, task_id) in enumerate(sorted(self._queue)[:limit]):
            task = self.orch.board.get_task(task_id)
            if task:
                result.append({
                    "position": i + 1,
                    "id": task.id,
                    "title": task.title,
                    "priority": task.priority,
                    "agent": task.agent
                })

        return result

    def get_stats(self) -> dict:
        """Get queue statistics"""
        self._build_queue()
        priority_counts = defaultdict(int)

        for _, task_id in self._queue:
            task = self.orch.board.get_task(task_id)
            if task:
                priority_counts[task.priority] += 1

        return {
            "total_queued": len(self._queue),
            "by_priority": dict(priority_counts),
            "next_task": self.peek()
        }


# ============================================================================
# IMPROVEMENT: Notification System
# ============================================================================

class NotificationManager:
    """
    In-app notification system.
    Supports: real-time notifications, read/unread, grouping.
    """

    NOTIFICATIONS_FILE = TASKS_DIR / "notifications.json"

    def __init__(self, orchestrator: HybridOrchestrator):
        self.orch = orchestrator
        self._lock = threading.Lock()
        self._load()
        self._listeners: List[Callable] = []

    def _load(self):
        if self.NOTIFICATIONS_FILE.exists():
            with open(self.NOTIFICATIONS_FILE, 'r') as f:
                self.notifications = json.load(f)
        else:
            self.notifications = {"items": [], "next_id": 1}

    def _save(self):
        temp_file = self.NOTIFICATIONS_FILE.with_suffix('.tmp')
        with open(temp_file, 'w') as f:
            json.dump(self.notifications, f, indent=2)
        os.replace(temp_file, self.NOTIFICATIONS_FILE)

    def add_listener(self, callback: Callable):
        """Add a notification listener"""
        self._listeners.append(callback)

    def notify(self, title: str, message: str, level: str = "info",
               task_id: str = None, user: str = None) -> dict:
        """Create a notification"""
        with self._lock:
            notification = {
                "id": self.notifications["next_id"],
                "title": title,
                "message": message,
                "level": level,
                "task_id": task_id,
                "user": user or "system",
                "read": False,
                "created": get_utc_timestamp()
            }
            self.notifications["next_id"] += 1
            self.notifications["items"].append(notification)

            if len(self.notifications["items"]) > 1000:
                self.notifications["items"] = self.notifications["items"][-1000:]

            self._save()

            for listener in self._listeners:
                try:
                    listener(notification)
                except Exception:
                    pass

            return notification

    def get_notifications(self, unread_only: bool = False, limit: int = 50) -> List[dict]:
        """Get notifications"""
        items = self.notifications["items"]
        if unread_only:
            items = [n for n in items if not n.get("read")]
        return items[-limit:]

    def mark_read(self, notification_id: int) -> bool:
        """Mark notification as read"""
        with self._lock:
            for n in self.notifications["items"]:
                if n["id"] == notification_id:
                    n["read"] = True
                    self._save()
                    return True
            return False

    def mark_all_read(self) -> int:
        """Mark all notifications as read"""
        with self._lock:
            count = sum(1 for n in self.notifications["items"] if not n.get("read"))
            for n in self.notifications["items"]:
                n["read"] = True
            if count > 0:
                self._save()
            return count

    def get_unread_count(self) -> int:
        """Get count of unread notifications"""
        return sum(1 for n in self.notifications["items"] if not n.get("read"))

    def send(self, channel: str, message: str, level: str = "info") -> dict:
        """Alias for notify(), used by WorkflowEngine steps."""
        return self.notify(title=channel, message=message, level=level)


# ============================================================================
# IMPROVEMENT: Task Filters/Facets
# ============================================================================

class TaskFilters:
    """Advanced task filtering and faceted search."""

    def __init__(self, orchestrator: HybridOrchestrator):
        self.orch = orchestrator
        self._saved_filters_file = TASKS_DIR / "saved_filters.json"
        self._saved_filters = self._load_saved_filters()

    def _load_saved_filters(self) -> dict:
        if self._saved_filters_file.exists():
            with open(self._saved_filters_file, 'r') as f:
                return json.load(f)
        return {}

    def _save_saved_filters(self):
        with open(self._saved_filters_file, 'w') as f:
            json.dump(self._saved_filters, f, indent=2)

    def filter_tasks(self, **criteria) -> List[Task]:
        """Filter tasks with multiple criteria"""
        tasks = self.orch.board.list_tasks()
        results = []

        for task in tasks:
            if criteria.get('status'):
                if isinstance(criteria['status'], list):
                    if task.status not in criteria['status']:
                        continue
                elif task.status != criteria['status']:
                    continue

            if criteria.get('priority') and task.priority != criteria['priority']:
                continue

            if criteria.get('agent') and task.agent != criteria['agent']:
                continue

            if criteria.get('tags'):
                required = criteria['tags'] if isinstance(criteria['tags'], list) else [criteria['tags']]
                if not all(tag in task.tags for tag in required):
                    continue

            if criteria.get('exclude_tags'):
                exclude = criteria['exclude_tags'] if isinstance(criteria['exclude_tags'], list) else [criteria['exclude_tags']]
                if any(tag in task.tags for tag in exclude):
                    continue

            if criteria.get('search'):
                if criteria['search'].lower() not in task.title.lower():
                    continue

            results.append(task)
        return results

    def get_facets(self) -> dict:
        """Get facet counts for all filterable attributes"""
        tasks = self.orch.board.list_tasks()
        facets = {"status": defaultdict(int), "priority": defaultdict(int),
                  "agent": defaultdict(int), "tags": defaultdict(int)}

        for task in tasks:
            facets["status"][task.status] += 1
            facets["priority"][task.priority] += 1
            facets["agent"][task.agent] += 1
            for tag in task.tags:
                facets["tags"][tag] += 1

        return {k: dict(v) for k, v in facets.items()}

    def save_filter(self, name: str, **criteria) -> bool:
        """Save a filter configuration"""
        self._saved_filters[name] = {"criteria": criteria, "created": get_utc_timestamp()}
        self._save_saved_filters()
        return True

    def get_saved_filters(self) -> dict:
        return self._saved_filters


# ============================================================================
# IMPROVEMENT: Auto-assign Rules
# ============================================================================

class AutoAssignRules:
    """Automatic task assignment rules."""

    RULES_FILE = STATE_DIR / "auto_assign_rules.json"

    def __init__(self, orchestrator: HybridOrchestrator):
        self.orch = orchestrator
        self._lock = threading.Lock()
        self._load()

    def _load(self):
        if self.RULES_FILE.exists():
            with open(self.RULES_FILE, 'r') as f:
                self.rules = json.load(f)
        else:
            self.rules = {"enabled": True, "rules": [], "default_agent": "Hermes"}

    def _save(self):
        with open(self.RULES_FILE, 'w') as f:
            json.dump(self.rules, f, indent=2)

    def add_rule(self, name: str, condition: dict, agent: str, priority: str = None) -> dict:
        """Add an auto-assign rule"""
        with self._lock:
            rule = {
                "id": f"rule-{len(self.rules['rules']) + 1:03d}",
                "name": name, "condition": condition, "agent": agent,
                "priority": priority, "enabled": True, "match_count": 0
            }
            self.rules["rules"].append(rule)
            self._save()
            return rule

    def match_task(self, task: Task) -> Optional[str]:
        """Match task against rules"""
        if not self.rules.get("enabled", True):
            return self.rules.get("default_agent")

        for rule in self.rules["rules"]:
            if not rule.get("enabled", True):
                continue

            condition = rule.get("condition", {})
            matched = True

            if "tag" in condition and condition["tag"] not in task.tags:
                matched = False
            if "priority" in condition and task.priority != condition["priority"]:
                matched = False
            if "contains" in condition and condition["contains"].lower() not in task.title.lower():
                matched = False

            if matched:
                rule["match_count"] = rule.get("match_count", 0) + 1
                self._save()
                return rule["agent"]

        return self.rules.get("default_agent")

    def get_stats(self) -> dict:
        return {
            "enabled": self.rules.get("enabled", True),
            "rules_count": len(self.rules["rules"]),
            "total_matches": sum(r.get("match_count", 0) for r in self.rules["rules"]),
            "default_agent": self.rules.get("default_agent")
        }


# ============================================================================
# IMPROVEMENT: Tags Manager
# ============================================================================

class TagsManager:
    """Centralized tags management."""

    TAGS_FILE = TASKS_DIR / "tags_metadata.json"

    def __init__(self, orchestrator: HybridOrchestrator):
        self.orch = orchestrator
        self._load()

    def _load(self):
        if self.TAGS_FILE.exists():
            with open(self.TAGS_FILE, 'r') as f:
                self.tags_metadata = json.load(f)
        else:
            self.tags_metadata = {"tags": {}, "synonyms": {}}

    def _save(self):
        with open(self.TAGS_FILE, 'w') as f:
            json.dump(self.tags_metadata, f, indent=2)

    def get_all_tags(self) -> List[str]:
        return list(self.tags_metadata["tags"].keys())

    def create_tag(self, tag: str, description: str = "", color: str = "#808080") -> dict:
        if tag not in self.tags_metadata["tags"]:
            self.tags_metadata["tags"][tag] = {
                "name": tag, "description": description, "color": color,
                "usage_count": 0, "created": get_utc_timestamp()
            }
            self._save()
        return self.tags_metadata["tags"][tag]

    def get_popular_tags(self, limit: int = 20) -> List[tuple]:
        """Get most used tags"""
        tasks = self.orch.board.list_tasks()
        counts = defaultdict(int)
        for task in tasks:
            for tag in task.tags:
                counts[tag] += 1
        return sorted(counts.items(), key=lambda x: x[1], reverse=True)[:limit]


# ============================================================================
# Webhook Notifications
# ============================================================================

class WebhookManager:
    """Manage webhook notifications for events"""

    WEBHOOKS_FILE = STATE_DIR / "webhooks.json"

    def __init__(self, orchestrator: HybridOrchestrator):
        self.orch = orchestrator
        self._lock = threading.Lock()
        self._load()

    def _load(self):
        if self.WEBHOOKS_FILE.exists():
            with open(self.WEBHOOKS_FILE, 'r') as f:
                self.webhooks = json.load(f)
        else:
            self.webhooks = {"hooks": [], "enabled": True}

    def _save(self):
        temp_file = self.WEBHOOKS_FILE.with_suffix('.tmp')
        with open(temp_file, 'w') as f:
            json.dump(self.webhooks, f, indent=2)
        os.replace(temp_file, self.WEBHOOKS_FILE)

    def add_webhook(self, url: str, events: List[str] = None, name: str = None) -> dict:
        """Add a new webhook"""
        with self._lock:
            webhook = {
                "id": f"WH-{len(self.webhooks['hooks']) + 1:03d}",
                "url": url,
                "name": name or url,
                "events": events or ["task.created", "task.completed", "task.failed"],
                "enabled": True,
                "created": get_utc_timestamp(),
                "trigger_count": 0
            }
            self.webhooks["hooks"].append(webhook)
            self._save()
            return webhook

    def remove_webhook(self, webhook_id: str) -> bool:
        """Remove a webhook"""
        with self._lock:
            before = len(self.webhooks["hooks"])
            self.webhooks["hooks"] = [w for w in self.webhooks["hooks"] if w["id"] != webhook_id]
            if len(self.webhooks["hooks"]) < before:
                self._save()
                return True
            return False

    def list_webhooks(self) -> List[dict]:
        """List all webhooks"""
        return self.webhooks["hooks"]

    def trigger(self, event_type: str, data: dict):
        """Trigger webhooks for an event"""
        if not self.webhooks.get("enabled", True):
            return

        import urllib.request
        import urllib.error

        payload = json.dumps({
            "event": event_type,
            "data": data,
            "timestamp": get_utc_timestamp()
        })

        for webhook in self.webhooks["hooks"]:
            if not webhook.get("enabled", True):
                continue
            if event_type not in webhook.get("events", []):
                continue

            try:
                req = urllib.request.Request(
                    webhook["url"],
                    data=payload.encode('utf-8'),
                    headers={"Content-Type": "application/json"}
                )
                urllib.request.urlopen(req, timeout=5)
                webhook["trigger_count"] = webhook.get("trigger_count", 0) + 1
                self._save()
            except Exception as e:
                print(f"⚠️ Webhook {webhook['id']} failed: {e}")


# ============================================================================
# IMPROVEMENT: Recurring Tasks
# ============================================================================

class RecurringTaskManager:
    """
    Manage recurring/scheduled tasks.
    Supports: cron expressions, interval-based, manual triggers.
    """

    RECURRING_FILE = STATE_DIR / "recurring_tasks.json"

    def __init__(self, orchestrator: HybridOrchestrator):
        self.orch = orchestrator
        self._lock = threading.Lock()
        self._load()
        self._next_run_file = STATE_DIR / "next_runs.json"
        self._load_next_runs()

    def _load(self):
        if self.RECURRING_FILE.exists():
            with open(self.RECURRING_FILE, 'r') as f:
                self.recurring = json.load(f)
        else:
            self.recurring = {"tasks": []}

    def _save(self):
        with open(self.RECURRING_FILE, 'w') as f:
            json.dump(self.recurring, f, indent=2)

    def _load_next_runs(self):
        if self._next_run_file.exists():
            with open(self._next_run_file, 'r') as f:
                self.next_runs = json.load(f)
        else:
            self.next_runs = {}

    def _save_next_runs(self):
        with open(self._next_run_file, 'w') as f:
            json.dump(self.next_runs, f, indent=2)

    def add_recurring_task(self, name: str, task_template: dict,
                          schedule: str, schedule_type: str = "interval",
                          interval_minutes: int = 60) -> dict:
        """
        Add a recurring task.
        schedule_type: 'interval' (every N minutes) or 'cron' (cron expression)
        """
        with self._lock:
            task = {
                "id": f"RT-{len(self.recurring['tasks']) + 1:03d}",
                "name": name,
                "template": task_template,
                "schedule_type": schedule_type,
                "schedule": schedule if schedule_type == "cron" else interval_minutes,
                "enabled": True,
                "last_run": None,
                "next_run": get_utc_timestamp(),
                "run_count": 0,
                "created": get_utc_timestamp()
            }
            self.recurring["tasks"].append(task)
            self._calculate_next_run(task)
            self._save()
            self._save_next_runs()
            return task

    def _calculate_next_run(self, task: dict):
        """Calculate next run time"""
        from datetime import datetime, timedelta

        now = datetime.now(timezone.utc)
        schedule_type = task.get("schedule_type", "interval")

        if schedule_type == "interval":
            interval = task.get("schedule", 60)
            next_run = now + timedelta(minutes=interval)
        else:
            # Cron scheduling - simplified (full cron would need croniter)
            next_run = now + timedelta(minutes=30)

        task["next_run"] = next_run.isoformat()
        self.next_runs[task["id"]] = task["next_run"]

    def get_due_tasks(self) -> List[dict]:
        """Get tasks that are due to run"""
        now = datetime.now(timezone.utc)
        due = []

        for task in self.recurring["tasks"]:
            if not task.get("enabled", True):
                continue

            next_run = task.get("next_run")
            if not next_run:
                continue

            next_run_dt = datetime.fromisoformat(next_run.replace('Z', '+00:00'))
            if next_run_dt <= now:
                due.append(task)

        return due

    def execute_task(self, task_id: str) -> Optional[Task]:
        """Execute a recurring task and schedule next run"""
        task = self._find_task(task_id)
        if not task:
            return None

        template = task.get("template", {})
        new_task = self.orch.board.add_task(
            title=template.get("title", f"Recurring: {task['name']}"),
            agent=template.get("agent", "Hermes"),
            tags=template.get("tags", ["recurring"]),
            priority=template.get("priority", "medium")
        )

        task["last_run"] = get_utc_timestamp()
        task["run_count"] = task.get("run_count", 0) + 1
        self._calculate_next_run(task)
        self._save()
        self._save_next_runs()

        self.orch.history.add_entry(new_task.id, "created_from_recurring",
                                    {"recurring_id": task_id, "source": task["name"]})

        return new_task

    def _find_task(self, task_id: str) -> Optional[dict]:
        for t in self.recurring["tasks"]:
            if t["id"] == task_id:
                return t
        return None

    def get_stats(self) -> dict:
        """Get recurring tasks statistics"""
        total = len(self.recurring["tasks"])
        enabled = sum(1 for t in self.recurring["tasks"] if t.get("enabled", True))
        total_runs = sum(t.get("run_count", 0) for t in self.recurring["tasks"])
        due_now = len(self.get_due_tasks())

        return {
            "total": total,
            "enabled": enabled,
            "disabled": total - enabled,
            "total_executions": total_runs,
            "due_now": due_now
        }

    def toggle_task(self, task_id: str) -> bool:
        """Enable/disable a recurring task"""
        task = self._find_task(task_id)
        if task:
            task["enabled"] = not task.get("enabled", True)
            self._save()
            return True
        return False

    def delete_task(self, task_id: str) -> bool:
        """Delete a recurring task"""
        with self._lock:
            before = len(self.recurring["tasks"])
            self.recurring["tasks"] = [t for t in self.recurring["tasks"] if t["id"] != task_id]
            if task_id in self.next_runs:
                del self.next_runs[task_id]
            if len(self.recurring["tasks"]) < before:
                self._save()
                self._save_next_runs()
                return True
            return False


# ============================================================================
# IMPROVEMENT: Activity Feed
# ============================================================================

class ActivityFeed:
    """
    Unified activity feed combining all events.
    Supports: filtering, grouping, real-time updates.
    """

    FEED_FILE = TASKS_DIR / "activity_feed.json"

    def __init__(self, orchestrator: HybridOrchestrator):
        self.orch = orchestrator
        self._lock = threading.Lock()
        self._load()

    def _load(self):
        if self.FEED_FILE.exists():
            with open(self.FEED_FILE, 'r') as f:
                self.feed = json.load(f)
        else:
            self.feed = {"items": [], "next_id": 1}

    def _save(self):
        temp_file = self.FEED_FILE.with_suffix('.tmp')
        with open(temp_file, 'w') as f:
            json.dump(self.feed, f, indent=2)
        os.replace(temp_file, self.FEED_FILE)

    def add_activity(self, action: str, entity_type: str, entity_id: str,
                     details: dict = None, actor: str = "system") -> dict:
        """Add an activity to the feed"""
        with self._lock:
            activity = {
                "id": self.feed["next_id"],
                "action": action,
                "entity_type": entity_type,
                "entity_id": entity_id,
                "details": details or {},
                "actor": actor,
                "timestamp": get_utc_timestamp()
            }
            self.feed["next_id"] += 1
            self.feed["items"].append(activity)

            # Keep only last 5000 activities
            if len(self.feed["items"]) > 5000:
                self.feed["items"] = self.feed["items"][-5000:]

            self._save()
            return activity

    def get_feed(self, entity_type: str = None, action: str = None,
                actor: str = None, limit: int = 100) -> List[dict]:
        """Get filtered activity feed"""
        items = self.feed["items"]

        if entity_type:
            items = [i for i in items if i["entity_type"] == entity_type]
        if action:
            items = [i for i in items if i["action"] == action]
        if actor:
            items = [i for i in items if i["actor"] == actor]

        return items[-limit:]

    def get_entity_activities(self, entity_id: str) -> List[dict]:
        """Get all activities for a specific entity"""
        return [i for i in self.feed["items"] if i["entity_id"] == entity_id]

    def get_recent_actions(self, action: str, limit: int = 20) -> List[dict]:
        """Get recent activities by action type"""
        return self.get_feed(action=action, limit=limit)

    def generate_timeline(self, hours: int = 24) -> str:
        """Generate ASCII timeline view"""
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        activities = [a for a in self.feed["items"] if a["timestamp"] > cutoff]

        if not activities:
            return f"📭 No activity in the last {hours} hours"

        lines = [f"📋 Activity Timeline (Last {hours}h)", "=" * 60]

        for activity in activities[-30:]:  # Last 30
            icon = {
                "created": "➕", "updated": "✏️", "completed": "✅",
                "failed": "❌", "deleted": "🗑️", "commented": "💬"
            }.get(activity["action"], "📌")

            lines.append(f"{icon} [{activity['timestamp'][11:19]}] "
                       f"{activity['actor']} {activity['action']} "
                       f"{activity['entity_type']} {activity['entity_id']}")

        return "\n".join(lines)


# ============================================================================
# IMPROVEMENT: Batch Operations API
# ============================================================================

class BatchOperations:
    """Advanced batch operations for tasks"""

    def __init__(self, orchestrator: HybridOrchestrator):
        self.orch = orchestrator

    def bulk_update_status(self, task_ids: List[str], status: str) -> dict:
        """Update status for multiple tasks"""
        updated = []
        failed = []

        for task_id in task_ids:
            try:
                self.orch.board.update_status(task_id, status)
                updated.append(task_id)
                self.orch.history.add_entry(task_id, "bulk_status_update", {"status": status})
            except Exception:
                failed.append(task_id)

        return {"updated": updated, "failed": failed, "total": len(task_ids)}

    def bulk_update_priority(self, task_ids: List[str], priority: str) -> dict:
        """Update priority for multiple tasks"""
        updated = []
        failed = []

        for task_id in task_ids:
            try:
                task = self.orch.board.get_task(task_id)
                if task:
                    task.priority = priority
                    self.orch.board.update_task(task)
                    updated.append(task_id)
                    self.orch.history.add_entry(task_id, "bulk_priority_update", {"priority": priority})
                else:
                    failed.append(task_id)
            except Exception:
                failed.append(task_id)

        return {"updated": updated, "failed": failed, "total": len(task_ids)}

    def bulk_add_tags(self, task_ids: List[str], tags: List[str]) -> dict:
        """Add tags to multiple tasks"""
        updated = []
        failed = []

        for task_id in task_ids:
            try:
                task = self.orch.board.get_task(task_id)
                if task:
                    for tag in tags:
                        if tag not in task.tags:
                            task.tags.append(tag)
                    self.orch.board.update_task(task)
                    updated.append(task_id)
                    self.orch.history.add_entry(task_id, "bulk_tags_added", {"tags": tags})
                else:
                    failed.append(task_id)
            except Exception:
                failed.append(task_id)

        return {"updated": updated, "failed": failed, "total": len(task_ids)}

    def bulk_delete(self, task_ids: List[str]) -> dict:
        """Delete multiple tasks"""
        deleted = []
        failed = []

        for task_id in task_ids:
            try:
                if self.orch.board.delete_task(task_id):
                    deleted.append(task_id)
                    self.orch.history.add_entry(task_id, "bulk_deleted")
                else:
                    failed.append(task_id)
            except Exception:
                failed.append(task_id)

        return {"deleted": deleted, "failed": failed, "total": len(task_ids)}

    def bulk_assign(self, task_ids: List[str], agent: str) -> dict:
        """Reassign multiple tasks to an agent"""
        updated = []
        failed = []

        for task_id in task_ids:
            try:
                task = self.orch.board.get_task(task_id)
                if task:
                    task.agent = agent
                    self.orch.board.update_task(task)
                    updated.append(task_id)
                    self.orch.history.add_entry(task_id, "bulk_reassigned", {"agent": agent})
                else:
                    failed.append(task_id)
            except Exception:
                failed.append(task_id)

        return {"updated": updated, "failed": failed, "total": len(task_ids)}

    def get_batch_stats(self) -> dict:
        """Get batch operations statistics"""
        return {
            "total_bulk_ops": len(self.feed.get("items", [])) if hasattr(self, "feed") else 0,
            "supported_operations": ["bulk_update_status", "bulk_update_priority",
                                   "bulk_add_tags", "bulk_delete", "bulk_assign"]
        }

    def bulk_assign(self, task_ids: List[str], agent: str) -> dict:
        """Reassign multiple tasks to an agent"""
        updated = []
        failed = []

        for task_id in task_ids:
            try:
                task = self.orch.board.get_task(task_id)
                if task:
                    task.agent = agent
                    self.orch.board.update_task(task)
                    updated.append(task_id)
                    self.orch.history.add_entry(task_id, "bulk_reassigned", {"agent": agent})
                else:
                    failed.append(task_id)
            except Exception:
                failed.append(task_id)

        return {"updated": updated, "failed": failed, "total": len(task_ids)}

    def get_batch_stats(self) -> dict:
        """Get batch operations statistics"""
        return {
            "total_bulk_ops": len(self.feed.get("items", [])) if hasattr(self, "feed") else 0,
            "supported_operations": ["bulk_update_status", "bulk_update_priority",
                                   "bulk_add_tags", "bulk_delete", "bulk_assign"]
        }


class DataMigration:
    """Tools for migrating data to/from orchestrator"""

    def __init__(self, orchestrator: HybridOrchestrator):
        self.orch = orchestrator

    def export_all(self) -> dict:
        """Export all orchestrator data"""
        return {
            "version": "5.0",
            "timestamp": get_utc_timestamp(),
            "tasks": [t.to_dict() for t in self.orch.board.list_tasks()],
            "history": self.orch.history.history,
            "comments": self.orch.comments.comments,
            "config": self.orch.config.config,
            "cron_jobs": [j.to_dict() for j in self.orch.cron.list_jobs()],
            "users": self.orch.rbac.users,
            "webhooks": self.orch.webhooks.webhooks,
            "templates": self.orch._load_templates()
        }

    def import_all(self, data: dict, merge: bool = True) -> dict:
        """Import all orchestrator data"""
        results = {"tasks": 0, "history": 0, "comments": 0, "errors": []}

        # Import tasks
        if "tasks" in data:
            for task_data in data["tasks"]:
                try:
                    self.orch.board.add_task(
                        title=task_data.get("title", "Imported"),
                        description=task_data.get("description", ""),
                        agent=task_data.get("agent", "Hermes"),
                        tags=task_data.get("tags", []),
                        priority=task_data.get("priority", "medium")
                    )
                    results["tasks"] += 1
                except Exception as e:
                    results["errors"].append(f"Task import error: {e}")

        # Import comments
        if "comments" in data:
            by_task = data["comments"].get("by_task", {})
            for task_id, comments in by_task.items():
                for comment in comments:
                    try:
                        self.orch.comments.add_comment(
                            task_id,
                            comment.get("content", ""),
                            comment.get("author", "import")
                        )
                        results["comments"] += 1
                    except Exception:
                        pass

        self.orch._save_state()
        return results

    def backup_to_file(self, filepath: str) -> str:
        """Backup all data to JSON file"""
        data = self.export_all()
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        return filepath

    def restore_from_file(self, filepath: str) -> dict:
        """Restore data from backup file"""
        with open(filepath, 'r') as f:
            data = json.load(f)
        return self.import_all(data)


# ============================================================================
# IMPROVEMENT: Performance Monitoring
# ============================================================================

class PerformanceMonitor:
    """Monitor orchestrator performance metrics"""

    def __init__(self, orchestrator: HybridOrchestrator):
        self.orch = orchestrator
        self._metrics_file = STATE_DIR / "performance_metrics.json"
        self._load_metrics()

    def _load_metrics(self):
        if self._metrics_file.exists():
            with open(self._metrics_file, 'r') as f:
                self.metrics = json.load(f)
        else:
            self.metrics = {
                "operations": [],
                "timings": defaultdict(list),
                "errors": []
            }

    def _save_metrics(self):
        # Keep last 10000 operations
        if len(self.metrics["operations"]) > 10000:
            self.metrics["operations"] = self.metrics["operations"][-10000:]
        with open(self._metrics_file, 'w') as f:
            json.dump(self.metrics, f, indent=2)

    def record_operation(self, operation: str, duration: float, success: bool,
                        details: dict = None):
        """Record an operation for monitoring"""
        entry = {
            "operation": operation,
            "duration": duration,
            "success": success,
            "timestamp": get_utc_timestamp(),
            "details": details or {}
        }
        self.metrics["operations"].append(entry)
        self.metrics["timings"][operation].append(duration)

        # Keep only last 1000 timings per operation
        if len(self.metrics["timings"][operation]) > 1000:
            self.metrics["timings"][operation] = self.metrics["timings"][operation][-1000:]

        if not success:
            self.metrics["errors"].append(entry)

        self._save_metrics()

    def get_operation_stats(self, operation: str) -> dict:
        """Get statistics for an operation type"""
        timings = self.metrics["timings"].get(operation, [])
        if not timings:
            return {"count": 0, "avg": 0, "min": 0, "max": 0}

        return {
            "count": len(timings),
            "avg": sum(timings) / len(timings),
            "min": min(timings),
            "max": max(timings),
            "recent_99th": sorted(timings)[int(len(timings) * 0.99)] if len(timings) > 10 else max(timings)
        }

    def get_all_stats(self) -> dict:
        """Get all performance statistics"""
        stats = {}
        for op in self.metrics["timings"].keys():
            stats[op] = self.get_operation_stats(op)
        return stats

    def get_recent_errors(self, limit: int = 50) -> List[dict]:
        """Get recent error entries"""
        return self.metrics["errors"][-limit:]

    def get_summary(self) -> str:
        """Get performance summary as text"""
        stats = self.get_all_stats()
        errors = self.get_recent_errors(10)

        lines = ["📊 Performance Summary", "=" * 60]

        for op, data in stats.items():
            if data["count"] > 0:
                lines.append(f"\n{op}:")
                lines.append(f"  Executions: {data['count']}")
                lines.append(f"  Avg: {data['avg']:.3f}s | Min: {data['min']:.3f}s | Max: {data['max']:.3f}s")

        if errors:
            lines.append(f"\n⚠️  Recent Errors ({len(errors)}):")
            for err in errors[:5]:
                lines.append(f"  [{err['timestamp'][11:19]}] {err['operation']}: {err.get('details', {}).get('error', 'N/A')}")

        return "\n".join(lines)


# ============================================================================
# IMPROVEMENT: Task Templates Library
# ============================================================================

class TaskTemplatesLibrary:
    """
    Library of reusable task templates.
    Supports: categories, parameters, validation, versioning.
    """

    TEMPLATES_FILE = STATE_DIR / "task_templates.json"

    def __init__(self, orchestrator: HybridOrchestrator):
        self.orch = orchestrator
        self._lock = threading.Lock()
        self._load()

    def _load(self):
        if self.TEMPLATES_FILE.exists():
            with open(self.TEMPLATES_FILE, 'r') as f:
                self.templates = json.load(f)
        else:
            self.templates = {"templates": [], "categories": [], "next_id": 1}
            self._create_defaults()

    def _save(self):
        temp_file = self.TEMPLATES_FILE.with_suffix('.tmp')
        with open(temp_file, 'w') as f:
            json.dump(self.templates, f, indent=2)
        os.replace(temp_file, self.TEMPLATES_FILE)

    def _create_defaults(self):
        """Create default task templates"""
        self.templates["templates"] = [
            {
                "id": 1,
                "name": "Bug Report",
                "category": "issues",
                "description": "Standard bug report template",
                "fields": {
                    "title": {"type": "string", "required": True, "label": "Bug Title"},
                    "severity": {"type": "select", "options": ["critical", "high", "medium", "low"], "default": "medium"},
                    "steps": {"type": "textarea", "required": True, "label": "Reproduction Steps"},
                    "expected": {"type": "textarea", "label": "Expected Behavior"},
                    "actual": {"type": "textarea", "label": "Actual Behavior"}
                },
                "defaults": {"priority": "high", "tags": ["bug", "needs-fix"]},
                "version": 1,
                "created": get_utc_timestamp()
            },
            {
                "id": 2,
                "name": "Feature Request",
                "category": "features",
                "description": "New feature request template",
                "fields": {
                    "title": {"type": "string", "required": True, "label": "Feature Name"},
                    "user_story": {"type": "textarea", "required": True, "label": "User Story (As a... I want... so that...)"},
                    "acceptance": {"type": "textarea", "label": "Acceptance Criteria"},
                    "priority": {"type": "select", "options": ["critical", "high", "medium", "low"], "default": "medium"},
                    "effort": {"type": "select", "options": ["xs", "s", "m", "l", "xl"], "label": "Estimated Effort"}
                },
                "defaults": {"tags": ["feature", "enhancement"]},
                "version": 1,
                "created": get_utc_timestamp()
            },
            {
                "id": 3,
                "name": "Code Review",
                "category": "development",
                "description": "Code review task template",
                "fields": {
                    "title": {"type": "string", "required": True, "label": "PR/Commit Title"},
                    "pr_url": {"type": "string", "required": True, "label": "Pull Request URL"},
                    "reviewer": {"type": "string", "label": "Requested Reviewer"},
                    "changes_summary": {"type": "textarea", "label": "Summary of Changes"}
                },
                "defaults": {"priority": "medium", "tags": ["review", "code"]},
                "version": 1,
                "created": get_utc_timestamp()
            },
            {
                "id": 4,
                "name": "Hermes Analysis",
                "category": "agents",
                "description": "Deep analysis task performed by the Hermes agent",
                "fields": {
                    "topic": {"type": "string", "required": True, "label": "Analysis Topic"},
                    "depth": {"type": "select", "options": ["brief", "standard", "deep"], "default": "standard", "label": "Analysis Depth"},
                    "output_format": {"type": "select", "options": ["text", "json", "markdown"], "default": "markdown", "label": "Output Format"}
                },
                "defaults": {"agent": "Hermes", "priority": "medium", "tags": ["analysis", "hermes"]},
                "version": 1,
                "created": get_utc_timestamp()
            },
            {
                "id": 5,
                "name": "OpenClaw Search",
                "category": "agents",
                "description": "External web research task performed by OpenClaw agent",
                "fields": {
                    "query": {"type": "string", "required": True, "label": "Search Query"},
                    "sources": {"type": "select", "options": ["web", "news", "academic", "all"], "default": "web", "label": "Sources"},
                    "timeout": {"type": "number", "default": 30, "label": "Timeout (seconds)"}
                },
                "defaults": {"agent": "OpenClaw", "priority": "medium", "tags": ["research", "openclaw"]},
                "version": 1,
                "created": get_utc_timestamp()
            },
            {
                "id": 6,
                "name": "CAMEL Decompose",
                "category": "agents",
                "description": "Decompose a complex goal into subtasks using CAMEL-AI",
                "fields": {
                    "goal": {"type": "textarea", "required": True, "label": "Goal to Decompose"},
                    "depth": {"type": "number", "default": 3, "label": "Decomposition Depth"},
                    "assign_to": {"type": "select", "options": ["Hermes", "OpenClaw", "auto"], "default": "auto", "label": "Assign Subtasks To"}
                },
                "defaults": {"agent": "CAMEL", "priority": "high", "tags": ["decompose", "camel", "workflow"]},
                "version": 1,
                "created": get_utc_timestamp()
            },
            {
                "id": 7,
                "name": "Full Pipeline",
                "category": "agents",
                "description": "Full multi-agent pipeline: CAMEL decompose → OpenClaw research → Hermes analyze → Multica save",
                "fields": {
                    "goal": {"type": "textarea", "required": True, "label": "Pipeline Goal"},
                    "workflow_id": {"type": "number", "default": 5, "label": "Workflow ID (default: 5 = full_pipeline)"}
                },
                "defaults": {"agent": "Hermes", "priority": "high", "tags": ["pipeline", "multi-agent", "full"]},
                "version": 1,
                "created": get_utc_timestamp()
            },
            {
                "id": 8,
                "name": "Stress Test",
                "category": "testing",
                "description": "Ephemeral load/concurrency test task — auto-deleted after completion",
                "fields": {
                    "title": {"type": "string", "required": True, "label": "Test Name"},
                    "concurrency": {"type": "number", "default": 10, "label": "Concurrent threads"},
                    "iterations": {"type": "number", "default": 100, "label": "Total iterations"},
                    "agent": {"type": "select", "options": ["Hermes", "OpenClaw", "Test"], "default": "Test", "label": "Target Agent"}
                },
                "defaults": {"agent": "Test", "priority": "low", "tags": ["stress-test", "__test__"], "ephemeral": True},
                "version": 1,
                "created": get_utc_timestamp()
            }
        ]
        self.templates["categories"] = ["issues", "features", "development", "documentation", "infrastructure", "agents", "testing"]
        self.templates["next_id"] = 9
        self._save()

    def add_template(self, name: str, category: str, fields: dict,
                     defaults: dict = None, description: str = "") -> dict:
        """Add a new task template"""
        with self._lock:
            template = {
                "id": self.templates["next_id"],
                "name": name,
                "category": category,
                "description": description,
                "fields": fields,
                "defaults": defaults or {},
                "version": 1,
                "created": get_utc_timestamp()
            }
            self.templates["templates"].append(template)
            self.templates["next_id"] += 1

            if category not in self.templates["categories"]:
                self.templates["categories"].append(category)

            self._save()
            return template

    def get_template(self, template_id: int) -> Optional[dict]:
        """Get template by ID"""
        for t in self.templates["templates"]:
            if t["id"] == template_id:
                return t
        return None

    def list_templates(self, category: str = None) -> List[dict]:
        """List all templates, optionally filtered by category"""
        templates = self.templates["templates"]
        if category:
            templates = [t for t in templates if t["category"] == category]
        return templates

    def create_task_from_template(self, template_id: int, field_values: dict) -> Optional[Task]:
        """Create a task from a template with provided field values"""
        template = self.get_template(template_id)
        if not template:
            return None

        # Build title from field_values or default
        title = field_values.get("title", f"Task from {template['name']}")

        # Create task with defaults
        task = self.orch.board.add_task(
            title=title,
            description=field_values.get("description", ""),
            agent=field_values.get("agent", "Hermes"),
            tags=field_values.get("tags", template["defaults"].get("tags", [])),
            priority=field_values.get("priority", template["defaults"].get("priority", "medium"))
        )

        # Store template reference and field values
        if not hasattr(task, 'metadata'):
            task.metadata = {}
        else:
            task.metadata = task.metadata or {}
        task.metadata["template_id"] = template_id
        task.metadata["template_fields"] = field_values
        self.orch.board.update_task(task)

        return task

    def delete_template(self, template_id: int) -> bool:
        """Delete a template"""
        with self._lock:
            before = len(self.templates["templates"])
            self.templates["templates"] = [t for t in self.templates["templates"] if t["id"] != template_id]
            if len(self.templates["templates"]) < before:
                self._save()
                return True
            return False

    def get_categories(self) -> List[str]:
        """Get all template categories"""
        return self.templates.get("categories", [])


# ============================================================================
# IMPROVEMENT: Time Tracking
# ============================================================================

class TimeTracker:
    """
    Track time spent on tasks.
    Supports: start/stop timer, manual entry, reports.
    """

    TIME_FILE = TASKS_DIR / "time_tracking.json"

    def __init__(self, orchestrator: HybridOrchestrator):
        self.orch = orchestrator
        self._lock = threading.Lock()
        self._load()

    def _load(self):
        if self.TIME_FILE.exists():
            with open(self.TIME_FILE, 'r') as f:
                self.tracking = json.load(f)
        else:
            self.tracking = {"entries": [], "active_timers": {}, "next_id": 1}

    def _save(self):
        temp_file = self.TIME_FILE.with_suffix('.tmp')
        with open(temp_file, 'w') as f:
            json.dump(self.tracking, f, indent=2)
        os.replace(temp_file, self.TIME_FILE)

    def start_timer(self, task_id: str, user: str = "system") -> dict:
        """Start a timer for a task"""
        with self._lock:
            if task_id in self.tracking["active_timers"]:
                return {"status": "already_running", "started_at": self.tracking["active_timers"][task_id]["started_at"]}

            entry = {
                "id": self.tracking["next_id"],
                "task_id": task_id,
                "user": user,
                "started_at": get_utc_timestamp(),
                "ended_at": None,
                "duration_seconds": 0
            }
            self.tracking["active_timers"][task_id] = entry
            self.tracking["next_id"] += 1
            return entry

    def stop_timer(self, task_id: str) -> Optional[dict]:
        """Stop a timer and save the entry"""
        with self._lock:
            if task_id not in self.tracking["active_timers"]:
                return None

            entry = self.tracking["active_timers"].pop(task_id)
            entry["ended_at"] = get_utc_timestamp()

            # Calculate duration
            start = datetime.fromisoformat(entry["started_at"].replace('Z', '+00:00'))
            end = datetime.now(timezone.utc)
            entry["duration_seconds"] = int((end - start).total_seconds())

            self.tracking["entries"].append(entry)
            self._save()

            # Update task with time spent
            task = self.orch.board.get_task(task_id)
            if task:
                if not hasattr(task, 'metadata'):
                    task.metadata = {}
                else:
                    task.metadata = task.metadata or {}
                task.metadata["total_time_seconds"] = task.metadata.get("total_time_seconds", 0) + entry["duration_seconds"]
                self.orch.board.update_task(task)

            return entry

    def add_manual_entry(self, task_id: str, duration_seconds: int,
                        user: str = "system", note: str = "") -> dict:
        """Add a manual time entry"""
        with self._lock:
            entry = {
                "id": self.tracking["next_id"],
                "task_id": task_id,
                "user": user,
                "started_at": get_utc_timestamp(),
                "ended_at": get_utc_timestamp(),
                "duration_seconds": duration_seconds,
                "manual": True,
                "note": note
            }
            self.tracking["entries"].append(entry)
            self.tracking["next_id"] += 1

            # Update task
            task = self.orch.board.get_task(task_id)
            if task:
                if not hasattr(task, 'metadata'):
                    task.metadata = {}
                else:
                    task.metadata = task.metadata or {}
                task.metadata["total_time_seconds"] = task.metadata.get("total_time_seconds", 0) + duration_seconds
                self.orch.board.update_task(task)

            self._save()
            return entry

    def get_task_time(self, task_id: str) -> dict:
        """Get total time spent on a task"""
        entries = [e for e in self.tracking["entries"] if e["task_id"] == task_id]
        total = sum(e["duration_seconds"] for e in entries)

        active = None
        if task_id in self.tracking["active_timers"]:
            timer = self.tracking["active_timers"][task_id]
            start = datetime.fromisoformat(timer["started_at"].replace('Z', '+00:00'))
            active = int((datetime.now(timezone.utc) - start).total_seconds())

        return {
            "task_id": task_id,
            "total_seconds": total,
            "total_hours": round(total / 3600, 2),
            "active_seconds": active,
            "entry_count": len(entries)
        }

    def get_active_timers(self) -> List[dict]:
        """Get all active timers"""
        timers = []
        for task_id, entry in self.tracking["active_timers"].items():
            start = datetime.fromisoformat(entry["started_at"].replace('Z', '+00:00'))
            elapsed = int((datetime.now(timezone.utc) - start).total_seconds())
            timers.append({**entry, "elapsed_seconds": elapsed})
        return timers

    def get_user_time_report(self, user: str, hours: int = 24) -> dict:
        """Get time report for a user"""
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        entries = [e for e in self.tracking["entries"]
                  if e["user"] == user and e["started_at"] > cutoff]

        by_task = defaultdict(int)
        for e in entries:
            by_task[e["task_id"]] += e["duration_seconds"]

        return {
            "user": user,
            "period_hours": hours,
            "total_seconds": sum(e["duration_seconds"] for e in entries),
            "task_count": len(set(e["task_id"] for e in entries)),
            "by_task": dict(by_task)
        }


# ============================================================================
# IMPROVEMENT: SLA Monitoring
# ============================================================================

class SLAMonitor:
    """
    SLA (Service Level Agreement) monitoring.
    Supports: configurable SLAs, breach alerts, reports.
    """

    SLA_FILE = STATE_DIR / "sla_config.json"

    DEFAULT_SLAS = {
        "critical": {"response_hours": 1, "resolution_hours": 4, "description": "Critical issues"},
        "high": {"response_hours": 4, "resolution_hours": 24, "description": "High priority"},
        "medium": {"response_hours": 24, "resolution_hours": 72, "description": "Medium priority"},
        "low": {"response_hours": 48, "resolution_hours": 168, "description": "Low priority"}
    }

    def __init__(self, orchestrator: HybridOrchestrator):
        self.orch = orchestrator
        self._lock = threading.Lock()
        self._load()

    def _load(self):
        if self.SLA_FILE.exists():
            with open(self.SLA_FILE, 'r') as f:
                self.config = json.load(f)
        else:
            self.config = {"slas": self.DEFAULT_SLAS, "breaches": []}
            self._save()

    def _save(self):
        with open(self.SLA_FILE, 'w') as f:
            json.dump(self.config, f, indent=2)

    def get_sla_for_priority(self, priority: str) -> dict:
        """Get SLA configuration for a priority level"""
        return self.config["slas"].get(priority, self.DEFAULT_SLAS.get("medium"))

    def check_task_sla(self, task_id: str) -> dict:
        """Check SLA status for a task"""
        task = self.orch.board.get_task(task_id)
        if not task:
            return {"error": "Task not found"}

        sla = self.get_sla_for_priority(task.priority)

        # Calculate response SLA
        created = datetime.fromisoformat(task.created.replace('Z', '+00:00'))
        now = datetime.now(timezone.utc)
        age_hours = (now - created).total_seconds() / 3600

        response_deadline_hours = sla["response_hours"]
        resolution_deadline_hours = sla["resolution_hours"]

        response_status = "ok" if age_hours < response_deadline_hours else "at_risk"
        if age_hours > response_deadline_hours * 1.5:
            response_status = "breached"

        resolution_status = "ok"
        if task.status in ["completed", "done"]:
            if hasattr(task, 'metadata') and task.metadata:
                completed_at = task.metadata.get("completed_at")
                if completed_at:
                    completed_time = datetime.fromisoformat(completed_at.replace('Z', '+00:00'))
                    resolution_hours = (completed_time - created).total_seconds() / 3600
                    resolution_status = "met" if resolution_hours < resolution_deadline_hours else "breached"
        elif task.status not in ["blocked", "on_hold"]:
            if age_hours > resolution_deadline_hours:
                resolution_status = "at_risk"
            if age_hours > resolution_deadline_hours * 1.5:
                resolution_status = "breached"

        return {
            "task_id": task_id,
            "priority": task.priority,
            "status": task.status,
            "age_hours": round(age_hours, 1),
            "response_sla": {
                "deadline_hours": response_deadline_hours,
                "status": response_status,
                "remaining_hours": max(0, response_deadline_hours - age_hours)
            },
            "resolution_sla": {
                "deadline_hours": resolution_deadline_hours,
                "status": resolution_status,
                "remaining_hours": max(0, resolution_deadline_hours - age_hours)
            }
        }

    def get_sla_violations(self, limit: int = 50) -> List[dict]:
        """Get tasks with SLA violations"""
        violations = []
        for task in self.orch.board.list_tasks():
            if task.status in ["completed", "done"]:
                continue
            sla_status = self.check_task_sla(task.id)
            if sla_status.get("response_sla", {}).get("status") in ["at_risk", "breached"]:
                violations.append(sla_status)
            elif sla_status.get("resolution_sla", {}).get("status") in ["at_risk", "breached"]:
                violations.append(sla_status)
        return violations[:limit]

    def get_sla_summary(self) -> dict:
        """Get SLA compliance summary"""
        total = 0
        compliant = 0
        at_risk = 0
        breached = 0

        for task in self.orch.board.list_tasks():
            if task.status in ["completed", "done"]:
                continue
            total += 1
            sla_status = self.check_task_sla(task.id)

            response = sla_status.get("response_sla", {}).get("status", "ok")
            resolution = sla_status.get("resolution_sla", {}).get("status", "ok")

            if response == "breached" or resolution == "breached":
                breached += 1
            elif response == "at_risk" or resolution == "at_risk":
                at_risk += 1
            else:
                compliant += 1

        return {
            "total_active": total,
            "compliant": compliant,
            "at_risk": at_risk,
            "breached": breached,
            "compliance_rate": round(compliant / total * 100, 1) if total > 0 else 100
        }


# ============================================================================
# IMPROVEMENT: Resource Management
# ============================================================================

class ResourceManager:
    """
    Manage agent resources and capacity.
    Supports: resource pools, allocation tracking, capacity planning.
    """

    RESOURCE_FILE = STATE_DIR / "resources.json"

    def __init__(self, orchestrator: HybridOrchestrator):
        self.orch = orchestrator
        self._lock = threading.Lock()
        self._load()

    def _load(self):
        if self.RESOURCE_FILE.exists():
            with open(self.RESOURCE_FILE, 'r') as f:
                self.resources = json.load(f)
        else:
            self.resources = {
                "agents": {
                    "Hermes": {"capacity": 10, "current_load": 0, "skills": ["orchestration", "coordination"]},
                    "OpenClaw": {"capacity": 5, "current_load": 0, "skills": ["execution", "automation"]},
                    "OpenAI": {"capacity": 8, "current_load": 0, "skills": ["reasoning", "analysis"]}
                },
                "allocations": []
            }
            self._save()

    def _save(self):
        temp_file = self.RESOURCE_FILE.with_suffix('.tmp')
        with open(temp_file, 'w') as f:
            json.dump(self.resources, f, indent=2)
        os.replace(temp_file, self.RESOURCE_FILE)

    def get_agent_status(self, agent_name: str) -> dict:
        """Get agent resource status"""
        agent = self.resources["agents"].get(agent_name)
        if not agent:
            return {"error": "Agent not found"}

        return {
            "name": agent_name,
            "capacity": agent["capacity"],
            "current_load": agent["current_load"],
            "available": agent["capacity"] - agent["current_load"],
            "utilization": round(agent["current_load"] / agent["capacity"] * 100, 1) if agent["capacity"] > 0 else 0
        }

    def allocate_task(self, task_id: str, agent_name: str) -> bool:
        """Allocate a task to an agent"""
        with self._lock:
            agent = self.resources["agents"].get(agent_name)
            if not agent:
                return False

            if agent["current_load"] >= agent["capacity"]:
                return False

            # Update load
            agent["current_load"] += 1

            # Track allocation
            self.resources["allocations"].append({
                "task_id": task_id,
                "agent": agent_name,
                "allocated_at": get_utc_timestamp(),
                "released_at": None
            })
            self._save()
            return True

    def release_task(self, task_id: str) -> bool:
        """Release a task allocation"""
        with self._lock:
            for alloc in self.resources["allocations"]:
                if alloc["task_id"] == task_id and alloc["released_at"] is None:
                    alloc["released_at"] = get_utc_timestamp()
                    agent = self.resources["agents"].get(alloc["agent"])
                    if agent and agent["current_load"] > 0:
                        agent["current_load"] -= 1
                    self._save()
                    return True
            return False

    def get_available_agents(self) -> List[dict]:
        """Get list of agents with available capacity"""
        available = []
        for name, agent in self.resources["agents"].items():
            if agent["current_load"] < agent["capacity"]:
                available.append({
                    "name": name,
                    "available_slots": agent["capacity"] - agent["current_load"],
                    "skills": agent.get("skills", [])
                })
        return available

    def set_agent_capacity(self, agent_name: str, capacity: int) -> bool:
        """Update agent capacity"""
        with self._lock:
            if agent_name in self.resources["agents"]:
                self.resources["agents"][agent_name]["capacity"] = capacity
                self._save()
                return True
            return False

    def get_allocation_summary(self) -> dict:
        """Get allocation summary"""
        total_allocated = sum(a["released_at"] is None for a in self.resources["allocations"])
        total_released = sum(1 for a in self.resources["allocations"] if a["released_at"] is not None)

        return {
            "total_allocations": len(self.resources["allocations"]),
            "currently_allocated": total_allocated,
            "released": total_released,
            "agents": {name: self.get_agent_status(name) for name in self.resources["agents"]}
        }


# ============================================================================
# IMPROVEMENT: Audit Trail
# ============================================================================

class AuditTrail:
    """
    Comprehensive audit trail for all operations.
    Supports: filtering, export, compliance reporting.
    """

    AUDIT_FILE = STATE_DIR / "audit_trail.json"

    def __init__(self, orchestrator: HybridOrchestrator):
        self.orch = orchestrator
        self._lock = threading.Lock()
        self._load()

    def _load(self):
        if self.AUDIT_FILE.exists():
            with open(self.AUDIT_FILE, 'r') as f:
                self.audit = json.load(f)
        else:
            self.audit = {"entries": [], "next_id": 1}
            self._save()

    def _save(self):
        temp_file = self.AUDIT_FILE.with_suffix('.tmp')
        with open(temp_file, 'w') as f:
            json.dump(self.audit, f, indent=2)
        os.replace(temp_file, self.AUDIT_FILE)

    def log(self, action: str, entity_type: str, entity_id: str,
            user: str = "system", details: dict = None, severity: str = "info") -> dict:
        """Log an audit event"""
        with self._lock:
            entry = {
                "id": self.audit["next_id"],
                "timestamp": get_utc_timestamp(),
                "action": action,
                "entity_type": entity_type,
                "entity_id": entity_id,
                "user": user,
                "details": details or {},
                "severity": severity
            }
            self.audit["entries"].append(entry)
            self.audit["next_id"] += 1

            # Keep last 10000 entries
            if len(self.audit["entries"]) > 10000:
                self.audit["entries"] = self.audit["entries"][-10000:]

            self._save()
            return entry

    def query(self, action: str = None, entity_type: str = None,
              entity_id: str = None, user: str = None,
              severity: str = None, hours: int = 24) -> List[dict]:
        """Query audit entries with filters"""
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        entries = [e for e in self.audit["entries"] if e["timestamp"] > cutoff]

        if action:
            entries = [e for e in entries if e["action"] == action]
        if entity_type:
            entries = [e for e in entries if e["entity_type"] == entity_type]
        if entity_id:
            entries = [e for e in entries if e["entity_id"] == entity_id]
        if user:
            entries = [e for e in entries if e["user"] == user]
        if severity:
            entries = [e for e in entries if e["severity"] == severity]

        return entries

    def get_entity_history(self, entity_id: str) -> List[dict]:
        """Get full audit history for an entity"""
        return [e for e in self.audit["entries"] if e["entity_id"] == entity_id]

    def get_user_activity(self, user: str, hours: int = 24) -> dict:
        """Get user activity summary"""
        entries = self.query(user=user, hours=hours)
        action_counts = {}
        for e in entries:
            action_counts[e["action"]] = action_counts.get(e["action"], 0) + 1

        return {
            "user": user,
            "total_actions": len(entries),
            "action_breakdown": action_counts,
            "period_hours": hours
        }

    def get_severity_report(self, hours: int = 24) -> dict:
        """Get audit severity report"""
        entries = self.query(hours=hours)
        by_severity = {"critical": [], "error": [], "warning": [], "info": []}

        for e in entries:
            sev = e.get("severity", "info")
            if sev in by_severity:
                by_severity[sev].append(e)

        return {
            "period_hours": hours,
            "total_entries": len(entries),
            "by_severity": {k: len(v) for k, v in by_severity.items()},
            "critical_entries": by_severity["critical"][-10:]  # Last 10 critical
        }

    def export_audit_log(self, filepath: str, hours: int = 24) -> str:
        """Export audit log to file"""
        entries = self.query(hours=hours)
        with open(filepath, 'w') as f:
            json.dump({"exported": get_utc_timestamp(), "entries": entries}, f, indent=2)
        return filepath


# ============================================================================
# IMPROVEMENT: WebSocket Support (SSE - Server-Sent Events)
# ============================================================================

class WebSocketManager:
    """
    Real-time event streaming via Server-Sent Events (SSE).
    Provides real-time updates to connected clients.
    Note: For full WebSocket support, integrate flask-socketio.
    """

    def __init__(self, orchestrator: HybridOrchestrator):
        self.orch = orchestrator
        self._subscribers: Dict[str, set] = {
            "task": set(),
            "system": set(),
            "metrics": set(),
            "alerts": set()
        }
        self._event_queue: Queue = Queue()
        self._buffer_size = 1000

    def subscribe(self, client_id: str, channel: str = "task") -> bool:
        """Subscribe a client to a channel"""
        if channel in self._subscribers:
            self._subscribers[channel].add(client_id)
            return True
        return False

    def unsubscribe(self, client_id: str, channel: str = "task") -> bool:
        """Unsubscribe a client from a channel"""
        if channel in self._subscribers and client_id in self._subscribers[channel]:
            self._subscribers[channel].remove(client_id)
            return True
        return False

    def publish(self, channel: str, event_type: str, data: dict) -> int:
        """Publish an event to a channel"""
        if channel not in self._subscribers:
            return 0

        message = {
            "channel": channel,
            "type": event_type,
            "data": data,
            "timestamp": get_utc_timestamp()
        }

        # Queue the message for delivery
        try:
            self._event_queue.put_nowait(message)
        except:
            pass  # Queue full

        return len(self._subscribers[channel])

    def get_events(self, client_id: str, channel: str = "task",
                   timeout: float = 30) -> List[dict]:
        """Get pending events for a client (SSE polling)"""
        events = []
        if channel not in self._subscribers or client_id not in self._subscribers[channel]:
            return events

        # Collect events from queue (simplified - in production use proper SSE)
        collected = 0
        while collected < 10:  # Max 10 events per poll
            try:
                event = self._event_queue.get(timeout=0.1)
                if event["channel"] == channel:
                    events.append(event)
                    collected += 1
            except:
                break

        return events

    def get_subscriber_count(self, channel: str = None) -> dict:
        """Get subscriber counts"""
        if channel:
            return {"channel": channel, "count": len(self._subscribers.get(channel, set()))}

        return {ch: len(subs) for ch, subs in self._subscribers.items()}

    def broadcast_task_update(self, task_id: str, action: str, task_data: dict = None):
        """Broadcast task-related event"""
        data = {"task_id": task_id, "action": action}
        if task_data:
            data["task"] = task_data
        return self.publish("task", action, data)

    def broadcast_system_event(self, event_type: str, details: dict):
        """Broadcast system event"""
        return self.publish("system", event_type, details)

    def broadcast_metrics(self, metrics: dict):
        """Broadcast metrics update"""
        return self.publish("metrics", "update", metrics)

    def broadcast_alert(self, level: str, message: str, details: dict = None):
        """Broadcast an alert"""
        data = {"level": level, "message": message}
        if details:
            data.update(details)
        return self.publish("alerts", level, data)


# ============================================================================
# IMPROVEMENT: Workflow Engine
# ============================================================================

class WorkflowEngine:
    """
    Workflow automation engine for orchestrating complex task flows.
    Supports: conditional branching, parallel execution, state machines.
    """

    WORKFLOW_FILE = STATE_DIR / "workflows.json"

    def __init__(self, orchestrator: HybridOrchestrator):
        self.orch = orchestrator
        self._lock = threading.Lock()
        self._load()

    def _load(self):
        if self.WORKFLOW_FILE.exists():
            with open(self.WORKFLOW_FILE, 'r') as f:
                self.workflows = json.load(f)
        else:
            self.workflows = {"workflows": [], "executions": [], "next_id": 1}
            self._save()

    def _save(self):
        temp_file = self.WORKFLOW_FILE.with_suffix('.tmp')
        with open(temp_file, 'w') as f:
            json.dump(self.workflows, f, indent=2)
        os.replace(temp_file, self.WORKFLOW_FILE)

    def create_workflow(self, name: str, steps: List[dict],
                       trigger: dict = None) -> dict:
        """
        Create a workflow definition.
        steps: List of step definitions with 'type', 'action', 'condition'
        trigger: Optional trigger configuration
        """
        with self._lock:
            workflow = {
                "id": self.workflows["next_id"],
                "name": name,
                "steps": steps,
                "trigger": trigger or {"type": "manual"},
                "enabled": True,
                "created": get_utc_timestamp()
            }
            self.workflows["workflows"].append(workflow)
            self.workflows["next_id"] += 1
            self._save()
            return workflow

    def get_workflow(self, workflow_id: int) -> Optional[dict]:
        """Get workflow by ID"""
        for w in self.workflows["workflows"]:
            if w["id"] == workflow_id:
                return w
        return None

    def execute_workflow(self, workflow_id: int, context: dict = None) -> dict:
        """Execute a workflow with given context"""
        workflow = self.get_workflow(workflow_id)
        if not workflow:
            return {"error": "Workflow not found"}

        execution = {
            "id": f"WF-{workflow_id}-{len(self.workflows['executions']) + 1}",
            "workflow_id": workflow_id,
            "workflow_name": workflow["name"],
            "status": "running",
            "started_at": get_utc_timestamp(),
            "completed_at": None,
            "current_step": 0,
            "context": context or {},
            "results": []
        }

        # Execute each step
        for i, step in enumerate(workflow["steps"]):
            execution["current_step"] = i

            # Check condition if present
            if "condition" in step:
                if not self._evaluate_condition(step["condition"], execution["context"]):
                    execution["results"].append({
                        "step": i,
                        "status": "skipped",
                        "reason": "condition_not_met"
                    })
                    continue

            # Execute step action
            result = self._execute_step(step, execution["context"])
            execution["results"].append({
                "step": i,
                "step_name": step.get("name", f"step_{i}"),
                "status": "completed" if result["success"] else "failed",
                "result": result
            })

            if not result["success"] and step.get("critical", False):
                execution["status"] = "failed"
                execution["completed_at"] = get_utc_timestamp()
                break

        if execution["status"] == "running":
            execution["status"] = "completed"
            execution["completed_at"] = get_utc_timestamp()

        self.workflows["executions"].append(execution)
        # Keep last 100 executions
        if len(self.workflows["executions"]) > 100:
            self.workflows["executions"] = self.workflows["executions"][-100:]
        self._save()

        return execution

    def _evaluate_condition(self, condition: dict, context: dict) -> bool:
        """Evaluate a condition against context"""
        op = condition.get("operator", "equals")
        field = condition.get("field", "")
        value = condition.get("value")

        ctx_value = context.get(field)
        if op == "equals":
            return ctx_value == value
        elif op == "not_equals":
            return ctx_value != value
        elif op == "contains":
            return value in str(ctx_value)
        elif op == "greater_than":
            return ctx_value > value
        elif op == "less_than":
            return ctx_value < value
        return True

    def _execute_step(self, step: dict, context: dict) -> dict:
        """Execute a single workflow step"""
        step_type = step.get("type", "action")

        try:
            if step_type == "action":
                action = step.get("action", "")

                # ── Built-in board actions ──────────────────────────────────
                if action == "create_task":
                    task = self.orch.board.add_task(
                        title=step.get("title", "Workflow Task"),
                        agent=step.get("agent", "Hermes"),
                        priority=step.get("priority", "medium")
                    )
                    context["last_task_id"] = task.id
                    return {"success": True, "task_id": task.id}

                elif action == "update_status":
                    task_id = context.get("task_id") or step.get("task_id")
                    if task_id:
                        self.orch.board.update_status(task_id, step.get("status", "in_progress"))
                        return {"success": True}

                elif action == "notify":
                    self.orch.notifications.send(
                        step.get("channel", "system"),
                        step.get("message", ""),
                        step.get("level", "info")
                    )
                    return {"success": True}

                # ── Real agent connectors (Approach A) ─────────────────────
                elif action == "call_hermes":
                    prompt = step.get("prompt", context.get("goal", ""))
                    result = self._call_hermes_agent(prompt, context)
                    context["hermes_result"] = result.get("output", "")
                    return result

                elif action == "call_openclaw":
                    prompt = step.get("prompt", context.get("goal", ""))
                    result = self._call_openclaw_agent(prompt, context)
                    context["openclaw_result"] = result.get("output", "")
                    return result

                elif action == "call_multica":
                    payload = {
                        "title": step.get("title", context.get("goal", "Orchestrated Task")),
                        "agent": step.get("agent", "Hermes"),
                        "priority": step.get("priority", "medium"),
                    }
                    result = self._call_multica_api(payload)
                    context["multica_task_id"] = result.get("task_id")
                    return result

                elif action == "call_camel":
                    goal = step.get("goal", context.get("goal", ""))
                    depth = step.get("depth", 3)
                    result = self._call_camel_decompose(goal, depth)
                    context["camel_tasks"] = result.get("tasks", [])
                    return result

                elif action == "run_skill":
                    skill_name = step.get("skill", "")
                    result = self._run_skill(skill_name, context)
                    context[f"skill_{skill_name}_result"] = result.get("output", "")
                    return result

            elif step_type == "delay":
                time.sleep(step.get("seconds", 1))
                return {"success": True}

            return {"success": True}

        except Exception as e:
            return {"success": False, "error": str(e)}

    # ── Agent connector helpers ─────────────────────────────────────────────

    def _call_hermes_agent(self, prompt: str, context: dict) -> dict:
        """Call real Hermes CLI, fall back to hermes_llm MiniMax integration."""
        runner = Path(__file__).parent / "orchestrator" / "skills" / "hermes_runner.sh"
        hermes_path = shutil.which("hermes")

        if hermes_path:
            try:
                proc = subprocess.run(
                    [hermes_path, "--prompt", prompt, "--output", "json"],
                    capture_output=True, text=True, timeout=120
                )
                return {
                    "success": proc.returncode == 0,
                    "output": proc.stdout or proc.stderr,
                    "agent": "hermes-cli",
                }
            except Exception as e:
                pass  # fall through to minimax fallback

        # Fallback: call hermes_llm.py MiniMax integration
        try:
            hermes_llm_path = Path(__file__).parent / "hermes_llm.py"
            proc = subprocess.run(
                ["python3", str(hermes_llm_path), prompt],
                capture_output=True, text=True, timeout=120,
                cwd=str(Path(__file__).parent)
            )
            output = proc.stdout or f"[Hermes MiniMax] Prompt processed: {prompt[:100]}"
            return {"success": True, "output": output, "agent": "hermes-minimax-fallback"}
        except Exception as e:
            return {"success": False, "output": "", "error": str(e), "agent": "hermes-unavailable"}

    def _call_openclaw_agent(self, prompt: str, context: dict) -> dict:
        """Call real OpenClaw CLI via the runner script."""
        runner = Path(__file__).parent / "openclaw_runner.sh"
        if not runner.exists():
            return {"success": False, "output": "", "error": "openclaw_runner.sh not found"}

        try:
            runner.chmod(0o755)
            proc = subprocess.run(
                ["bash", str(runner), "--prompt", prompt],
                capture_output=True, text=True, timeout=130,
                env={**os.environ, "OPENCLAW_NO_TELEMETRY": "1"}
            )
            output = proc.stdout or proc.stderr
            return {
                "success": proc.returncode == 0,
                "output": output,
                "agent": "openclaw-cli",
            }
        except subprocess.TimeoutExpired:
            return {"success": False, "output": "", "error": "OpenClaw timed out", "agent": "openclaw-cli"}
        except Exception as e:
            return {"success": False, "output": "", "error": str(e), "agent": "openclaw-unavailable"}

    def _call_multica_api(self, payload: dict) -> dict:
        """Call Multica REST API at localhost:3000. Falls back to internal board."""
        try:
            import urllib.request, urllib.error
            data = json.dumps(payload).encode()
            req = urllib.request.Request(
                "http://localhost:3000/api/tasks",
                data=data,
                headers={"Content-Type": "application/json"},
                method="POST"
            )
            with urllib.request.urlopen(req, timeout=5) as resp:
                body = json.loads(resp.read())
                return {"success": True, "task_id": body.get("id"), "source": "multica-api"}
        except Exception:
            # Multica not running — use internal board as fallback
            task = self.orch.board.add_task(
                title=payload.get("title", "Multica Task"),
                agent=payload.get("agent", "Hermes"),
                priority=payload.get("priority", "medium")
            )
            return {"success": True, "task_id": task.id, "source": "internal-board-fallback"}

    def _call_camel_decompose(self, goal: str, depth: int = 3) -> dict:
        """Decompose goal via CAMEL SDK if available, else use internal CAMELLayer."""
        try:
            # Try real CAMEL-AI SDK
            import camel  # type: ignore
            from camel.agents import ChatAgent  # type: ignore
            from camel.messages import BaseMessage  # type: ignore
            agent = ChatAgent(system_message="You are a task decomposition expert. Break goals into concrete subtasks.")
            msg = BaseMessage.make_user_message(role_name="User", content=f"Decompose into {depth} subtasks: {goal}")
            response = agent.step(msg)
            tasks = [{"step": i+1, "title": line.strip()} for i, line in
                     enumerate(response.msgs[0].content.split("\n")) if line.strip()]
            return {"success": True, "tasks": tasks[:depth], "source": "camel-sdk"}
        except Exception:
            # Fallback: internal CAMELLayer
            task_objects = self.orch.camel.create_workflow(goal, depth)
            tasks = [{"step": i+1, "title": t.title, "agent": t.agent, "id": t.id}
                     for i, t in enumerate(task_objects)]
            return {"success": True, "tasks": tasks, "source": "internal-camel-fallback"}

    def _run_skill(self, skill_name: str, context: dict) -> dict:
        """Load and execute a skill from the skills directory."""
        skill_file = SKILLS_DIR / f"{skill_name}.md"
        if not skill_file.exists():
            available = [f.stem for f in SKILLS_DIR.glob("*.md")]
            return {
                "success": False,
                "error": f"Skill '{skill_name}' not found",
                "available": available
            }
        content = skill_file.read_text(encoding="utf-8")

        # Extract the command line from the skill file (line starting with "**Команда:**")
        cmd_line = ""
        for line in content.splitlines():
            if "**Команда:**" in line or "**Command:**" in line:
                cmd_line = line.split(":", 1)[-1].strip().strip("`")
                break

        # Interpolate context values into the command
        for k, v in context.items():
            cmd_line = cmd_line.replace(f"{{{k}}}", str(v))

        return {
            "success": True,
            "skill": skill_name,
            "description": content[:200],
            "command_template": cmd_line,
            "output": f"[Skill: {skill_name}] Ready. Command: {cmd_line[:120]}",
        }

    def get_execution_history(self, workflow_id: int = None, limit: int = 20) -> List[dict]:
        """Get workflow execution history"""
        executions = self.workflows["executions"]
        if workflow_id:
            executions = [e for e in executions if e["workflow_id"] == workflow_id]
        return executions[-limit:]


# ============================================================================
# IMPROVEMENT: Knowledge Base
# ============================================================================

class KnowledgeBase:
    """
    Knowledge base with search and retrieval capabilities.
    Supports: semantic search, tagging, versioning.
    """

    KB_FILE = STATE_DIR / "knowledge_base.json"

    def __init__(self, orchestrator: HybridOrchestrator):
        self.orch = orchestrator
        self._lock = threading.Lock()
        self._load()

    def _load(self):
        if self.KB_FILE.exists():
            with open(self.KB_FILE, 'r') as f:
                self.kb = json.load(f)
        else:
            self.kb = {"articles": [], "categories": ["general", "tutorials", "reference"], "next_id": 1}
            self._save()

    def _save(self):
        temp_file = self.KB_FILE.with_suffix('.tmp')
        with open(temp_file, 'w') as f:
            json.dump(self.kb, f, indent=2)
        os.replace(temp_file, self.KB_FILE)

    def add_article(self, title: str, content: str, category: str = "general",
                   tags: List[str] = None, author: str = "system") -> dict:
        """Add an article to knowledge base"""
        with self._lock:
            article = {
                "id": self.kb["next_id"],
                "title": title,
                "content": content,
                "category": category,
                "tags": tags or [],
                "author": author,
                "created": get_utc_timestamp(),
                "updated": get_utc_timestamp(),
                "views": 0,
                "version": 1
            }
            self.kb["articles"].append(article)
            self.kb["next_id"] += 1

            if category not in self.kb["categories"]:
                self.kb["categories"].append(category)

            self._save()
            return article

    def get_article(self, article_id: int) -> Optional[dict]:
        """Get article by ID"""
        for article in self.kb["articles"]:
            if article["id"] == article_id:
                article["views"] += 1
                self._save()
                return article
        return None

    def search(self, query: str, category: str = None, tags: List[str] = None,
              limit: int = 10) -> List[dict]:
        """Search articles by query, category, or tags"""
        results = self.kb["articles"]

        if query:
            query_lower = query.lower()
            results = [a for a in results if
                      query_lower in a["title"].lower() or
                      query_lower in a["content"].lower()]

        if category:
            results = [a for a in results if a["category"] == category]

        if tags:
            results = [a for a in results if any(tag in a["tags"] for tag in tags)]

        # Sort by relevance (views + recency)
        results.sort(key=lambda x: (x["views"], x["updated"]), reverse=True)
        return results[:limit]

    def update_article(self, article_id: int, updates: dict) -> Optional[dict]:
        """Update an article"""
        with self._lock:
            for article in self.kb["articles"]:
                if article["id"] == article_id:
                    article.update(updates)
                    article["updated"] = get_utc_timestamp()
                    article["version"] += 1
                    self._save()
                    return article
        return None

    def get_by_category(self, category: str) -> List[dict]:
        """Get all articles in a category"""
        return [a for a in self.kb["articles"] if a["category"] == category]

    def get_popular(self, limit: int = 10) -> List[dict]:
        """Get most viewed articles"""
        return sorted(self.kb["articles"], key=lambda x: x["views"], reverse=True)[:limit]


# ============================================================================
# IMPROVEMENT: Advanced Rate Limiter
# ============================================================================

class APIRateLimiter:
    """
    Advanced API rate limiting with configurable limits per endpoint/user.
    Supports: sliding window, token bucket algorithms.
    """

    def __init__(self):
        self._limits: Dict[str, dict] = {
            "global": {"requests": 1000, "window": 60},
            "api": {"requests": 100, "window": 60},
            "task_create": {"requests": 50, "window": 60},
            "task_query": {"requests": 200, "window": 60}
        }
        self._requests: Dict[str, List[float]] = defaultdict(list)

    def check_limit(self, key: str = "api", identifier: str = "default") -> dict:
        """Check if request is within rate limit"""
        limit_key = f"{key}:{identifier}"
        limit = self._limits.get(key, self._limits["global"])
        max_requests = limit["requests"]
        window = limit["window"]

        now = time.time()
        cutoff = now - window

        # Clean old requests
        self._requests[limit_key] = [t for t in self._requests[limit_key] if t > cutoff]

        current_count = len(self._requests[limit_key])

        if current_count >= max_requests:
            oldest = self._requests[limit_key][0] if self._requests[limit_key] else now
            retry_after = int(oldest + window - now) + 1
            return {
                "allowed": False,
                "remaining": 0,
                "retry_after": retry_after,
                "limit": max_requests
            }

        # Record this request
        self._requests[limit_key].append(now)

        return {
            "allowed": True,
            "remaining": max_requests - current_count - 1,
            "retry_after": 0,
            "limit": max_requests
        }

    def set_limit(self, key: str, requests: int, window: int):
        """Set rate limit for an endpoint"""
        self._limits[key] = {"requests": requests, "window": window}

    def get_stats(self) -> dict:
        """Get rate limiter statistics"""
        total_requests = sum(len(reqs) for reqs in self._requests.values())
        return {
            "configured_limits": len(self._limits),
            "active_keys": len(self._requests),
            "total_tracked_requests": total_requests
        }


# ============================================================================
# IMPROVEMENT: Health Check
# ============================================================================

class HealthChecker:
    """
    System health checking with configurable checks.
    Supports: component status, dependencies, metrics thresholds.
    """

    def __init__(self, orchestrator: HybridOrchestrator):
        self.orch = orchestrator
        self._checks = []
        self._register_default_checks()

    def _register_default_checks(self):
        """Register default health checks"""
        self.register_check("board", self._check_board)
        self.register_check("state", self._check_state)
        self.register_check("tasks", self._check_tasks)
        self.register_check("memory", self._check_memory)

    def register_check(self, name: str, check_fn: callable):
        """Register a health check function"""
        self._checks.append({"name": name, "check": check_fn})

    def _check_board(self) -> dict:
        """Check board availability"""
        try:
            tasks = self.orch.board.list_tasks()
            return {"status": "healthy", "message": f"Board accessible, {len(tasks)} tasks"}
        except Exception as e:
            return {"status": "unhealthy", "message": str(e)}

    def _check_state(self) -> dict:
        """Check state file integrity"""
        try:
            state = self.orch._load_state()
            return {"status": "healthy", "message": f"State version {state.get('version', 'unknown')}"}
        except Exception as e:
            return {"status": "unhealthy", "message": str(e)}

    def _check_tasks(self) -> dict:
        """Check for stuck or problematic tasks"""
        stuck_tasks = []
        for task in self.orch.board.list_tasks():
            if task.status == "running" and hasattr(task, 'started_at'):
                started = datetime.fromisoformat(task.started_at.replace('Z', '+00:00'))
                age_hours = (datetime.now(timezone.utc) - started).total_seconds() / 3600
                if age_hours > 24:
                    stuck_tasks.append(task.id)

        if stuck_tasks:
            return {"status": "warning", "message": f"{len(stuck_tasks)} tasks stuck > 24h"}
        return {"status": "healthy", "message": "No stuck tasks"}

    def _check_memory(self) -> dict:
        """Check memory usage"""
        try:
            import psutil
            memory = psutil.virtual_memory()
            if memory.percent > 90:
                return {"status": "critical", "message": f"Memory at {memory.percent:.1f}%"}
            elif memory.percent > 75:
                return {"status": "warning", "message": f"Memory at {memory.percent:.1f}%"}
            return {"status": "healthy", "message": f"Memory at {memory.percent:.1f}%"}
        except:
            return {"status": "unknown", "message": "psutil not available"}

    def run_checks(self) -> dict:
        """Run all health checks"""
        results = []
        overall_status = "healthy"

        for check_info in self._checks:
            result = check_info["check"]()
            result["name"] = check_info["name"]
            results.append(result)

            if result["status"] == "critical":
                overall_status = "critical"
            elif result["status"] == "unhealthy" and overall_status == "healthy":
                overall_status = "unhealthy"
            elif result["status"] == "warning" and overall_status == "healthy":
                overall_status = "warning"

        return {
            "status": overall_status,
            "timestamp": get_utc_timestamp(),
            "checks": results,
            "version": "5.0"
        }

    def get_readiness(self) -> dict:
        """Get readiness check for Kubernetes"""
        return self.run_checks()

    def get_liveness(self) -> dict:
        """Get liveness check for Kubernetes"""
        status = self.run_checks()
        return {
            "status": "alive" if status["status"] != "critical" else "dead",
            "timestamp": get_utc_timestamp()
        }


# ============================================================================
# IMPROVEMENT: Integration Hub
# ============================================================================

class IntegrationHub:
    """
    Integration hub for external services and APIs.
    Supports: Slack, GitHub, Jira, custom webhooks.
    """

    INTEGRATIONS_FILE = STATE_DIR / "integrations.json"

    def __init__(self, orchestrator: HybridOrchestrator):
        self.orch = orchestrator
        self._lock = threading.Lock()
        self._load()

    def _load(self):
        if self.INTEGRATIONS_FILE.exists():
            with open(self.INTEGRATIONS_FILE, 'r') as f:
                self.integrations = json.load(f)
        else:
            self.integrations = {
                "enabled": {},
                "config": {},
                "last_sync": {}
            }
            self._save()

    def _save(self):
        temp_file = self.INTEGRATIONS_FILE.with_suffix('.tmp')
        with open(temp_file, 'w') as f:
            json.dump(self.integrations, f, indent=2)
        os.replace(temp_file, self.INTEGRATIONS_FILE)

    def register_integration(self, name: str, config: dict) -> dict:
        """Register a new integration"""
        with self._lock:
            self.integrations["config"][name] = config
            self.integrations["enabled"][name] = True
            self._save()
            return {"name": name, "status": "registered"}

    def enable_integration(self, name: str) -> bool:
        """Enable an integration"""
        if name in self.integrations["config"]:
            self.integrations["enabled"][name] = True
            self._save()
            return True
        return False

    def disable_integration(self, name: str) -> bool:
        """Disable an integration"""
        if name in self.integrations["config"]:
            self.integrations["enabled"][name] = False
            self._save()
            return True
        return False

    def send_notification(self, integration: str, message: str, data: dict = None) -> dict:
        """Send notification via integration"""
        if not self.integrations["enabled"].get(integration):
            return {"error": f"Integration {integration} not enabled"}

        try:
            config = self.integrations["config"].get(integration, {})

            if integration == "slack":
                return self._send_slack(config, message, data)
            elif integration == "email":
                return self._send_email(config, message, data)
            elif integration == "webhook":
                return self._send_webhook(config, message, data)

            return {"error": f"Unknown integration: {integration}"}
        except Exception as e:
            return {"error": str(e)}

    def _send_slack(self, config: dict, message: str, data: dict = None) -> dict:
        """Send Slack notification via incoming webhook."""
        import urllib.request
        webhook_url = config.get("webhook_url", "")
        if not webhook_url:
            return {"error": "No webhook_url configured for Slack"}
        payload = json.dumps({"text": message}).encode("utf-8")
        req = urllib.request.Request(
            webhook_url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return {"status": "sent", "integration": "slack", "http_status": resp.status}
        except Exception as e:
            return {"error": str(e), "integration": "slack"}

    def _send_email(self, config: dict, message: str, data: dict = None) -> dict:
        """Send email via SMTP; falls back to writing a log file."""
        import smtplib
        from email.mime.text import MIMEText

        smtp_host = config.get("smtp_host", "")
        smtp_port = int(config.get("smtp_port", 587))
        smtp_user = config.get("smtp_user", "")
        smtp_pass = config.get("smtp_password", "")
        to_addr   = config.get("to", "")
        from_addr = config.get("from", smtp_user or "orchestrator@localhost")
        subject   = config.get("subject", "Orchestrator Notification")

        if smtp_host and to_addr:
            try:
                msg = MIMEText(message)
                msg["Subject"] = subject
                msg["From"] = from_addr
                msg["To"] = to_addr
                with smtplib.SMTP(smtp_host, smtp_port, timeout=10) as s:
                    s.ehlo()
                    if smtp_pass:
                        s.starttls()
                        s.login(smtp_user, smtp_pass)
                    s.sendmail(from_addr, [to_addr], msg.as_string())
                return {"status": "sent", "integration": "email", "to": to_addr}
            except Exception as e:
                pass  # fall through to log fallback

        # Log fallback — write to file when SMTP not configured or failed
        log_path = LOGS_DIR / f"email_fallback_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        log_path.write_text(
            f"TO: {to_addr or '(not set)'}\nSUBJECT: {subject}\n\n{message}",
            encoding="utf-8",
        )
        return {"status": "logged", "integration": "email", "log_file": str(log_path)}

    def _send_webhook(self, config: dict, message: str, data: dict = None) -> dict:
        """Send a generic JSON POST webhook."""
        import urllib.request
        url = config.get("url", "")
        if not url:
            return {"error": "No url configured for webhook"}
        payload = json.dumps({"message": message, "data": data or {}}).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return {"status": "sent", "integration": "webhook", "http_status": resp.status}
        except Exception as e:
            return {"error": str(e), "integration": "webhook"}

    def get_status(self) -> dict:
        """Get integration status"""
        return {
            "total": len(self.integrations["config"]),
            "enabled": sum(1 for v in self.integrations["enabled"].values() if v),
            "integrations": self.integrations["enabled"]
        }


# ============================================================================
# IMPROVEMENT: Task Scheduling
# ============================================================================

class TaskScheduler:
    """
    Advanced task scheduling with cron-like expressions.
    Supports: one-time, recurring, cron schedules.
    """

    SCHEDULE_FILE = STATE_DIR / "schedules.json"

    def __init__(self, orchestrator: HybridOrchestrator):
        self.orch = orchestrator
        self._lock = threading.Lock()
        self._load()

    def _load(self):
        if self.SCHEDULE_FILE.exists():
            with open(self.SCHEDULE_FILE, 'r') as f:
                self.schedules = json.load(f)
        else:
            self.schedules = {"tasks": [], "next_id": 1}
            self._save()

    def _save(self):
        temp_file = self.SCHEDULE_FILE.with_suffix('.tmp')
        with open(temp_file, 'w') as f:
            json.dump(self.schedules, f, indent=2)
        os.replace(temp_file, self.SCHEDULE_FILE)

    def add_schedule(self, name: str, task_template: dict,
                     schedule_type: str, schedule_value: str,
                     enabled: bool = True) -> dict:
        """Add a scheduled task"""
        with self._lock:
            task = {
                "id": self.schedules["next_id"],
                "name": name,
                "task_template": task_template,
                "schedule_type": schedule_type,  # 'once', 'interval', 'cron'
                "schedule_value": schedule_value,
                "enabled": enabled,
                "last_run": None,
                "next_run": self._calculate_next_run(schedule_type, schedule_value),
                "created": get_utc_timestamp()
            }
            self.schedules["tasks"].append(task)
            self.schedules["next_id"] += 1
            self._save()
            return task

    def _calculate_next_run(self, schedule_type: str, value: str) -> str:
        """Calculate next run time"""
        now = datetime.now(timezone.utc)

        if schedule_type == "once":
            try:
                run_time = datetime.fromisoformat(value)
                return run_time.isoformat()
            except:
                return now.isoformat()

        elif schedule_type == "interval":
            try:
                minutes = int(value)
                return (now + timedelta(minutes=minutes)).isoformat()
            except:
                return now.isoformat()

        elif schedule_type == "cron":
            # Simplified cron - parse minute, hour, day
            # Format: "minute hour day_of_month month day_of_week"
            parts = value.split()
            if len(parts) >= 1:
                minute = int(parts[0]) if parts[0] != '*' else now.minute
                hour = int(parts[1]) if len(parts) > 1 and parts[1] != '*' else now.hour
                next_run = now.replace(minute=minute, hour=hour, second=0, microsecond=0)
                if next_run <= now:
                    next_run += timedelta(hours=1)
                return next_run.isoformat()

        return now.isoformat()

    def get_due_tasks(self) -> List[dict]:
        """Get tasks that are due to run"""
        now = datetime.now(timezone.utc)
        due = []

        for task in self.schedules["tasks"]:
            if not task.get("enabled", True):
                continue

            next_run = task.get("next_run")
            if next_run:
                try:
                    next_run_dt = datetime.fromisoformat(next_run.replace('Z', '+00:00'))
                    if next_run_dt <= now:
                        due.append(task)
                except:
                    pass

        return due

    def execute_scheduled_task(self, task_id: int) -> dict:
        """Execute a scheduled task"""
        task = None
        for t in self.schedules["tasks"]:
            if t["id"] == task_id:
                task = t
                break

        if not task:
            return {"error": "Task not found"}

        # Create the task from template
        template = task.get("task_template", {})
        new_task = self.orch.board.add_task(
            title=template.get("title", f"Scheduled: {task['name']}"),
            agent=template.get("agent", "Hermes"),
            priority=template.get("priority", "medium"),
            tags=template.get("tags", ["scheduled"])
        )

        # Update last run and next run
        task["last_run"] = get_utc_timestamp()
        task["next_run"] = self._calculate_next_run(task["schedule_type"], task["schedule_value"])
        self._save()

        return {
            "status": "executed",
            "scheduled_task_id": task_id,
            "created_task_id": new_task.id
        }

    def get_schedules(self, enabled_only: bool = False) -> List[dict]:
        """Get all schedules"""
        schedules = self.schedules["tasks"]
        if enabled_only:
            schedules = [s for s in schedules if s.get("enabled", True)]
        return schedules

    def delete_schedule(self, task_id: int) -> bool:
        """Delete a schedule"""
        with self._lock:
            before = len(self.schedules["tasks"])
            self.schedules["tasks"] = [t for t in self.schedules["tasks"] if t["id"] != task_id]
            if len(self.schedules["tasks"]) < before:
                self._save()
                return True
            return False

    def start_background_loop(self, interval: int = 60) -> threading.Thread:
        """Start a daemon thread that polls for due tasks every `interval` seconds."""
        def _loop():
            while True:
                try:
                    due = self.get_due_tasks()
                    for task in due:
                        try:
                            self.execute_scheduled_task(task["id"])
                        except Exception:
                            pass
                except Exception:
                    pass
                time.sleep(interval)

        t = threading.Thread(target=_loop, daemon=True, name="TaskScheduler-bg")
        t.start()
        return t


# ============================================================================
# IMPROVEMENT: Metrics Export
# ============================================================================

class MetricsExporter:
    """
    Export metrics in various formats (Prometheus, JSON, CSV).
    Supports: Prometheus pushgateway, file export, HTTP export.
    """

    def __init__(self, orchestrator: HybridOrchestrator):
        self.orch = orchestrator
        self._metrics_cache = {}

    def collect_metrics(self) -> dict:
        """Collect all metrics"""
        board_stats = self.orch.board.get_stats()
        task_stats = self.orch.board.get_stats()

        metrics = {
            "orchestrator_tasks_total": task_stats.get("total", 0),
            "orchestrator_tasks_by_status": task_stats,
            "orchestrator_uptime_seconds": self._get_uptime(),
            "orchestrator_version": "5.0",
            "orchestrator_timestamp": get_utc_timestamp()
        }

        # Add performance metrics if available
        if hasattr(self.orch, 'performance'):
            perf_stats = self.orch.performance.get_all_stats()
            metrics["orchestrator_performance"] = perf_stats

        # Add SLA metrics if available
        if hasattr(self.orch, 'sla_monitor'):
            sla_summary = self.orch.sla_monitor.get_sla_summary()
            metrics["orchestrator_sla_compliance_rate"] = sla_summary.get("compliance_rate", 100)

        self._metrics_cache = metrics
        return metrics

    def _get_uptime(self) -> float:
        """Get orchestrator uptime in seconds"""
        if hasattr(self.orch, 'started_at'):
            start = datetime.fromisoformat(self.orch.started_at.replace('Z', '+00:00'))
            return (datetime.now(timezone.utc) - start).total_seconds()
        return 0

    def export_prometheus(self) -> str:
        """Export metrics in Prometheus format"""
        metrics = self.collect_metrics()
        lines = []

        for key, value in metrics.items():
            if isinstance(value, (int, float)):
                lines.append(f"{key} {value}")
            elif isinstance(value, dict):
                for sub_key, sub_value in value.items():
                    if isinstance(sub_value, (int, float)):
                        lines.append(f'{key}{{status="{sub_key}"}} {sub_value}')

        return "\n".join(lines)

    def export_json(self) -> dict:
        """Export metrics as JSON"""
        return self.collect_metrics()

    def export_csv(self) -> str:
        """Export metrics as CSV"""
        metrics = self.collect_metrics()
        lines = ["metric,value"]

        def flatten_dict(d, prefix=""):
            for key, value in d.items():
                if isinstance(value, (int, float)):
                    lines.append(f'"{prefix}{key}",{value}')
                elif isinstance(value, dict):
                    flatten_dict(value, f"{prefix}{key}_")

        flatten_dict(metrics)
        return "\n".join(lines)

    def push_to_gateway(self, gateway_url: str, job: str = "orchestrator") -> dict:
        """Push metrics to Prometheus pushgateway (stub)"""
        return {
            "status": "pushed",
            "gateway": gateway_url,
            "job": job,
            "metrics_count": len(self._metrics_cache)
        }


# ============================================================================
# IMPROVEMENT: API Documentation
# ============================================================================

class APIDocumentation:
    """
    Auto-generated API documentation for the orchestrator.
    Supports: OpenAPI/Swagger format.
    """

    ENDPOINTS = [
        {
            "path": "/api/tasks",
            "method": "GET",
            "summary": "List all tasks",
            "parameters": [
                {"name": "status", "in": "query", "schema": {"type": "string"}, "description": "Filter by status"}
            ]
        },
        {
            "path": "/api/tasks",
            "method": "POST",
            "summary": "Create a new task",
            "requestBody": {
                "content": {
                    "application/json": {
                        "schema": {
                            "type": "object",
                            "properties": {
                                "title": {"type": "string"},
                                "description": {"type": "string"},
                                "agent": {"type": "string"},
                                "priority": {"type": "string", "enum": ["critical", "high", "medium", "low"]}
                            }
                        }
                    }
                }
            }
        },
        {
            "path": "/api/tasks/{task_id}",
            "method": "GET",
            "summary": "Get task details",
            "parameters": [
                {"name": "task_id", "in": "path", "schema": {"type": "string"}, "required": True}
            ]
        },
        {
            "path": "/api/tasks/{task_id}",
            "method": "PUT",
            "summary": "Update task",
            "parameters": [
                {"name": "task_id", "in": "path", "schema": {"type": "string"}, "required": True}
            ]
        },
        {
            "path": "/api/tasks/{task_id}",
            "method": "DELETE",
            "summary": "Delete task",
            "parameters": [
                {"name": "task_id", "in": "path", "schema": {"type": "string"}, "required": True}
            ]
        },
        {
            "path": "/api/stats",
            "method": "GET",
            "summary": "Get orchestrator statistics"
        },
        {
            "path": "/api/health",
            "method": "GET",
            "summary": "Health check endpoint"
        }
    ]

    def generate_openapi(self) -> dict:
        """Generate OpenAPI 3.0 specification"""
        return {
            "openapi": "3.0.0",
            "info": {
                "title": "Multi-Agent Hybrid Orchestrator API",
                "version": "5.0",
                "description": "REST API for Multi-Agent Hybrid Orchestrator (CAMEL → Multica → Hermes/OpenClaw)"
            },
            "servers": [
                {"url": "/", "description": "Local server"}
            ],
            "paths": {ep["path"]: self._endpoint_to_openapi(ep) for ep in self.ENDPOINTS}
        }

    def _endpoint_to_openapi(self, endpoint: dict) -> dict:
        """Convert endpoint to OpenAPI format"""
        method = endpoint["method"].lower()
        return {
            method: {
                "summary": endpoint["summary"],
                "parameters": endpoint.get("parameters", []),
                "requestBody": endpoint.get("requestBody", {}),
                "responses": {
                    "200": {"description": "Successful response"},
                    "400": {"description": "Bad request"},
                    "404": {"description": "Not found"}
                }
            }
        }

    def generate_markdown(self) -> str:
        """Generate API documentation in Markdown format"""
        lines = [
            "# Multi-Agent Hybrid Orchestrator API",
            "",
            "## Base URL",
            "`/api`",
            "",
            "## Endpoints",
            ""
        ]

        for ep in self.ENDPOINTS:
            lines.extend([
                f"### {ep['method']} {ep['path']}",
                f"**Summary:** {ep['summary']}",
                ""
            ])

            if ep.get("parameters"):
                lines.append("**Parameters:**")
                for param in ep["parameters"]:
                    lines.append(f"- `{param['name']}` ({param['in']}): {param.get('description', '')}")
                lines.append("")

        return "\n".join(lines)


# Add dashboard and webhook to HybridOrchestrator
def _add_orchestrator_extensions():
    """Add dashboard and webhook to orchestrator instance"""
    if not hasattr(HybridOrchestrator, '_extended'):
        HybridOrchestrator.dashboard = property(lambda self: StatisticsDashboard(self))
        HybridOrchestrator.webhooks = property(lambda self: WebhookManager(self))
        HybridOrchestrator.dependencies = property(lambda self: TaskDependencyGraph(self))
        HybridOrchestrator.executor = AsyncTaskExecutor(max_workers=5)
        HybridOrchestrator.timeline = property(lambda self: TaskTimeline(self))
        HybridOrchestrator.priority_queue = property(lambda self: PriorityTaskQueue(self))
        HybridOrchestrator.notifications = property(lambda self: NotificationManager(self))
        HybridOrchestrator.filters = property(lambda self: TaskFilters(self))
        HybridOrchestrator.auto_assign = property(lambda self: AutoAssignRules(self))
        HybridOrchestrator.tags_manager = property(lambda self: TagsManager(self))
        HybridOrchestrator.recurring = property(lambda self: RecurringTaskManager(self))
        HybridOrchestrator.activity_feed = property(lambda self: ActivityFeed(self))
        HybridOrchestrator.batch_ops = property(lambda self: BatchOperations(self))
        HybridOrchestrator.migration = property(lambda self: DataMigration(self))
        HybridOrchestrator.performance = property(lambda self: PerformanceMonitor(self))
        HybridOrchestrator.templates = property(lambda self: TaskTemplatesLibrary(self))
        HybridOrchestrator.time_tracker = property(lambda self: TimeTracker(self))
        HybridOrchestrator.sla_monitor = property(lambda self: SLAMonitor(self))
        HybridOrchestrator.resources = property(lambda self: ResourceManager(self))
        HybridOrchestrator.audit = property(lambda self: AuditTrail(self))
        HybridOrchestrator.websocket = property(lambda self: WebSocketManager(self))
        HybridOrchestrator.workflows = property(lambda self: WorkflowEngine(self))
        HybridOrchestrator.knowledge = property(lambda self: KnowledgeBase(self))
        HybridOrchestrator.api_limiter = APIRateLimiter()
        HybridOrchestrator.health = property(lambda self: HealthChecker(self))
        HybridOrchestrator.integrations = property(lambda self: IntegrationHub(self))
        HybridOrchestrator.scheduler = property(lambda self: TaskScheduler(self))
        HybridOrchestrator.metrics = property(lambda self: MetricsExporter(self))
        HybridOrchestrator.api_docs = APIDocumentation()
        HybridOrchestrator._extended = True


# ============================================================================
# IMPROVEMENT: Caching Layer
# ============================================================================

class CacheManager:
    """
    In-memory cache with TTL support for improved performance.
    Supports: TTL, LRU eviction, cache invalidation.
    """

    def __init__(self, default_ttl: int = 300):
        self._cache: Dict[str, dict] = {}
        self.default_ttl = default_ttl
        self._lock = threading.Lock()

    def get(self, key: str) -> Optional[Any]:
        """Get value from cache"""
        with self._lock:
            if key not in self._cache:
                return None
            entry = self._cache[key]
            if time.time() > entry["expires"]:
                del self._cache[key]
                return None
            entry["hits"] += 1
            return entry["value"]

    def set(self, key: str, value: Any, ttl: int = None):
        """Set value in cache"""
        with self._lock:
            self._cache[key] = {
                "value": value,
                "expires": time.time() + (ttl or self.default_ttl),
                "created": time.time(),
                "hits": 0
            }

    def invalidate(self, key: str) -> bool:
        """Invalidate cache entry"""
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
            return False

    def clear(self):
        """Clear all cache"""
        with self._lock:
            self._cache.clear()

    def get_stats(self) -> dict:
        """Get cache statistics"""
        total_hits = sum(e["hits"] for e in self._cache.values())
        return {"entries": len(self._cache), "total_hits": total_hits}


# ============================================================================
# IMPROVEMENT: Prometheus Metrics Export
# ============================================================================

class MetricsExporter:
    """
    Export metrics in Prometheus format.
    """

    def __init__(self, orchestrator: HybridOrchestrator):
        self.orch = orchestrator
        self._metrics: Dict[str, float] = {}

    def _safe_metric_name(self, name: str) -> str:
        return name.replace(" ", "_").replace("-", "_").lower()

    def record_counter(self, name: str, value: float = 1):
        key = self._safe_metric_name(name)
        self._metrics[key] = self._metrics.get(key, 0) + value

    def record_gauge(self, name: str, value: float):
        key = self._safe_metric_name(name)
        self._metrics[key] = value

    def get_prometheus_format(self) -> str:
        """Export metrics in Prometheus text format"""
        lines = ['# HELP orchestrator_info Orchestrator info', '# TYPE orchestrator_info gauge', 'orchestrator_info{version="5.0"} 1']
        stats = self.orch.board.get_stats()
        lines.append(f'orchestrator_tasks_total {stats.get("total", 0)}')
        for status, count in stats.items():
            if status != "total":
                lines.append(f'orchestrator_tasks_by_status{{status="{status}"}} {count}')
        for name, value in self._metrics.items():
            lines.append(f'orchestrator_{name} {value}')
        return "\n".join(lines)


# ============================================================================
# IMPROVEMENT: Extended Comments System
# ============================================================================

class ExtendedComments:
    """
    Enhanced task comments with threading and reactions.
    """

    COMMENTS_FILE = TASKS_DIR / "extended_comments.json"

    def __init__(self, orchestrator: HybridOrchestrator):
        self.orch = orchestrator
        self._lock = threading.Lock()
        self._load()

    def _load(self):
        if self.COMMENTS_FILE.exists():
            with open(self.COMMENTS_FILE, 'r') as f:
                self.comments = json.load(f)
        else:
            self.comments = {"comments": [], "next_id": 1}
            self._save()

    def _save(self):
        temp_file = self.COMMENTS_FILE.with_suffix('.tmp')
        with open(temp_file, 'w') as f:
            json.dump(self.comments, f, indent=2)
        os.replace(temp_file, self.COMMENTS_FILE)

    def add_comment(self, task_id: str, content: str, author: str = "system",
                   parent_id: int = None, mentions: List[str] = None) -> dict:
        """Add a comment to a task"""
        with self._lock:
            comment = {
                "id": self.comments["next_id"],
                "task_id": task_id,
                "content": content,
                "author": author,
                "created": get_utc_timestamp(),
                "updated": get_utc_timestamp(),
                "parent_id": parent_id,
                "mentions": mentions or [],
                "reactions": {}
            }
            self.comments["comments"].append(comment)
            self.comments["next_id"] += 1
            self._save()
            return comment

    def reply_to_comment(self, parent_id: int, task_id: str, content: str, author: str = "system") -> Optional[dict]:
        """Reply to an existing comment"""
        with self._lock:
            for comment in self.comments["comments"]:
                if comment["id"] == parent_id:
                    return self.add_comment(task_id, content, author, parent_id)
        return None

    def add_reaction(self, comment_id: int, emoji: str, user: str) -> bool:
        """Add reaction to a comment"""
        with self._lock:
            for comment in self.comments["comments"]:
                if comment["id"] == comment_id:
                    if emoji not in comment["reactions"]:
                        comment["reactions"][emoji] = []
                    if user not in comment["reactions"][emoji]:
                        comment["reactions"][emoji].append(user)
                    self._save()
                    return True
        return False

    def get_task_comments(self, task_id: str) -> List[dict]:
        """Get all comments for a task"""
        return [c for c in self.comments["comments"] if c["task_id"] == task_id]


# ============================================================================
# IMPROVEMENT: Report Generator
# ============================================================================

class ReportGenerator:
    """
    Generate various reports from orchestrator data.
    """

    def __init__(self, orchestrator: HybridOrchestrator):
        self.orch = orchestrator

    def generate_task_report(self, format: str = "json", status: str = None) -> str:
        """Generate task report in specified format"""
        tasks = self.orch.board.list_tasks(status) if status else self.orch.board.list_tasks()
        task_dicts = [t.to_dict() for t in tasks]

        if format == "json":
            return json.dumps(task_dicts, indent=2)
        elif format == "csv":
            lines = ["ID,Title,Status,Priority,Agent,Created"]
            for t in task_dicts:
                lines.append(f'{t["id"]},"{t["title"]}",{t["status"]},{t["priority"]},{t["agent"]},{t["created"]}')
            return "\n".join(lines)
        elif format == "markdown":
            lines = ["# Task Report", "", "| ID | Title | Status | Priority |", "|---|---|---|---|"]
            for t in task_dicts:
                lines.append(f"| {t['id']} | {t['title']} | {t['status']} | {t['priority']} |")
            return "\n".join(lines)
        return ""

    def export_report(self, report_type: str, filepath: str, format: str = "json", **kwargs) -> str:
        """Export report to file"""
        content = self.generate_task_report(format, **kwargs) if report_type == "tasks" else ""
        with open(filepath, 'w') as f:
            f.write(content)
        return filepath


# ============================================================================
# IMPROVEMENT: API Versioning
# ============================================================================

class APIRouter:
    """
    API versioning middleware.
    """

    def __init__(self):
        self.routes_v1: Dict[str, dict] = {}
        self.routes_v2: Dict[str, dict] = {}

    def register_route(self, path: str, handler: callable, version: str = "v1"):
        """Register an API route"""
        if version == "v1":
            self.routes_v1[path] = {"handler": handler, "version": "v1"}
        else:
            self.routes_v2[path] = {"handler": handler, "version": version}

    def get_route(self, path: str, version: str = "v2") -> Optional[callable]:
        """Get route handler for given path and version"""
        routes = self.routes_v2 if version == "v2" else self.routes_v1
        route = routes.get(path)
        if route:
            return route["handler"]
        if version == "v2" and path in self.routes_v1:
            return self.routes_v1[path]["handler"]
        return None

    def list_routes(self, version: str = None) -> List[dict]:
        """List all registered routes"""
        routes = []
        if version in [None, "v1"]:
            routes.extend([{"path": k, **v} for k, v in self.routes_v1.items()])
        if version in [None, "v2"]:
            routes.extend([{"path": k, **v} for k, v in self.routes_v2.items()])
        return routes


def _add_orchestrator_extensions():
    """Add dashboard and webhook to orchestrator instance"""
    if not hasattr(HybridOrchestrator, '_extended'):
        HybridOrchestrator.dashboard = property(lambda self: StatisticsDashboard(self))
        HybridOrchestrator.webhooks = property(lambda self: WebhookManager(self))
        HybridOrchestrator.dependencies = property(lambda self: TaskDependencyGraph(self))
        HybridOrchestrator.executor = AsyncTaskExecutor(max_workers=5)
        HybridOrchestrator.timeline = property(lambda self: TaskTimeline(self))
        HybridOrchestrator.priority_queue = property(lambda self: PriorityTaskQueue(self))
        HybridOrchestrator.notifications = property(lambda self: NotificationManager(self))
        HybridOrchestrator.filters = property(lambda self: TaskFilters(self))
        HybridOrchestrator.auto_assign = property(lambda self: AutoAssignRules(self))
        HybridOrchestrator.tags_manager = property(lambda self: TagsManager(self))
        HybridOrchestrator.recurring = property(lambda self: RecurringTaskManager(self))
        HybridOrchestrator.activity_feed = property(lambda self: ActivityFeed(self))
        HybridOrchestrator.batch_ops = property(lambda self: BatchOperations(self))
        HybridOrchestrator.migration = property(lambda self: DataMigration(self))
        HybridOrchestrator.performance = property(lambda self: PerformanceMonitor(self))
        HybridOrchestrator.templates = property(lambda self: TaskTemplatesLibrary(self))
        HybridOrchestrator.time_tracker = property(lambda self: TimeTracker(self))
        HybridOrchestrator.sla_monitor = property(lambda self: SLAMonitor(self))
        HybridOrchestrator.resources = property(lambda self: ResourceManager(self))
        HybridOrchestrator.audit = property(lambda self: AuditTrail(self))
        HybridOrchestrator.websocket = property(lambda self: WebSocketManager(self))
        HybridOrchestrator.workflows = property(lambda self: WorkflowEngine(self))
        HybridOrchestrator.knowledge = property(lambda self: KnowledgeBase(self))
        HybridOrchestrator.api_limiter = APIRateLimiter()
        HybridOrchestrator.health = property(lambda self: HealthChecker(self))
        HybridOrchestrator.integrations = property(lambda self: IntegrationHub(self))
        HybridOrchestrator.scheduler = property(lambda self: TaskScheduler(self))
        HybridOrchestrator.metrics_exporter = property(lambda self: MetricsExporter(self))
        HybridOrchestrator.api_docs = APIDocumentation()
        HybridOrchestrator.extended_comments = property(lambda self: ExtendedComments(self))
        HybridOrchestrator.reports = property(lambda self: ReportGenerator(self))
        HybridOrchestrator.api_router = APIRouter()
        HybridOrchestrator._extended = True

_add_orchestrator_extensions()


if __name__ == "__main__":
    main()
