#!/usr/bin/env python3
"""
Orchestrator Monitor Daemon
===========================
Real-time monitoring for the multi-agent orchestrator.

Features:
- Watch task board changes
- Log status updates
- Generate alerts for failures
- Dashboard output
"""

import json
import time
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional

# Configuration
ORCHESTRATOR_DIR = Path("/workspace/orchestrator")
BOARD_FILE = ORCHESTRATOR_DIR / "tasks" / "hybrid_board.json"
LOG_FILE = ORCHESTRATOR_DIR / "logs" / "monitor.log"
STATE_FILE = ORCHESTRATOR_DIR / "state" / "hybrid_state.json"


class MonitorDaemon:
    """
    Real-time monitor for orchestrator tasks.
    Watch mode: continuously monitor board changes.
    Report mode: show current status.
    Alert mode: generate alerts for failures.
    """

    def __init__(self):
        self.board_file = BOARD_FILE
        self.log_file = LOG_FILE
        self.state_file = STATE_FILE
        self.last_state = None
        self.running = False

        # Ensure log directory exists
        self.log_file.parent.mkdir(parents=True, exist_ok=True)

    def _log(self, message: str):
        """Log message with timestamp"""
        timestamp = datetime.now().isoformat()
        log_line = f"[{timestamp}] {message}\n"
        with open(self.log_file, 'a') as f:
            f.write(log_line)
        print(log_line.strip())

    def _load_board(self) -> Optional[Dict]:
        """Load current board state"""
        if not self.board_file.exists():
            return None
        try:
            with open(self.board_file, 'r') as f:
                return json.load(f)
        except Exception:
            return None

    def _get_changes(self, old: Dict, new: Dict) -> List[str]:
        """Detect changes between states"""
        changes = []

        if not old:
            changes.append("Board initialized")
            return changes

        # Check stats changes
        old_stats = old.get('stats', {})
        new_stats = new.get('stats', {})

        for key in ['total', 'queued', 'running', 'completed', 'failed', 'blocked']:
            old_val = old_stats.get(key, 0)
            new_val = new_stats.get(key, 0)
            if old_val != new_val:
                changes.append(f"Stats: {key} {old_val} → {new_val}")

        # Check for new completed tasks
        old_completed = {t['id'] for t in old.get('tasks', []) if t.get('status') == 'completed'}
        new_completed = {t['id'] for t in new.get('tasks', []) if t.get('status') == 'completed'}
        for tid in new_completed - old_completed:
            task = next((t for t in new['tasks'] if t['id'] == tid), None)
            if task:
                changes.append(f"Completed: {tid} - {task.get('title', 'Unknown')}")

        # Check for failed tasks
        old_failed = {t['id'] for t in old.get('tasks', []) if t.get('status') == 'failed'}
        new_failed = {t['id'] for t in new.get('tasks', []) if t.get('status') == 'failed'}
        for tid in new_failed - old_failed:
            task = next((t for t in new['tasks'] if t['id'] == tid), None)
            if task:
                changes.append(f"FAILED: {tid} - {task.get('title', 'Unknown')}")
                self._generate_alert(tid, task)

        # Check for new tasks
        old_ids = {t['id'] for t in old.get('tasks', [])}
        new_ids = {t['id'] for t in new.get('tasks', [])}
        for tid in new_ids - old_ids:
            task = next((t for t in new['tasks'] if t['id'] == tid), None)
            if task:
                changes.append(f"New task: {tid} - {task.get('title', 'Unknown')} → {task.get('agent', 'Unknown')}")

        return changes

    def _generate_alert(self, task_id: str, task: Dict):
        """Generate alert for failed task"""
        alert_msg = f"""
⚠️  ALERT: Task Failed
{'=' * 40}
Task ID: {task_id}
Title: {task.get('title', 'Unknown')}
Agent: {task.get('agent', 'Unknown')}
Priority: {task.get('priority', 'Unknown')}
Created: {task.get('created', 'Unknown')}
Updated: {task.get('updated', 'Unknown')}
{'=' * 40}
"""
        alert_file = ORCHESTRATOR_DIR / "logs" / f"alert_{task_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(alert_file, 'w') as f:
            f.write(alert_msg)
        self._log(f"Alert generated: {alert_file}")

    def _format_dashboard(self, board: Dict) -> str:
        """Format dashboard view"""
        stats = board.get('stats', {})
        tasks = board.get('tasks', [])

        lines = [
            "=" * 60,
            "📊 ORCHESTRATOR MONITOR DASHBOARD",
            f"Time: {datetime.now().isoformat()}",
            "=" * 60,
            "",
            "📈 STATISTICS:",
            f"  Total:     {stats.get('total', 0)}",
            f"  Queued:    {stats.get('queued', 0)}",
            f"  Running:   {stats.get('running', 0)}",
            f"  Completed: {stats.get('completed', 0)}",
            f"  Failed:    {stats.get('failed', 0)}",
            f"  Blocked:   {stats.get('blocked', 0)}",
            "",
        ]

        # Group by status
        for status, icon in [('blocked', '🚫'), ('queued', '⏳'), ('running', '🔄'), ('completed', '✅'), ('failed', '❌')]:
            status_tasks = [t for t in tasks if t.get('status') == status]
            if status_tasks:
                lines.append(f"{icon} {status.upper()}:")
                for t in status_tasks[:5]:  # Show first 5
                    progress = t.get('progress', 0)
                    lines.append(f"    [{t['id']}] {t.get('title', 'Unknown')[:40]}")
                    if status == 'running':
                        lines.append(f"             Progress: {progress}%")
                if len(status_tasks) > 5:
                    lines.append(f"    ... and {len(status_tasks) - 5} more")
                lines.append("")

        lines.append("=" * 60)
        return "\n".join(lines)

    def watch(self, interval: int = 5):
        """
        Watch mode: continuously monitor board.
        Press Ctrl+C to stop.
        """
        self.running = True
        self._log("Monitor daemon started (watch mode)")

        print(f"Watching board: {self.board_file}")
        print(f"Interval: {interval} seconds")
        print("Press Ctrl+C to stop\n")

        while self.running:
            try:
                board = self._load_board()
                if board:
                    changes = self._get_changes(self.last_state, board)
                    if changes:
                        print(f"\n{'=' * 60}")
                        print(f"Changes detected at {datetime.now().isoformat()}:")
                        for change in changes:
                            print(f"  • {change}")
                        print()

                    self.last_state = board

                time.sleep(interval)

            except KeyboardInterrupt:
                self._log("Monitor daemon stopped by user")
                self.running = False
            except Exception as e:
                self._log(f"Error: {e}")
                time.sleep(interval)

    def report(self):
        """Report mode: show current status"""
        board = self._load_board()
        if board:
            print(self._format_dashboard(board))
        else:
            print("No board data found")

    def status(self) -> Dict:
        """Get current status summary"""
        board = self._load_board()
        if board:
            return {
                "status": "running",
                "timestamp": datetime.now().isoformat(),
                "board": str(self.board_file),
                "stats": board.get('stats', {}),
                "total_tasks": len(board.get('tasks', []))
            }
        return {"status": "no_data"}

    def save_state(self):
        """Save current monitor state"""
        state = self.status()
        with open(self.state_file, 'w') as f:
            json.dump(state, f, indent=2)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Orchestrator Monitor Daemon")
    parser.add_argument('mode', nargs='?', default='report',
                        choices=['watch', 'report', 'status'],
                        help='Mode: watch (continuous), report (once), status (json)')
    parser.add_argument('--interval', '-i', type=int, default=5,
                        help='Watch interval in seconds (default: 5)')

    args = parser.parse_args()

    monitor = MonitorDaemon()

    if args.mode == 'watch':
        monitor.watch(args.interval)
    elif args.mode == 'report':
        monitor.report()
    elif args.mode == 'status':
        import json as json_mod
        print(json_mod.dumps(monitor.status(), indent=2))


if __name__ == "__main__":
    main()
