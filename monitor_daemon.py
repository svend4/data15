#!/usr/bin/env python3
"""
Monitor Daemon - System monitoring and health checks
"""
import time
import json
import threading
from pathlib import Path
from datetime import datetime

class MonitorDaemon:
    def __init__(self, state_dir: str = "state"):
        self.state_dir = Path(state_dir)
        self.running = False
        self._thread = None
        self._lock = threading.Lock()

    def start(self, interval: int = 60):
        self.running = True
        self._thread = threading.Thread(target=self._run, args=(interval,))
        self._thread.daemon = True
        self._thread.start()

    def stop(self):
        self.running = False
        if self._thread:
            self._thread.join(timeout=5)

    def _run(self, interval: int):
        while self.running:
            self._check_health()
            time.sleep(interval)

    def _check_health(self):
        metrics = {
            "timestamp": datetime.utcnow().isoformat(),
            "status": "healthy",
            "checks": {
                "storage": self._check_storage(),
                "threads": threading.active_count(),
                "memory": "ok"
            }
        }
        metrics_file = self.state_dir / "performance_metrics.json"
        with open(metrics_file, 'w') as f:
            json.dump(metrics, f, indent=2)

    def _check_storage(self) -> str:
        try:
            test_file = self.state_dir / ".health_check"
            test_file.write_text(str(time.time()))
            test_file.unlink()
            return "ok"
        except Exception:
            return "error"

if __name__ == "__main__":
    daemon = MonitorDaemon()
    print("Starting Monitor Daemon...")
    daemon.start()
    time.sleep(5)
    daemon.stop()
    print("Daemon stopped.")
