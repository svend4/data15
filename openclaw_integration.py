#!/usr/bin/env python3
"""
OpenClaw Integration Module for Orchestrator
============================================
Provides real execution capabilities using OpenClaw agent.
"""

import subprocess
import json
import os
from pathlib import Path
from typing import Dict, Optional, List
from datetime import datetime

# Paths
ORCHESTRATOR_DIR = Path("/workspace/orchestrator")
OPENCLAW_RUNNER = ORCHESTRATOR_DIR / "openclaw_runner.sh"
LOGS_DIR = ORCHESTRATOR_DIR / "logs"

# Ensure logs directory exists
LOGS_DIR.mkdir(parents=True, exist_ok=True)


class OpenClawExecutor:
    """
    Real OpenClaw execution integration.
    Calls OpenClaw CLI for external agent tasks.
    """

    def __init__(self):
        self.runner_script = str(OPENCLAW_RUNNER)
        # Make runner executable
        os.chmod(self.runner_script, 0o755)

    def _get_timestamp(self) -> str:
        return datetime.utcnow().isoformat() + 'Z'

    def _save_log(self, task_id: str, log_content: str):
        """Save execution log to file"""
        log_file = LOGS_DIR / f"openclaw_{task_id}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.log"
        with open(log_file, 'w') as f:
            f.write(f"OpenClaw Execution Log\n")
            f.write(f"=" * 50 + "\n")
            f.write(f"Task: {task_id}\n")
            f.write(f"Time: {self._get_timestamp()}\n")
            f.write(f"=" * 50 + "\n\n")
            f.write(log_content)
        return str(log_file)

    def execute(self, task_id: str, prompt: str, timeout: int = 120) -> Dict:
        """
        Execute a task using OpenClaw.

        Args:
            task_id: ID of the task being executed
            prompt: The prompt/instruction for OpenClaw
            timeout: Maximum execution time in seconds

        Returns:
            Dict with execution results
        """
        result = {
            "task_id": task_id,
            "agent": "OpenClaw",
            "timestamp": self._get_timestamp(),
            "status": "unknown",
            "output": "",
            "log_file": "",
            "error": None
        }

        try:
            # Build command
            cmd = [
                "bash",
                self.runner_script,
                "--prompt", prompt,
                "--timeout", str(timeout)
            ]

            # Execute
            process = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout + 10  # Slightly longer than task timeout
            )

            output = process.stdout + process.stderr

            # Save log
            log_file = self._save_log(task_id, output)
            result["log_file"] = log_file

            if process.returncode == 0:
                result["status"] = "completed"
                result["output"] = process.stdout
            elif process.returncode == 124:  # timeout
                result["status"] = "timeout"
                result["output"] = output
                result["error"] = f"Execution timed out after {timeout} seconds"
            else:
                result["status"] = "failed"
                result["output"] = output
                result["error"] = f"Exit code: {process.returncode}"

        except subprocess.TimeoutExpired:
            result["status"] = "timeout"
            result["error"] = f"Execution timed out after {timeout + 10} seconds"
        except Exception as e:
            result["status"] = "error"
            result["error"] = str(e)

        return result

    def web_search(self, task_id: str, query: str, timeout: int = 60) -> Dict:
        """
        Execute a web search using OpenClaw.

        Args:
            task_id: ID of the task
            query: Search query

        Returns:
            Dict with search results
        """
        prompt = f"Search the web for: {query}. Return the most relevant results."
        return self.execute(task_id, prompt, timeout)

    def api_call(self, task_id: str, service: str, action: str, params: Dict = None) -> Dict:
        """
        Execute an API call using OpenClaw.

        Args:
            task_id: ID of the task
            service: Service name (twitter, stocks, flights, etc.)
            action: Action to perform
            params: Additional parameters

        Returns:
            Dict with API response
        """
        params_str = json.dumps(params) if params else "{}"
        prompt = f"Call {service} API: {action} with params {params_str}. Return the response."
        return self.execute(task_id, prompt, timeout=60)

    def check_status(self) -> Dict:
        """
        Check OpenClaw availability and version.

        Returns:
            Dict with status information
        """
        try:
            result = subprocess.run(
                ["bash", self.runner_script],
                capture_output=True,
                text=True,
                timeout=10
            )
            return {
                "status": "available",
                "version": result.stdout.strip() if result.stdout else "unknown",
                "error": None
            }
        except Exception as e:
            return {
                "status": "unavailable",
                "version": None,
                "error": str(e)
            }


# Global executor instance
_executor = None


def get_executor() -> OpenClawExecutor:
    """Get or create the global OpenClaw executor instance"""
    global _executor
    if _executor is None:
        _executor = OpenClawExecutor()
    return _executor


def execute_task(task_id: str, prompt: str, timeout: int = 120) -> Dict:
    """Convenience function to execute a task via OpenClaw"""
    return get_executor().execute(task_id, prompt, timeout)


def check_openclaw() -> Dict:
    """Check if OpenClaw is available"""
    return get_executor().check_status()


# CLI for testing
if __name__ == "__main__":
    import sys

    print("OpenClaw Integration Module")
    print("=" * 50)

    # Check status
    status = check_openclaw()
    print(f"Status: {status['status']}")
    if status['version']:
        print(f"Version: {status['version']}")
    if status['error']:
        print(f"Error: {status['error']}")

    if len(sys.argv) > 1:
        # Test execution
        test_prompt = " ".join(sys.argv[1:])
        print(f"\nExecuting: {test_prompt[:50]}...")
        result = execute_task("TEST-001", test_prompt, timeout=30)
        print(f"Status: {result['status']}")
        print(f"Output: {result['output'][:500]}...")
