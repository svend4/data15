#!/usr/bin/env python3
"""
OpenClaw Integration - External agent integration
"""
import subprocess
import json
from typing import Dict, Optional
from pathlib import Path

class OpenClawIntegration:
    def __init__(self, runner_script: str = None):
        self.runner_script = runner_script or str(Path(__file__).parent / "openclaw_runner.sh")

    def is_available(self) -> bool:
        try:
            result = subprocess.run(
                ["bash", "-c", "which openclaw || echo not_found"],
                capture_output=True,
                text=True,
                timeout=5
            )
            return "not_found" not in result.stdout
        except:
            return False

    def execute(self, prompt: str, timeout: int = 120) -> Dict:
        if not self.is_available():
            return {"error": "OpenClaw not available", "status": "unavailable"}

        try:
            result = subprocess.run(
                ["bash", self.runner_script, "--prompt", prompt],
                capture_output=True,
                text=True,
                timeout=timeout
            )
            return {
                "status": "success" if result.returncode == 0 else "failed",
                "output": result.stdout,
                "error": result.stderr if result.returncode != 0 else None
            }
        except subprocess.TimeoutExpired:
            return {"error": "Timeout", "status": "timeout"}
        except Exception as e:
            return {"error": str(e), "status": "error"}

if __name__ == "__main__":
    integration = OpenClawIntegration()
    print(f"OpenClaw available: {integration.is_available()}")
