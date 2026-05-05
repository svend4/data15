#!/usr/bin/env python3
"""
Orchestrator Full System Test
=============================
Comprehensive test of all orchestrator components.
"""

import subprocess
import json
import sys
from pathlib import Path

ORCHESTRATOR_DIR = Path("/workspace/orchestrator")

def run_cmd(cmd):
    """Run command and return output"""
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    return result.returncode, result.stdout, result.stderr

def test_result(name, passed, details=""):
    """Print test result"""
    icon = "✅" if passed else "❌"
    status = "PASS" if passed else "FAIL"
    print(f"{icon} [{status}] {name}")
    if details and not passed:
        print(f"   Details: {details}")
    return passed

def main():
    print("=" * 60)
    print("ORCHESTRATOR FULL SYSTEM TEST")
    print("=" * 60)
    print()

    results = []

    # Test 1: CAMEL Layer
    print("📦 CAMEL LAYER (Strategic)")
    print("-" * 40)

    code, out, err = run_cmd(f"cd {ORCHESTRATOR_DIR} && python3 hybrid_orchestrator.py plan 'Test project'")
    results.append(test_result("Plan decomposition", code == 0 and "RESEARCH" in out))

    code, out, err = run_cmd(f"cd {ORCHESTRATOR_DIR} && python3 hybrid_orchestrator.py camel 'Test task'")
    results.append(test_result("CAMEL workflow creation", code == 0 and "T-" in out))

    print()

    # Test 2: MULTICA Layer
    print("📋 MULTICA LAYER (Task Board)")
    print("-" * 40)

    code, out, err = run_cmd(f"cd {ORCHESTRATOR_DIR} && python3 hybrid_orchestrator.py board")
    results.append(test_result("Board display", code == 0 and "TASK BOARD" in out))

    code, out, err = run_cmd(f"cd {ORCHESTRATOR_DIR} && python3 hybrid_orchestrator.py add 'Test task from test'")
    results.append(test_result("Add task", code == 0 and "Created" in out))

    print()

    # Test 3: EXECUTION Layer
    print("🤖 EXECUTION LAYER (Agents)")
    print("-" * 40)

    code, out, err = run_cmd(f"cd {ORCHESTRATOR_DIR} && python3 orchestrator.py hermes-status")
    results.append(test_result("Hermes status", code == 0 and "available" in out))

    code, out, err = run_cmd(f"cd {ORCHESTRATOR_DIR} && python3 orchestrator.py openclaw-status")
    results.append(test_result("OpenClaw status", code == 0 and "available" in out))

    print()

    # Test 4: MONITOR
    print("📊 MONITOR DAEMON")
    print("-" * 40)

    code, out, err = run_cmd(f"cd {ORCHESTRATOR_DIR} && python3 monitor_daemon.py report")
    results.append(test_result("Monitor report", code == 0 and "DASHBOARD" in out))

    code, out, err = run_cmd(f"cd {ORCHESTRATOR_DIR} && python3 monitor_daemon.py status")
    results.append(test_result("Monitor status (JSON)", code == 0 and '"status"' in out))

    print()

    # Test 5: UNIFIED SCRIPT
    print("🔧 UNIFIED ORCHESTRATE SCRIPT")
    print("-" * 40)

    code, out, err = run_cmd(f"cd {ORCHESTRATOR_DIR} && ./orchestrate.sh help")
    results.append(test_result("Orchestrate help", code == 0 and "CAMEL LAYER" in out))

    code, out, err = run_cmd(f"cd {ORCHESTRATOR_DIR} && ./orchestrate.sh status")
    results.append(test_result("Orchestrate status", code == 0))

    print()

    # Test 6: FILE PERSISTENCE
    print("💾 PERSISTENCE")
    print("-" * 40)

    board_exists = (ORCHESTRATOR_DIR / "tasks" / "hybrid_board.json").exists()
    results.append(test_result("Board file exists", board_exists))

    state_exists = (ORCHESTRATOR_DIR / "state" / "hybrid_state.json").exists()
    results.append(test_result("State file exists", state_exists))

    memory_exists = Path("/memories/orchestrator_memory.md").exists()
    results.append(test_result("Memory file exists", memory_exists))

    print()

    # SUMMARY
    print("=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)

    passed = sum(results)
    total = len(results)
    percentage = (passed / total) * 100

    print(f"Passed: {passed}/{total} ({percentage:.1f}%)")

    if passed == total:
        print("\n🎉 ALL TESTS PASSED!")
    else:
        print(f"\n⚠️  {total - passed} tests failed")

    print("=" * 60)

    return 0 if passed == total else 1

if __name__ == "__main__":
    sys.exit(main())
