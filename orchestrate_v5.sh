#!/bin/bash
echo "==================================="
echo "Multi-Agent Hybrid Orchestrator v5.0"
echo "==================================="
STATE_DIR="${STATE_DIR:-state}"
PORT="${PORT:-5000}"
mkdir -p "$STATE_DIR" logs cache
python orchestrator_v5.py --state-dir "$STATE_DIR" --port "$PORT" "$@"
