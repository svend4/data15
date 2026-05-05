#!/bin/bash
echo "==================================="
echo "Orchestrator v4"
echo "==================================="
STATE_DIR="${STATE_DIR:-state}"
mkdir -p "$STATE_DIR"
python orchestrator_v4.py --state-dir "$STATE_DIR" "$@"
