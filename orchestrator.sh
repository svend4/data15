#!/bin/bash
echo "==================================="
echo "Multi-Agent Orchestrator"
echo "==================================="
STATE_DIR="state"
mkdir -p "$STATE_DIR"
python orchestrator.py "$@"
