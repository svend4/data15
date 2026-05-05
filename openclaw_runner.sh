#!/bin/bash
# OpenClaw Runner Script
echo "==================================="
echo "OpenClaw Agent Runner"
echo "==================================="
if ! command -v openclaw &> /dev/null; then
    echo "OpenClaw not found. Install with: npm install -g openclaw-cli"
fi
echo "Starting OpenClaw..."
openclaw start --config openclaw.json 2>/dev/null || echo "OpenClaw not configured"
