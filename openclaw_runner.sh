#!/bin/bash
# OpenClaw Runner Script v4
# Wrapper for calling OpenClaw agent from orchestrator with correct environment.
# Usage: ./openclaw_runner.sh --prompt "text" [--output file] [--timeout 120]

set -euo pipefail

# ── Environment ─────────────────────────────────────────────────────────────
export OPENCLAW_NO_TELEMETRY=1
unset npm_config_prefix

# Try to locate Node.js / OpenClaw via NVM
NVM_NODE_PATHS=(
    "$HOME/.nvm/versions/node/v22.22.2/bin"
    "$HOME/.nvm/versions/node/v22.12.0/bin"
    "/tmp/node-v22.12.0-linux-x64/bin"
    "/home/minimax/.nvm/versions/node/v22.22.2/bin"
)
for p in "${NVM_NODE_PATHS[@]}"; do
    [ -d "$p" ] && export PATH="$p:$PATH" && break
done

# ── Argument parsing ─────────────────────────────────────────────────────────
TIMEOUT=120
PROMPT=""
OUTPUT_FILE=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --prompt)   PROMPT="$2";      shift 2 ;;
        --output)   OUTPUT_FILE="$2"; shift 2 ;;
        --timeout)  TIMEOUT="$2";     shift 2 ;;
        *)          break ;;
    esac
done

# ── Locate openclaw binary ───────────────────────────────────────────────────
OPENCLAW_BIN=$(command -v openclaw 2>/dev/null || true)
if [ -z "$OPENCLAW_BIN" ]; then
    echo "[openclaw_runner] ERROR: openclaw binary not found in PATH" >&2
    echo "PATH=$PATH" >&2
    exit 1
fi

# ── Execute ──────────────────────────────────────────────────────────────────
if [ -z "$PROMPT" ]; then
    # No prompt — just print version
    "$OPENCLAW_BIN" --version 2>&1
    exit 0
fi

SESSION_ID="orch-$(date +%s)"

run_openclaw() {
    # OpenClaw agent mode: pass prompt as --message argument
    timeout "$TIMEOUT" "$OPENCLAW_BIN" agent \
        --local \
        --message "$PROMPT" \
        --session-id "$SESSION_ID" \
        2>&1
}

if [ -n "$OUTPUT_FILE" ]; then
    run_openclaw | tee "$OUTPUT_FILE"
else
    run_openclaw
fi
