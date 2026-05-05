#!/bin/bash
# OpenClaw Runner Script v3
# Wrapper for calling OpenClaw from orchestrator.py with proper environment
# Uses: openclaw agent --local --message "..."

# Disable telemetry
export OPENCLAW_NO_TELEMETRY=1

# Use Node.js 22 from manual installation
export PATH="/tmp/node-v22.12.0-linux-x64/bin:$PATH"

# Remove conflicting npm_config_prefix if set
unset npm_config_prefix

# Default settings
TIMEOUT=120
PROMPT=""
OUTPUT_FILE=""

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --prompt)
            PROMPT="$2"
            shift 2
            ;;
        --output)
            OUTPUT_FILE="$2"
            shift 2
            ;;
        --timeout)
            TIMEOUT="$2"
            shift 2
            ;;
        *)
            break
            ;;
    esac
done

# Prepare command
CMD="openclaw"

if [ -n "$PROMPT" ]; then
    # Run agent with local mode and message, using session-id
    SESSION_ID="orchestrator-$(date +%s)"
    if [ -n "$OUTPUT_FILE" ]; then
        timeout "$TIMEOUT" $CMD agent --local --message "$PROMPT" --session-id "$SESSION_ID" --timeout "$TIMEOUT" 2>&1 | tee "$OUTPUT_FILE"
    else
        timeout "$TIMEOUT" $CMD agent --local --message "$PROMPT" --session-id "$SESSION_ID" --timeout "$TIMEOUT" 2>&1
    fi
else
    # Just check version
    $CMD --version
fi