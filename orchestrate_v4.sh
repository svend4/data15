#!/bin/bash
# Orchestrator v4.0 Shell Interface
# All 5 improvements integrated

ORCH_DIR="/workspace/orchestrator"

show_help() {
    cat << 'EOF'
🎯 Hybrid Orchestrator v4.0 - Shell Interface
==============================================

CAMEL LAYER (Strategic):
  ./orchestrate_v4.sh camel <goal> [depth]  - Create workflow
  ./orchestrate_v4.sh plan <goal>           - Show decomposition

MULTICA LAYER (Operations):
  ./orchestrate_v4.sh board                 - Show task board
  ./orchestrate_v4.sh add <title>           - Add new task
  ./orchestrate_v4.sh start <id>            - Start task
  ./orchestrate_v4.sh complete <id>         - Complete task

EXECUTION LAYER (Agents):
  ./orchestrate_v4.sh analyze <topic>        - Hermes analysis (cached)
  ./orchestrate_v4.sh research <topic>      - OpenClaw research
  ./orchestrate_v4.sh both <topic>          - Both parallel

CONFIGURATION (NEW!):
  ./orchestrate_v4.sh config                - Show configuration
  ./orchestrate_v4.sh config set-key <key>  - Set API key
  ./orchestrate_v4.sh config cache-stats    - Show cache stats
  ./orchestrate_v4.sh config cache-clear    - Clear cache
  ./orchestrate_v4.sh config cache-toggle   - Toggle caching

CRON JOBS (NEW!):
  ./orchestrate_v4.sh cron-list             - List scheduled tasks
  ./orchestrate_v4.sh cron-add <name> <cmd> <cron> - Add task
  ./orchestrate_v4.sh cron-del <id>         - Delete task
  ./orchestrate_v4.sh cron-toggle <id>      - Enable/disable task

SYSTEM:
  ./orchestrate_v4.sh status                - Show status
  ./orchestrate_v4.sh web [--port 5000]     - Start web UI
  ./orchestrate_v4.sh help                  - This help

Improvements Applied:
  ✅ 1. API Key Configuration
  ✅ 2. Cron Job Integration
  ✅ 3. Flask Web UI
  ✅ 4. Deprecation Fixed
  ✅ 5. JSON Caching

EOF
}

case "$1" in
    camel|plan|board|add|start|complete|analyze|research|both|status|config|cron-list|cron-add|cron-del|cron-toggle)
        cd "$ORCH_DIR" && python3 orchestrator_v4.py "$@"
        ;;
    web)
        cd "$ORCH_DIR" && python3 orchestrator_v4.py --web "${@:2}"
        ;;
    help|-h|--help)
        show_help
        ;;
    *)
        echo "❌ Unknown command: $1"
        show_help
        exit 1
        ;;
esac
