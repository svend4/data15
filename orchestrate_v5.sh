#!/bin/bash
# Orchestrator v5.0 - PRODUCTION Shell Interface
# Part I: WebSocket, Rate Limiting, RBAC

ORCH_DIR="/workspace/orchestrator"

show_help() {
    cat << 'EOF'
🎯 Hybrid Orchestrator v5.0 - PRODUCTION
==========================================

TASK MANAGEMENT:
  ./orchestrate_v5.sh board              - Task board
  ./orchestrate_v5.sh add <title>       - Add task
  ./orchestrate_v5.sh analyze <topic>  - Hermes analysis
  ./orchestrate_v5.sh both <topic>      - Both agents
  ./orchestrate_v5.sh camel <goal> [d]  - CAMEL workflow

CONFIGURATION:
  ./orchestrate_v5.sh config            - Show config
  ./orchestrate_v5.sh config cache-stats - Cache stats
  ./orchestrate_v5.sh config cache-clear - Clear cache

CRON JOBS:
  ./orchestrate_v5.sh cron-list         - List tasks
  ./orchestrate_v5.sh cron-add <n> <c> <s> - Add

USERS (RBAC):
  ./orchestrate_v5.sh user-list         - List users
  ./orchestrate_v5.sh user-add <u> <p> [r] - Add user

MONITORING:
  ./orchestrate_v5.sh events [type] [n] - Recent events
  ./orchestrate_v5.sh rate-limit        - Rate limit status

SYSTEM:
  ./orchestrate_v5.sh status            - Full status
  ./orchestrate_v5.sh help              - This help

Part I Features:
  ✅ WebSocket Event Bus
  ✅ Rate Limiting + Retry
  ✅ Role-Based Access Control
  ✅ PostgreSQL Ready
  ✅ Message Queue Ready
EOF
}

case "$1" in
    board|add|analyze|research|both|status|config|camel)
        cd "$ORCH_DIR" && python3 orchestrator_v5.py "$@"
        ;;
    user-list|user-add|events|rate-limit|cron-list|cron-add)
        cd "$ORCH_DIR" && python3 orchestrator_v5.py "$@"
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
