#!/bin/bash
# ============================================================================
# Multi-Agent Orchestrator - Main Entry Point
# ============================================================================
# This script provides unified access to all orchestrator components.
#
# Components:
#   - CAMEL Layer: Strategic planning and goal decomposition
#   - MULTICA Layer: Task board management
#   - EXECUTION Layer: Hermes (internal) and OpenClaw (external) agents
#   - Monitor: Real-time task monitoring
#
# Usage:
#   ./orchestrate.sh [command] [args]
#
# ============================================================================

set -e

ORCHESTRATOR_DIR="/workspace/orchestrator"
PYTHON="python3"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# ============================================================================
# Help
# ============================================================================

show_help() {
    echo -e "${CYAN}Multi-Agent Orchestrator${NC}"
    echo "=============================="
    echo ""
    echo -e "${GREEN}CAMEL LAYER (Strategic):${NC}"
    echo "  ./orchestrate.sh camel <goal>      Create workflow from goal"
    echo "  ./orchestrate.sh plan <goal>       Show decomposition"
    echo ""
    echo -e "${GREEN}MULTICA LAYER (Tasks):${NC}"
    echo "  ./orchestrate.sh board             Show task board"
    echo "  ./orchestrate.sh add <title>       Add new task"
    echo "  ./orchestrate.sh start <id>        Start task"
    echo "  ./orchestrate.sh complete <id>     Complete task"
    echo ""
    echo -e "${GREEN}EXECUTION LAYER (Agents):${NC}"
    echo "  ./orchestrate.sh analyze <topic>   Hermes: internal analysis"
    echo "  ./orchestrate.sh research <topic>  OpenClaw: external research"
    echo "  ./orchestrate.sh both <topic>      Both agents parallel"
    echo ""
    echo -e "${GREEN}MONITORING:${NC}"
    echo "  ./orchestrate.sh monitor           Real-time monitor"
    echo "  ./orchestrate.sh status            Quick status check"
    echo "  ./orchestrate.sh help              Show this help"
    echo ""
}

# ============================================================================
# CAMEL Commands
# ============================================================================

cmd_camel() {
    local goal="$*"
    if [ -z "$goal" ]; then
        echo "Usage: ./orchestrate.sh camel <goal>"
        exit 1
    fi
    $PYTHON $ORCHESTRATOR_DIR/hybrid_orchestrator.py camel "$goal"
}

cmd_plan() {
    local goal="$*"
    if [ -z "$goal" ]; then
        echo "Usage: ./orchestrate.sh plan <goal>"
        exit 1
    fi
    $PYTHON $ORCHESTRATOR_DIR/hybrid_orchestrator.py plan "$goal"
}

# ============================================================================
# MULTICA Commands
# ============================================================================

cmd_board() {
    $PYTHON $ORCHESTRATOR_DIR/hybrid_orchestrator.py board
}

cmd_add() {
    local title="$*"
    if [ -z "$title" ]; then
        echo "Usage: ./orchestrate.sh add <title>"
        exit 1
    fi
    $PYTHON $ORCHESTRATOR_DIR/hybrid_orchestrator.py add "$title"
}

cmd_start() {
    local id="$1"
    if [ -z "$id" ]; then
        echo "Usage: ./orchestrate.sh start <task-id>"
        exit 1
    fi
    $PYTHON $ORCHESTRATOR_DIR/hybrid_orchestrator.py start "$id"
}

cmd_complete() {
    local id="$1"
    if [ -z "$id" ]; then
        echo "Usage: ./orchestrate.sh complete <task-id>"
        exit 1
    fi
    $PYTHON $ORCHESTRATOR_DIR/hybrid_orchestrator.py complete "$id"
}

# ============================================================================
# EXECUTION Commands
# ============================================================================

cmd_analyze() {
    local topic="$*"
    if [ -z "$topic" ]; then
        echo "Usage: ./orchestrate.sh analyze <topic>"
        exit 1
    fi
    $PYTHON $ORCHESTRATOR_DIR/hybrid_orchestrator.py analyze "$topic"
}

cmd_research() {
    local topic="$*"
    if [ -z "$topic" ]; then
        echo "Usage: ./orchestrate.sh research <topic>"
        exit 1
    fi
    $PYTHON $ORCHESTRATOR_DIR/hybrid_orchestrator.py research "$topic"
}

cmd_both() {
    local topic="$*"
    if [ -z "$topic" ]; then
        echo "Usage: ./orchestrate.sh both <topic>"
        exit 1
    fi
    $PYTHON $ORCHESTRATOR_DIR/hybrid_orchestrator.py both "$topic"
}

# ============================================================================
# MONITORING Commands
# ============================================================================

cmd_monitor() {
    echo -e "${YELLOW}Starting monitor (Ctrl+C to stop)...${NC}"
    $PYTHON $ORCHESTRATOR_DIR/monitor_daemon.py watch
}

cmd_status() {
    $PYTHON $ORCHESTRATOR_DIR/monitor_daemon.py status
}

# ============================================================================
# Main
# ============================================================================

COMMAND="$1"
shift 2>/dev/null || true

case "$COMMAND" in
    # CAMEL Layer
    camel|plan)
        cmd_$COMMAND "$@"
        ;;

    # MULTICA Layer
    board|add)
        cmd_$COMMAND "$@"
        ;;
    start|complete)
        cmd_$COMMAND "$1"
        ;;

    # EXECUTION Layer
    analyze|research|both)
        cmd_$COMMAND "$@"
        ;;

    # MONITORING
    monitor|status)
        cmd_$COMMAND
        ;;

    # Help
    help|--help|-h|"")
        show_help
        ;;

    # Legacy commands (redirect to original orchestrator)
    *)
        echo -e "${RED}Unknown command: $COMMAND${NC}"
        echo "Use ./orchestrate.sh help for available commands"
        exit 1
        ;;
esac
