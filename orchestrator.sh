#!/bin/bash
# Orchestrator CLI - Task Board Management
# Usage: ./orchestrator.sh [command] [args]

ORCHESTRATOR_DIR="/workspace/orchestrator"
BOARD_FILE="$ORCHESTRATOR_DIR/tasks/board.json"
LOGS_DIR="$ORCHESTRATOR_DIR/logs"
STATE_FILE="$ORCHESTRATOR_DIR/state/orchestrator_state.json"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Ensure directories exist
mkdir -p "$ORCHESTRATOR_DIR/tasks" "$LOGS_DIR" "$ORCHESTRATOR_DIR/state"

# Generate task ID
generate_id() {
    local count=$(jq '.stats.total' "$BOARD_FILE")
    echo "T-$(printf "%03d" $((count + 1)))"
}

# Get current timestamp
timestamp() {
    date -u +"%Y-%m-%dT%H:%M:%SZ"
}

# Add a new task
add_task() {
    local title="$1"
    local agent="${2:-Hermes}"
    local priority="${3:-medium}"
    local description="${4:-}"

    local id=$(generate_id)
    local created=$(timestamp)

    # Create task object
    local task=$(cat <<EOF
{
  "id": "$id",
  "title": "$title",
  "description": "$description",
  "agent": "$agent",
  "status": "queued",
  "priority": "$priority",
  "created": "$created",
  "updated": "$created",
  "progress": 0,
  "dependencies": [],
  "blocked_by": [],
  "subtasks": [],
  "comments": [],
  "runs": []
}
EOF
)

    # Add to board
    local temp=$(mktemp)
    jq ".tasks += [$task] | .stats.total += 1 | .stats.queued += 1 | .updated = \"$(timestamp)\"" "$BOARD_FILE" > "$temp"
    mv "$temp" "$BOARD_FILE"

    echo -e "${GREEN}✓${NC} Task created: $id - $title"
    echo "  Agent: $agent, Priority: $priority"
}

# List all tasks
list_tasks() {
    echo -e "${BLUE}=== Orchestrator Task Board ===${NC}"
    echo ""

    # Stats
    echo -e "${YELLOW}Stats:${NC}"
    echo "  Total: $(jq '.stats.total' $BOARD_FILE)"
    echo "  Queued: $(jq '.stats.queued' $BOARD_FILE)"
    echo "  Running: $(jq '.stats.running' $BOARD_FILE)"
    echo "  Completed: $(jq '.stats.completed' $BOARD_FILE)"
    echo "  Failed: $(jq '.stats.failed' $BOARD_FILE)"
    echo ""

    # Tasks by status
    echo -e "${YELLOW}=== QUEUED ===${NC}"
    jq -r '.tasks[] | select(.status == "queued") | "  [\( .id )] \( .title ) - \( .agent ) [\( .priority )]"' "$BOARD_FILE" 2>/dev/null || echo "  (empty)"

    echo ""
    echo -e "${BLUE}=== RUNNING ===${NC}"
    jq -r '.tasks[] | select(.status == "running") | "  [\( .id )] \( .title ) - \( .agent ) [\( .progress )%]" ' "$BOARD_FILE" 2>/dev/null || echo "  (empty)"

    echo ""
    echo -e "${GREEN}=== COMPLETED ===${NC}"
    jq -r '.tasks[] | select(.status == "completed") | "  [\( .id )] \( .title ) - \( .agent )"' "$BOARD_FILE" 2>/dev/null || echo "  (empty)"
}

# Show specific task
show_task() {
    local id="$1"
    jq ".tasks[] | select(.id == \"$id\")" "$BOARD_FILE"
}

# Update task status
update_status() {
    local id="$1"
    local new_status="$2"
    local progress="${3:-0}"

    local old_status=$(jq -r ".tasks[] | select(.id == \"$id\") | .status" "$BOARD_FILE")

    # Update stats
    local temp=$(mktemp)
    jq "if .stats.$old_status > 0 then .stats.$old_status -= 1 else . end | .stats.$new_status += 1" "$BOARD_FILE" > "$temp"
    mv "$temp" "$BOARD_FILE"

    # Update task
    temp=$(mktemp)
    jq "(.tasks[] | select(.id == \"$id\")) |= { .status = \"$new_status\", .progress = $progress, .updated = \"$(timestamp)\" }" "$BOARD_FILE" > "$temp"
    mv "$temp" "$BOARD_FILE"

    echo -e "${GREEN}✓${NC} Task $id status: $old_status → $new_status"
}

# Assign task to agent
assign_task() {
    local id="$1"
    local agent="$2"

    local temp=$(mktemp)
    jq "(.tasks[] | select(.id == \"$id\")) |= { .agent = \"$agent\", .status = \"dispatched\", .updated = \"$(timestamp)\" }" "$BOARD_FILE" > "$temp"
    mv "$temp" "$BOARD_FILE"

    echo -e "${GREEN}✓${NC} Task $id assigned to $agent"
}

# Add comment to task
add_comment() {
    local id="$1"
    local comment="$2"
    local author="${3:-Orchestrator}"

    local temp=$(mktemp)
    jq "(.tasks[] | select(.id == \"$id\")) | .comments += [{\"author\": \"$author\", \"text\": \"$comment\", \"timestamp\": \"$(timestamp)\"}]" "$BOARD_FILE" > "$temp"

    # Reconstruct with updated comment
    local task=$(jq ".tasks[] | select(.id == \"$id\")" "$BOARD_FILE")
    jq "(.tasks[] | select(.id == \"$id\")) |= $task" "$temp" > "${temp}.tmp"
    mv "${temp}.tmp" "$BOARD_FILE"
    rm -f "$temp"

    echo -e "${GREEN}✓${NC} Comment added to $id"
}

# Block task
block_task() {
    local id="$1"
    local blocked_by="$2"

    local temp=$(mktemp)
    jq "(.tasks[] | select(.id == \"$id\")) |= { .status = \"blocked\", .blocked_by = [.blocked_by[], \"$blocked_by\"], .updated = \"$(timestamp)\" } | .stats.queued -= 1 | .stats.blocked += 1" "$BOARD_FILE" > "$temp"
    mv "$temp" "$BOARD_FILE"

    echo -e "${YELLOW}!${NC} Task $id blocked by $blocked_by"
}

# Complete task
complete_task() {
    local id="$1"
    update_status "$id" "completed" 100

    # Create completion log
    echo "[$(timestamp)] Task $id completed" >> "$LOGS_DIR/$id.log"
}

# Show logs
show_logs() {
    local id="$1"
    if [ -f "$LOGS_DIR/$id.log" ]; then
        cat "$LOGS_DIR/$id.log"
    else
        echo "No logs for $id"
    fi
}

# Clear board
clear_board() {
    echo -n "Clear all tasks? (y/n): "
    read confirm
    if [ "$confirm" == "y" ]; then
        echo '{"version": "1.0", "created": "'$(timestamp)'", "updated": "'$(timestamp)'", "stats": {"total": 0, "queued": 0, "running": 0, "completed": 0, "failed": 0, "blocked": 0}, "tasks": []}' > "$BOARD_FILE"
        echo -e "${GREEN}✓${NC} Board cleared"
    fi
}

# Show help
show_help() {
    echo -e "${BLUE}Orchestrator CLI Commands:${NC}"
    echo ""
    echo "  ./orchestrator.sh add '<title>' [agent] [priority] [description]"
    echo "  ./orchestrator.sh list"
    echo "  ./orchestrator.sh show <task-id>"
    echo "  ./orchestrator.sh status <task-id> <status> [progress]"
    echo "  ./orchestrator.sh assign <task-id> <agent>"
    echo "  ./orchestrator.sh comment <task-id> '<comment>' [author]"
    echo "  ./orchestrator.sh block <task-id> <blocked-by-id>"
    echo "  ./orchestrator.sh complete <task-id>"
    echo "  ./orchestrator.sh logs <task-id>"
    echo "  ./orchestrator.sh clear"
    echo ""
    echo "Agents: Hermes, OpenClaw, Researcher, ReportWriter, DocxProcessor"
    echo "Statuses: queued, dispatched, running, completed, failed, blocked"
    echo "Priorities: low, medium, high, critical"
}

# Main command handler
case "$1" in
    add)
        add_task "$2" "$3" "$4" "$5"
        ;;
    list)
        list_tasks
        ;;
    show)
        show_task "$2"
        ;;
    status)
        update_status "$2" "$3" "$4"
        ;;
    assign)
        assign_task "$2" "$3"
        ;;
    comment)
        add_comment "$2" "$3" "$4"
        ;;
    block)
        block_task "$2" "$3"
        ;;
    complete)
        complete_task "$2"
        ;;
    logs)
        show_logs "$2"
        ;;
    clear)
        clear_board
        ;;
    help|--help|-h)
        show_help
        ;;
    *)
        echo -e "${RED}Unknown command:${NC} $1"
        show_help
        ;;
esac