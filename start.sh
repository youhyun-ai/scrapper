#!/bin/bash
# Starts both the scheduler and the Streamlit dashboard.
# Usage: ./start.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Activate the virtual environment
source venv/bin/activate

# Track background PIDs for cleanup
SCHEDULER_PID=""

cleanup() {
    echo ""
    echo "Shutting down..."
    if [ -n "$SCHEDULER_PID" ] && kill -0 "$SCHEDULER_PID" 2>/dev/null; then
        kill "$SCHEDULER_PID"
        wait "$SCHEDULER_PID" 2>/dev/null
        echo "Scheduler stopped."
    fi
    echo "Done."
    exit 0
}

trap cleanup SIGINT SIGTERM

# Start the scheduler in the background
echo "Starting scheduler..."
python scheduler.py &
SCHEDULER_PID=$!

# Start the Streamlit dashboard in the foreground
echo "Starting Streamlit dashboard..."
streamlit run app.py

# If streamlit exits on its own, clean up the scheduler
cleanup
