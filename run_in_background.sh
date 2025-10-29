#!/bin/bash
#
# Background daemon that emulates cron behavior
# Runs Slack scraper every 4 hours (at hours 0, 4, 8, 12, 16, 20)
# Ensures only one instance runs at a time using PID file
#

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PID_FILE="$SCRIPT_DIR/.run_in_background.pid"
LOG_FILE="$SCRIPT_DIR/background_daemon.log"

# Function to log messages
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Check if another instance is already running
if [ -f "$PID_FILE" ]; then
    OLD_PID=$(cat "$PID_FILE")
    if ps -p "$OLD_PID" > /dev/null 2>&1; then
        log "ERROR: Another instance is already running (PID: $OLD_PID)"
        log "If you're sure no other instance is running, delete: $PID_FILE"
        exit 1
    else
        log "Stale PID file found (PID $OLD_PID no longer running), removing..."
        rm -f "$PID_FILE"
    fi
fi

# Write our PID to the PID file
echo $$ > "$PID_FILE"
log "Started background daemon (PID: $$)"

# Cleanup function to remove PID file on exit
cleanup() {
    log "Shutting down background daemon (PID: $$)"
    rm -f "$PID_FILE"
    exit 0
}

# Trap signals for clean shutdown
trap cleanup SIGINT SIGTERM EXIT

# Function to calculate seconds until next scheduled run
# Runs at hours: 0, 4, 8, 12, 16, 20
calculate_sleep_seconds() {
    local current_hour=$(date +%H | sed 's/^0//')  # Remove leading zero
    local current_minute=$(date +%M | sed 's/^0//')
    local current_second=$(date +%S | sed 's/^0//')

    # Find next scheduled hour (0, 4, 8, 12, 16, 20)
    local next_hour
    if [ $current_hour -lt 4 ]; then
        next_hour=4
    elif [ $current_hour -lt 8 ]; then
        next_hour=8
    elif [ $current_hour -lt 12 ]; then
        next_hour=12
    elif [ $current_hour -lt 16 ]; then
        next_hour=16
    elif [ $current_hour -lt 20 ]; then
        next_hour=20
    else
        next_hour=24  # Will wrap to 0 (midnight next day)
    fi

    # Calculate seconds until next scheduled hour
    local hours_until_next=$((next_hour - current_hour))
    local seconds_until_next=$((hours_until_next * 3600 - current_minute * 60 - current_second))

    echo $seconds_until_next
}

# Main loop
log "Daemon will run scraper at hours: 0, 4, 8, 12, 16, 20"

while true; do
    # Calculate sleep time until next scheduled run
    SLEEP_SECONDS=$(calculate_sleep_seconds)
    SLEEP_HOURS=$(echo "scale=2; $SLEEP_SECONDS / 3600" | bc)

    log "Next run in $SLEEP_HOURS hours ($SLEEP_SECONDS seconds)"
    log "Sleeping until next scheduled run..."

    # Sleep until next scheduled time
    sleep $SLEEP_SECONDS

    # Run the scraper
    log "=================================================="
    log "Running scheduled Slack scraper job"
    log "=================================================="

    cd "$SCRIPT_DIR"
    ./run_cron.sh >> "$LOG_FILE" 2>&1
    EXIT_CODE=$?

    log "Scraper job completed with exit code: $EXIT_CODE"
    log "=================================================="
done
