#!/bin/bash
#
# Automated cron job for Slack talk scraping and calendar updates
# Runs every 4 hours, scrapes last 4 hours of messages, posts logs to Slack
#

# Change to script directory
cd "$(dirname "$0")"

# Load environment variables from .env file
if [ -f .env ]; then
    # Parse .env file, handle spaces around = and quoted values
    while IFS= read -r line || [ -n "$line" ]; do
        # Skip empty lines and comments
        [[ -z "$line" || "$line" =~ ^[[:space:]]*# ]] && continue

        # Remove leading/trailing whitespace and export
        if [[ "$line" =~ ^[[:space:]]*([A-Za-z_][A-Za-z0-9_]*)[[:space:]]*=[[:space:]]*(.+)[[:space:]]*$ ]]; then
            key="${BASH_REMATCH[1]}"
            value="${BASH_REMATCH[2]}"
            # Remove quotes if present
            value="${value%\"}"
            value="${value#\"}"
            export "$key=$value"
        fi
    done < .env
fi

# Calculate time window (last 4 hours to now)
END_DT=$(date -u +"%Y-%m-%dT%H:%M:%S")
START_DT=$(date -u -d '4 hours ago' +"%Y-%m-%dT%H:%M:%S")

echo "=================================================="
echo "Starting Slack scraper cron job"
echo "Time window: $START_DT to $END_DT"
echo "=================================================="

# Run the pipeline and capture ALL output
LOG_OUTPUT=$(uv run python run_stages.py \
    --start_datetime "$START_DT" \
    --end_datetime "$END_DT" \
    --use-cache \
    --overwrite 2>&1)

# Store exit code
EXIT_CODE=$?

echo "Pipeline completed with exit code: $EXIT_CODE"

# Post to Slack using Python script
uv run python run_upload_to_calendarbotlogs.py "$START_DT" "$END_DT" "$LOG_OUTPUT"

echo "=================================================="
echo "Cron job completed"
echo "=================================================="

exit $EXIT_CODE
