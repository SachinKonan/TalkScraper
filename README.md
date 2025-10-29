# Slack Talk Scraper & Calendar Bot

Automated system that scrapes academic talk announcements from Slack workspaces, extracts event details using Gemini AI, and adds them to Google Calendar.

## What It Does

This bot monitors multiple Slack workspaces for academic talk/event announcements, processes them through a 4-stage pipeline, and automatically adds them to a shared Google Calendar. It runs continuously in the background, checking for new messages every 4 hours.

## Information Flow

```
Slack Workspaces → Stage 1: Scrape → Stage 2: Aggregate → Stage 3: Gemini Extract → Stage 4: Calendar + Logs
```

### Monitored Slack Workspaces (configured in `lib/init_config.py`):
1. **ZLab** (`zhuanglabatprinceton.slack.com`)
   - Uses `ZLLAB_SLACK_TOKEN`
   - Posts logs to `#calendarbotlogs` channel

2. **AI Lab @ Princeton** (`ailab-princeton.slack.com`)
   - Uses `AILAB_SLACK_TOKEN`

3. **PrincetonCSGrad** (`princetoncsgrad.slack.com`)
   - Uses `CSGRAD_SLACK_TOKEN`

### Pipeline Stages:

**Stage 1 - Scraping** (`lib/stage1.py`)
- Scrapes messages from all channels in configured workspaces
- Replaces Slack user mentions (`<@U123>`) with actual names
- Extracts text, files, permalinks, timestamps
- Output: `output/stage1_messages_[start]_[end].json`

**Stage 2 - Aggregation** (`lib/stage2_aggregate.py`)
- Groups messages within 30-minute windows by workspace/channel/user
- Combines related messages with `[ADDITIONAL MESSAGE]` separator
- Output: `output/stage2_aggregated_[start]_[end].json`

**Stage 3 - Gemini Extraction** (`lib/stage3.py`)
- Uses Gemini 2.0 Flash with parallel sampling (candidate_count=3)
- Extracts 4 event types: Physical/Virtual Talk/Event
- Picks candidate with highest information density
- Resolves relative dates ("tomorrow", "Friday") using message timestamp
- Infers missing times (lunch timing, 3-hour talks)
- Output: `output/stage3_events_[start]_[end].json`

**Stage 4 - Calendar Integration** (`lib/stage4.py`)
- Adds events to Google Calendar (`sachinbronan@gmail.com`)
- Handles recurring events (weekly/biweekly/monthly)
- Deduplicates and overwrites existing events
- Reminders: 1 day before, 1 hour before
- No logs written to file (done by cron wrapper)

**Slack Logging** (`run_upload_to_calendarbotlogs.py`)
- Posts all stdout/stderr to `#calendarbotlogs` in ZLab workspace
- Format: "Just ran from [start] to [end], logs: ```[logs]```"
- Truncates if over 40k characters

## Code Structure

```
lib/
├── init_config.py          # Workspace/token/calendar configuration
├── models.py               # Pydantic models for all data structures
├── stage1.py               # Slack API scraping with user mention replacement
├── stage2_aggregate.py     # Time-window based message aggregation
├── stage3.py               # Gemini extraction with parallel sampling
└── stage4.py               # Google Calendar integration

run_stages.py               # Main pipeline orchestrator
run_cron.sh                 # Cron wrapper: time calculation + log capture
run_upload_to_calendarbotlogs.py  # Slack log posting
run_in_background.sh        # Background daemon (emulates cron)

tests/
├── test_stage1.py          # 16 tests for scraping
├── test_stage3.py          # 20 tests for extraction
└── test_stage4.py          # 23 tests for calendar integration
```

## Running the Bot

### Option 1: Background Daemon (Recommended)

Runs automatically every 4 hours at: 0:00, 4:00, 8:00, 12:00, 16:00, 20:00

```bash
# Start the daemon
nohup ./run_in_background.sh &

# Check if running
ps aux | grep run_in_background.sh

# View logs
tail -f background_daemon.log

# Stop the daemon
kill $(cat .run_in_background.pid)
```

### Option 2: Manual Run

```bash
# Run pipeline for specific date range
uv run python run_stages.py \
    --start_datetime 2025-10-01T00:00:00 \
    --end_datetime 2025-10-30T00:00:00 \
    --overwrite
```

## Command-Line Arguments (`run_stages.py`)

**Required:**
- `--start_datetime` - Start time in ISO format (YYYY-MM-DDTHH:MM:SS)
- `--end_datetime` - End time in ISO format (YYYY-MM-DDTHH:MM:SS)

**Optional:**
- `--use-cache` - Skip stages if output files exist (saves API calls)
- `--overwrite` - Delete and recreate existing calendar events
- `--skip-calendar` - Run stages 1-3 only (no calendar updates)

**Examples:**

```bash
# Full run with overwrite
uv run python run_stages.py \
    --start_datetime 2025-10-01T00:00:00 \
    --end_datetime 2025-10-30T00:00:00 \
    --overwrite

# Use cached results where available
uv run python run_stages.py \
    --start_datetime 2025-10-01T00:00:00 \
    --end_datetime 2025-10-30T00:00:00 \
    --use-cache \
    --overwrite

# Test extraction only (no calendar)
uv run python run_stages.py \
    --start_datetime 2025-10-29T00:00:00 \
    --end_datetime 2025-10-29T23:59:59 \
    --skip-calendar
```

## Environment Variables (`.env`)

```bash
ZLLAB_SLACK_TOKEN="xoxb-..."        # ZLab workspace token
AILAB_SLACK_TOKEN="xoxb-..."        # AI Lab workspace token
CSGRAD_SLACK_TOKEN="xoxb-..."       # CS Grad workspace token
GEMINI_API_KEY="AIzaSy..."          # Google Gemini API key
```