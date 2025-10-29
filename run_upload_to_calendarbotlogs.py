#!/usr/bin/env python3
"""
Upload logs to #calendarbotlogs channel in ZLab workspace
"""

import os
import sys
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError


def post_to_slack(start_dt: str, end_dt: str, logs: str) -> bool:
    """
    Post logs to #calendarbotlogs channel

    Args:
        start_dt: Start datetime string
        end_dt: End datetime string
        logs: Full log output to post

    Returns:
        True if successful, False otherwise
    """
    # Get token from environment
    token = os.getenv('ZLLAB_SLACK_TOKEN')
    if not token:
        print("ERROR: ZLLAB_SLACK_TOKEN not set", file=sys.stderr)
        return False

    # Initialize Slack client
    client = WebClient(token=token)

    # Format message
    message = f"Just ran from {start_dt} to {end_dt}, logs:\n```\n{logs}\n```"

    # Truncate if too long (Slack has 40k character limit for messages)
    if len(message) > 40000:
        truncated_logs = logs[:39000]
        message = f"Just ran from {start_dt} to {end_dt}, logs (truncated):\n```\n{truncated_logs}\n...\n[truncated]\n```"

    try:
        # Post to #calendarbotlogs
        response = client.chat_postMessage(
            channel='calendarbotlogs',
            text=message,
            unfurl_links=False,
            unfurl_media=False
        )

        if response['ok']:
            print("✓ Logs posted to #calendarbotlogs")
            return True
        else:
            print(f"✗ Failed to post to Slack: {response}", file=sys.stderr)
            return False

    except SlackApiError as e:
        print(f"✗ Slack API error: {e.response['error']}", file=sys.stderr)
        return False
    except Exception as e:
        print(f"✗ Error posting to Slack: {e}", file=sys.stderr)
        return False


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python run_upload_to_calendarbotlogs.py <start_dt> <end_dt> <logs>")
        sys.exit(1)

    start_dt = sys.argv[1]
    end_dt = sys.argv[2]
    logs = sys.argv[3]

    success = post_to_slack(start_dt, end_dt, logs)
    sys.exit(0 if success else 1)
