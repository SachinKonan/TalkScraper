#!/usr/bin/env python3
"""
Stage 1: Slack Message Scraper
Scrapes public/external Slack channels and outputs standardized JSON
"""

import os
import json
from pathlib import Path
from datetime import datetime
from typing import Optional
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from .models import SlackMessage
from .init_config import SLACK_CONFIG


def get_user_name(client: WebClient, user_id: str, user_cache: dict) -> str:
    """Get user's display name with caching"""
    if user_id in user_cache:
        return user_cache[user_id]

    try:
        response = client.users_info(user=user_id)
        user = response['user']
        name = user.get('real_name') or user.get('name') or user_id
        user_cache[user_id] = name
        return name
    except SlackApiError:
        user_cache[user_id] = user_id
        return user_id


def extract_text_from_message(message: dict, client, user_cache: dict) -> str:
    """Extract text content from Slack message and replace user mentions with names"""
    import re

    text_parts = []

    # Main message text
    if 'text' in message:
        text_parts.append(message['text'])

    # Attachments
    if 'attachments' in message:
        for att in message['attachments']:
            if 'text' in att:
                text_parts.append(att['text'])
            if 'fallback' in att:
                text_parts.append(att['fallback'])

    # Blocks (for rich formatting)
    if 'blocks' in message:
        for block in message['blocks']:
            if block.get('type') == 'rich_text':
                for element in block.get('elements', []):
                    for item in element.get('elements', []):
                        if item.get('type') == 'text':
                            text_parts.append(item.get('text', ''))

    text = ' '.join(text_parts).strip()

    # Replace Slack user mentions (<@U07LR0EMG2H>) with actual names
    def replace_mention(match):
        user_id = match.group(1)
        return get_user_name(client, user_id, user_cache)

    text = re.sub(r'<@(U\w+)>', replace_mention, text)

    return text


def scrape_workspace(
    workspace_config: dict,
    start_timestamp: float,
    end_timestamp: float
) -> list[SlackMessage]:
    """
    Scrape a single workspace and return list of SlackMessage objects

    Args:
        workspace_config: Workspace configuration from SLACK_CONFIG
        start_timestamp: Unix timestamp for start of range
        end_timestamp: Unix timestamp for end of range

    Returns:
        List of SlackMessage objects
    """
    workspace_name = workspace_config['workspace_name']
    token = os.getenv(workspace_config['token_env_var'])

    if not token:
        raise ValueError(f"Missing token for {workspace_name}: {workspace_config['token_env_var']}")

    client = WebClient(token=token)
    user_cache = {}
    messages = []
    message_index = 0  # Track message index for traceability

    try:
        # Get list of channels (with pagination)
        channels = []
        cursor = None

        while True:
            response = client.conversations_list(
                types='public_channel,private_channel',
                exclude_archived=True,
                cursor=cursor,
                limit=200
            )

            channels.extend(response['channels'])

            cursor = response.get('response_metadata', {}).get('next_cursor')
            if not cursor:
                break

        for channel in channels:
            channel_name = channel['name']
            channel_id = channel['id']
            is_external = channel.get('is_ext_shared', False)
            is_private = channel.get('is_private', False)

            # Skip aggregated-talks
            if channel_name == 'aggregated-talks':
                continue

            # Determine channel type (RFC only allows 'external' or 'public')
            if is_external:
                channel_type = 'external'
            else:
                channel_type = 'public'

            # Skip private channels that aren't external-shared
            if is_private and not is_external:
                continue

            # Get messages in time range
            try:
                history = client.conversations_history(
                    channel=channel_id,
                    oldest=str(start_timestamp),
                    latest=str(end_timestamp),
                    limit=1000
                )

                for msg in history.get('messages', []):
                    # Skip threaded replies (only count top-level messages)
                    if msg.get('thread_ts') and msg.get('ts') != msg.get('thread_ts'):
                        continue

                    user_id = msg.get('user', 'unknown')
                    user_name = get_user_name(client, user_id, user_cache)

                    # Extract text (replaces user mentions with names)
                    textract = extract_text_from_message(msg, client, user_cache)

                    if not textract:
                        continue

                    # Convert timestamp to datetime
                    ts = float(msg['ts'])
                    dt = datetime.fromtimestamp(ts).strftime('%Y-%m-%dT%H:%M:%S')

                    # Get permalink (as list for aggregation support)
                    try:
                        permalink_response = client.chat_getPermalink(
                            channel=channel_id,
                            message_ts=msg['ts']
                        )
                        permalink = [permalink_response['permalink']]
                    except SlackApiError:
                        permalink = []

                    # File paths (for now, empty - RFC doesn't specify handling)
                    file_paths = []

                    slack_msg = SlackMessage(
                        workspace_name=workspace_name,
                        channel_name=channel_name,
                        channel_type=channel_type,
                        sending_user_name=user_name,
                        datetime=dt,
                        textract=textract,
                        file_paths=file_paths,
                        permalink=permalink,
                        original_indices=[message_index]
                    )
                    messages.append(slack_msg)
                    message_index += 1

            except SlackApiError as e:
                # Skip silently if bot not in channel (like old code)
                if e.response.get('error') == 'not_in_channel':
                    continue
                # Print other errors
                print(f"Error fetching messages from #{channel_name}: {e}")
                continue

    except SlackApiError as e:
        raise RuntimeError(f"Failed to fetch channels for {workspace_name}: {e}")

    return messages


def scrape_all_workspaces(
    start_dt: datetime,
    end_dt: datetime,
    output_path: Optional[Path] = None
) -> list[SlackMessage]:
    """
    Scrape all configured workspaces and save to JSON

    Args:
        start_dt: Start datetime
        end_dt: End datetime
        output_path: Optional path to save JSON output

    Returns:
        List of all SlackMessage objects
    """
    start_timestamp = start_dt.timestamp()
    end_timestamp = end_dt.timestamp()

    all_messages = []

    for workspace_config in SLACK_CONFIG:
        workspace_name = workspace_config['workspace_name']
        print(f"Scraping workspace: {workspace_name}")

        try:
            messages = scrape_workspace(workspace_config, start_timestamp, end_timestamp)
            all_messages.extend(messages)
            print(f"  Collected {len(messages)} messages")
        except Exception as e:
            print(f"  Error: {e}")
            continue

    # Save to JSON if path provided
    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(
                [msg.model_dump() for msg in all_messages],
                f,
                indent=2,
                ensure_ascii=False
            )
        print(f"\nSaved {len(all_messages)} messages to {output_path}")

    return all_messages


def main(start_dt: datetime, end_dt: datetime, output_path: Path) -> Path:
    """
    Main entry point for Stage 1

    Args:
        start_dt: Start datetime
        end_dt: End datetime
        output_path: Path to save JSON output

    Returns:
        Path to saved JSON file
    """
    print("="*80)
    print("STAGE 1: Slack Message Scraping")
    print("="*80)
    print(f"Time range: {start_dt} to {end_dt}")
    print(f"Output: {output_path}")
    print("="*80)

    messages = scrape_all_workspaces(start_dt, end_dt, output_path)

    print(f"\n✅ Stage 1 complete: {len(messages)} messages scraped")
    return output_path
