#!/usr/bin/env python3
"""
Slack Workspace Scraper
Fetches messages from Slack workspaces and saves to CSV cache
"""

import os
import csv
import requests
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from .init_config import SLACK_CONFIG

# Load environment variables from .env file
load_dotenv()

# Cache for user names to reduce API calls
USER_NAME_CACHE = {}


def get_channels(client, include_external=True):
    """Fetch all accessible channels"""
    try:
        channels = []
        cursor = None

        while True:
            response = client.conversations_list(
                exclude_archived=True,
                types="public_channel,private_channel" if include_external else "public_channel",
                cursor=cursor,
                limit=200
            )

            channels.extend(response['channels'])

            cursor = response.get('response_metadata', {}).get('next_cursor')
            if not cursor:
                break

        return channels
    except SlackApiError as e:
        print(f"Error fetching channels: {e}")
        return []


def get_channel_type(channel):
    """Determine the channel type"""
    if channel.get('is_ext_shared'):
        return 'external'
    elif channel.get('is_private'):
        return 'private'
    else:
        return 'public'


def get_user_name(client, user_id):
    """Get user's display name from user ID with caching"""
    # Return cached name if available
    cache_key = f"{client.token[:20]}_{user_id}"  # Include workspace in cache key
    if cache_key in USER_NAME_CACHE:
        return USER_NAME_CACHE[cache_key]

    try:
        result = client.users_info(user=user_id)
        user = result['user']

        # Try to get the best display name
        name = (user.get('profile', {}).get('display_name') or
                user.get('profile', {}).get('real_name') or
                user.get('real_name') or
                user.get('name', user_id))

        # Cache the result
        USER_NAME_CACHE[cache_key] = name
        return name

    except SlackApiError as e:
        error_msg = e.response.get('error', str(e))
        if error_msg == 'missing_scope':
            # Only warn once per workspace
            workspace_key = f"warned_{client.token[:20]}"
            if workspace_key not in USER_NAME_CACHE:
                print(f"\n  ⚠️  Missing 'users:read' scope - user names will show as IDs")
                USER_NAME_CACHE[workspace_key] = True

        # Cache the user_id so we don't keep retrying
        USER_NAME_CACHE[cache_key] = user_id
        return user_id


def download_file(file_info, workspace_name, channel_name, client):
    """Download a file from Slack and save it locally using SDK (PDF/PNG/JPG only)"""
    try:
        file_name = file_info.get('name', 'unnamed_file')
        file_id = file_info.get('id')
        file_size = file_info.get('size', 0)
        file_type = file_info.get('filetype', '').lower()

        # Only process PDF, PNG, JPG files
        if file_type not in ['pdf', 'png', 'jpg', 'jpeg']:
            return None

        if not file_id:
            print(f"\n  ⚠️  No file ID for {file_name}")
            return None

        # Create directory structure in cache
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        safe_channel = "".join(c for c in channel_name if c.isalnum() or c in (' ', '-', '_')).strip()
        safe_workspace = "".join(c for c in workspace_name if c.isalnum() or c in (' ', '-', '_')).strip()

        # Get repo root
        repo_root = Path(__file__).parent.parent
        download_dir = repo_root / 'cache' / 'downloads' / safe_workspace / safe_channel
        download_dir.mkdir(parents=True, exist_ok=True)

        # Create unique filename
        file_path = download_dir / f"{timestamp}_{file_name}"

        # Get file info with download URL
        try:
            file_response = client.files_info(file=file_id)
            file_data = file_response['file']

            # Try different URL fields in order of preference
            file_url = (file_data.get('url_private_download') or
                       file_data.get('url_private'))

            if not file_url:
                print(f"\n  ⚠️  No download URL for {file_name}")
                return None

            # Download using the client's token
            headers = {'Authorization': f'Bearer {client.token}'}
            response = requests.get(file_url, headers=headers, stream=True, timeout=30)
            response.raise_for_status()

            # Check if we got HTML instead of the file
            content_type = response.headers.get('content-type', '')
            if 'text/html' in content_type.lower():
                print(f"\n  ⚠️  Received HTML for {file_name} - bot may need 'files:read' scope")
                return None

            # Save file
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            # Verify file size
            actual_size = os.path.getsize(file_path)
            if actual_size < 100 and file_size > 1000:
                print(f"\n  ⚠️  File {file_name} is only {actual_size} bytes (expected ~{file_size})")
                os.remove(file_path)
                return None

            return str(file_path)

        except SlackApiError as e:
            error_msg = e.response.get('error', str(e))
            if error_msg == 'missing_scope':
                print(f"\n  ⚠️  Missing scope for {file_name} - add 'files:read' to bot token")
            else:
                print(f"\n  ⚠️  Slack API error for {file_name}: {error_msg}")
            return None

    except Exception as e:
        print(f"\n  ⚠️  Error downloading {file_name}: {e}")
        return None


def fetch_original_message(client, share_attachment):
    """Attempt to fetch the full original message from a share attachment"""
    try:
        # Extract channel and timestamp from share attachment
        channel_id = share_attachment.get('channel_id')
        message_ts = share_attachment.get('ts')

        if not channel_id or not message_ts:
            return None

        # Try to fetch the message
        response = client.conversations_history(
            channel=channel_id,
            latest=message_ts,
            inclusive=True,
            limit=1
        )

        messages = response.get('messages', [])
        if messages and messages[0].get('ts') == message_ts:
            return messages[0]

        return None
    except SlackApiError:
        # Bot doesn't have access to the original channel
        return None


def get_messages_from_channel(client, channel_id, oldest_timestamp):
    """Fetch messages from a specific channel since oldest_timestamp"""
    try:
        messages = []
        cursor = None

        while True:
            response = client.conversations_history(
                channel=channel_id,
                oldest=str(oldest_timestamp),
                cursor=cursor,
                limit=200
            )

            messages.extend(response['messages'])

            cursor = response.get('response_metadata', {}).get('next_cursor')
            if not cursor:
                break

        return messages
    except SlackApiError as e:
        if e.response['error'] == 'not_in_channel':
            # Bot needs to be added to the channel
            return []
        print(f"  ⚠️  Error fetching messages from channel {channel_id}: {e.response.get('error', str(e))}")
        return []


def get_thread_replies(client, channel_id, thread_ts):
    """Fetch replies to a thread"""
    try:
        response = client.conversations_replies(
            channel=channel_id,
            ts=thread_ts,
            limit=100
        )
        # Exclude the parent message (first message)
        return response['messages'][1:] if len(response['messages']) > 1 else []
    except SlackApiError as e:
        print(f"  ⚠️  Error fetching thread replies: {e.response.get('error', str(e))}")
        return []


def format_message_text(message, client, workspace_name, channel_name):
    """Extract and format message text, handling various message types

    Returns:
        tuple: (formatted_text, list_of_file_paths)
    """
    # Basic text
    text = message.get('text', '')
    file_paths = []

    # Handle files - download them (PDF/PNG/JPG only)
    if 'files' in message:
        file_info_list = []
        for file in message['files']:
            file_name = file.get('name', 'unnamed_file')
            file_type = file.get('filetype', 'unknown').lower()
            file_size = file.get('size', 0)

            # Only process PDF, PNG, JPG files
            if file_type not in ['pdf', 'png', 'jpg', 'jpeg']:
                continue

            # Download the file
            local_path = download_file(file, workspace_name, channel_name, client)

            if local_path:
                file_paths.append(local_path)
                size_kb = file_size / 1024 if file_size else 0
                file_info_list.append(f"[File: {file_name} ({file_type}, {size_kb:.1f}KB) -> {local_path}]")
            else:
                file_info_list.append(f"[File: {file_name} ({file_type}) - download failed]")

        if file_info_list:
            text += ' ' + ' '.join(file_info_list)

    # Handle attachments
    if 'attachments' in message:
        for attachment in message['attachments']:
            # Check if it's a repost/share
            if attachment.get('is_share'):
                # Try to fetch the full original message
                original_message = fetch_original_message(client, attachment)

                if original_message:
                    # Successfully fetched original - format it fully
                    original_user_id = original_message.get('user', 'Unknown')
                    original_user = get_user_name(client, original_user_id)
                    original_text = original_message.get('text', '')
                    original_channel = attachment.get('channel_name', 'unknown')
                    original_ts = datetime.fromtimestamp(float(attachment.get('ts', 0))).strftime('%Y-%m-%d %H:%M:%S')

                    text += f" [Shared from @{original_user} in #{original_channel} at {original_ts}]: {original_text}"

                    # If original message has files, download them too (PDF/PNG/JPG only)
                    if 'files' in original_message:
                        for file in original_message['files']:
                            file_name = file.get('name', 'unnamed_file')
                            file_type = file.get('filetype', 'unknown').lower()

                            # Only process PDF, PNG, JPG files
                            if file_type not in ['pdf', 'png', 'jpg', 'jpeg']:
                                continue

                            local_path = download_file(file, workspace_name, channel_name, client)
                            if local_path:
                                file_paths.append(local_path)
                                text += f" [Original message file: {file_name} ({file_type}) -> {local_path}]"
                else:
                    # Couldn't fetch original - use attachment metadata
                    original_text = attachment.get('text', '')
                    original_channel = attachment.get('channel_name', 'unknown')
                    author_id = attachment.get('author_id')
                    author_name = attachment.get('author_name', 'Unknown')

                    if author_id:
                        try:
                            author_name = get_user_name(client, author_id)
                        except:
                            pass

                    text += f" [Shared from @{author_name} in #{original_channel}]: {original_text}"
            # Skip link preview attachments (they don't have is_share and don't have file_id)
            # Only keep attachments with actual text content that aren't just link previews
            elif 'text' in attachment and not attachment.get('is_app_unfurl'):
                # This filters out most link previews while keeping rich text attachments
                pass

    return text.strip(), file_paths


def aggregate_messages(messages_data):
    """
    Aggregate consecutive messages from the same user within 1 minute.

    Args:
        messages_data: List of dicts with keys: user_id, user_name, timestamp, text, file_paths

    Returns:
        List of aggregated message dicts
    """
    if not messages_data:
        return []

    # Sort by timestamp to ensure chronological order
    sorted_messages = sorted(messages_data, key=lambda x: x['timestamp'])

    aggregated = []
    current_group = None

    for msg in sorted_messages:
        if current_group is None:
            # Start new group
            current_group = {
                'user_id': msg['user_id'],
                'user_name': msg['user_name'],
                'timestamp': msg['timestamp'],
                'texts': [msg['text']],
                'file_paths': msg['file_paths'].copy()
            }
        elif (current_group['user_id'] == msg['user_id'] and
              abs(msg['timestamp'] - current_group['timestamp']) <= 60):
            # Same user within 1 minute - aggregate
            current_group['texts'].append(msg['text'])
            current_group['file_paths'].extend(msg['file_paths'])
        else:
            # Different user or more than 1 minute apart - save current group and start new
            aggregated.append({
                'user_name': current_group['user_name'],
                'timestamp': current_group['timestamp'],
                'text': '\n'.join(current_group['texts']),
                'file_paths': current_group['file_paths']
            })

            current_group = {
                'user_id': msg['user_id'],
                'user_name': msg['user_name'],
                'timestamp': msg['timestamp'],
                'texts': [msg['text']],
                'file_paths': msg['file_paths'].copy()
            }

    # Don't forget the last group
    if current_group:
        aggregated.append({
            'user_name': current_group['user_name'],
            'timestamp': current_group['timestamp'],
            'text': '\n'.join(current_group['texts']),
            'file_paths': current_group['file_paths']
        })

    return aggregated


def get_message_permalink(client, channel_id, message_ts):
    """Get permalink for a Slack message using API"""
    try:
        response = client.chat_getPermalink(
            channel=channel_id,
            message_ts=str(message_ts)
        )
        return response['permalink']
    except SlackApiError as e:
        print(f"\n  ⚠️  Error getting permalink: {e.response.get('error', str(e))}")
        return ""


def scrape_workspace(workspace_config, csv_writer, start_timestamp):
    """Scrape messages from a single workspace and write to CSV"""
    workspace_name = workspace_config['workspace_name']
    token_env_var = workspace_config['token_env_var']

    # Get token from environment
    token = os.getenv(token_env_var)
    if not token:
        print(f"  ❌ Token not found for {token_env_var}")
        return 0

    print(f"\n{'='*80}")
    print(f"WORKSPACE: {workspace_name}")
    print(f"{'='*80}")

    # Create Slack client
    client = WebClient(token=token)

    # Fetch all channels
    channels = get_channels(client, include_external=True)

    if not channels:
        print(f"  ⚠️  No accessible channels found")
        return 0

    print(f"Found {len(channels)} accessible channels")

    total_messages = 0

    for channel in channels:
        channel_id = channel['id']
        channel_name = channel.get('name', channel_id)
        channel_type = get_channel_type(channel)

        # Skip #aggregated-talks channel
        if channel_name == 'aggregated-talks':
            print(f"  ⊘ Skipping #{channel_name} (aggregated channel)")
            continue

        print(f"  📺 Scraping #{channel_name} ({channel_type})...", end='')

        # Fetch messages from the time range
        messages = get_messages_from_channel(client, channel_id, start_timestamp)

        if messages:
            # Collect all messages first (including thread replies)
            all_messages_data = []

            for message in messages:
                message_text, file_paths = format_message_text(message, client, workspace_name, channel_name)
                user = message.get('user', message.get('bot_id', 'Unknown'))
                user_name = get_user_name(client, user) if user and user != 'Unknown' else user
                timestamp = float(message['ts'])

                all_messages_data.append({
                    'user_id': user,
                    'user_name': user_name,
                    'timestamp': timestamp,
                    'text': message_text,
                    'file_paths': file_paths
                })

                # Check for thread replies
                if message.get('reply_count', 0) > 0:
                    thread_ts = message.get('thread_ts') or message.get('ts')
                    replies = get_thread_replies(client, channel_id, thread_ts)

                    for reply in replies:
                        # Only include replies from the time range
                        if float(reply['ts']) >= start_timestamp:
                            reply_text, reply_file_paths = format_message_text(reply, client, workspace_name, channel_name)
                            reply_user = reply.get('user', reply.get('bot_id', 'Unknown'))
                            reply_user_name = get_user_name(client, reply_user) if reply_user and reply_user != 'Unknown' else reply_user
                            reply_timestamp = float(reply['ts'])

                            all_messages_data.append({
                                'user_id': reply_user,
                                'user_name': reply_user_name,
                                'timestamp': reply_timestamp,
                                'text': reply_text,
                                'file_paths': reply_file_paths
                            })

            # Aggregate consecutive messages from same user within 1 minute
            aggregated_messages = aggregate_messages(all_messages_data)

            # Write aggregated messages to CSV
            for msg in aggregated_messages:
                time_str = datetime.fromtimestamp(msg['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
                file_paths_str = '; '.join(msg['file_paths']) if msg['file_paths'] else ''

                # Get permalink for the message
                permalink = get_message_permalink(client, channel_id, msg['timestamp'])

                csv_writer.writerow([
                    workspace_name,
                    channel_name,
                    channel_type,
                    msg['user_name'],
                    time_str,
                    msg['text'],
                    file_paths_str,
                    permalink
                ])

            print(f" {len(aggregated_messages)} messages (aggregated from {len(all_messages_data)})")
            total_messages += len(aggregated_messages)
        else:
            print(f" 0 messages")

    print(f"\n✅ Total messages from {workspace_name}: {total_messages}")
    return total_messages


def main(start_timestamp: float, end_timestamp: float, overwrite_cache: bool = False) -> str:
    """
    Scrape Slack workspaces for a specific time range and save to cache.

    Args:
        start_timestamp: Unix timestamp for start of time range
        end_timestamp: Unix timestamp for end of time range
        overwrite_cache: If True, overwrite existing cache file

    Returns:
        Path to the created/cached CSV file
    """
    # Get repo root
    repo_root = Path(__file__).parent.parent

    # Convert timestamps to datetime for formatting
    dt_start = datetime.fromtimestamp(start_timestamp)
    dt_end = datetime.fromtimestamp(end_timestamp)

    # Format timestamps for filename
    dt_start_str = dt_start.strftime('%Y%m%d_%H')
    dt_end_str = dt_end.strftime('%Y%m%d_%H')

    # Create cache directory
    cache_dir = repo_root / 'cache'
    cache_dir.mkdir(parents=True, exist_ok=True)

    # Create output filename
    output_filename = cache_dir / f"slack_messages_from_{dt_start_str}_{dt_end_str}.csv"

    # Check if cache exists
    if output_filename.exists() and not overwrite_cache:
        print(f"✓ Using cached file: {output_filename}")
        return str(output_filename)

    if output_filename.exists() and overwrite_cache:
        print(f"⟳ Overwriting cached file: {output_filename}")
    else:
        print(f"➜ Creating new cache: {output_filename}")

    print(f"\n🤖 Slack Workspace Scraper")
    print(f"📅 Fetching messages from {dt_start.strftime('%Y-%m-%d %H:00')} to {dt_end.strftime('%Y-%m-%d %H:00')}")
    print(f"📁 File attachments (PDF/PNG/JPG only) will be downloaded to: {cache_dir / 'downloads'}/")
    print(f"🔗 Shared messages will be fetched from original channels when possible")
    print(f"⊘ Skipping #aggregated-talks channel")

    grand_total = 0

    # Open CSV file for writing
    with open(output_filename, 'w', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile)

        # Write header
        csv_writer.writerow([
            'workspace', 'channel_name', 'channel_type', 'user_name', 'time', 'message', 'file_paths',
            'original_slack_message_link'
        ])

        # Process each workspace
        for workspace_config in SLACK_CONFIG:
            try:
                count = scrape_workspace(workspace_config, csv_writer, start_timestamp)
                grand_total += count
            except Exception as e:
                print(f"❌ Error processing {workspace_config['workspace_name']}: {e}")
                import traceback
                traceback.print_exc()

    print(f"\n{'='*80}")
    print(f"🎉 SCRAPING COMPLETE")
    print(f"{'='*80}")
    print(f"Total messages collected: {grand_total}")
    print(f"Output file: {output_filename}")
    print(f"{'='*80}\n")

    return str(output_filename)


if __name__ == "__main__":
    import sys

    # Parse command line arguments (for standalone usage)
    # Format: python scrape_workspaces.py start_timestamp end_timestamp [overwrite]
    if len(sys.argv) < 3:
        print("Usage: python scrape_workspaces.py <start_timestamp> <end_timestamp> [overwrite]")
        print("Timestamps should be Unix timestamps (float)")
        sys.exit(1)

    start_timestamp = float(sys.argv[1])
    end_timestamp = float(sys.argv[2])
    overwrite = sys.argv[3].lower() == 'true' if len(sys.argv) > 3 else False

    output_path = main(start_timestamp, end_timestamp, overwrite)
    print(f"\n✓ Slack data saved to: {output_path}")
