import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from pprint import pprint

# Load environment variables from .env file
load_dotenv()

# Configuration
OAUTH_TOKEN = os.getenv("AILAB_SLACK_TOKEN")
WORKSPACE_NAME = "AI Lab @ Princeton"
CHANNEL_NAME = "talks"
MINUTES_AGO = 15

if not OAUTH_TOKEN:
    raise ValueError("AILAB_SLACK_TOKEN not found in environment variables")

# Create Slack client
client = WebClient(token=OAUTH_TOKEN)

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

def get_channel_id(channel_name):
    """Get channel ID from channel name"""
    try:
        result = client.conversations_list(types="public_channel,private_channel")
        for channel in result['channels']:
            if channel['name'] == channel_name:
                return channel['id']
        return None
    except SlackApiError as e:
        print(f"Error fetching channel list: {e.response['error']}")
        return None

def get_user_name(user_id):
    """Get user's display name from user ID"""
    try:
        result = client.users_info(user=user_id)
        user = result['user']
        return user.get('real_name') or user.get('name', 'Unknown User')
    except SlackApiError:
        return user_id

def format_timestamp(ts):
    """Convert Slack timestamp to readable format"""
    dt = datetime.fromtimestamp(float(ts))
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def print_message(msg, indent=0):
    """Print a message with optional indentation for reposts"""
    prefix = "  " * indent
    user_id = msg.get('user', 'Unknown')
    user_name = get_user_name(user_id)
    timestamp = format_timestamp(msg.get('ts', msg.get('message_ts', '')))
    text = msg.get('text', '[No text]')

    print(f"{prefix}[{timestamp}] {user_name}")
    print(f"{prefix}{text}")

    # Check if this is a repost/shared message
    if 'attachments' in msg:
        for attachment in msg['attachments']:
            # Check if it's a message attachment (repost)
            if attachment.get('is_share'):
                print(f"{prefix}  📎 Reposted message:")

                # Get original message details
                original_author_id = attachment.get('author_id')
                original_author = get_user_name(original_author_id) if original_author_id else 'Unknown'
                original_text = attachment.get('text', '[No text]')
                original_channel = attachment.get('channel_name', 'unknown-channel')
                original_ts = attachment.get('ts')
                original_time = format_timestamp(original_ts) if original_ts else 'Unknown time'

                print(f"{prefix}    Original from: {original_author} in #{original_channel}")
                print(f"{prefix}    Posted at: {original_time}")
                print(f"{prefix}    Content: {original_text}")

def scrape_recent_messages():
    """Scrape messages from the last 15 minutes"""
    print(f"Scraping #{CHANNEL_NAME} from '{WORKSPACE_NAME}'...\n")

    # Get channel ID
    channel_id = get_channel_id(CHANNEL_NAME)
    if not channel_id:
        print(f"Error: Channel #{CHANNEL_NAME} not found")
        return

    # Calculate timestamp for 15 minutes ago
    oldest_time = datetime.now() - timedelta(days=1)
    oldest_timestamp = oldest_time.timestamp()

    try:
        # Fetch messages
        result = client.conversations_history(
            channel=channel_id,
            oldest=str(oldest_timestamp),
            limit=100
        )

        messages = result['messages']

        if not messages:
            print(f"No messages found in the last {MINUTES_AGO} minutes.")
            return

        print(f"Found {len(messages)} message(s) in the last {MINUTES_AGO} minutes:\n")
        print("=" * 80)

        # Print messages in reverse order (oldest first)
        for msg in reversed(messages):
            print()
            print_message(msg)
            print("-" * 80)

    except SlackApiError as e:
        print(f"Error fetching messages: {e.response['error']}")

if __name__ == "__main__":
    print('Fetching all accessible channels!')
    print('='*100)
    pprint(
        get_channels(client)
    )
    print('='*100)
    print(f'Scraping recent messages from: {CHANNEL_NAME}')
    #scrape_recent_messages()