#!/usr/bin/env python3
"""
Slack Channel Summary - Lists all accessible channels across all workspaces
"""

import os
from datetime import datetime
from dotenv import load_dotenv
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from pprint import pprint
from init_config import SLACK_CONFIG

# Load environment variables from .env file
load_dotenv()


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


def format_channel_summary(channel):
    """Format channel information into a readable summary"""
    return {
        'name': channel.get('name', 'Unknown'),
        'id': channel.get('id', 'Unknown'),
        'is_private': channel.get('is_private', False),
        'is_shared': channel.get('is_shared', False),
        'is_ext_shared': channel.get('is_ext_shared', False),
        'is_org_shared': channel.get('is_org_shared', False),
        'member_count': channel.get('num_members', 0),
        'topic': channel.get('topic', {}).get('value', ''),
        'purpose': channel.get('purpose', {}).get('value', ''),
        'created': datetime.fromtimestamp(channel.get('created', 0)).strftime('%Y-%m-%d'),
    }


def print_channel_details(channel_summary):
    """Print detailed channel information in a nice format"""
    print(f"\n  📺 #{channel_summary['name']}")
    print(f"     ID: {channel_summary['id']}")
    print(f"     Members: {channel_summary['member_count']}")
    print(f"     Created: {channel_summary['created']}")

    # Channel type indicators
    flags = []
    if channel_summary['is_private']:
        flags.append("🔒 Private")
    if channel_summary['is_shared']:
        flags.append("🔗 Shared")
    if channel_summary['is_ext_shared']:
        flags.append("🌐 Externally Shared")
    if channel_summary['is_org_shared']:
        flags.append("🏢 Org Shared")

    if flags:
        print(f"     Type: {' | '.join(flags)}")

    if channel_summary['topic']:
        print(f"     Topic: {channel_summary['topic'][:100]}{'...' if len(channel_summary['topic']) > 100 else ''}")

    if channel_summary['purpose']:
        print(f"     Purpose: {channel_summary['purpose'][:100]}{'...' if len(channel_summary['purpose']) > 100 else ''}")


def summarize_workspace(workspace_config):
    """Fetch and summarize all channels for a single workspace"""
    workspace_name = workspace_config['workspace_name']
    token_env_var = workspace_config['token_env_var']

    # Get token from environment
    token = os.getenv(token_env_var)
    if not token:
        print(f"  ❌ Token not found for {token_env_var}")
        return None

    print(f"\n{'='*80}")
    print(f"WORKSPACE: {workspace_name}")
    print(f"{'='*80}")

    # Create Slack client
    client = WebClient(token=token)

    # Fetch all channels
    channels = get_channels(client, include_external=True)

    if not channels:
        print(f"  ⚠️  No accessible channels found")
        return None

    # Categorize channels
    public_channels = []
    private_channels = []
    shared_channels = []
    ext_shared_channels = []

    for channel in channels:
        summary = format_channel_summary(channel)

        if channel.get('is_ext_shared'):
            ext_shared_channels.append(summary)
        elif channel.get('is_shared') or channel.get('is_org_shared'):
            shared_channels.append(summary)
        elif channel.get('is_private'):
            private_channels.append(summary)
        else:
            public_channels.append(summary)

    # Print summary statistics
    print(f"\n📊 SUMMARY:")
    print(f"  Total Channels: {len(channels)}")
    print(f"  Public: {len(public_channels)}")
    print(f"  Private: {len(private_channels)}")
    print(f"  Shared: {len(shared_channels)}")
    print(f"  Externally Shared: {len(ext_shared_channels)}")

    # Print public channels
    if public_channels:
        print(f"\n📢 PUBLIC CHANNELS ({len(public_channels)}):")
        for channel in sorted(public_channels, key=lambda x: x['name']):
            print_channel_details(channel)

    # Print private channels
    if private_channels:
        print(f"\n🔒 PRIVATE CHANNELS ({len(private_channels)}):")
        for channel in sorted(private_channels, key=lambda x: x['name']):
            print_channel_details(channel)

    # Print shared channels
    if shared_channels:
        print(f"\n🔗 SHARED CHANNELS ({len(shared_channels)}):")
        for channel in sorted(shared_channels, key=lambda x: x['name']):
            print_channel_details(channel)

    # Print externally shared channels
    if ext_shared_channels:
        print(f"\n🌐 EXTERNALLY SHARED CHANNELS ({len(ext_shared_channels)}):")
        for channel in sorted(ext_shared_channels, key=lambda x: x['name']):
            print_channel_details(channel)

    return {
        'workspace': workspace_name,
        'total': len(channels),
        'public': len(public_channels),
        'private': len(private_channels),
        'shared': len(shared_channels),
        'ext_shared': len(ext_shared_channels),
    }


def main():
    """Main function to summarize all workspaces"""
    print(f"\n🤖 Slack Channel Summary")
    print(f"📅 Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🔍 Scanning {len(SLACK_CONFIG)} workspace(s)...\n")

    results = []

    for workspace_config in SLACK_CONFIG:
        try:
            result = summarize_workspace(workspace_config)
            if result:
                results.append(result)
        except Exception as e:
            print(f"❌ Error processing {workspace_config['workspace_name']}: {e}")

    # Print overall summary
    print(f"\n{'='*80}")
    print(f"📈 OVERALL SUMMARY ACROSS ALL WORKSPACES")
    print(f"{'='*80}")

    if results:
        total_channels = sum(r['total'] for r in results)
        total_public = sum(r['public'] for r in results)
        total_private = sum(r['private'] for r in results)
        total_shared = sum(r['shared'] for r in results)
        total_ext_shared = sum(r['ext_shared'] for r in results)

        print(f"\nWorkspaces scanned: {len(results)}")
        print(f"Total channels accessible: {total_channels}")
        print(f"  - Public: {total_public}")
        print(f"  - Private: {total_private}")
        print(f"  - Shared: {total_shared}")
        print(f"  - Externally Shared: {total_ext_shared}")

        print(f"\nBreakdown by workspace:")
        for result in results:
            print(f"  {result['workspace']}: {result['total']} channels")
    else:
        print("No workspaces were successfully scanned.")

    print(f"\n{'='*80}\n")


if __name__ == "__main__":
    main()
