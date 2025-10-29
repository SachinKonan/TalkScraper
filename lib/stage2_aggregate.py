#!/usr/bin/env python3
"""
Stage 2: Message Aggregation
Aggregates messages from same user in same channel within 30-minute window
"""

from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional
from collections import defaultdict

from .models import SlackMessage


def parse_datetime(dt_str: str) -> datetime:
    """Parse datetime string to datetime object"""
    return datetime.strptime(dt_str, '%Y-%m-%dT%H:%M:%S')


def aggregate_messages(messages: list[SlackMessage]) -> list[SlackMessage]:
    """
    Aggregate messages from same user in same channel within 30 minutes

    Args:
        messages: List of SlackMessage objects

    Returns:
        List of aggregated SlackMessage objects
    """
    if not messages:
        return []

    # Group messages by workspace, channel, and user
    groups = defaultdict(list)
    for msg in messages:
        key = (msg.workspace_name, msg.channel_name, msg.sending_user_name)
        groups[key].append(msg)

    # Sort each group by datetime
    for key in groups:
        groups[key].sort(key=lambda m: parse_datetime(m.datetime))

    # Aggregate within each group
    aggregated = []

    for (workspace, channel, user), group_messages in groups.items():
        i = 0
        while i < len(group_messages):
            # Start a new aggregation group
            current = group_messages[i]
            current_dt = parse_datetime(current.datetime)

            # Collect all messages within 30 minutes
            textract_parts = [current.textract]
            urls = current.urls.copy()
            permalinks = current.permalink.copy()
            file_paths = current.file_paths.copy()
            original_indices = current.original_indices.copy()
            latest_datetime = current.datetime

            j = i + 1
            while j < len(group_messages):
                next_msg = group_messages[j]
                next_dt = parse_datetime(next_msg.datetime)

                # Check if within 30 minutes of the first message in group
                if (next_dt - current_dt) <= timedelta(minutes=30):
                    textract_parts.append(next_msg.textract)
                    urls.extend(next_msg.urls)
                    permalinks.extend(next_msg.permalink)
                    file_paths.extend(next_msg.file_paths)
                    original_indices.extend(next_msg.original_indices)
                    latest_datetime = next_msg.datetime  # Use latest datetime
                    j += 1
                else:
                    break

            # Create aggregated message
            aggregated_msg = SlackMessage(
                workspace_name=workspace,
                channel_name=channel,
                channel_type=current.channel_type,
                sending_user_name=user,
                datetime=latest_datetime,
                textract=' [ADDITIONAL MESSAGE] '.join(textract_parts),
                urls=list(dict.fromkeys(urls)),  # Deduplicate while preserving order
                file_paths=file_paths,
                permalink=permalinks,
                original_indices=original_indices
            )
            aggregated.append(aggregated_msg)

            # Move to next non-aggregated message
            i = j

    # Sort final result by datetime
    aggregated.sort(key=lambda m: parse_datetime(m.datetime))

    return aggregated


def main(input_path: Path, output_path: Path) -> Path:
    """
    Main entry point for Stage 2

    Args:
        input_path: Path to Stage 1 JSON output
        output_path: Path to save Stage 2 JSON output

    Returns:
        Path to saved JSON file
    """
    print("="*80)
    print("STAGE 2: Message Aggregation")
    print("="*80)
    print(f"Input: {input_path}")
    print(f"Output: {output_path}")
    print("="*80)

    # Load Stage 1 messages
    messages = SlackMessage.from_json_file(input_path)
    print(f"Loaded {len(messages)} messages")

    # Aggregate messages
    aggregated = aggregate_messages(messages)
    print(f"Aggregated into {len(aggregated)} message groups")

    # Save to JSON
    SlackMessage.to_json_file(aggregated, output_path)
    print(f"Saved to {output_path}")

    print(f"\n✅ Stage 2 complete: {len(messages)} → {len(aggregated)} messages")
    return output_path
