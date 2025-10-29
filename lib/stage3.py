#!/usr/bin/env python3
"""
Stage 3: Event Extraction with Gemini
Extracts talk/event information from Slack messages using Gemini AI with parallel processing
"""

import os
import time
from pathlib import Path
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from google import genai
from google.genai.types import GenerateContentConfig

from .models import (
    SlackMessage,
    MessageExtract,
    PhysicalTalkInfo,
    VirtualTalkInfo,
    PhysicalEventInfo,
    VirtualEventInfo
)

# Retry configuration
MAX_RETRIES = 5
INITIAL_RETRY_DELAY = 1
MAX_RETRY_DELAY = 32


def get_gemini_client() -> Optional[genai.Client]:
    """Initialize Gemini client"""
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise ValueError("GEMINI_API_KEY not found in environment")
    return genai.Client(api_key=api_key)


def format_datetime_readable(iso_datetime: str) -> str:
    """Convert ISO datetime to human-readable format

    Args:
        iso_datetime: DateTime string in format YYYY-MM-DDTHH:MM:SS

    Returns:
        Human-readable format like "Thursday, October 23, 2025 at 4:00 PM"
    """
    from datetime import datetime

    try:
        dt = datetime.fromisoformat(iso_datetime)
        day_of_week = dt.strftime('%A')
        month = dt.strftime('%B')
        day = dt.day
        year = dt.year
        time_12hr = dt.strftime('%I:%M %p').lstrip('0')  # Remove leading zero from hour

        return f"{day_of_week}, {month} {day}, {year} at {time_12hr}"
    except:
        return iso_datetime  # Return original if parsing fails


def build_extraction_prompt(message: SlackMessage) -> str:
    """
    Build Gemini prompt for event extraction

    Args:
        message: SlackMessage to extract events from

    Returns:
        Formatted prompt string
    """
    readable_datetime = format_datetime_readable(message.datetime)
    prompt = f"""
You are analyzing a Slack message to extract information about academic talks and events.

MESSAGE CONTEXT:
- Channel: #{message.channel_name}
- Workspace: {message.workspace_name}
- Sent by: {message.sending_user_name}
- Sent at: {message.datetime} ({readable_datetime})

MESSAGE TEXT:
{message.textract}

TASK:
Extract ALL talks and events mentioned in this message. For each, determine if it is:
1. PhysicalTalkInfo - An in-person talk by a specific speaker
2. VirtualTalkInfo - An online/virtual talk by a specific speaker
3. PhysicalEventInfo - An in-person event (workshop, reading group, etc.)
4. VirtualEventInfo - An online/virtual event

IMPORTANT GUIDELINES:

1. **Names**: Extract first and last names in lowercase. If last name is not mentioned, set to null.

2. **Dates**: Format as YYYY-MM-DDTHH:MM. Use '_' for unknown parts.
   - ALWAYS use the message sent date ({readable_datetime}) to resolve relative dates
   - Example: If message says "tomorrow at 3pm" and sent on Thursday, Oct 23, output: "2025-10-24T15:00"
   - Example: If message says "this Friday at 1pm" and sent on Thursday, Oct 23, output: "2025-10-24T13:00"
   - Example: If message says "next Monday" and sent on Thursday, Oct 23, output: "2025-10-27T__:__"
   - Only use underscores when date is truly unknown/ambiguous (not mentioned or inferrable)
   - Example: If message says "October 25 at 2pm", output: "2025-10-25T14:00"
   - **Lunch timing inference**: If lunch time is mentioned (e.g., "lunch at 11:45"), the talk/event likely starts around that time or shortly after
        - If lunch is mentioned, but no specific time, or no other time of the event, then assume the event starts at 11:45 AM and set the event to then as well, mark lunch is provided
        - If pixl lunch is mentioned, note that the event always happens at Friday at 11:45 AM and goes for 1 hr. although there may be more time detail you can use in the message.
   - **Start time inference**: If only end-time is mentioned, infer start-time as 3 hours before the end-time (e.g., "ends at 5pm" → start at 2pm)

3. **Relative Date Resolution**: Use the sent datetime to calculate:
   - "today" = same date as message
   - "tomorrow" = next day after message
   - "Friday", "this Friday" = next Friday from message date (or same day if message sent on Friday)
   - "next Monday" = next occurrence of Monday after message date
   - Account for the day of week in the message timestamp

4. **Physical vs Virtual**:
   - If zoom link or virtual meeting mentioned = Virtual
   - If room number/building mentioned = Physical
   - If unclear, assume Physical

5. **Talk vs Event**:
   - If specific speaker name mentioned = Talk
   - If generic event name (reading group, workshop, etc.) = Event

6. **Recurring Events** (Events only, NOT talks):
   - Check if message mentions: "weekly", "every week", "bi-weekly", "every other week", "twice a month", "monthly", "every month", etc.
   - Set is_recurring field: 'weekly' | 'biweekly' | 'monthly' | 'unknown' | 'none'
   - 'unknown' = event recurs but pattern unclear
   - 'none' = event does not recur (default)
   - Talks are always one-time, so they don't have is_recurring field

7. **Deduplication**: If you see "[ADDITIONAL MESSAGE]" in the text, this means multiple messages were aggregated.
   - The same information may appear multiple times
   - Only extract UNIQUE events - do not create duplicates for repeated information
   - Example: "Talk by John on Friday [ADDITIONAL MESSAGE] Talk by John on Friday" = 1 event, not 2

8. **Descriptions**: Be concise, up to 3 sentences. Focus on content, not logistics.

9. **Categories** (for talks only): Vision | Robotics | Machine Learning | Reinforcement Learning | Unrecognized

EXAMPLES:

Example 1: Message sent on Thursday, October 23, 2025 at 9:00 AM
Input: "Prof. John Smith from MIT will present on deep learning tomorrow at 2pm in Room 105"
Output: PhysicalTalkInfo(first_name="john", last_name="smith", talk_date="2025-10-24T14:00", location="Room 105", short_description="Prof. John Smith from MIT will present on deep learning.", lunch_provided=false, category="Machine Learning")

Example 2: Message sent on Thursday, October 23, 2025 at 10:30 AM
Input: "Join us for the AI Reading Group this Friday at 1pm via Zoom: https://zoom.us/j/123"
Output: VirtualEventInfo(simple_event_name="ai reading group", event_date="2025-10-24T13:00", virtual_link="https://zoom.us/j/123", short_description="AI Reading Group meeting.", is_academic=true, is_recurring="none")

Example 3: Message sent on Wednesday, October 22, 2025 at 11:00 AM
Input: "Dr. Sarah will talk about robotics today at noon in the lab. Lunch provided!"
Output: PhysicalTalkInfo(first_name="sarah", last_name=null, talk_date="2025-10-22T12:00", location="the lab", short_description="Dr. Sarah will talk about robotics.", lunch_provided=true, category="Robotics")

Example 4: Message sent on Monday, October 20, 2025 at 2:00 PM
Input: "Our weekly AI Reading Group meets every Friday at 3pm via Zoom: https://zoom.us/j/456"
Output: VirtualEventInfo(simple_event_name="ai reading group", event_date="2025-10-24T15:00", virtual_link="https://zoom.us/j/456", short_description="Weekly AI Reading Group meeting.", is_academic=true, is_recurring="weekly")

Example 5: Aggregated message sent on Friday, October 24, 2025 at 10:00 AM
Input: "We will have talks from Siyang and Xindi on Friday at 12:10pm in Room 101 [ADDITIONAL MESSAGE] We will have talks from Siyang and Xindi on Friday at 12:10pm in Room 101"
Output: [
  PhysicalTalkInfo(first_name="siyang", last_name=null, talk_date="2025-10-24T12:10", location="Room 101", short_description="Talk by Siyang.", lunch_provided=false, category="Unrecognized"),
  PhysicalTalkInfo(first_name="xindi", last_name=null, talk_date="2025-10-24T12:10", location="Room 101", short_description="Talk by Xindi.", lunch_provided=false, category="Unrecognized")
]
(Note: Only 2 events extracted despite duplicate text, one for each unique speaker)

Return a JSON object with an "events" array containing all extracted events. If no events found, return empty array.
"""
    return prompt


def calculate_extract_density(extract: MessageExtract) -> float:
    """
    Calculate information density of a MessageExtract.
    Higher density = more events and more fields filled.

    Args:
        extract: MessageExtract to score

    Returns:
        Density score (higher is better)
    """
    if not extract or not extract.events:
        return 0.0

    total_density = 0.0

    for event_info in extract.events:
        # event_info is directly the PhysicalTalkInfo/VirtualTalkInfo/etc. object
        event_dict = event_info.model_dump()

        # Count non-null, non-empty fields
        filled_fields = 0
        total_fields = 0

        for key, value in event_dict.items():
            total_fields += 1

            # Check if field is meaningfully filled
            if value is not None:
                if isinstance(value, str):
                    # Non-empty string that's not all underscores
                    if value.strip() and not all(c in '_-' for c in value.replace('T', '').replace(':', '')):
                        filled_fields += 1
                elif isinstance(value, bool):
                    # Booleans always count
                    filled_fields += 1
                else:
                    # Other non-null values
                    filled_fields += 1

        # Density for this event = filled_fields / total_fields
        event_density = filled_fields / total_fields if total_fields > 0 else 0
        total_density += event_density

    # Return total density (sum of all event densities)
    # This naturally favors extracts with more events
    return total_density


def extract_events_with_retry(
    client: genai.Client,
    message: SlackMessage
) -> MessageExtract:
    """
    Extract events from message with retry logic and parallel sampling.
    Uses candidate_count=3 to generate multiple candidates and picks the one
    with highest information density.

    Args:
        client: Gemini client
        message: SlackMessage to extract from

    Returns:
        MessageExtract with list of events (highest density candidate)
    """
    prompt = build_extraction_prompt(message)
    retry_delay = INITIAL_RETRY_DELAY
    last_error = None

    for attempt in range(MAX_RETRIES):
        try:
            response = client.models.generate_content(
                model="gemini-2.0-flash-exp",
                contents=prompt,
                config=GenerateContentConfig(
                    response_mime_type="application/json",
                    response_schema=MessageExtract,
                    candidate_count=3,  # Generate 3 candidates
                )
            )

            # Collect all candidates and their densities
            candidates = []

            # Access candidates directly (response.parsed only gives first one)
            for candidate in response.candidates:
                try:
                    # Get the JSON text from the candidate
                    if hasattr(candidate, 'content') and hasattr(candidate.content, 'parts'):
                        json_text = candidate.content.parts[0].text

                        # Parse JSON to MessageExtract
                        import json
                        extract_dict = json.loads(json_text)
                        message_extract = MessageExtract(**extract_dict)

                        # Calculate density
                        density = calculate_extract_density(message_extract)
                        candidates.append((message_extract, density))
                except Exception as e:
                    # Skip malformed candidates
                    print(f"  Warning: Skipping malformed candidate: {e}")
                    continue

            # Pick candidate with highest density
            if candidates:
                best_extract, best_density = max(candidates, key=lambda x: x[1])
                print(f"  → Selected best candidate with density {best_density:.2f} (from {len(candidates)} candidates)")
                return best_extract
            else:
                # No valid candidates, return empty
                print("  Warning: No valid candidates found")
                return MessageExtract(events=[])

        except Exception as e:
            last_error = e
            error_str = str(e).lower()

            # Check for rate limit errors
            if 'rate' in error_str or 'quota' in error_str or 'resource_exhausted' in error_str:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2, MAX_RETRY_DELAY)
                    continue
                else:
                    # Return empty extract on final failure
                    return MessageExtract(events=[])

            # For other errors, retry with shorter delay
            elif attempt < MAX_RETRIES - 1:
                time.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, MAX_RETRY_DELAY)
                continue
            else:
                # Return empty extract on final failure
                return MessageExtract(events=[])

    # Return empty extract if all retries exhausted
    return MessageExtract(events=[])


def extract_all_events(
    messages: list[SlackMessage],
    output_path: Optional[Path] = None,
    max_workers: int = 5
) -> list[MessageExtract]:
    """
    Extract events from all messages using Gemini with parallel processing

    Args:
        messages: List of SlackMessage objects
        output_path: Optional path to save JSON output
        max_workers: Number of parallel workers (default: 5)

    Returns:
        List of MessageExtract objects (one per input message)
    """
    client = get_gemini_client()

    # Process messages in parallel using ThreadPoolExecutor
    extracts = [None] * len(messages)  # Preserve order
    completed_count = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_index = {
            executor.submit(extract_events_with_retry, client, msg): i
            for i, msg in enumerate(messages)
        }

        # Process completed futures
        for future in as_completed(future_to_index):
            index = future_to_index[future]
            completed_count += 1
            print(f"Processing message {completed_count}/{len(messages)}...")

            try:
                extract = future.result()
                extracts[index] = extract

                if extract.events:
                    print(f"  Found {len(extract.events)} event(s)")
            except Exception as e:
                print(f"  Error: {e}")
                extracts[index] = MessageExtract(events=[])

    # Save to JSON if path provided
    if output_path:
        MessageExtract.to_json_file(extracts, output_path)
        print(f"\nSaved {len(extracts)} extractions to {output_path}")

    return extracts


def main(input_path: Path, output_path: Path) -> Path:
    """
    Main entry point for Stage 3

    Args:
        input_path: Path to Stage 2 JSON output (aggregated messages)
        output_path: Path to save Stage 3 JSON output

    Returns:
        Path to saved JSON file
    """
    print("="*80)
    print("STAGE 3: Event Extraction with Gemini (5 parallel workers)")
    print("="*80)
    print(f"Input: {input_path}")
    print(f"Output: {output_path}")
    print("="*80)

    # Load Stage 2 messages (aggregated)
    messages = SlackMessage.from_json_file(input_path)
    print(f"Loaded {len(messages)} aggregated messages")

    # Extract events with parallel processing
    extracts = extract_all_events(messages, output_path, max_workers=5)

    # Summary
    total_events = sum(len(extract.events) for extract in extracts)
    print(f"\n✅ Stage 3 complete: {total_events} events extracted from {len(messages)} messages")

    return output_path
