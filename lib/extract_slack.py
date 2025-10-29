#!/usr/bin/env python3
"""
Slack Talk Extractor
Uses Gemini AI to extract talk/seminar information from Slack messages
"""

import os
import csv
import re
import time
from pathlib import Path
from dotenv import load_dotenv
from google import genai
from google.genai.types import GenerateContentConfig
from pydantic import BaseModel

# Load environment variables
load_dotenv()

# Retry configuration
MAX_RETRIES = 5
INITIAL_RETRY_DELAY = 1  # seconds
MAX_RETRY_DELAY = 32  # seconds

# Initialize Gemini client
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if GEMINI_API_KEY:
    genai_client = genai.Client(api_key=GEMINI_API_KEY)
else:
    genai_client = None
    print("⚠️  GEMINI_API_KEY not found - talk extraction will be skipped")


# Pydantic models for Gemini structured output
class TalkInfo(BaseModel):
    presenter_first_name: str
    presenter_last_name: str
    month: int  # 1-12
    day: int    # 1-31
    hour: int   # 0-23 (24-hour format)
    minute: int # 0-59
    location: str
    lunch_provided: bool
    short_description: str
    category: str  # Vision | Robotics | Machine Learning | Reinforcement Learning | Unrecognized


class MessageAnalysis(BaseModel):
    talks: list[TalkInfo]  # List of talks found in the message (can be empty)


def extract_urls(text):
    """Extract all URLs from text"""
    url_pattern = r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
    urls = re.findall(url_pattern, text)
    return urls


def extract_talk_info_with_gemini(message_text, channel_name):
    """
    Use Gemini to extract talk information from message text.
    Gemini will fetch content from URLs automatically.
    A single message may contain multiple talks.
    Implements retry logic with exponential backoff.

    Returns:
        MessageAnalysis object with list of talks, or None if extraction fails
    """
    if not genai_client:
        return None

    # Extract URLs from message
    urls = extract_urls(message_text)

    # Build prompt with URLs
    url_context = ""
    if urls:
        url_context = f"\n\nThe message contains these URLs which may have additional details: {', '.join(urls)}"

    prompt = f"""
You are analyzing Slack messages from the channel "#{channel_name}" to identify academic talks/seminars and extract key information.

IMPORTANT: A single message may contain information about MULTIPLE talks. You must extract ALL talks mentioned in the message.

Message text:
{message_text}
{url_context}

For EACH talk/seminar/colloquium mentioned in the message, extract the following information:
- presenter_first_name: First name of the speaker (string, empty "" if not found)
- presenter_last_name: Last name of the speaker (string, empty "" if not found)
- month: Month of the talk as integer 1-12 (e.g., October = 10, use 0 if not found)
- day: Day of the month as integer 1-31 (e.g., 22, use 0 if not found)
- hour: Hour in 24-hour format as integer 0-23 (e.g., 2 PM = 14, 11:45 AM = 11, use 0 if not found)
- minute: Minute as integer 0-59 (e.g., 30, 45, use 0 if not found)
- location: Where the talk will be held - room number, building, etc. (string, empty "" if not found)
- lunch_provided: Whether lunch/food is mentioned (boolean, false if not mentioned)
- short_description: Brief 1-2 sentence summary of the talk topic (string, empty "" if not found)
- category: Categorize the talk as one of: "Vision", "Robotics", "Machine Learning", "Reinforcement Learning", or "Unrecognized" (string)

Return a JSON object with a "talks" array containing all talk objects found. If no talks are found, return an empty array.
"""

    # Retry with exponential backoff
    retry_delay = INITIAL_RETRY_DELAY
    last_error = None

    for attempt in range(MAX_RETRIES):
        try:
            # Call Gemini with URL fetching enabled
            tools = [{"url_context": {}}]

            response = genai_client.models.generate_content(
                model="gemini-2.5-flash",
                contents=prompt,
                config=GenerateContentConfig(
                    response_mime_type="application/json",
                    response_schema=MessageAnalysis,
                    #tools=tools,
                )
            )

            # Parse structured response
            message_analysis: MessageAnalysis = response.parsed
            return message_analysis

        except Exception as e:
            last_error = e
            error_str = str(e).lower()

            # Check if it's a rate limit error or transient error
            if 'rate' in error_str or 'quota' in error_str or 'resource_exhausted' in error_str:
                if attempt < MAX_RETRIES - 1:
                    print(f"\n  ⚠️  Rate limit/quota error (attempt {attempt + 1}/{MAX_RETRIES}), retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2, MAX_RETRY_DELAY)  # Exponential backoff
                    continue
                else:
                    print(f"\n  ❌ Rate limit exceeded after {MAX_RETRIES} attempts")
                    return None

            # For other errors, retry with shorter delay
            elif attempt < MAX_RETRIES - 1:
                print(f"\n  ⚠️  Gemini error (attempt {attempt + 1}/{MAX_RETRIES}): {e}, retrying in {retry_delay}s...")
                time.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, MAX_RETRY_DELAY)
                continue
            else:
                print(f"\n  ❌ Gemini extraction failed after {MAX_RETRIES} attempts: {e}")
                return None

    print(f"\n  ❌ Gemini extraction failed after {MAX_RETRIES} attempts: {last_error}")
    return None


def main(slack_path: str, overwrite: bool = False) -> str:
    """
    Extract talk information from Slack messages CSV using Gemini.

    Args:
        slack_path: Path to the input CSV from scrape_workspaces
        overwrite: If True, overwrite existing output file

    Returns:
        Path to the created/cached output CSV file
    """
    # Get repo root
    repo_root = Path(__file__).parent.parent

    # Parse filename to extract timestamps
    slack_filename = Path(slack_path).name
    # Expected format: slack_messages_from_YYYYMMDD_HH_YYYYMMDD_HH.csv
    match = re.search(r'slack_messages_from_(\d{8}_\d{2})_(\d{8}_\d{2})\.csv', slack_filename)

    if not match:
        raise ValueError(f"Invalid filename format: {slack_filename}")

    dt_start_str = match.group(1)
    dt_end_str = match.group(2)

    # Create output filename
    cache_dir = repo_root / 'cache'
    cache_dir.mkdir(parents=True, exist_ok=True)
    output_filename = cache_dir / f"gemini_from_{dt_start_str}_{dt_end_str}.csv"

    # Check if output exists
    if output_filename.exists() and not overwrite:
        print(f"✓ Using cached file: {output_filename}")
        return str(output_filename)

    if output_filename.exists() and overwrite:
        print(f"⟳ Overwriting cached file: {output_filename}")
    else:
        print(f"➜ Creating new cache: {output_filename}")

    print(f"\n🤖 Gemini Talk Extractor")
    print(f"📄 Input file: {slack_path}")
    print(f"📄 Output file: {output_filename}")
    print(f"🤖 Processing messages with Gemini AI...")

    # Read input CSV
    with open(slack_path, 'r', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        input_rows = list(reader)

    print(f"Found {len(input_rows)} messages to process")

    # Process each row and add Gemini columns
    # Note: One input row may generate multiple output rows if multiple talks found
    output_rows = []
    processed_count = 0

    for i, row in enumerate(input_rows, 1):
        try:
            message_text = row['message']
            channel_name = row['channel_name']

            # Extract talk information with Gemini
            message_analysis = extract_talk_info_with_gemini(message_text, channel_name)

            # Process each talk found in the message
            if message_analysis and message_analysis.talks:
                # Multiple talks may exist in one message
                for talk in message_analysis.talks:
                    gemini_data = {
                        'gemini_is_talk': True,
                        'gemini_presenter_first_name': talk.presenter_first_name,
                        'gemini_presenter_last_name': talk.presenter_last_name,
                        'gemini_month': talk.month,
                        'gemini_day': talk.day,
                        'gemini_hour': talk.hour,
                        'gemini_minute': talk.minute,
                        'gemini_location': talk.location,
                        'gemini_lunch_provided': talk.lunch_provided,
                        'gemini_short_description': talk.short_description,
                        'gemini_category': talk.category
                    }

                    # Combine original row with Gemini data for this talk
                    output_row = {**row, **gemini_data}
                    output_rows.append(output_row)
            else:
                # No talks found - create one row with empty fields
                gemini_data = {
                    'gemini_is_talk': False,
                    'gemini_presenter_first_name': '',
                    'gemini_presenter_last_name': '',
                    'gemini_month': 0,
                    'gemini_day': 0,
                    'gemini_hour': 0,
                    'gemini_minute': 0,
                    'gemini_location': '',
                    'gemini_lunch_provided': False,
                    'gemini_short_description': '',
                    'gemini_category': ''
                }
                output_row = {**row, **gemini_data}
                output_rows.append(output_row)

            processed_count += 1
            if processed_count % 10 == 0:
                print(f"  Processed {processed_count}/{len(input_rows)} messages...")

        except Exception as e:
            print(f"\n  ⚠️  Error processing row {i}: {e}")
            # Add row with empty Gemini fields
            gemini_data = {
                'gemini_is_talk': False,
                'gemini_presenter_first_name': '',
                'gemini_presenter_last_name': '',
                'gemini_month': 0,
                'gemini_day': 0,
                'gemini_hour': 0,
                'gemini_minute': 0,
                'gemini_location': '',
                'gemini_lunch_provided': False,
                'gemini_short_description': '',
                'gemini_category': ''
            }
            output_row = {**row, **gemini_data}
            output_rows.append(output_row)

    # Write output CSV
    if output_rows:
        with open(output_filename, 'w', newline='', encoding='utf-8') as outfile:
            fieldnames = list(output_rows[0].keys())
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(output_rows)

        print(f"\n✅ Gemini extraction complete!")
        print(f"Processed {processed_count} messages")
        print(f"Output saved to: {output_filename}")

        # Count talks found
        talks_found = sum(1 for row in output_rows if row['gemini_is_talk'])
        print(f"📊 Found {talks_found} talk(s) in the messages")
    else:
        # Create empty CSV with headers when no messages
        print("⚠️  No messages to process - creating empty output file")
        with open(output_filename, 'w', newline='', encoding='utf-8') as outfile:
            fieldnames = [
                'workspace', 'channel_name', 'channel_type', 'user_name', 'time',
                'message', 'file_paths', 'gemini_is_talk', 'gemini_presenter_first_name',
                'gemini_presenter_last_name', 'gemini_month', 'gemini_day', 'gemini_hour',
                'gemini_minute', 'gemini_location', 'gemini_lunch_provided',
                'gemini_short_description', 'gemini_category'
            ]
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()

    return str(output_filename)


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python extract_slack.py <slack_csv_path> [overwrite]")
        sys.exit(1)

    slack_path = sys.argv[1]
    overwrite = sys.argv[2].lower() == 'true' if len(sys.argv) > 2 else False

    output_path = main(slack_path, overwrite)
    print(f"\n✓ Gemini data saved to: {output_path}")
