#!/usr/bin/env python3
"""
Google Calendar Integration
Adds talks from Gemini extraction to Google Calendar with deduplication
"""

import csv
from datetime import datetime, timedelta
from pathlib import Path
from google.oauth2 import service_account
from googleapiclient.discovery import build

# Path to credentials
CREDENTIALS_FILE = Path(__file__).parent.parent / 'credentials.json'

# Scopes needed for calendar access
SCOPES = ['https://www.googleapis.com/auth/calendar']

# Calendar configuration
CALENDAR_ID = 'primary'  # Service account's primary calendar
TIMEZONE = 'America/New_York'
SLACK_EMAIL = 'aggregated-talks-aaaarxlyhn2ukxh4dzcdaqrvsm@zhuanglabatprinceton.slack.com'


def get_calendar_service():
    """Create and return authenticated Google Calendar service"""
    credentials = service_account.Credentials.from_service_account_file(
        str(CREDENTIALS_FILE),
        scopes=SCOPES
    )
    service = build('calendar', 'v3', credentials=credentials)
    return service


def check_duplicate_event(service, first_name, last_name, year, month, day):
    """
    Check if an event already exists for this presenter on this date.

    Args:
        service: Google Calendar service instance
        first_name: Presenter first name
        last_name: Presenter last name
        year: Event year
        month: Event month (1-12)
        day: Event day (1-31)

    Returns:
        True if duplicate exists, False otherwise
    """
    try:
        # Create date range for the specific day
        start_date = datetime(year, month, day, 0, 0, 0)
        end_date = start_date + timedelta(days=1)

        # Format for RFC3339 timestamp
        time_min = start_date.strftime('%Y-%m-%dT%H:%M:%S') + '-04:00'
        time_max = end_date.strftime('%Y-%m-%dT%H:%M:%S') + '-04:00'

        # Search for events on this day
        events_result = service.events().list(
            calendarId=CALENDAR_ID,
            timeMin=time_min,
            timeMax=time_max,
            singleEvents=True,
            orderBy='startTime'
        ).execute()

        events = events_result.get('items', [])

        # Check if any event has the same presenter name
        search_title = f"{first_name} {last_name} Talk"
        for event in events:
            event_title = event.get('summary', '')
            if event_title == search_title:
                return True

        return False

    except Exception as e:
        print(f"  ⚠️  Error checking for duplicate: {e}")
        return False


def create_calendar_event(service, talk_data):
    """
    Create a Google Calendar event for a talk.

    Args:
        service: Google Calendar service instance
        talk_data: Dictionary with talk information

    Returns:
        Created event object or None if failed
    """
    try:
        first_name = talk_data['gemini_presenter_first_name']
        last_name = talk_data['gemini_presenter_last_name']
        month = int(talk_data['gemini_month'])
        day = int(talk_data['gemini_day'])
        hour = int(talk_data['gemini_hour'])
        minute = int(talk_data['gemini_minute'])
        location = talk_data['gemini_location']
        description = talk_data['gemini_short_description']
        category = talk_data['gemini_category']

        # Use current year (2025)
        year = 2025

        # Create event title
        title = f"{first_name} {last_name} Talk"

        # Create event description with category
        full_description = description
        if category:
            full_description += f"\n\nCategory: {category}"

        # Add original message context
        workspace = talk_data.get('workspace', '')
        channel = talk_data.get('channel_name', '')
        if workspace and channel:
            full_description += f"\n\nSource: {workspace} - #{channel}"

        # Add Slack notification email to description
        full_description += f"\n\nNotifications: {SLACK_EMAIL}"

        # Create datetime objects for start and end (1 hour duration)
        start_dt = datetime(year, month, day, hour, minute)
        end_dt = start_dt + timedelta(hours=1)

        # Format for Google Calendar API
        # Note: Service accounts cannot add attendees without Domain-Wide Delegation
        # So we include the Slack email in the description instead
        event = {
            'summary': title,
            'location': location,
            'description': full_description,
            'start': {
                'dateTime': start_dt.strftime('%Y-%m-%dT%H:%M:%S'),
                'timeZone': TIMEZONE,
            },
            'end': {
                'dateTime': end_dt.strftime('%Y-%m-%dT%H:%M:%S'),
                'timeZone': TIMEZONE,
            },
            'reminders': {
                'useDefault': False,
                'overrides': [
                    {'method': 'email', 'minutes': 1440},  # 1 day before
                    {'method': 'email', 'minutes': 30},    # 30 minutes before
                ],
            },
        }

        # Create the event
        created_event = service.events().insert(
            calendarId=CALENDAR_ID,
            body=event
        ).execute()

        return created_event

    except Exception as e:
        print(f"  ❌ Error creating event: {e}")
        return None


def main(gemini_csv_path: str) -> int:
    """
    Add talks from Gemini CSV to Google Calendar with deduplication.

    Args:
        gemini_csv_path: Path to the Gemini extraction CSV

    Returns:
        Number of new events created
    """
    print(f"📅 Adding talks to Google Calendar...")
    print(f"📄 Input file: {gemini_csv_path}")

    # Authenticate with Google Calendar
    try:
        service = get_calendar_service()
        print(f"✓ Authenticated with Google Calendar")
    except Exception as e:
        print(f"❌ Failed to authenticate with Google Calendar: {e}")
        return 0

    # Read Gemini CSV
    with open(gemini_csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    print(f"Found {len(rows)} message(s) in CSV")

    # Filter for actual talks (case-insensitive)
    talks = [row for row in rows if str(row.get('gemini_is_talk', '')).lower() == 'true']
    print(f"Found {len(talks)} talk(s) to process")

    if not talks:
        print("⚠️  No talks found - nothing to add to calendar")
        return 0

    # Process each talk
    new_events_count = 0
    duplicate_count = 0
    error_count = 0

    for i, talk in enumerate(talks, 1):
        try:
            # Validate required fields
            first_name = talk['gemini_presenter_first_name']
            last_name = talk['gemini_presenter_last_name']
            month = int(talk['gemini_month'])
            day = int(talk['gemini_day'])
            hour = int(talk['gemini_hour'])
            minute = int(talk['gemini_minute'])

            # Skip if missing critical info
            if not first_name or not last_name:
                print(f"  ⊘ Talk {i}: Skipping - missing presenter name")
                error_count += 1
                continue

            if month == 0 or day == 0:
                print(f"  ⊘ Talk {i}: Skipping - missing date ({first_name} {last_name})")
                error_count += 1
                continue

            # Check for duplicate
            year = 2025
            is_duplicate = check_duplicate_event(service, first_name, last_name, year, month, day)

            if is_duplicate:
                print(f"  ↻ Talk {i}: Already exists - {first_name} {last_name} on {month}/{day}")
                duplicate_count += 1
                continue

            # Create event
            print(f"  ➕ Talk {i}: Creating event - {first_name} {last_name} on {month}/{day} at {hour}:{minute:02d}")
            event = create_calendar_event(service, talk)

            if event:
                print(f"     ✓ Event created: {event.get('htmlLink', 'N/A')}")
                new_events_count += 1
            else:
                error_count += 1

        except Exception as e:
            print(f"  ❌ Talk {i}: Error processing - {e}")
            error_count += 1

    # Summary
    print(f"\n📊 Summary:")
    print(f"  ✅ New events created: {new_events_count}")
    print(f"  ↻ Duplicates skipped: {duplicate_count}")
    print(f"  ❌ Errors/incomplete: {error_count}")

    return new_events_count


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python add_to_calendar.py <gemini_csv_path>")
        sys.exit(1)

    gemini_csv_path = sys.argv[1]
    new_count = main(gemini_csv_path)
    print(f"\n✓ Created {new_count} new calendar event(s)")
