#!/usr/bin/env python3
"""
Stage 4: Google Calendar Integration
Adds events from Stage 3 Gemini extraction to Google Calendar with deduplication
"""

from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Union
from google.oauth2 import service_account
from googleapiclient.discovery import build

from .init_config import CALENDAR_ID
from .models import (
    MessageExtract,
    PhysicalTalkInfo,
    VirtualTalkInfo,
    PhysicalEventInfo,
    VirtualEventInfo
)

# Path to credentials
CREDENTIALS_FILE = Path(__file__).parent.parent / 'credentials.json'

# Scopes needed for calendar access
SCOPES = ['https://www.googleapis.com/auth/calendar']

# Calendar configuration
TIMEZONE = 'America/New_York'


def get_calendar_service():
    """Create and return authenticated Google Calendar service"""
    credentials = service_account.Credentials.from_service_account_file(
        str(CREDENTIALS_FILE),
        scopes=SCOPES
    )
    service = build('calendar', 'v3', credentials=credentials)
    return service


def parse_event_datetime(date_str: str) -> Optional[dict]:
    """
    Parse event datetime string and return dict with date/time info.

    Args:
        date_str: DateTime in format YYYY-MM-DDTHH:MM (underscores for unknown)

    Returns:
        Dict with {year, month, day, hour, minute, has_time, all_day} or None if invalid
    """
    # Replace underscores with None values
    parts = date_str.replace('_', '0')

    try:
        # Split date and time
        date_part, time_part = parts.split('T')
        year, month, day = map(int, date_part.split('-'))
        hour, minute = map(int, time_part.split(':'))

        # Check if year/month/day are valid
        if year == 0 or month == 0 or day == 0:
            return None

        # Check if time was specified (not all underscores)
        has_time = (hour != 0 or minute != 0) and '____-__-__T__:__' not in date_str
        all_day = not has_time

        return {
            'year': year,
            'month': month,
            'day': day,
            'hour': hour if has_time else 8,  # Default 8 AM for all-day
            'minute': minute if has_time else 0,
            'has_time': has_time,
            'all_day': all_day
        }
    except:
        return None


def generate_event_title(event: Union[PhysicalTalkInfo, VirtualTalkInfo, PhysicalEventInfo, VirtualEventInfo]) -> str:
    """Generate calendar event title based on event type"""
    if isinstance(event, (PhysicalTalkInfo, VirtualTalkInfo)):
        # Talks: "[first_name]'s Talk"
        return f"{event.first_name}'s Talk"
    else:
        # Events: use simple_event_name as-is
        return event.simple_event_name


def generate_event_description(event: Union[PhysicalTalkInfo, VirtualTalkInfo, PhysicalEventInfo, VirtualEventInfo], permalinks: list[str] = None) -> str:
    """Generate calendar event description based on event type"""
    desc_parts = [event.short_description]

    # Add type-specific information
    if isinstance(event, (PhysicalTalkInfo, VirtualTalkInfo)):
        # Talk-specific info
        desc_parts.append(f"\nCategory: {event.category}")
        if isinstance(event, PhysicalTalkInfo) and event.lunch_provided:
            desc_parts.append("Lunch provided: Yes")
    else:
        # Event-specific info
        desc_parts.append(f"\nAcademic: {'Yes' if event.is_academic else 'No'}")
        if event.is_recurring != 'none':
            desc_parts.append(f"Recurring: {event.is_recurring}")

    # Add location or virtual link
    if isinstance(event, PhysicalTalkInfo):
        desc_parts.append(f"\nLocation: {event.location}")
    elif isinstance(event, PhysicalEventInfo):
        desc_parts.append(f"\nLocation: {event.location}")
    elif isinstance(event, VirtualTalkInfo):
        desc_parts.append(f"\nZoom Link: {event.virtual_link}")
    elif isinstance(event, VirtualEventInfo):
        desc_parts.append(f"\nZoom Link: {event.virtual_link}")

    # Add Slack message permalinks
    if permalinks:
        desc_parts.append(f"\n\nSource Slack messages:")
        for link in permalinks[:3]:  # Limit to first 3
            desc_parts.append(f"  {link}")

    return '\n'.join(desc_parts)


def get_recurrence_rule(is_recurring: str) -> Optional[list[str]]:
    """Generate RRULE for recurring events"""
    if is_recurring == 'weekly':
        return ['RRULE:FREQ=WEEKLY']
    elif is_recurring == 'biweekly':
        return ['RRULE:FREQ=WEEKLY;INTERVAL=2']
    elif is_recurring == 'monthly':
        return ['RRULE:FREQ=MONTHLY']
    elif is_recurring == 'unknown' or is_recurring == 'none':
        return None
    return None


def check_duplicate_event(service, title: str, year: int, month: int, day: int) -> Optional[dict]:
    """
    Check if an event already exists with this title on this date.

    Returns:
        Event dict if duplicate found, None otherwise
        For recurring events, returns the master event (with recurrence rules)
    """
    try:
        # Create date range for the specific day
        start_date = datetime(year, month, day, 0, 0, 0)
        end_date = start_date + timedelta(days=1)

        # Format for RFC3339 timestamp
        time_min = start_date.strftime('%Y-%m-%dT%H:%M:%S-04:00')
        time_max = end_date.strftime('%Y-%m-%dT%H:%M:%S-04:00')

        # Search for events on this day (expands recurring events into instances)
        events_result = service.events().list(
            calendarId=CALENDAR_ID,
            timeMin=time_min,
            timeMax=time_max,
            singleEvents=True,
            orderBy='startTime'
        ).execute()

        events = events_result.get('items', [])

        # Check if any event has the same title
        for event in events:
            if event.get('summary', '') == title:
                # If this is an instance of a recurring event, get the master event
                if 'recurringEventId' in event:
                    try:
                        print(f"     → Found recurring event instance, fetching master event...")
                        master_event = service.events().get(
                            calendarId=CALENDAR_ID,
                            eventId=event['recurringEventId']
                        ).execute()
                        print(f"     → Master event fetched: {master_event.get('id')}")
                        # Verify it has recurrence rules
                        if 'recurrence' in master_event:
                            print(f"     → Confirmed master has recurrence rules: {master_event['recurrence']}")
                            return master_event
                        else:
                            print(f"  ⚠️  Master event missing recurrence rules, treating as single event")
                            return event
                    except Exception as e:
                        print(f"  ⚠️  Could not fetch master recurring event: {e}")
                        print(f"  ⚠️  WARNING: Will only delete this occurrence, not entire series!")
                        # Return the instance with a flag
                        event['_instance_only'] = True
                        return event
                else:
                    # Single event, return as-is
                    return event

        return None
    except Exception as e:
        print(f"  ⚠️  Error checking for duplicate: {e}")
        return None


def delete_event(service, event_id: str, is_recurring: bool = False) -> bool:
    """
    Delete a calendar event by ID

    Args:
        service: Google Calendar service
        event_id: Event ID to delete
        is_recurring: If True, indicates this is a recurring event (all occurrences will be deleted)

    Returns:
        True if deleted successfully, False otherwise
    """
    try:
        service.events().delete(
            calendarId=CALENDAR_ID,
            eventId=event_id
        ).execute()

        if is_recurring:
            print(f"     ✓ Deleted recurring event series (all future occurrences)")

        return True
    except Exception as e:
        print(f"  ❌ Error deleting event: {e}")
        return False


def create_calendar_event(
    service,
    event: Union[PhysicalTalkInfo, VirtualTalkInfo, PhysicalEventInfo, VirtualEventInfo],
    permalinks: list[str] = None
) -> Optional[dict]:
    """Create a Google Calendar event"""
    try:
        # Parse date/time
        date_field = event.talk_date if isinstance(event, (PhysicalTalkInfo, VirtualTalkInfo)) else event.event_date
        dt_info = parse_event_datetime(date_field)

        if not dt_info:
            print(f"  ⊘ Skipping - invalid date: {date_field}")
            return None

        # Generate title and description
        title = generate_event_title(event)
        description = generate_event_description(event, permalinks)

        # Create start/end datetimes
        start_dt = datetime(dt_info['year'], dt_info['month'], dt_info['day'], dt_info['hour'], dt_info['minute'])

        # Determine end time
        if dt_info['all_day']:
            # All-day event: 8 AM - 5 PM
            end_dt = datetime(dt_info['year'], dt_info['month'], dt_info['day'], 17, 0)
        else:
            # 1 hour duration
            end_dt = start_dt + timedelta(hours=1)

        # Create event body
        calendar_event = {
            'summary': title,
            'description': description,
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
                    {'method': 'email', 'minutes': 60},    # 1 hour before
                ],
            },
        }

        # Add location for physical events
        if isinstance(event, (PhysicalTalkInfo, PhysicalEventInfo)):
            calendar_event['location'] = event.location

        # Add recurrence rule for recurring events
        if isinstance(event, (PhysicalEventInfo, VirtualEventInfo)):
            recurrence = get_recurrence_rule(event.is_recurring)
            if recurrence:
                calendar_event['recurrence'] = recurrence

        # Create the event
        created_event = service.events().insert(
            calendarId=CALENDAR_ID,
            body=calendar_event
        ).execute()

        return created_event
    except Exception as e:
        print(f"  ❌ Error creating event: {e}")
        return None


def process_message_extract(
    service,
    extract: MessageExtract,
    message_permalinks: list[str],
    overwrite: bool = False
) -> dict:
    """
    Process a single MessageExtract and add events to calendar

    Returns:
        Dict with {created: int, duplicates: int, errors: int}
    """
    stats = {'created': 0, 'duplicates': 0, 'errors': 0}

    for event in extract.events:
        try:
            # Parse date to check for duplicates
            date_field = event.talk_date if isinstance(event, (PhysicalTalkInfo, VirtualTalkInfo)) else event.event_date
            dt_info = parse_event_datetime(date_field)

            if not dt_info:
                stats['errors'] += 1
                continue

            title = generate_event_title(event)

            # Check for duplicate
            existing_event = check_duplicate_event(
                service,
                title,
                dt_info['year'],
                dt_info['month'],
                dt_info['day']
            )

            if existing_event:
                if overwrite:
                    # Check if it's a recurring event
                    is_recurring = 'recurrence' in existing_event
                    event_type = "recurring event series" if is_recurring else "event"

                    print(f"  🔄 Overwriting {event_type}: {title} on {dt_info['month']}/{dt_info['day']}/{dt_info['year']}")
                    if delete_event(service, existing_event['id'], is_recurring=is_recurring):
                        # Create new event
                        created = create_calendar_event(service, event, message_permalinks)
                        if created:
                            print(f"     ✓ Event recreated")
                            stats['created'] += 1
                        else:
                            stats['errors'] += 1
                    else:
                        stats['errors'] += 1
                else:
                    print(f"  ↻ Event already exists: {title} on {dt_info['month']}/{dt_info['day']}/{dt_info['year']}")
                    stats['duplicates'] += 1
            else:
                # Create new event
                print(f"  ➕ Creating event: {title} on {dt_info['month']}/{dt_info['day']}/{dt_info['year']}")
                created = create_calendar_event(service, event, message_permalinks)
                if created:
                    print(f"     ✓ Event created: {created.get('htmlLink', 'N/A')}")
                    stats['created'] += 1
                else:
                    stats['errors'] += 1

        except Exception as e:
            print(f"  ❌ Error processing event: {e}")
            stats['errors'] += 1

    return stats


def main(input_path: Path, overwrite: bool = False) -> dict:
    """
    Main entry point for Stage 4

    Args:
        input_path: Path to Stage 3 JSON output
        overwrite: If True, delete and recreate existing events

    Returns:
        Dict with {created, duplicates, errors} counts
    """
    print("="*80)
    print("STAGE 4: Google Calendar Integration")
    print("="*80)
    print(f"Input: {input_path}")
    print(f"Overwrite mode: {'ON' if overwrite else 'OFF'}")
    print("="*80)

    # Authenticate with Google Calendar
    try:
        service = get_calendar_service()
        print("✓ Authenticated with Google Calendar")
    except Exception as e:
        print(f"❌ Failed to authenticate: {e}")
        return {'created': 0, 'duplicates': 0, 'errors': 0}

    # Load Stage 3 output
    extracts = MessageExtract.from_json_file(input_path)
    print(f"✓ Loaded {len(extracts)} message extractions")

    # Load corresponding Stage 2 messages for permalinks
    # Derive Stage 2 path from Stage 3 path
    stage2_path = Path(str(input_path).replace('stage3_events', 'stage2_aggregated'))
    permalinks_map = {}
    if stage2_path.exists():
        from .models import SlackMessage
        messages = SlackMessage.from_json_file(stage2_path)
        for i, msg in enumerate(messages):
            permalinks_map[i] = msg.permalink

    # Process all extracts
    total_stats = {'created': 0, 'duplicates': 0, 'errors': 0}
    total_events = sum(len(extract.events) for extract in extracts)

    print(f"\nProcessing {total_events} event(s) from {len(extracts)} message(s)...")

    for i, extract in enumerate(extracts):
        if not extract.events:
            continue

        print(f"\nMessage {i+1}/{len(extracts)}: {len(extract.events)} event(s)")

        # Get permalinks for this message
        message_permalinks = permalinks_map.get(i, [])

        # Process events
        stats = process_message_extract(service, extract, message_permalinks, overwrite)

        # Update totals
        total_stats['created'] += stats['created']
        total_stats['duplicates'] += stats['duplicates']
        total_stats['errors'] += stats['errors']

    # Summary
    print(f"\n{'='*80}")
    print("📊 Summary:")
    print(f"  ✅ New events created: {total_stats['created']}")
    print(f"  ↻ Duplicates skipped: {total_stats['duplicates']}")
    print(f"  ❌ Errors/incomplete: {total_stats['errors']}")
    print(f"{'='*80}")

    return total_stats
