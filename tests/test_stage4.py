#!/usr/bin/env python3
"""
Unit tests for Stage 4: Google Calendar Integration
Tests event creation, deduplication, recurring events, and all event types
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

from lib.models import (
    PhysicalTalkInfo,
    VirtualTalkInfo,
    PhysicalEventInfo,
    VirtualEventInfo,
    MessageExtract
)
from lib.stage4 import (
    parse_event_datetime,
    generate_event_title,
    generate_event_description,
    get_recurrence_rule,
    check_duplicate_event,
    create_calendar_event
)


class TestDateTimeParsing(unittest.TestCase):
    """Test datetime parsing logic"""

    def test_parse_full_datetime(self):
        """Test parsing complete datetime"""
        result = parse_event_datetime("2025-10-25T14:30")
        self.assertIsNotNone(result)
        self.assertEqual(result['year'], 2025)
        self.assertEqual(result['month'], 10)
        self.assertEqual(result['day'], 25)
        self.assertEqual(result['hour'], 14)
        self.assertEqual(result['minute'], 30)
        self.assertTrue(result['has_time'])
        self.assertFalse(result['all_day'])

    def test_parse_date_only(self):
        """Test parsing date with no time (underscores)"""
        result = parse_event_datetime("2025-10-25T__:__")
        self.assertIsNotNone(result)
        self.assertEqual(result['year'], 2025)
        self.assertEqual(result['month'], 10)
        self.assertEqual(result['day'], 25)
        self.assertEqual(result['hour'], 8)  # Default 8 AM
        self.assertEqual(result['minute'], 0)
        self.assertFalse(result['has_time'])
        self.assertTrue(result['all_day'])

    def test_parse_unknown_date(self):
        """Test parsing completely unknown date"""
        result = parse_event_datetime("____-__-__T__:__")
        self.assertIsNone(result)

    def test_parse_partial_date(self):
        """Test parsing date with missing year"""
        result = parse_event_datetime("____-10-25T14:30")
        self.assertIsNone(result)


class TestEventTitleGeneration(unittest.TestCase):
    """Test event title generation"""

    def test_physical_talk_title(self):
        """Test title for physical talk"""
        talk = PhysicalTalkInfo(
            first_name="john",
            last_name="doe",
            talk_date="2025-10-25T14:00",
            location="Room 101",
            short_description="ML talk",
            lunch_provided=False,
            category="Machine Learning"
        )
        title = generate_event_title(talk)
        self.assertEqual(title, "john's Talk")

    def test_virtual_talk_title(self):
        """Test title for virtual talk"""
        talk = VirtualTalkInfo(
            first_name="jane",
            last_name=None,
            talk_date="2025-10-25T14:00",
            virtual_link="https://zoom.us/j/123",
            short_description="Vision talk",
            category="Vision"
        )
        title = generate_event_title(talk)
        self.assertEqual(title, "jane's Talk")

    def test_physical_event_title(self):
        """Test title for physical event"""
        event = PhysicalEventInfo(
            simple_event_name="ai reading group",
            event_date="2025-10-26T13:00",
            location="Conference Room",
            short_description="Weekly reading group",
            lunch_provided=False,
            is_academic=True,
            is_recurring="weekly"
        )
        title = generate_event_title(event)
        self.assertEqual(title, "ai reading group")

    def test_virtual_event_title(self):
        """Test title for virtual event"""
        event = VirtualEventInfo(
            simple_event_name="webinar",
            event_date="2025-10-27T14:00",
            virtual_link="https://zoom.us/j/456",
            short_description="Online webinar",
            is_academic=False,
            is_recurring="none"
        )
        title = generate_event_title(event)
        self.assertEqual(title, "webinar")


class TestEventDescriptionGeneration(unittest.TestCase):
    """Test event description generation"""

    def test_physical_talk_description(self):
        """Test description includes talk-specific info"""
        talk = PhysicalTalkInfo(
            first_name="john",
            last_name="doe",
            talk_date="2025-10-25T14:00",
            location="Room 101",
            short_description="Machine learning talk",
            lunch_provided=True,
            category="Machine Learning"
        )
        desc = generate_event_description(talk)
        self.assertIn("Machine learning talk", desc)
        self.assertIn("Category: Machine Learning", desc)
        self.assertIn("Lunch provided: Yes", desc)
        self.assertIn("Location: Room 101", desc)

    def test_virtual_event_description(self):
        """Test description includes virtual link"""
        event = VirtualEventInfo(
            simple_event_name="workshop",
            event_date="2025-10-26T14:00",
            virtual_link="https://zoom.us/j/789",
            short_description="Virtual workshop",
            is_academic=True,
            is_recurring="weekly"
        )
        desc = generate_event_description(event)
        self.assertIn("Virtual workshop", desc)
        self.assertIn("Academic: Yes", desc)
        self.assertIn("Recurring: weekly", desc)
        self.assertIn("Zoom Link: https://zoom.us/j/789", desc)

    def test_description_with_permalinks(self):
        """Test description includes Slack permalinks"""
        talk = PhysicalTalkInfo(
            first_name="alice",
            last_name="smith",
            talk_date="2025-10-25T10:00",
            location="Lab",
            short_description="Research talk",
            lunch_provided=False,
            category="Robotics"
        )
        permalinks = [
            "https://slack.com/archives/C123/p456",
            "https://slack.com/archives/C123/p789"
        ]
        desc = generate_event_description(talk, permalinks)
        self.assertIn("Source Slack messages:", desc)
        self.assertIn(permalinks[0], desc)
        self.assertIn(permalinks[1], desc)


class TestRecurrenceRules(unittest.TestCase):
    """Test recurrence rule generation"""

    def test_weekly_recurrence(self):
        """Test weekly recurrence rule"""
        rrule = get_recurrence_rule("weekly")
        self.assertEqual(rrule, ['RRULE:FREQ=WEEKLY'])

    def test_biweekly_recurrence(self):
        """Test biweekly recurrence rule"""
        rrule = get_recurrence_rule("biweekly")
        self.assertEqual(rrule, ['RRULE:FREQ=WEEKLY;INTERVAL=2'])

    def test_monthly_recurrence(self):
        """Test monthly recurrence rule"""
        rrule = get_recurrence_rule("monthly")
        self.assertEqual(rrule, ['RRULE:FREQ=MONTHLY'])

    def test_no_recurrence(self):
        """Test non-recurring events"""
        rrule = get_recurrence_rule("none")
        self.assertIsNone(rrule)

    def test_unknown_recurrence(self):
        """Test unknown recurrence pattern"""
        rrule = get_recurrence_rule("unknown")
        self.assertIsNone(rrule)


class TestDuplicateDetection(unittest.TestCase):
    """Test duplicate event detection"""

    def test_check_duplicate_found(self):
        """Test finding duplicate event"""
        mock_service = Mock()
        mock_service.events().list().execute.return_value = {
            'items': [
                {'summary': "john's Talk", 'id': 'event123'},
                {'summary': "jane's Talk", 'id': 'event456'}
            ]
        }

        result = check_duplicate_event(mock_service, "john's Talk", 2025, 10, 25)
        self.assertIsNotNone(result)
        self.assertEqual(result['id'], 'event123')

    def test_check_duplicate_not_found(self):
        """Test when no duplicate exists"""
        mock_service = Mock()
        mock_service.events().list().execute.return_value = {
            'items': [
                {'summary': "jane's Talk", 'id': 'event456'}
            ]
        }

        result = check_duplicate_event(mock_service, "john's Talk", 2025, 10, 25)
        self.assertIsNone(result)

    def test_check_duplicate_empty_calendar(self):
        """Test when calendar has no events"""
        mock_service = Mock()
        mock_service.events().list().execute.return_value = {
            'items': []
        }

        result = check_duplicate_event(mock_service, "john's Talk", 2025, 10, 25)
        self.assertIsNone(result)


class TestEventCreation(unittest.TestCase):
    """Test calendar event creation"""

    def test_create_physical_talk_event(self):
        """Test creating physical talk event"""
        mock_service = Mock()
        mock_service.events().insert().execute.return_value = {
            'id': 'new_event_123',
            'htmlLink': 'https://calendar.google.com/event123'
        }

        talk = PhysicalTalkInfo(
            first_name="john",
            last_name="doe",
            talk_date="2025-10-25T14:00",
            location="Room 101",
            short_description="ML talk",
            lunch_provided=True,
            category="Machine Learning"
        )

        result = create_calendar_event(mock_service, talk)
        self.assertIsNotNone(result)
        self.assertEqual(result['id'], 'new_event_123')

        # Verify event was created with correct data
        call_args = mock_service.events().insert.call_args
        self.assertIsNotNone(call_args)
        event_body = call_args[1]['body']
        self.assertEqual(event_body['summary'], "john's Talk")
        self.assertEqual(event_body['location'], "Room 101")

    def test_create_all_day_event(self):
        """Test creating all-day event (unknown time)"""
        mock_service = Mock()
        mock_service.events().insert().execute.return_value = {
            'id': 'all_day_event_123'
        }

        event = PhysicalEventInfo(
            simple_event_name="workshop",
            event_date="2025-10-26T__:__",  # Unknown time
            location="Lab",
            short_description="All-day workshop",
            lunch_provided=False,
            is_academic=True,
            is_recurring="none"
        )

        result = create_calendar_event(mock_service, event)
        self.assertIsNotNone(result)

    def test_create_recurring_event(self):
        """Test creating recurring event with RRULE"""
        mock_service = Mock()
        mock_service.events().insert().execute.return_value = {
            'id': 'recurring_event_123'
        }

        event = VirtualEventInfo(
            simple_event_name="weekly meeting",
            event_date="2025-10-27T15:00",
            virtual_link="https://zoom.us/j/123",
            short_description="Weekly team meeting",
            is_academic=True,
            is_recurring="weekly"
        )

        result = create_calendar_event(mock_service, event)
        self.assertIsNotNone(result)

        # Verify recurrence was included in API call
        call_args = mock_service.events().insert.call_args
        event_body = call_args[1]['body']
        self.assertIn('recurrence', event_body)
        self.assertEqual(event_body['recurrence'], ['RRULE:FREQ=WEEKLY'])

    def test_create_event_invalid_date(self):
        """Test handling invalid date"""
        mock_service = Mock()

        event = PhysicalTalkInfo(
            first_name="john",
            last_name="doe",
            talk_date="____-__-__T__:__",  # Invalid date
            location="Room 101",
            short_description="Talk",
            lunch_provided=False,
            category="Unrecognized"
        )

        result = create_calendar_event(mock_service, event)
        self.assertIsNone(result)


if __name__ == '__main__':
    unittest.main()
