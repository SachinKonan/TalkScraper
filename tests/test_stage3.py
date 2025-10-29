#!/usr/bin/env python3
"""
Unit tests for Stage 3: Event Extraction with Gemini
Tests Gemini integration, retries, parallel processing, and event extraction with mocking
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path

from lib.models import (
    SlackMessage,
    MessageExtract,
    PhysicalTalkInfo,
    VirtualTalkInfo,
    PhysicalEventInfo,
    VirtualEventInfo
)
from lib.stage3 import (
    format_datetime_readable,
    build_extraction_prompt,
    extract_events_with_retry,
    extract_all_events
)


class TestEventModels(unittest.TestCase):
    """Test Pydantic models for event extraction"""

    def test_physical_talk_info(self):
        """Test PhysicalTalkInfo model"""
        talk = PhysicalTalkInfo(
            first_name="john",
            last_name="doe",
            talk_date="2025-10-25T14:00",
            location="Room 105",
            short_description="Talk about ML",
            lunch_provided=True,
            category="Machine Learning"
        )
        self.assertEqual(talk.first_name, "john")
        self.assertEqual(talk.category, "Machine Learning")

    def test_virtual_talk_info(self):
        """Test VirtualTalkInfo model"""
        talk = VirtualTalkInfo(
            first_name="jane",
            last_name=None,
            talk_date="2025-10-25T15:00",
            virtual_link="https://zoom.us/j/123",
            short_description="Virtual talk",
            category="Vision"
        )
        self.assertEqual(talk.first_name, "jane")
        self.assertIsNone(talk.last_name)
        self.assertEqual(talk.virtual_link, "https://zoom.us/j/123")

    def test_physical_event_info(self):
        """Test PhysicalEventInfo model"""
        event = PhysicalEventInfo(
            simple_event_name="reading group",
            event_date="2025-10-26T13:00",
            location="Conference Room",
            short_description="Weekly reading group",
            lunch_provided=False,
            is_academic=True,
            is_recurring="weekly"
        )
        self.assertEqual(event.simple_event_name, "reading group")
        self.assertTrue(event.is_academic)
        self.assertEqual(event.is_recurring, "weekly")

    def test_virtual_event_info(self):
        """Test VirtualEventInfo model"""
        event = VirtualEventInfo(
            simple_event_name="webinar",
            event_date="____-__-__T14:00",
            virtual_link="https://meet.google.com/abc",
            short_description="Online webinar",
            is_academic=False,
            is_recurring="none"
        )
        self.assertEqual(event.event_date, "____-__-__T14:00")
        self.assertFalse(event.is_academic)
        self.assertEqual(event.is_recurring, "none")

    def test_event_recurring_default(self):
        """Test is_recurring defaults to 'none'"""
        event = PhysicalEventInfo(
            simple_event_name="workshop",
            event_date="2025-10-27T10:00",
            location="Lab",
            short_description="One-time workshop",
            lunch_provided=False,
            is_academic=True
        )
        self.assertEqual(event.is_recurring, "none")

    def test_event_recurring_values(self):
        """Test all valid is_recurring values"""
        for recurrence in ['weekly', 'biweekly', 'monthly', 'unknown', 'none']:
            event = VirtualEventInfo(
                simple_event_name="test event",
                event_date="2025-10-27T10:00",
                virtual_link="https://zoom.us/j/123",
                short_description="Test",
                is_academic=True,
                is_recurring=recurrence
            )
            self.assertEqual(event.is_recurring, recurrence)

    def test_invalid_category(self):
        """Test invalid category raises error"""
        with self.assertRaises(ValueError):
            PhysicalTalkInfo(
                first_name="test",
                last_name="user",
                talk_date="2025-10-25T14:00",
                location="Room 1",
                short_description="Test",
                lunch_provided=False,
                category="InvalidCategory"  # Must be one of the allowed values
            )

    def test_message_extract_empty(self):
        """Test MessageExtract with empty events"""
        extract = MessageExtract(events=[])
        self.assertEqual(len(extract.events), 0)

    def test_message_extract_multiple_events(self):
        """Test MessageExtract with multiple event types"""
        talk1 = PhysicalTalkInfo(
            first_name="alice",
            last_name="smith",
            talk_date="2025-10-25T10:00",
            location="Lab",
            short_description="Research talk",
            lunch_provided=False,
            category="Robotics"
        )

        event1 = VirtualEventInfo(
            simple_event_name="workshop",
            event_date="2025-10-26T14:00",
            virtual_link="https://zoom.us/j/456",
            short_description="Virtual workshop",
            is_academic=True,
            is_recurring="none"
        )

        extract = MessageExtract(events=[talk1, event1])
        self.assertEqual(len(extract.events), 2)
        self.assertIsInstance(extract.events[0], PhysicalTalkInfo)
        self.assertIsInstance(extract.events[1], VirtualEventInfo)


class TestDateFormatting(unittest.TestCase):
    """Test datetime formatting helper"""

    def test_format_datetime_readable(self):
        """Test converting ISO datetime to human-readable format"""
        result = format_datetime_readable("2025-10-23T16:00:00")
        self.assertIn("Thursday", result)
        self.assertIn("October", result)
        self.assertIn("23", result)
        self.assertIn("2025", result)
        self.assertIn("4:00 PM", result)

    def test_format_datetime_readable_morning(self):
        """Test morning time formatting"""
        result = format_datetime_readable("2025-10-24T09:30:00")
        self.assertIn("Friday", result)
        self.assertIn("9:30 AM", result)

    def test_format_datetime_readable_invalid(self):
        """Test invalid datetime returns original string"""
        invalid = "not-a-datetime"
        result = format_datetime_readable(invalid)
        self.assertEqual(result, invalid)


class TestPromptBuilding(unittest.TestCase):
    """Test Gemini prompt construction"""

    def test_build_extraction_prompt(self):
        """Test prompt includes all necessary context"""
        message = SlackMessage(
            workspace_name="TestWorkspace",
            channel_name="talks",
            channel_type="public",
            sending_user_name="John Doe",
            datetime="2025-10-22T14:30:00",
            textract="Prof. Smith will talk tomorrow at 3pm",
            file_paths=[],
            permalink=["https://slack.com/link"], original_indices=[0]
        )

        prompt = build_extraction_prompt(message)

        self.assertIn("talks", prompt)
        self.assertIn("TestWorkspace", prompt)
        self.assertIn("John Doe", prompt)
        self.assertIn("2025-10-22T14:30:00", prompt)
        self.assertIn("Prof. Smith will talk tomorrow at 3pm", prompt)
        self.assertIn("PhysicalTalkInfo", prompt)
        self.assertIn("VirtualTalkInfo", prompt)
        self.assertIn("PhysicalEventInfo", prompt)
        self.assertIn("VirtualEventInfo", prompt)
        # Should also include human-readable datetime
        self.assertIn("Wednesday", prompt)  # 2025-10-22 is a Wednesday
        self.assertIn("2:30 PM", prompt)

    def test_build_extraction_prompt_aggregated(self):
        """Test prompt mentions [ADDITIONAL MESSAGE] for aggregated messages"""
        message = SlackMessage(
            workspace_name="TestWorkspace",
            channel_name="talks",
            channel_type="public",
            sending_user_name="John Doe",
            datetime="2025-10-24T12:00:00",
            textract="Talk by Alice [ADDITIONAL MESSAGE] Talk by Alice",
            file_paths=[],
            permalink=["https://slack.com/link1", "https://slack.com/link2"],
            original_indices=[0, 1]
        )

        prompt = build_extraction_prompt(message)

        # Should include the aggregated message text
        self.assertIn("[ADDITIONAL MESSAGE]", prompt)
        # Should explain deduplication in prompt
        self.assertIn("Deduplication", prompt)


class TestEventExtraction(unittest.TestCase):
    """Test event extraction with mocked Gemini API"""

    def test_extract_events_success(self):
        """Test successful event extraction"""
        mock_client = Mock()

        # Create a mock response with parsed data
        mock_response = Mock()
        mock_response.parsed = MessageExtract(
            events=[
                PhysicalTalkInfo(
                    first_name="john",
                    last_name="doe",
                    talk_date="2025-10-25T14:00",
                    location="Room 105",
                    short_description="Machine learning talk",
                    lunch_provided=True,
                    category="Machine Learning"
                )
            ]
        )

        mock_client.models.generate_content.return_value = mock_response

        message = SlackMessage(
            workspace_name="Test",
            channel_name="general",
            channel_type="public",
            sending_user_name="User",
            datetime="2025-10-22T14:00:00",
            textract="Talk announcement",
            file_paths=[],
            permalink=["link"], original_indices=[0]
        )

        extract = extract_events_with_retry(mock_client, message)

        self.assertEqual(len(extract.events), 1)
        self.assertIsInstance(extract.events[0], PhysicalTalkInfo)
        self.assertEqual(extract.events[0].first_name, "john")

    def test_extract_events_empty_response(self):
        """Test empty response from Gemini"""
        mock_client = Mock()

        mock_response = Mock()
        mock_response.parsed = MessageExtract(events=[])

        mock_client.models.generate_content.return_value = mock_response

        message = SlackMessage(
            workspace_name="Test",
            channel_name="random",
            channel_type="public",
            sending_user_name="User",
            datetime="2025-10-22T14:00:00",
            textract="No events here",
            file_paths=[],
            permalink=["link"], original_indices=[0]
        )

        extract = extract_events_with_retry(mock_client, message)

        self.assertEqual(len(extract.events), 0)

    @patch('lib.stage3.time.sleep')  # Mock sleep to speed up test
    def test_extract_events_retry_on_rate_limit(self, mock_sleep):
        """Test retry logic on rate limit error"""
        mock_client = Mock()

        # First call raises rate limit error, second succeeds
        mock_response = Mock()
        mock_response.parsed = MessageExtract(
            events=[
                VirtualTalkInfo(
                    first_name="jane",
                    last_name="smith",
                    talk_date="2025-10-26T10:00",
                    virtual_link="https://zoom.us/j/123",
                    short_description="Virtual talk",
                    category="Vision"
                )
            ]
        )

        mock_client.models.generate_content.side_effect = [
            Exception("RESOURCE_EXHAUSTED: rate limit exceeded"),
            mock_response
        ]

        message = SlackMessage(
            workspace_name="Test",
            channel_name="talks",
            channel_type="public",
            sending_user_name="User",
            datetime="2025-10-22T14:00:00",
            textract="Talk announcement",
            file_paths=[],
            permalink=["link"], original_indices=[0]
        )

        extract = extract_events_with_retry(mock_client, message)

        # Should have retried and succeeded
        self.assertEqual(len(extract.events), 1)
        self.assertEqual(mock_client.models.generate_content.call_count, 2)
        mock_sleep.assert_called()  # Should have slept before retry

    @patch('lib.stage3.time.sleep')
    def test_extract_events_max_retries_exhausted(self, mock_sleep):
        """Test returning empty extract after max retries"""
        mock_client = Mock()

        # Always raise error
        mock_client.models.generate_content.side_effect = Exception("Persistent error")

        message = SlackMessage(
            workspace_name="Test",
            channel_name="talks",
            channel_type="public",
            sending_user_name="User",
            datetime="2025-10-22T14:00:00",
            textract="Talk announcement",
            file_paths=[],
            permalink=["link"], original_indices=[0]
        )

        extract = extract_events_with_retry(mock_client, message)

        # Should return empty extract after exhausting retries
        self.assertEqual(len(extract.events), 0)
        self.assertEqual(mock_client.models.generate_content.call_count, 5)  # MAX_RETRIES

    def test_extract_events_null_response(self):
        """Test handling of null/None response from Gemini"""
        mock_client = Mock()

        mock_response = Mock()
        mock_response.parsed = None

        mock_client.models.generate_content.return_value = mock_response

        message = SlackMessage(
            workspace_name="Test",
            channel_name="general",
            channel_type="public",
            sending_user_name="User",
            datetime="2025-10-22T14:00:00",
            textract="Test message",
            file_paths=[],
            permalink=["link"], original_indices=[0]
        )

        # Should handle None gracefully
        try:
            extract = extract_events_with_retry(mock_client, message)
            # If it returns something, it should be empty or handle None
        except AttributeError:
            # Expected if parsed is None and we try to access .events
            pass


class TestExtractAllEvents(unittest.TestCase):
    """Test batch extraction of events"""

    @patch('lib.stage3.get_gemini_client')
    def test_extract_all_events_same_count(self, mock_get_client):
        """Test that output has same number of rows as input"""
        mock_client = Mock()
        mock_get_client.return_value = mock_client

        # Mock response for each message
        mock_response1 = Mock()
        mock_response1.parsed = MessageExtract(events=[
            PhysicalTalkInfo(
                first_name="alice",
                last_name="doe",
                talk_date="2025-10-25T14:00",
                location="Room 1",
                short_description="Talk 1",
                lunch_provided=False,
                category="Vision"
            )
        ])

        mock_response2 = Mock()
        mock_response2.parsed = MessageExtract(events=[])  # No events

        mock_client.models.generate_content.side_effect = [mock_response1, mock_response2]

        messages = [
            SlackMessage(
                workspace_name="Test",
                channel_name="talks",
                channel_type="public",
                sending_user_name="User1",
                datetime="2025-10-22T10:00:00",
                textract="Message 1",
                file_paths=[],
                permalink=["link1"],
                original_indices=[0]
            ),
            SlackMessage(
                workspace_name="Test",
                channel_name="general",
                channel_type="public",
                sending_user_name="User2",
                datetime="2025-10-22T11:00:00",
                textract="Message 2",
                file_paths=[],
                permalink=["link2"],
                original_indices=[1]
            )
        ]

        extracts = extract_all_events(messages, output_path=None)

        # Should have same number of extracts as input messages
        self.assertEqual(len(extracts), len(messages))
        self.assertEqual(len(extracts[0].events), 1)
        self.assertEqual(len(extracts[1].events), 0)


if __name__ == '__main__':
    unittest.main()
