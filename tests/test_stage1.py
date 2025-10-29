#!/usr/bin/env python3
"""
Unit tests for Stage 1: Slack Message Scraping
Tests schema validation, error handling, and functionality with mocked Slack API
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
from slack_sdk.errors import SlackApiError

from lib.models import SlackMessage
from lib.stage1 import (
    get_user_name,
    extract_text_from_message,
    scrape_workspace,
    scrape_all_workspaces
)


class TestSlackMessageSchema(unittest.TestCase):
    """Test SlackMessage Pydantic model"""

    def test_valid_schema(self):
        """Test valid message passes validation"""
        msg = SlackMessage(
            workspace_name="TestWorkspace",
            channel_name="general",
            channel_type="public",
            sending_user_name="John Doe",
            datetime="2025-10-22T14:30:00",
            textract="Hello world",
            file_paths=[],
            permalink=["https://slack.com/archives/C123/p456"],
            original_indices=[0]
        )
        self.assertEqual(msg.workspace_name, "TestWorkspace")
        self.assertEqual(msg.channel_type, "public")

    def test_invalid_channel_type(self):
        """Test invalid channel_type raises validation error"""
        with self.assertRaises(ValueError):
            SlackMessage(
                workspace_name="TestWorkspace",
                channel_name="general",
                channel_type="invalid",  # Must be 'public' or 'external'
                sending_user_name="John Doe",
                datetime="2025-10-22T14:30:00",
                textract="Hello",
                file_paths=[],
                permalink=[],
                original_indices=[0]
            )

    def test_external_channel_type(self):
        """Test external channel type is valid"""
        msg = SlackMessage(
            workspace_name="TestWorkspace",
            channel_name="external-channel",
            channel_type="external",
            sending_user_name="Jane Smith",
            datetime="2025-10-22T15:00:00",
            textract="External message",
            file_paths=[],
            permalink=["https://slack.com/link"],
            original_indices=[0]
        )
        self.assertEqual(msg.channel_type, "external")


class TestUserNameRetrieval(unittest.TestCase):
    """Test user name retrieval with caching"""

    def test_get_user_name_success(self):
        """Test successful user name retrieval"""
        mock_client = Mock()
        mock_client.users_info.return_value = {
            'user': {
                'real_name': 'John Doe',
                'name': 'johndoe'
            }
        }

        user_cache = {}
        name = get_user_name(mock_client, 'U123', user_cache)

        self.assertEqual(name, 'John Doe')
        self.assertEqual(user_cache['U123'], 'John Doe')
        mock_client.users_info.assert_called_once_with(user='U123')

    def test_get_user_name_cached(self):
        """Test cached user name doesn't make API call"""
        mock_client = Mock()
        user_cache = {'U123': 'Cached User'}

        name = get_user_name(mock_client, 'U123', user_cache)

        self.assertEqual(name, 'Cached User')
        mock_client.users_info.assert_not_called()

    def test_get_user_name_api_error(self):
        """Test API error returns user ID"""
        mock_client = Mock()
        mock_client.users_info.side_effect = SlackApiError("Error", {"error": "user_not_found"})

        user_cache = {}
        name = get_user_name(mock_client, 'U123', user_cache)

        self.assertEqual(name, 'U123')
        self.assertEqual(user_cache['U123'], 'U123')


class TestTextExtraction(unittest.TestCase):
    """Test text extraction from Slack messages"""

    def test_extract_simple_text(self):
        """Test extracting simple text"""
        mock_client = Mock()
        user_cache = {}
        message = {'text': 'Hello world'}
        text = extract_text_from_message(message, mock_client, user_cache)
        self.assertEqual(text, 'Hello world')

    def test_extract_with_attachments(self):
        """Test extracting text with attachments"""
        mock_client = Mock()
        user_cache = {}
        message = {
            'text': 'Main text',
            'attachments': [
                {'text': 'Attachment 1'},
                {'fallback': 'Attachment 2'}
            ]
        }
        text = extract_text_from_message(message, mock_client, user_cache)
        self.assertIn('Main text', text)
        self.assertIn('Attachment 1', text)
        self.assertIn('Attachment 2', text)

    def test_extract_empty_message(self):
        """Test extracting from empty message"""
        mock_client = Mock()
        user_cache = {}
        message = {}
        text = extract_text_from_message(message, mock_client, user_cache)
        self.assertEqual(text, '')

    def test_replace_user_mentions(self):
        """Test that user mentions are replaced with names"""
        mock_client = Mock()
        mock_client.users_info.return_value = {
            'user': {'real_name': 'John Doe'}
        }
        user_cache = {}
        message = {'text': 'Hello <@U12345> how are you?'}
        text = extract_text_from_message(message, mock_client, user_cache)
        self.assertEqual(text, 'Hello John Doe how are you?')
        # Verify user was cached
        self.assertEqual(user_cache['U12345'], 'John Doe')

    def test_replace_multiple_user_mentions(self):
        """Test replacing multiple user mentions"""
        mock_client = Mock()
        user_cache = {
            'U111': 'Alice',
            'U222': 'Bob'
        }
        message = {'text': 'Meeting with <@U111> and <@U222> tomorrow'}
        text = extract_text_from_message(message, mock_client, user_cache)
        self.assertEqual(text, 'Meeting with Alice and Bob tomorrow')
        # Should use cache, not call API
        mock_client.users_info.assert_not_called()


class TestWorkspaceScraping(unittest.TestCase):
    """Test workspace scraping with mocked Slack API"""

    @patch('lib.stage1.WebClient')
    @patch.dict('os.environ', {'TEST_TOKEN': 'xoxb-test-token'})
    def test_scrape_workspace_success(self, mock_webclient_class):
        """Test successful workspace scraping"""
        # Mock WebClient instance
        mock_client = Mock()
        mock_webclient_class.return_value = mock_client

        # Mock channels list
        mock_client.conversations_list.return_value = {
            'channels': [
                {
                    'name': 'general',
                    'id': 'C123',
                    'is_ext_shared': False,
                    'is_private': False
                }
            ]
        }

        # Mock message history
        mock_client.conversations_history.return_value = {
            'messages': [
                {
                    'user': 'U123',
                    'text': 'Test message',
                    'ts': '1729702800.0',  # 2024-10-23 17:00:00
                    'thread_ts': '1729702800.0'
                }
            ]
        }

        # Mock user info
        mock_client.users_info.return_value = {
            'user': {'real_name': 'Test User'}
        }

        # Mock permalink
        mock_client.chat_getPermalink.return_value = {
            'permalink': 'https://slack.com/link'
        }

        workspace_config = {
            'workspace_name': 'TestWorkspace',
            'token_env_var': 'TEST_TOKEN'
        }

        messages = scrape_workspace(
            workspace_config,
            start_timestamp=1729702000.0,
            end_timestamp=1729705600.0
        )

        self.assertEqual(len(messages), 1)
        self.assertIsInstance(messages[0], SlackMessage)
        self.assertEqual(messages[0].workspace_name, 'TestWorkspace')
        self.assertEqual(messages[0].channel_name, 'general')
        self.assertEqual(messages[0].sending_user_name, 'Test User')

    @patch('lib.stage1.WebClient')
    def test_scrape_workspace_missing_token(self, mock_webclient_class):
        """Test error when token is missing"""
        workspace_config = {
            'workspace_name': 'TestWorkspace',
            'token_env_var': 'NONEXISTENT_TOKEN'
        }

        with self.assertRaises(ValueError) as context:
            scrape_workspace(workspace_config, 0.0, 1.0)

        self.assertIn('Missing token', str(context.exception))

    @patch('lib.stage1.WebClient')
    @patch.dict('os.environ', {'TEST_TOKEN': 'xoxb-test-token'})
    def test_scrape_workspace_api_error(self, mock_webclient_class):
        """Test handling of Slack API errors"""
        mock_client = Mock()
        mock_webclient_class.return_value = mock_client

        # Mock API error
        mock_client.conversations_list.side_effect = SlackApiError(
            "Error",
            {"error": "invalid_auth"}
        )

        workspace_config = {
            'workspace_name': 'TestWorkspace',
            'token_env_var': 'TEST_TOKEN'
        }

        with self.assertRaises(RuntimeError) as context:
            scrape_workspace(workspace_config, 0.0, 1.0)

        self.assertIn('Failed to fetch channels', str(context.exception))

    @patch('lib.stage1.WebClient')
    @patch.dict('os.environ', {'TEST_TOKEN': 'xoxb-test-token'})
    def test_skip_threaded_replies(self, mock_webclient_class):
        """Test that threaded replies are skipped"""
        mock_client = Mock()
        mock_webclient_class.return_value = mock_client

        mock_client.conversations_list.return_value = {
            'channels': [{'name': 'general', 'id': 'C123', 'is_ext_shared': False, 'is_private': False}]
        }

        # One top-level message and one threaded reply
        mock_client.conversations_history.return_value = {
            'messages': [
                {'user': 'U123', 'text': 'Top level', 'ts': '100.0', 'thread_ts': '100.0'},
                {'user': 'U456', 'text': 'Reply', 'ts': '101.0', 'thread_ts': '100.0'}  # Should be skipped
            ]
        }

        mock_client.users_info.return_value = {'user': {'real_name': 'User'}}
        mock_client.chat_getPermalink.return_value = {'permalink': 'link'}

        workspace_config = {'workspace_name': 'Test', 'token_env_var': 'TEST_TOKEN'}
        messages = scrape_workspace(workspace_config, 0.0, 200.0)

        # Should only get the top-level message
        self.assertEqual(len(messages), 1)
        self.assertIn('Top level', messages[0].textract)


class TestScrapeAllWorkspaces(unittest.TestCase):
    """Test scraping all configured workspaces"""

    @patch('lib.stage1.scrape_workspace')
    @patch('lib.stage1.SLACK_CONFIG', [
        {'workspace_name': 'Workspace1', 'token_env_var': 'TOKEN1'},
        {'workspace_name': 'Workspace2', 'token_env_var': 'TOKEN2'}
    ])
    def test_scrape_all_workspaces(self, mock_scrape):
        """Test scraping multiple workspaces"""
        # Mock returning messages
        mock_scrape.side_effect = [
            [Mock(spec=SlackMessage)],  # Workspace 1
            [Mock(spec=SlackMessage), Mock(spec=SlackMessage)]  # Workspace 2
        ]

        start_dt = datetime(2025, 10, 22, 10, 0)
        end_dt = datetime(2025, 10, 22, 12, 0)

        messages = scrape_all_workspaces(start_dt, end_dt, output_path=None)

        self.assertEqual(len(messages), 3)
        self.assertEqual(mock_scrape.call_count, 2)


if __name__ == '__main__':
    unittest.main()
