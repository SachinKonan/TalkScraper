#!/usr/bin/env python3
"""
Pydantic models for Slack scraping and event extraction
"""

import json
from pathlib import Path
from typing import Literal, Optional, TypeVar, Type
from pydantic import BaseModel, Field

T = TypeVar('T', bound=BaseModel)


# ============================================================================
# Stage 1: Slack Message Schema
# ============================================================================

class SlackMessage(BaseModel):
    """Standardized schema for scraped Slack messages"""
    workspace_name: str
    channel_name: str
    channel_type: Literal['external', 'public']
    sending_user_name: str
    datetime: str  # YYYY-MM-DDTHH:mm:ss
    textract: str
    urls: list[str] = Field(default_factory=list)
    file_paths: list[str] = Field(default_factory=list)
    permalink: list[str]  # List to support aggregated messages
    original_indices: list[int]  # Row indices from Stage 1 (for traceability)

    @classmethod
    def from_json_file(cls, path: Path) -> list['SlackMessage']:
        """Load list of SlackMessage from JSON file"""
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return [cls(**item) for item in data]

    @staticmethod
    def to_json_file(messages: list['SlackMessage'], path: Path):
        """Save list of SlackMessage to JSON file"""
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(
                [msg.model_dump() for msg in messages],
                f,
                indent=2,
                ensure_ascii=False
            )


# ============================================================================
# Stage 2: Event Extraction Schema
# ============================================================================

class PhysicalTalkInfo(BaseModel):
    """Physical talk event"""
    first_name: str = Field(description="Lowercase first name")
    last_name: Optional[str] = Field(default=None, description="Lowercase last name")
    talk_date: str = Field(description="YYYY-MM-DDTHH:MM, use '_' for unknown parts")
    location: str = Field(description="Physical location")
    short_description: str = Field(description="Up to 3 sentences describing speaker and event")
    lunch_provided: bool
    category: Literal['Vision', 'Robotics', 'Machine Learning', 'Reinforcement Learning', 'Unrecognized']


class VirtualTalkInfo(BaseModel):
    """Virtual talk event"""
    first_name: str = Field(description="Lowercase first name")
    last_name: Optional[str] = Field(default=None, description="Lowercase last name")
    talk_date: str = Field(description="YYYY-MM-DDTHH:MM, use '_' for unknown parts")
    virtual_link: str = Field(description="Link to the virtual talk")
    short_description: str = Field(description="3 sentences describing speaker and event")
    category: Literal['Vision', 'Robotics', 'Machine Learning', 'Reinforcement Learning', 'Unrecognized']


class PhysicalEventInfo(BaseModel):
    """Physical event (non-talk)"""
    simple_event_name: str = Field(description="Lowercase event name")
    event_date: str = Field(description="YYYY-MM-DDTHH:MM, use '_' for unknown parts")
    location: str = Field(description="Physical location")
    short_description: str = Field(description="Up to 3 sentences describing the event")
    lunch_provided: bool
    is_academic: bool
    is_recurring: Literal['weekly', 'biweekly', 'monthly', 'unknown', 'none'] = Field(
        default='none',
        description="Recurrence pattern: 'weekly', 'biweekly', 'monthly', 'unknown' (recurs but unclear), or 'none'"
    )


class VirtualEventInfo(BaseModel):
    """Virtual event (non-talk)"""
    simple_event_name: str = Field(description="Lowercase event name")
    event_date: str = Field(description="YYYY-MM-DDTHH:MM, use '_' for unknown parts")
    virtual_link: str = Field(description="Link to the virtual event")
    short_description: str = Field(description="Up to 3 sentences describing the event")
    is_academic: bool
    is_recurring: Literal['weekly', 'biweekly', 'monthly', 'unknown', 'none'] = Field(
        default='none',
        description="Recurrence pattern: 'weekly', 'biweekly', 'monthly', 'unknown' (recurs but unclear), or 'none'"
    )


class MessageExtract(BaseModel):
    """Extraction result for a single message"""
    events: list[PhysicalTalkInfo | VirtualTalkInfo | PhysicalEventInfo | VirtualEventInfo] = Field(
        default_factory=list,
        description="List of events extracted from the message"
    )

    @classmethod
    def from_json_file(cls, path: Path) -> list['MessageExtract']:
        """Load list of MessageExtract from JSON file"""
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return [cls(**item) for item in data]

    @staticmethod
    def to_json_file(extracts: list['MessageExtract'], path: Path):
        """Save list of MessageExtract to JSON file"""
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(
                [extract.model_dump() for extract in extracts],
                f,
                indent=2,
                ensure_ascii=False
            )
