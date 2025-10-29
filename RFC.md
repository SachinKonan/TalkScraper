# Slack Bot

## Stage 1:
- Scrape all public/extended conversations mentioned in lib.init_config.SLACK_CONFIG, convert these into a standardized schema of:
  - workspace_name: str,
    channel_name: str,
    channel_type: 'external' or 'public',
    sending_user_name: str,
    datetime: str (YYYY-MM-DDTHH:mm:ss),
    textract: str (text extracted from slack message),
    file_paths: list[str],
    permalink: str
- only count replies that are sent as a new message, dont count replies that are in the same thread
- you will accept a start_dt -> end_dt and only look within that range.
- maybe this schema should be json now that i think about it
- cache the json to disk. 
- Create a mock'ed unitest (no external calls) that ensure that we have this schema. test for errors from the slack api call. dont cache the file to disk so mock that

## Stage 2:
- Extract the json file from Stage 1, for each item, we want to call gemini and for each, we want to extract a MessageExtract object, which has a list[PhysicalTalkInfo | VirtualTalkInfo | PhysicalEventInfo | VirtualEventInfo]
- TalkInfo has the following fields:
  - first_name: str # lower-case
  - last_name: str | None # lower-case
  - talk_date: str # YYYY-MM-DDTHH:MM (if any part is unknown, fill with '_')
  - location: str # (should be a physical location)
  - short_description: str # describe the speaker and the event in up to three sentences
  - lunch_provided: boolean
  - category: str # can be one of: Vision | Robotics | Machine Learning | Reinforcement Learning | Unrecognized
- VirtualTalkInfo has the following fields:
  - first_name: str # lower-case
  - last_name: str | None # lower-case
  - talk_date: str # YYYY-MM-DDTHH:MM (if any part is unknown, fill with '_')
  - virtual_link: str # (should be a link to the talk)
  - short_description: str # describe the speaker and the event in three sentences
  - category: str # can be one of: Vision | Robotics | Machine Learning | Reinforcement Learning | Unrecognized
- PhysicalEventInfo has the following fields:
    - simple_event_name: str # this should be the event name in lower-case
    - event_date: str # YYYY-MM-DDTHH:MM (if any part is unknown, fill with '_')
    - location: str # should be a physical location
    - short_description: str # describe the event in up to three-sentences
    - lunch_provided: boolean
    - is_academic: boolean
- VirtualEventInfo has the following fields:
    - simple_event_name: str # this should be the event name in lower-case
    - event_date: str # YYYY-MM-DDTHH:MM (if any part is unknown, fill with '_')
    - virtual_link: str # should be a link to the event
    - short_description: str # describe the event in up to three-sentences
    - is_academic: boolean
- provide gemini with the textract from the message + the datetime the user sent it + the channel
  - provide some examples of how to extract relative dates or to fill in unknown dates
- make sure you get an output json schema that has the same number of rows in the source json, but each element is a list of jsons that is either Talk/VirtualTalk/PhyiscalEvent/VirtualEvent
  - use pydantic models to make serialization easy
- creaate a mocke'd unitest (no external gemini calls) to test retries on gemini, null responses from gemini, good responses from gemini. dont cache the file to disk, so mock that. got it?
- 