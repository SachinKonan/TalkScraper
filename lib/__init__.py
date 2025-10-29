"""
Slack Scraper Library
Modular components for scraping Slack and extracting talk information
"""

from .scrape_workspaces import main as scrape_workspaces
from .extract_slack import main as extract_slack
from .add_to_calendar import main as add_to_calendar

__all__ = ['scrape_workspaces', 'extract_slack', 'add_to_calendar']
