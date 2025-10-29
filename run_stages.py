#!/usr/bin/env python3
"""
Functional API for Slack scraping and event extraction
Runs Stage 1-4: Scraping → Aggregation → Extraction → Calendar
"""

import argparse
from datetime import datetime
from pathlib import Path

from lib import stage1, stage2_aggregate, stage3, stage4
from dotenv import load_dotenv

load_dotenv()

def main():
    """Run Stage 1-4 pipeline"""
    parser = argparse.ArgumentParser(
        description='Slack scraping and event extraction pipeline'
    )
    parser.add_argument('--start_datetime', required=True,
                       help='Start datetime in format YYYY-MM-DDTHH:MM:SS')
    parser.add_argument('--end_datetime', required=True,
                       help='End datetime in format YYYY-MM-DDTHH:MM:SS')
    parser.add_argument('--output_dir', default='output',
                       help='Output directory for JSON files')
    parser.add_argument('--skip-calendar', action='store_true',
                       help='Skip Stage 4 (Google Calendar integration)')
    parser.add_argument('--overwrite', action='store_true',
                       help='Overwrite existing calendar events (delete and recreate)')
    parser.add_argument('--use-cache', action='store_true',
                       help='Use cached output files if they exist (skip stages that are already done)')

    args = parser.parse_args()

    # Parse datetimes
    start_dt = datetime.fromisoformat(args.start_datetime)
    end_dt = datetime.fromisoformat(args.end_datetime)

    # Create output paths
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = f"{start_dt.strftime('%Y%m%d_%H%M')}_{end_dt.strftime('%Y%m%d_%H%M')}"
    stage1_output = output_dir / f"stage1_messages_{timestamp}.json"
    stage2_output = output_dir / f"stage2_aggregated_{timestamp}.json"
    stage3_output = output_dir / f"stage3_events_{timestamp}.json"

    # Run Stage 1: Scraping
    print("\n" + "="*80)
    print("Running Stage 1: Slack Message Scraping")
    print("="*80)
    if args.use_cache and stage1_output.exists():
        print(f"✓ Using cached file: {stage1_output}")
    else:
        stage1.main(start_dt, end_dt, stage1_output)

    # Run Stage 2: Aggregation
    print("\n" + "="*80)
    print("Running Stage 2: Message Aggregation")
    print("="*80)
    if args.use_cache and stage2_output.exists():
        print(f"✓ Using cached file: {stage2_output}")
    else:
        stage2_aggregate.main(stage1_output, stage2_output)

    # Run Stage 3: Event Extraction
    print("\n" + "="*80)
    print("Running Stage 3: Event Extraction with Gemini")
    print("="*80)
    if args.use_cache and stage3_output.exists():
        print(f"✓ Using cached file: {stage3_output}")
    else:
        stage3.main(stage2_output, stage3_output)

    # Run Stage 4: Google Calendar Integration (optional)
    if not args.skip_calendar:
        print("\n" + "="*80)
        print("Running Stage 4: Google Calendar Integration")
        print("="*80)
        stage4.main(stage3_output, overwrite=args.overwrite)
    else:
        print("\n⊘ Skipping Stage 4 (Google Calendar)")

    print("\n" + "="*80)
    print("PIPELINE COMPLETE")
    print("="*80)
    print(f"Stage 1 output: {stage1_output}")
    print(f"Stage 2 output: {stage2_output}")
    print(f"Stage 3 output: {stage3_output}")
    if not args.skip_calendar:
        print("Stage 4: Events added to Google Calendar")
    print("="*80)


if __name__ == "__main__":
    main()
