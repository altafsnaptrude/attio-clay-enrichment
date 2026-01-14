#!/usr/bin/env python3
"""
Attio-Clay Enrichment Pipeline

This script runs hourly via GitHub Actions to:
1. Find unenriched leads in Attio
2. Send them to Clay webhook for enrichment

Note: Clay enriches data automatically and can send results back via
a webhook action configured in Clay, or you can manually update Attio
from Clay's UI.

Environment variables required:
- ATTIO_API_KEY: Attio API access token
- CLAY_WEBHOOK_URL: Clay webhook URL for receiving data
"""

import os
import sys
import time
from datetime import datetime

from attio_client import AttioClient
from clay_client import ClayClient


# Configuration
BATCH_SIZE = 50  # Max records to process per run
RATE_LIMIT_SECONDS = 0.5  # Delay between API calls


def log(message: str):
    """Print timestamped log message."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")


def send_to_clay(attio: AttioClient, clay: ClayClient) -> list:
    """
    Find unenriched records in Attio and send them to Clay.

    Returns:
        List of record IDs that were sent to Clay
    """
    log("Querying Attio for unenriched records...")
    unenriched = attio.query_unenriched_records(limit=BATCH_SIZE)

    log(f"Found {len(unenriched)} records needing enrichment")

    if not unenriched:
        return []

    sent_ids = []

    for i, record in enumerate(unenriched):
        record_data = attio.extract_record_data(record)
        record_id = record_data.get("attio_record_id")
        email = record_data.get("email")

        if not record_id or not email:
            log(f"  Skipping record - missing ID or email")
            continue

        log(f"  [{i+1}/{len(unenriched)}] Processing {email}...")

        # Send to Clay first
        success = clay.send_for_enrichment(record_data)

        if success:
            log(f"    Sent to Clay successfully")
            # Mark as sent in Attio
            if attio.mark_sent_to_clay(record_id):
                sent_ids.append(record_id)
                log(f"    Updated Attio status to sent_to_clay")
            else:
                log(f"    Warning: Failed to update Attio status")
        else:
            log(f"    Failed to send to Clay")
            attio.mark_failed(record_id, "Failed to send to Clay webhook")

        # Rate limiting
        time.sleep(RATE_LIMIT_SECONDS)

    log(f"Sent {len(sent_ids)} records to Clay")
    return sent_ids


def main():
    """Main entry point."""
    log("=" * 60)
    log("Attio-Clay Enrichment Pipeline Starting")
    log("=" * 60)

    # Validate environment
    required_vars = ["ATTIO_API_KEY", "CLAY_WEBHOOK_URL"]
    missing = [var for var in required_vars if not os.environ.get(var)]

    if missing:
        log(f"ERROR: Missing required environment variables: {', '.join(missing)}")
        sys.exit(1)

    # Initialize clients
    try:
        attio = AttioClient()
        clay = ClayClient()
    except Exception as e:
        log(f"ERROR: Failed to initialize clients: {e}")
        sys.exit(1)

    # Send unenriched records to Clay
    log("")
    log("Sending unenriched records to Clay webhook")
    log("-" * 40)
    try:
        sent_ids = send_to_clay(attio, clay)
    except Exception as e:
        log(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        sent_ids = []

    # Summary
    log("")
    log("=" * 60)
    log(f"Pipeline Complete - Sent {len(sent_ids)} records to Clay")
    log("=" * 60)


if __name__ == "__main__":
    main()
