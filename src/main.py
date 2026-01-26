#!/usr/bin/env python3
"""
Attio-Clay Enrichment Pipeline

This script runs hourly via GitHub Actions to:
1. Link enriched contacts to their companies in Attio
2. Find unenriched leads in Attio
3. Send them to Clay webhook for enrichment

Clay enriches data and sends results back directly to Attio via HTTP API.
This script then links the enriched contacts to their companies.

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


def link_companies(attio: AttioClient) -> list:
    """
    Find enriched records without company linked and link them.

    Returns:
        List of record IDs that were linked to companies
    """
    log("Querying Attio for enriched records needing company linking...")
    records = attio.query_enriched_without_company(limit=BATCH_SIZE)

    log(f"Found {len(records)} records needing company linking")

    if not records:
        return []

    linked_ids = []

    for i, record in enumerate(records):
        values = record.get("values", {})
        record_id = record.get("id", {}).get("record_id")

        # Get the enriched company name
        enriched_company = attio._extract_text_value(values, "enriched_company_name")

        if not record_id or not enriched_company:
            log(f"  Skipping record - missing ID or company name")
            continue

        log(f"  [{i+1}/{len(records)}] Linking to company: {enriched_company}...")

        # Find or create the company
        company_id = attio.find_or_create_company(enriched_company)

        if not company_id:
            log(f"    Failed to find/create company")
            continue

        # Link person to company
        success = attio.link_person_to_company(record_id, company_id)

        if success:
            log(f"    Successfully linked to company")
            linked_ids.append(record_id)
        else:
            log(f"    Failed to link to company")

        # Rate limiting
        time.sleep(RATE_LIMIT_SECONDS)

    log(f"Linked {len(linked_ids)} records to companies")
    return linked_ids


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

    # PHASE 1: Link enriched records to companies
    log("")
    log("PHASE 1: Linking enriched records to companies")
    log("-" * 40)
    try:
        linked_ids = link_companies(attio)
    except Exception as e:
        log(f"ERROR during company linking: {e}")
        import traceback
        traceback.print_exc()
        linked_ids = []

    # PHASE 2: Send unenriched records to Clay
    log("")
    log("PHASE 2: Sending unenriched records to Clay webhook")
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
    log("Pipeline Complete")
    log(f"  - Linked {len(linked_ids)} records to companies")
    log(f"  - Sent {len(sent_ids)} records to Clay for enrichment")
    log("=" * 60)


if __name__ == "__main__":
    main()
