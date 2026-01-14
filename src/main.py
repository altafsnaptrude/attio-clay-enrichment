#!/usr/bin/env python3
"""
Main enrichment pipeline script.

This script runs hourly via GitHub Actions and:
1. Queries Attio for unenriched leads
2. Sends them to Clay for enrichment
3. Checks for completed enrichments from previous runs
4. Updates Attio with enriched data
"""

import sys
import time
from datetime import datetime, timedelta, timezone

# Add src to path for imports
sys.path.insert(0, "src")

from config import BATCH_SIZE, RATE_LIMIT_SECONDS, ENRICHMENT_TIMEOUT_HOURS, ATTIO_API_KEY, CLAY_API_KEY, CLAY_TABLE_ID
from attio_client import AttioClient
from clay_client import ClayClient


def check_config():
    """Verify required environment variables are set."""
    missing = []
    if not ATTIO_API_KEY:
        missing.append("ATTIO_API_KEY")
    if not CLAY_API_KEY:
        missing.append("CLAY_API_KEY")
    if not CLAY_TABLE_ID:
        missing.append("CLAY_TABLE_ID")

    if missing:
        print(f"ERROR: Missing required environment variables: {', '.join(missing)}")
        sys.exit(1)


def process_pending_enrichments(attio: AttioClient, clay: ClayClient) -> tuple[int, int]:
    """
    Check for completed enrichments from previous runs.

    Returns:
        Tuple of (enriched_count, failed_count)
    """
    print("\n--- Checking for completed enrichments ---")

    pending = attio.query_pending_records(limit=100)
    print(f"Found {len(pending)} records pending enrichment")

    if not pending:
        return 0, 0

    enriched_count = 0
    failed_count = 0
    now = datetime.now(timezone.utc)
    timeout_threshold = now - timedelta(hours=ENRICHMENT_TIMEOUT_HOURS)

    # Get Attio IDs to look up in Clay
    attio_ids = [r["record_id"] for r in pending if r["record_id"]]

    # Find matching Clay rows
    clay_rows = clay.find_rows_by_attio_ids(attio_ids)
    print(f"Found {len(clay_rows)} matching rows in Clay")

    for record in pending:
        record_id = record["record_id"]
        sent_at = record.get("sent_at")

        # Check if we have enrichment results
        if record_id in clay_rows:
            clay_row = clay_rows[record_id]

            # Extract enriched data from Clay row
            enriched_data = {
                "job_title": clay_row.get("enriched_job_title") or clay_row.get("job_title"),
                "company": clay_row.get("enriched_company") or clay_row.get("company"),
                "linkedin_url": clay_row.get("enriched_linkedin") or clay_row.get("linkedin_url"),
                "phone": clay_row.get("enriched_phone") or clay_row.get("phone"),
            }

            # Check if we actually got any enrichment
            if any(enriched_data.values()):
                if attio.mark_enriched(record_id, enriched_data):
                    print(f"  ✅ Enriched {record_id}")
                    enriched_count += 1
                else:
                    print(f"  ❌ Failed to update {record_id}")
                    failed_count += 1
            else:
                # Clay processed but no data found
                if attio.mark_failed(record_id, "No enrichment data returned from Clay"):
                    print(f"  ⚠️ No data for {record_id}")
                    failed_count += 1

        # Check for stuck/timed out records
        elif sent_at and sent_at < timeout_threshold:
            if attio.mark_failed(record_id, f"Enrichment timed out after {ENRICHMENT_TIMEOUT_HOURS} hours"):
                print(f"  ⏰ Timed out {record_id}")
                failed_count += 1

    return enriched_count, failed_count


def send_for_enrichment(attio: AttioClient, clay: ClayClient) -> tuple[int, int]:
    """
    Find unenriched records and send them to Clay.

    Returns:
        Tuple of (sent_count, error_count)
    """
    print("\n--- Sending new records for enrichment ---")

    records = attio.query_unenriched_records(limit=BATCH_SIZE)
    print(f"Found {len(records)} records needing enrichment")

    if not records:
        return 0, 0

    sent_count = 0
    error_count = 0

    for record in records:
        record_id = record["record_id"]
        email = record.get("email")

        if not email:
            print(f"  ⚠️ Skipping {record_id} - no email")
            continue

        print(f"  Processing {record_id} ({email})...")

        # Send to Clay
        clay_row_id = clay.send_for_enrichment(record)

        if clay_row_id:
            # Update Attio status
            if attio.mark_sent_to_clay(record_id, clay_row_id):
                print(f"    ✅ Sent to Clay (row: {clay_row_id})")
                sent_count += 1
            else:
                print(f"    ❌ Failed to update Attio status")
                error_count += 1
        else:
            print(f"    ❌ Failed to send to Clay")
            attio.mark_failed(record_id, "Failed to send to Clay")
            error_count += 1

        # Rate limiting
        time.sleep(RATE_LIMIT_SECONDS)

    return sent_count, error_count


def main():
    """Main entry point for the enrichment pipeline."""
    print("=" * 60)
    print(f"Attio-Clay Enrichment Pipeline")
    print(f"Started at: {datetime.now().isoformat()}")
    print("=" * 60)

    # Verify configuration
    check_config()

    # Initialize clients
    attio = AttioClient()
    clay = ClayClient()

    # Step 1: Process any completed enrichments from previous runs
    enriched, failed = process_pending_enrichments(attio, clay)
    print(f"\nPending results: {enriched} enriched, {failed} failed")

    # Step 2: Send new records for enrichment
    sent, errors = send_for_enrichment(attio, clay)
    print(f"\nNew records: {sent} sent to Clay, {errors} errors")

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Records enriched:      {enriched}")
    print(f"Records sent to Clay:  {sent}")
    print(f"Errors:                {failed + errors}")
    print(f"Completed at:          {datetime.now().isoformat()}")
    print("=" * 60)

    # Exit with error code if there were failures
    if failed + errors > 0:
        print("\n⚠️ Completed with some errors")
    else:
        print("\n✅ Completed successfully")


if __name__ == "__main__":
    main()
