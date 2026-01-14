#!/usr/bin/env python3
"""
Attio-Clay Enrichment Pipeline

This script runs hourly via GitHub Actions to:
1. Find unenriched leads in Attio
2. Send them to Clay for enrichment
3. Update Attio with enriched data from Clay

Environment variables required:
- ATTIO_API_KEY: Attio API access token
- CLAY_API_KEY: Clay API key
- CLAY_TABLE_ID: Clay table ID for enrichment
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
CLAY_PROCESSING_WAIT = 120  # Seconds to wait for Clay to process


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
        
        # Mark as sent in Attio first
        if not attio.mark_sent_to_clay(record_id):
            log(f"    Failed to update Attio status, skipping")
            continue
        
        # Send to Clay
        clay_row_id = clay.send_for_enrichment(record_data)
        
        if clay_row_id:
            log(f"    Sent to Clay (row: {clay_row_id})")
            sent_ids.append(record_id)
        else:
            log(f"    Failed to send to Clay")
            attio.mark_failed(record_id, "Failed to send to Clay")
        
        # Rate limiting
        time.sleep(RATE_LIMIT_SECONDS)
    
    log(f"Sent {len(sent_ids)} records to Clay")
    return sent_ids


def update_from_clay(attio: AttioClient, clay: ClayClient):
    """
    Check Clay for enriched data and update Attio records.
    """
    log("Querying Attio for records awaiting enrichment...")
    pending = attio.query_sent_to_clay_records(limit=100)
    
    log(f"Found {len(pending)} records with 'sent_to_clay' status")
    
    if not pending:
        return
    
    # Extract record IDs
    pending_ids = []
    for record in pending:
        record_id = record.get("id", {}).get("record_id")
        if record_id:
            pending_ids.append(record_id)
    
    log(f"Checking Clay for enrichment results...")
    enriched_data = clay.get_enriched_rows(pending_ids)
    
    log(f"Found {len(enriched_data)} enriched records in Clay")
    
    updated = 0
    for record_id, data in enriched_data.items():
        log(f"  Updating {record_id}...")
        
        if attio.mark_enriched(record_id, data):
            updated += 1
            log(f"    Updated with: job_title={data.get('job_title')}, company={data.get('company')}")
        else:
            log(f"    Failed to update Attio")
        
        time.sleep(RATE_LIMIT_SECONDS)
    
    log(f"Updated {updated} records in Attio")


def main():
    """Main entry point."""
    log("=" * 60)
    log("Attio-Clay Enrichment Pipeline Starting")
    log("=" * 60)
    
    # Validate environment
    required_vars = ["ATTIO_API_KEY", "CLAY_API_KEY", "CLAY_TABLE_ID"]
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
    
    # Phase 1: Check for results from previous runs
    log("")
    log("Phase 1: Updating Attio with enriched data from Clay")
    log("-" * 40)
    try:
        update_from_clay(attio, clay)
    except Exception as e:
        log(f"ERROR in Phase 1: {e}")
    
    # Phase 2: Send new records to Clay
    log("")
    log("Phase 2: Sending unenriched records to Clay")
    log("-" * 40)
    try:
        sent_ids = send_to_clay(attio, clay)
    except Exception as e:
        log(f"ERROR in Phase 2: {e}")
        sent_ids = []
    
    # Summary
    log("")
    log("=" * 60)
    log("Pipeline Complete")
    log("=" * 60)


if __name__ == "__main__":
    main()
