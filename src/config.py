"""Configuration for the enrichment pipeline."""

import os

# Attio API
ATTIO_API_KEY = os.environ.get("ATTIO_API_KEY")
ATTIO_BASE_URL = "https://api.attio.com/v2"

# Clay API
CLAY_API_KEY = os.environ.get("CLAY_API_KEY")
CLAY_TABLE_ID = os.environ.get("CLAY_TABLE_ID")
CLAY_BASE_URL = "https://api.clay.com/v3/sources"

# Enrichment settings
BATCH_SIZE = 50  # Max records to process per run
RATE_LIMIT_SECONDS = 0.5  # Delay between API calls
ENRICHMENT_TIMEOUT_HOURS = 2  # Mark as stuck after this many hours

# Fields that indicate a record needs enrichment
ENRICHMENT_FIELDS = ["job_title", "primary_company", "linkedin"]
