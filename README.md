# Attio-Clay Enrichment Pipeline

Automatically enriches leads in Attio that weren't auto-enriched, using Clay for data enrichment.

## How It Works

```
Every hour (via GitHub Actions):

1. Check for completed enrichments from previous runs
   └── Update Attio records with enriched data

2. Find new unenriched leads in Attio
   └── Has email, missing job_title/company/linkedin
   └── Send to Clay for enrichment

3. Next hour: Repeat
```

## Setup

### 1. Attio Custom Attributes

The following attributes should exist on the People object:
- `clay_enrichment_status` (text) - Status: pending, sent_to_clay, enriched, failed, skipped
- `clay_sent_at` (timestamp) - When record was sent to Clay
- `clay_enriched_at` (timestamp) - When enrichment completed
- `clay_row_id` (text) - Clay's row ID for tracking
- `enrichment_error` (text) - Error message if failed

### 2. Clay Table

Create a Clay table with:
- **Input columns:** `attio_record_id`, `email`, `first_name`, `last_name`, `company_name`
- **Enrichment columns:** Configure your preferred enrichment sources (LinkedIn, Clearbit, etc.)
- **Output columns:** `enriched_job_title`, `enriched_company`, `enriched_linkedin`, `enriched_phone`

### 3. GitHub Secrets

Add these secrets to your repository (Settings → Secrets → Actions):

| Secret | Description |
|--------|-------------|
| `ATTIO_API_KEY` | Your Attio API access token |
| `CLAY_API_KEY` | Your Clay API key |
| `CLAY_TABLE_ID` | The ID of your Clay enrichment table |

### 4. Enable GitHub Actions

The workflow runs automatically every hour. You can also trigger it manually from the Actions tab.

## Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
export ATTIO_API_KEY="your-attio-key"
export CLAY_API_KEY="your-clay-key"
export CLAY_TABLE_ID="your-table-id"

# Run the pipeline
python src/main.py
```

## Configuration

Edit `src/config.py` to adjust:
- `BATCH_SIZE` - Max records to process per run (default: 50)
- `RATE_LIMIT_SECONDS` - Delay between API calls (default: 0.5)
- `ENRICHMENT_TIMEOUT_HOURS` - Mark as failed after this many hours (default: 2)

## Troubleshooting

### Records not being enriched
1. Check if the record has an email address
2. Check if `clay_enrichment_status` is already set
3. Verify the Clay table is configured correctly

### Enrichment failing
1. Check GitHub Actions logs for errors
2. Verify API keys are correct
3. Check Clay table for the row and its enrichment status

## License

MIT
