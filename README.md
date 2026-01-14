# Attio-Clay Enrichment Pipeline

Automated lead enrichment pipeline that runs hourly via GitHub Actions.

## How It Works

1. **Query Attio** for People records needing enrichment (have email, missing job_title/company/linkedin)
2. **Send to Clay** for enrichment via Clay's API
3. **Update Attio** with enriched data on the next run

## Setup

### 1. Clay Table Setup

Create a Clay table with these columns:

**Input columns:**
- `attio_record_id` (Text) - Links back to Attio
- `email` (Email) - Primary lookup field
- `first_name` (Text)
- `last_name` (Text)
- `company_name` (Text)

**Enrichment columns (Clay fills these):**
- `enriched_job_title` - From LinkedIn/enrichment
- `enriched_company` - From enrichment
- `enriched_linkedin` - LinkedIn URL
- `enriched_phone` - Phone number (optional)

### 2. GitHub Secrets

Add these secrets in GitHub repo Settings → Secrets → Actions:

| Secret | Description |
|--------|-------------|
| `ATTIO_API_KEY` | Your Attio API access token |
| `CLAY_API_KEY` | Your Clay API key |
| `CLAY_TABLE_ID` | The Clay table ID (from table URL) |

### 3. Test

1. Go to Actions tab
2. Select "Attio-Clay Enrichment"
3. Click "Run workflow"
4. Check logs for successful execution

## Enrichment Logic

A record needs enrichment if:
- Has email address (required for lookup)
- `clay_enrichment_status` is empty/null
- Missing at least one of: `job_title`, `company`, `linkedin`

## Attio Custom Attributes

The pipeline uses these custom attributes on People:

| Attribute | Purpose |
|-----------|---------|
| `clay_enrichment_status` | Tracks status: sent_to_clay, enriched, failed |
| `clay_sent_at` | When record was sent to Clay |
| `clay_enriched_at` | When enrichment completed |
| `clay_row_id` | Reference to Clay row |
| `enrichment_error` | Error message if failed |

## Schedule

Runs every hour at minute 0 (`:00`).

To change frequency, edit `.github/workflows/enrichment.yml`:

```yaml
schedule:
  - cron: '0 * * * *'  # Every hour
  # - cron: '*/30 * * * *'  # Every 30 minutes
  # - cron: '0 */6 * * *'  # Every 6 hours
```

## Manual Run

Trigger manually from GitHub Actions UI, or via CLI:

```bash
gh workflow run enrichment.yml
```
