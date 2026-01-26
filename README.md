# Attio-Clay Enrichment Pipeline

Automated lead enrichment pipeline that runs hourly via GitHub Actions.

## How It Works

```
┌─────────┐     ┌─────────┐     ┌─────────┐     ┌─────────────────┐
│  Attio  │ ──► │  Clay   │ ──► │  Attio  │ ──► │ GitHub Actions  │
│ (query) │     │(enrich) │     │ (update)│     │ (link company)  │
└─────────┘     └─────────┘     └─────────┘     └─────────────────┘
```

1. **Query Attio** for People records needing enrichment (have email, missing job_title/company/linkedin)
2. **Send to Clay** for enrichment via webhook
3. **Clay enriches** and updates Attio directly via HTTP API (job_title, linkedin, enriched_company_name)
4. **GitHub Actions links** enriched contacts to their companies in Attio

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
| `clay_enrichment_status` | Tracks status: sent_to_clay, enriched, company_linked, failed |
| `clay_sent_at` | When record was sent to Clay |
| `clay_enriched_at` | When enrichment completed |
| `clay_row_id` | Reference to Clay row |
| `enriched_company_name` | Company name from Clay (text) - used for auto-linking |
| `enrichment_error` | Error message if failed |

## Company Linking

When Clay enriches a contact, it stores the company name in `enriched_company_name` (a text field).
The GitHub Actions pipeline then:

1. Finds records with `enriched_company_name` but no `company` link
2. Searches Attio for existing Company with that name
3. Creates the Company if not found
4. Links the Person to the Company

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
