"""
Attio API client for lead enrichment pipeline.
"""

import os
import requests
from typing import Optional
from datetime import datetime, timezone


class AttioClient:
    """Client for interacting with the Attio API."""

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.environ.get("ATTIO_API_KEY")
        if not self.api_key:
            raise ValueError("ATTIO_API_KEY is required")

        self.base_url = "https://api.attio.com/v2"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def query_unenriched_records(self, limit: int = 50) -> list:
        """
        Query for People records that need enrichment.

        Criteria:
        - Has email address
        - clay_enrichment_status is empty OR null
        - Missing job_title OR company OR linkedin
        """
        url = f"{self.base_url}/objects/people/records/query"

        payload = {
            "limit": limit,
            "sorts": [
                {"attribute": "created_at", "direction": "desc"}
            ]
        }

        response = requests.post(url, headers=self.headers, json=payload)

        if not response.ok:
            print(f"Error querying records: {response.status_code} - {response.text}")
            return []

        data = response.json()
        records = data.get("data", [])

        # Filter for records needing enrichment
        unenriched = []
        for record in records:
            if self._needs_enrichment(record):
                unenriched.append(record)

        return unenriched

    def _needs_enrichment(self, record: dict) -> bool:
        """Check if a record needs enrichment."""
        values = record.get("values", {})

        # Must have email
        email = self._extract_email(values)
        if not email:
            return False

        # Check enrichment status - skip if already processed
        status = self._extract_text_value(values, "clay_enrichment_status")
        if status in ["sent_to_clay", "enriched", "skipped", "failed"]:
            return False

        # Check if missing key fields
        job_title = self._extract_text_value(values, "job_title")
        has_company = self._has_company(values)
        linkedin = self._extract_text_value(values, "linkedin")

        # Needs enrichment if missing any key field
        return not job_title or not has_company or not linkedin

    def _extract_email(self, values: dict) -> Optional[str]:
        """Extract primary email from record values."""
        email_addresses = values.get("email_addresses", [])
        if not email_addresses:
            return None

        for entry in email_addresses:
            if isinstance(entry, dict):
                email = entry.get("email_address") or entry.get("original_email_address")
                if email:
                    return email

        return None

    def _extract_text_value(self, values: dict, field: str) -> Optional[str]:
        """Extract a text value from record values."""
        field_data = values.get(field, [])
        if not field_data:
            return None

        if isinstance(field_data, list) and len(field_data) > 0:
            first = field_data[0]
            if isinstance(first, dict):
                # Handle different attribute types
                attr_type = first.get("attribute_type", "")

                if attr_type == "text":
                    return first.get("value")
                elif attr_type == "personal-name":
                    return first.get("full_name")
                else:
                    # Try common value keys
                    return first.get("value") or first.get("full_name")
            return str(first)

        return None

    def _has_company(self, values: dict) -> bool:
        """Check if record has a company reference."""
        company_data = values.get("company", [])
        if not company_data:
            return False

        if isinstance(company_data, list) and len(company_data) > 0:
            first = company_data[0]
            if isinstance(first, dict):
                # Company is a record-reference type
                return bool(first.get("target_record_id"))

        return False

    def _extract_name(self, values: dict) -> tuple:
        """Extract first_name and last_name from name field."""
        name_data = values.get("name", [])
        if not name_data:
            return None, None

        if isinstance(name_data, list) and len(name_data) > 0:
            first = name_data[0]
            if isinstance(first, dict):
                return first.get("first_name"), first.get("last_name")

        return None, None

    def update_record(self, record_id: str, updates: dict) -> bool:
        """Update a People record with new values."""
        url = f"{self.base_url}/objects/people/records/{record_id}"

        # Convert updates to Attio format
        values = {}
        for key, value in updates.items():
            if value is not None:
                values[key] = value

        payload = {
            "data": {
                "values": values
            }
        }

        response = requests.patch(url, headers=self.headers, json=payload)

        if not response.ok:
            print(f"Error updating record {record_id}: {response.status_code} - {response.text}")
            return False

        return True

    def mark_sent_to_clay(self, record_id: str) -> bool:
        """Mark a record as sent to Clay."""
        return self.update_record(record_id, {
            "clay_enrichment_status": "sent_to_clay",
            "clay_sent_at": datetime.now(timezone.utc).isoformat(),
        })

    def mark_enriched(self, record_id: str, enriched_data: dict) -> bool:
        """Mark a record as enriched and update with Clay data."""
        updates = {
            "clay_enrichment_status": "enriched",
            "clay_enriched_at": datetime.now(timezone.utc).isoformat(),
        }

        # Add enriched fields if present
        if enriched_data.get("job_title"):
            updates["job_title"] = enriched_data["job_title"]
        if enriched_data.get("linkedin_url"):
            updates["linkedin"] = enriched_data["linkedin_url"]
        if enriched_data.get("clay_row_id"):
            updates["clay_row_id"] = enriched_data["clay_row_id"]

        return self.update_record(record_id, updates)

    def mark_failed(self, record_id: str, error_message: str) -> bool:
        """Mark a record as failed with error message."""
        return self.update_record(record_id, {
            "clay_enrichment_status": "failed",
            "enrichment_error": error_message[:500],
        })

    def extract_record_data(self, record: dict) -> dict:
        """Extract relevant data from an Attio record for Clay."""
        values = record.get("values", {})
        record_id = record.get("id", {}).get("record_id")

        # Extract name from the name field
        first_name, last_name = self._extract_name(values)

        return {
            "attio_record_id": record_id,
            "email": self._extract_email(values),
            "first_name": first_name,
            "last_name": last_name,
        }
