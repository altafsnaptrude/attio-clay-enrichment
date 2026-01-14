"""Attio API client for querying and updating People records."""

import requests
from datetime import datetime, timedelta
from typing import Any
from config import ATTIO_API_KEY, ATTIO_BASE_URL, ENRICHMENT_TIMEOUT_HOURS


class AttioClient:
    def __init__(self):
        self.base_url = ATTIO_BASE_URL
        self.headers = {
            "Authorization": f"Bearer {ATTIO_API_KEY}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _extract_value(self, values: dict, field: str) -> str | None:
        """Extract a value from Attio's nested value structure."""
        field_data = values.get(field, [])
        if not field_data or not isinstance(field_data, list) or len(field_data) == 0:
            return None

        first_value = field_data[0]
        if isinstance(first_value, str):
            return first_value
        if isinstance(first_value, dict):
            # Handle different field types
            if "value" in first_value:
                return first_value["value"]
            if "email_address" in first_value:
                return first_value["email_address"]
            if "original_email_address" in first_value:
                return first_value["original_email_address"]
            if "first_name" in first_value:
                return first_value["first_name"]
            if "full_name" in first_value:
                return first_value["full_name"]
        return None

    def query_unenriched_records(self, limit: int = 50) -> list[dict]:
        """
        Query Attio for People records that need enrichment.

        A record needs enrichment if:
        - Has an email address
        - clay_enrichment_status is empty OR "pending"
        - Missing job_title OR company OR linkedin
        """
        url = f"{self.base_url}/objects/people/records/query"

        payload = {
            "filter": {
                "$and": [
                    # Must have email
                    {"attribute": "email_addresses", "$is_not_empty": True},
                    # Status is empty or pending (not sent_to_clay, enriched, failed, skipped)
                    {
                        "$or": [
                            {"attribute": "clay_enrichment_status", "$is_empty": True},
                            {"attribute": "clay_enrichment_status", "$eq": "pending"}
                        ]
                    },
                    # Missing at least one key field
                    {
                        "$or": [
                            {"attribute": "job_title", "$is_empty": True},
                            {"attribute": "primary_company", "$is_empty": True},
                            {"attribute": "linkedin", "$is_empty": True}
                        ]
                    }
                ]
            },
            "limit": limit,
            "sorts": [{"attribute": "created_at", "direction": "desc"}]
        }

        response = requests.post(url, headers=self.headers, json=payload)

        if not response.ok:
            print(f"Error querying unenriched records: {response.status_code}")
            print(response.text)
            return []

        data = response.json()
        records = data.get("data", [])

        # Transform to simpler format
        result = []
        for record in records:
            record_id = record.get("id", {}).get("record_id")
            values = record.get("values", {})

            result.append({
                "record_id": record_id,
                "email": self._extract_value(values, "email_addresses"),
                "first_name": self._extract_value(values, "first_name"),
                "last_name": self._extract_value(values, "last_name"),
                "job_title": self._extract_value(values, "job_title"),
                "company": self._extract_value(values, "primary_company"),
                "linkedin": self._extract_value(values, "linkedin"),
            })

        return result

    def query_pending_records(self, limit: int = 50) -> list[dict]:
        """
        Query for records that were sent to Clay but not yet enriched.
        Used to check for results and retry stuck records.
        """
        url = f"{self.base_url}/objects/people/records/query"

        payload = {
            "filter": {
                "attribute": "clay_enrichment_status",
                "$eq": "sent_to_clay"
            },
            "limit": limit,
            "sorts": [{"attribute": "clay_sent_at", "direction": "asc"}]
        }

        response = requests.post(url, headers=self.headers, json=payload)

        if not response.ok:
            print(f"Error querying pending records: {response.status_code}")
            print(response.text)
            return []

        data = response.json()
        records = data.get("data", [])

        result = []
        for record in records:
            record_id = record.get("id", {}).get("record_id")
            values = record.get("values", {})

            sent_at_raw = self._extract_value(values, "clay_sent_at")
            sent_at = None
            if sent_at_raw:
                try:
                    sent_at = datetime.fromisoformat(sent_at_raw.replace("Z", "+00:00"))
                except:
                    pass

            result.append({
                "record_id": record_id,
                "email": self._extract_value(values, "email_addresses"),
                "clay_row_id": self._extract_value(values, "clay_row_id"),
                "sent_at": sent_at,
            })

        return result

    def update_record(self, record_id: str, updates: dict[str, Any]) -> bool:
        """Update a People record with new values."""
        url = f"{self.base_url}/objects/people/records/{record_id}"

        # Convert updates to Attio's expected format
        values = {}
        for key, value in updates.items():
            if value is not None:
                values[key] = value

        payload = {"data": {"values": values}}

        response = requests.patch(url, headers=self.headers, json=payload)

        if not response.ok:
            print(f"Error updating record {record_id}: {response.status_code}")
            print(response.text)
            return False

        return True

    def mark_sent_to_clay(self, record_id: str, clay_row_id: str = None) -> bool:
        """Mark a record as sent to Clay for enrichment."""
        updates = {
            "clay_enrichment_status": "sent_to_clay",
            "clay_sent_at": datetime.utcnow().isoformat() + "Z",
        }
        if clay_row_id:
            updates["clay_row_id"] = clay_row_id

        return self.update_record(record_id, updates)

    def mark_enriched(self, record_id: str, enriched_data: dict) -> bool:
        """Mark a record as successfully enriched and update with enriched data."""
        updates = {
            "clay_enrichment_status": "enriched",
            "clay_enriched_at": datetime.utcnow().isoformat() + "Z",
        }

        # Add enriched fields if present
        if enriched_data.get("job_title"):
            updates["job_title"] = enriched_data["job_title"]
        if enriched_data.get("company"):
            updates["primary_company"] = enriched_data["company"]
        if enriched_data.get("linkedin_url"):
            updates["linkedin"] = enriched_data["linkedin_url"]
        if enriched_data.get("phone"):
            updates["phone_numbers"] = enriched_data["phone"]

        return self.update_record(record_id, updates)

    def mark_failed(self, record_id: str, error_message: str) -> bool:
        """Mark a record as failed enrichment."""
        updates = {
            "clay_enrichment_status": "failed",
            "enrichment_error": error_message[:500],  # Limit error length
        }
        return self.update_record(record_id, updates)
