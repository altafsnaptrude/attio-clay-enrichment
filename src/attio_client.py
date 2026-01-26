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

    def query_enriched_without_company(self, limit: int = 50) -> list:
        """
        Query for People records that have been enriched but don't have a company linked.

        Criteria:
        - clay_enrichment_status is "enriched"
        - enriched_company_name has a value
        - company is empty/null
        """
        url = f"{self.base_url}/objects/people/records/query"

        payload = {
            "limit": limit,
            "sorts": [
                {"attribute": "clay_enriched_at", "direction": "desc"}
            ]
        }

        response = requests.post(url, headers=self.headers, json=payload)

        if not response.ok:
            print(f"Error querying records: {response.status_code} - {response.text}")
            return []

        data = response.json()
        records = data.get("data", [])

        # Filter for records needing company linking
        needs_linking = []
        for record in records:
            if self._needs_company_linking(record):
                needs_linking.append(record)

        return needs_linking

    def _needs_company_linking(self, record: dict) -> bool:
        """Check if a record needs company linking."""
        values = record.get("values", {})

        # Must have enriched status
        status = self._extract_text_value(values, "clay_enrichment_status")
        if status != "enriched":
            return False

        # Must have enriched_company_name
        enriched_company = self._extract_text_value(values, "enriched_company_name")
        if not enriched_company:
            return False

        # Must NOT already have a company linked
        if self._has_company(values):
            return False

        return True

    def search_company(self, company_name: str) -> Optional[str]:
        """
        Search for a company by name in Attio.

        Returns:
            Company record_id if found, None otherwise
        """
        url = f"{self.base_url}/objects/companies/records/query"

        payload = {
            "limit": 10,
            "filter": {
                "name": {
                    "$contains": company_name
                }
            }
        }

        response = requests.post(url, headers=self.headers, json=payload)

        if not response.ok:
            print(f"Error searching companies: {response.status_code} - {response.text}")
            return None

        data = response.json()
        records = data.get("data", [])

        if not records:
            return None

        # Return the first matching company's record_id
        # Try to find exact match first
        for record in records:
            values = record.get("values", {})
            name = self._extract_company_name(values)
            if name and name.lower() == company_name.lower():
                return record.get("id", {}).get("record_id")

        # If no exact match, return first result
        return records[0].get("id", {}).get("record_id")

    def _extract_company_name(self, values: dict) -> Optional[str]:
        """Extract company name from values."""
        name_data = values.get("name", [])
        if not name_data:
            return None

        if isinstance(name_data, list) and len(name_data) > 0:
            first = name_data[0]
            if isinstance(first, dict):
                return first.get("value")
            return str(first)

        return None

    def create_company(self, company_name: str) -> Optional[str]:
        """
        Create a new company in Attio.

        Returns:
            Company record_id if created, None otherwise
        """
        url = f"{self.base_url}/objects/companies/records"

        payload = {
            "data": {
                "values": {
                    "name": company_name
                }
            }
        }

        response = requests.post(url, headers=self.headers, json=payload)

        if not response.ok:
            print(f"Error creating company: {response.status_code} - {response.text}")
            return None

        data = response.json()
        return data.get("data", {}).get("id", {}).get("record_id")

    def find_or_create_company(self, company_name: str) -> Optional[str]:
        """
        Find a company by name, or create it if it doesn't exist.

        Returns:
            Company record_id
        """
        # First, try to find existing company
        company_id = self.search_company(company_name)

        if company_id:
            print(f"    Found existing company: {company_name}")
            return company_id

        # Create new company
        print(f"    Creating new company: {company_name}")
        company_id = self.create_company(company_name)

        return company_id

    def link_person_to_company(self, person_record_id: str, company_record_id: str) -> bool:
        """
        Link a person record to a company record.

        Args:
            person_record_id: The person's record ID
            company_record_id: The company's record ID

        Returns:
            True if successful, False otherwise
        """
        url = f"{self.base_url}/objects/people/records/{person_record_id}"

        payload = {
            "data": {
                "values": {
                    "company": company_record_id
                }
            }
        }

        response = requests.patch(url, headers=self.headers, json=payload)

        if not response.ok:
            print(f"Error linking person to company: {response.status_code} - {response.text}")
            return False

        return True

    def mark_company_linked(self, record_id: str) -> bool:
        """Mark a record as having company linked."""
        return self.update_record(record_id, {
            "clay_enrichment_status": "company_linked",
        })
