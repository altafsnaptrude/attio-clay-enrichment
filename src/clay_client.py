"""Clay API client for sending records and retrieving enrichment results."""

import requests
from typing import Any
from config import CLAY_API_KEY, CLAY_TABLE_ID, CLAY_BASE_URL


class ClayClient:
    def __init__(self):
        self.base_url = CLAY_BASE_URL
        self.table_id = CLAY_TABLE_ID
        self.headers = {
            "Authorization": f"Bearer {CLAY_API_KEY}",
            "Content-Type": "application/json",
        }

    def add_row(self, data: dict[str, Any]) -> dict | None:
        """
        Add a row to the Clay table for enrichment.

        Args:
            data: Dict with keys like attio_record_id, email, first_name, etc.

        Returns:
            Response data including row_id, or None on error.
        """
        url = f"{self.base_url}/{self.table_id}/add_row"

        payload = {"data": data}

        response = requests.post(url, headers=self.headers, json=payload)

        if not response.ok:
            print(f"Error adding row to Clay: {response.status_code}")
            print(response.text)
            return None

        return response.json()

    def get_rows(self, limit: int = 100, cursor: str = None) -> dict | None:
        """
        Get rows from the Clay table.

        Returns:
            Dict with 'data' (list of rows) and 'cursor' for pagination.
        """
        url = f"{self.base_url}/{self.table_id}/rows"

        params = {"limit": limit}
        if cursor:
            params["cursor"] = cursor

        response = requests.get(url, headers=self.headers, params=params)

        if not response.ok:
            print(f"Error getting rows from Clay: {response.status_code}")
            print(response.text)
            return None

        return response.json()

    def get_row_by_id(self, row_id: str) -> dict | None:
        """Get a specific row by its ID."""
        url = f"{self.base_url}/{self.table_id}/rows/{row_id}"

        response = requests.get(url, headers=self.headers)

        if not response.ok:
            print(f"Error getting row {row_id} from Clay: {response.status_code}")
            print(response.text)
            return None

        return response.json()

    def find_rows_by_attio_ids(self, attio_record_ids: list[str]) -> dict[str, dict]:
        """
        Find Clay rows that match the given Attio record IDs.

        Returns:
            Dict mapping attio_record_id to row data.
        """
        result = {}
        cursor = None

        # Paginate through all rows to find matches
        while True:
            response = self.get_rows(limit=100, cursor=cursor)
            if not response:
                break

            rows = response.get("data", [])
            for row in rows:
                attio_id = row.get("attio_record_id")
                if attio_id and attio_id in attio_record_ids:
                    result[attio_id] = row

            # Check if we found all requested IDs
            if len(result) >= len(attio_record_ids):
                break

            # Check for more pages
            cursor = response.get("cursor")
            if not cursor:
                break

        return result

    def send_for_enrichment(self, record: dict) -> str | None:
        """
        Send a record to Clay for enrichment.

        Args:
            record: Dict with attio_record_id, email, first_name, last_name, company

        Returns:
            Clay row ID if successful, None otherwise.
        """
        payload = {
            "attio_record_id": record["record_id"],
            "email": record.get("email"),
            "first_name": record.get("first_name"),
            "last_name": record.get("last_name"),
            "company_name": record.get("company"),
        }

        # Remove None values
        payload = {k: v for k, v in payload.items() if v is not None}

        response = self.add_row(payload)

        if response:
            return response.get("id") or response.get("row_id")

        return None
