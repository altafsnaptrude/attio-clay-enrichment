"""
Clay webhook client for lead enrichment.

Clay uses webhooks to receive data, not a traditional REST API.
Data is POSTed to a webhook URL and Clay processes it automatically.
"""

import os
import requests
from typing import Optional


class ClayClient:
    """Client for sending data to Clay via webhook."""

    def __init__(self, webhook_url: Optional[str] = None):
        self.webhook_url = webhook_url or os.environ.get("CLAY_WEBHOOK_URL")

        if not self.webhook_url:
            raise ValueError("CLAY_WEBHOOK_URL is required")

        self.headers = {
            "Content-Type": "application/json",
        }

    def send_for_enrichment(self, record_data: dict) -> bool:
        """
        Send a record to Clay webhook for enrichment.

        Args:
            record_data: Dictionary with attio_record_id, email, first_name, last_name, company_name

        Returns:
            True if successful, False otherwise
        """
        try:
            response = requests.post(
                self.webhook_url,
                headers=self.headers,
                json=record_data,
                timeout=30
            )

            if response.ok:
                print(f"Successfully sent to Clay webhook: {response.status_code}")
                return True
            else:
                print(f"Error sending to Clay webhook: {response.status_code} - {response.text}")
                return False

        except Exception as e:
            print(f"Exception sending to Clay webhook: {e}")
            return False

    def add_row(self, data: dict) -> bool:
        """Alias for send_for_enrichment for backwards compatibility."""
        return self.send_for_enrichment(data)
