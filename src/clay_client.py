"""
Clay API client for lead enrichment.
"""

import os
import requests
from typing import Optional


class ClayClient:
    """Client for interacting with Clay API."""
    
    def __init__(self, api_key: Optional[str] = None, table_id: Optional[str] = None):
        self.api_key = api_key or os.environ.get("CLAY_API_KEY")
        self.table_id = table_id or os.environ.get("CLAY_TABLE_ID")
        
        if not self.api_key:
            raise ValueError("CLAY_API_KEY is required")
        
        self.base_url = "https://api.clay.com/v1"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
    
    def add_row(self, data: dict) -> Optional[str]:
        """
        Add a row to the Clay table for enrichment.
        
        Args:
            data: Dictionary with fields to enrich (email, first_name, etc.)
        
        Returns:
            Row ID if successful, None otherwise
        """
        if not self.table_id:
            print("Error: CLAY_TABLE_ID not configured")
            return None
        
        url = f"{self.base_url}/tables/{self.table_id}/rows"
        
        # Ensure attio_record_id is included for tracking
        payload = {
            "data": data
        }
        
        try:
            response = requests.post(url, headers=self.headers, json=payload)
            
            if response.ok:
                result = response.json()
                return result.get("id") or result.get("row_id")
            else:
                print(f"Error adding row to Clay: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            print(f"Exception adding row to Clay: {e}")
            return None
    
    def get_rows(self, limit: int = 100) -> list:
        """
        Get rows from the Clay table.
        
        Returns:
            List of row dictionaries
        """
        if not self.table_id:
            print("Error: CLAY_TABLE_ID not configured")
            return []
        
        url = f"{self.base_url}/tables/{self.table_id}/rows"
        
        params = {
            "limit": limit
        }
        
        try:
            response = requests.get(url, headers=self.headers, params=params)
            
            if response.ok:
                result = response.json()
                return result.get("data", []) or result.get("rows", [])
            else:
                print(f"Error getting rows from Clay: {response.status_code} - {response.text}")
                return []
                
        except Exception as e:
            print(f"Exception getting rows from Clay: {e}")
            return []
    
    def get_enriched_rows(self, attio_record_ids: list) -> dict:
        """
        Get enriched data for specific Attio record IDs.
        
        Args:
            attio_record_ids: List of Attio record IDs to look up
        
        Returns:
            Dictionary mapping attio_record_id to enriched data
        """
        rows = self.get_rows(limit=500)
        
        results = {}
        for row in rows:
            row_data = row.get("data", row)  # Handle different response formats
            attio_id = row_data.get("attio_record_id")
            
            if attio_id and attio_id in attio_record_ids:
                # Check if enrichment is complete (has enriched fields)
                enriched_data = {
                    "clay_row_id": row.get("id") or row.get("row_id"),
                    "job_title": row_data.get("enriched_job_title") or row_data.get("job_title"),
                    "company": row_data.get("enriched_company") or row_data.get("company"),
                    "linkedin_url": row_data.get("enriched_linkedin") or row_data.get("linkedin_url") or row_data.get("linkedin"),
                    "phone": row_data.get("enriched_phone") or row_data.get("phone"),
                }
                
                # Only include if we got some enriched data
                if any([enriched_data["job_title"], enriched_data["company"], enriched_data["linkedin_url"]]):
                    results[attio_id] = enriched_data
        
        return results
    
    def send_for_enrichment(self, record_data: dict) -> Optional[str]:
        """
        Send a record to Clay for enrichment.
        
        This is an alias for add_row with the expected field structure.
        
        Args:
            record_data: Dictionary with attio_record_id, email, first_name, last_name, company_name
        
        Returns:
            Row ID if successful, None otherwise
        """
        return self.add_row(record_data)
