"""Pasta Evangelists scraper - extracts event templates and locations from API."""

import re
from typing import Any

from src.extract.base_scraper import BaseScraper
from src.models.raw_provider_data import RawProviderData


API_BASE = "https://pensa.pastaevangelists.com/api/v2"
SITE_BASE = "https://plan.pastaevangelists.com"
PROVIDER_NAME = "Pasta Evangelists"
CONTACT_EMAIL = "events@pastaevangelists.com"

HTML_TAG_RE = re.compile(r"<[^>]+>")


class PastaEvangelistsScraper(BaseScraper):
    """
    Scraper for Pasta Evangelists experiences.
    
    Fetches event templates and locations from their public API.
    The API provides paginated endpoints for event_templates and event_locations.
    
    IMPORTANT: This scraper returns locations as separate entities rather than
    embedding them in every event. The old implementation incorrectly assigned
    ALL locations to ALL events. The normalizer will handle proper event-location
    relationships based on what's available in the API response.
    """
    
    @property
    def provider_name(self) -> str:
        """Return the provider name."""
        return PROVIDER_NAME
    
    @property
    def provider_metadata(self) -> dict[str, Any]:
        """Return provider metadata."""
        return {
            "website": SITE_BASE,
            "contact_email": CONTACT_EMAIL,
            "source_name": "Pasta Evangelists API",
            "source_base_url": API_BASE
        }
    
    def scrape(self) -> RawProviderData:
        """
        Execute scraping logic and return raw structured data.
        
        Returns:
            RawProviderData containing provider info, locations, and event templates
            
        Raises:
            Exception: If scraping fails (network error, API error, etc.)
        """
        # Fetch locations first
        raw_locations = self._fetch_all_pages("event_locations")
        
        # Fetch event templates
        raw_templates = self._fetch_all_pages("event_templates")
        
        return RawProviderData(
            provider_name=PROVIDER_NAME,
            provider_website=SITE_BASE,
            provider_contact_email=CONTACT_EMAIL,
            source_name="Pasta Evangelists API",
            source_base_url=API_BASE,
            raw_locations=raw_locations,
            raw_templates=raw_templates,
            raw_events=[]  # This provider uses templates, not specific occurrences
        )
    
    def _fetch_all_pages(self, endpoint: str) -> list[dict[str, Any]]:
        """
        Paginate through an API endpoint and return all items.
        
        Args:
            endpoint: API endpoint name (e.g., "event_templates", "event_locations")
            
        Returns:
            List of all items from all pages
        """
        page = 1
        items = []
        
        while True:
            url = f"{API_BASE}/{endpoint}"
            params = {"page": page, "per_page": 50}
            
            response = self.fetch_url(url, params=params)
            data = response.json()
            
            # Extract items from response
            page_items = data.get("data", [])
            items.extend(page_items)
            
            # Check if there are more pages
            meta = data.get("meta", {})
            if meta.get("next_page") is None:
                break
            
            page += 1
        
        return items
