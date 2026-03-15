"""Caravan Coffee School scraper - extracts coffee class experiences from Eventbrite."""

import json
import re
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from src.extract.base_scraper import BaseScraper
from src.models.raw_provider_data import RawProviderData


BASE_URL = "https://caravanandco.com/pages/coffee-school"
PROVIDER_NAME = "Caravan Coffee Roasters"
LOCATION_ADDRESS = "Lambworks Roastery Brewbar, North Road, London, N7 9DP"

# Regex to extract numeric event ID from Eventbrite URL
_EB_ID_RE = re.compile(r"-(\d{10,})(?:\?|$)")


class CaravanCoffeeScraper(BaseScraper):
    """
    Scraper for Caravan Coffee School experiences.
    
    Extracts coffee class data from their website and Eventbrite event pages.
    Handles Eventbrite's fragile page structure carefully, using JSON-LD
    when available and falling back to API calls when needed.
    """
    
    @property
    def provider_name(self) -> str:
        """Return the provider name."""
        return PROVIDER_NAME
    
    @property
    def provider_metadata(self) -> dict[str, Any]:
        """Return provider metadata."""
        return {
            "website": "https://caravanandco.com",
            "contact_email": None,
            "source_name": "Caravan Coffee Website + Eventbrite",
            "source_base_url": BASE_URL
        }
    
    def scrape(self) -> RawProviderData:
        """
        Execute scraping logic and return raw structured data.
        
        Returns:
            RawProviderData containing provider info, location, and events
            
        Raises:
            Exception: If scraping fails (network error, parsing error, etc.)
        """
        # Fetch the main coffee school page
        response = self.fetch_url(BASE_URL)
        soup = BeautifulSoup(response.text, "lxml")
        
        # Extract location data
        raw_locations = self._extract_locations()
        
        # Collect all "SIGN ME UP" Eventbrite links in order
        eventbrite_links = []
        for a in soup.find_all("a", href=True):
            text = a.get_text(strip=True).upper()
            if text == "SIGN ME UP" and "eventbrite.com" in a["href"]:
                eventbrite_links.append(a["href"])
        
        # Known class names on the page
        class_names = [
            "LONDON ROASTERY TOUR & TASTING",
            "HOME FILTER CLASS",
            "HOME ESPRESSO CLASS",
            "MILK & LATTE ART CLASS",
        ]
        
        # Find each heading and extract associated content
        headings_found = []
        for heading in soup.find_all(["h2", "h3"]):
            name = self._clean_text(heading.get_text())
            if not name:
                continue
            if name.upper() not in class_names:
                continue
            headings_found.append((heading, name))
        
        raw_templates = []
        raw_events = []
        
        for idx, (heading, name) in enumerate(headings_found):
            # Extract description from sibling elements
            description = self._extract_description(heading)
            
            # Extract image - look for nearest img before this heading
            image = self._extract_image(heading)
            
            # Match with Eventbrite link by index
            eventbrite_url = eventbrite_links[idx] if idx < len(eventbrite_links) else None
            
            # Fetch price and location from Eventbrite
            price = None
            location_data = None
            occurrences = []
            
            if eventbrite_url:
                details = self._get_eventbrite_details(eventbrite_url)
                price = details.get("price")
                location_data = details.get("location")
                occurrences = details.get("occurrences", [])
            
            # Create template record
            template = {
                "title": name.title(),
                "description": description,
                "price": price,
                "source_url": eventbrite_url or BASE_URL,
                "image_url": image,
            }
            raw_templates.append(template)
            
            # Add occurrences if found
            for occurrence in occurrences:
                occurrence["title"] = name.title()
                occurrence["description"] = description
                occurrence["image_url"] = image
                occurrence["source_url"] = eventbrite_url
                occurrence["price"] = price
                if location_data:
                    occurrence["location_data"] = location_data
                raw_events.append(occurrence)
        
        return RawProviderData(
            provider_name=PROVIDER_NAME,
            provider_website="https://caravanandco.com",
            provider_contact_email=None,
            source_name="Caravan Coffee Website + Eventbrite",
            source_base_url=BASE_URL,
            raw_locations=raw_locations,
            raw_templates=raw_templates,
            raw_events=raw_events
        )
    
    def _extract_locations(self) -> list[dict[str, Any]]:
        """
        Extract location data for Caravan Coffee School.
        
        Returns:
            List containing the single location record
        """
        return [{
            "location_name": "Lambworks Roastery Brewbar",
            "formatted_address": LOCATION_ADDRESS,
            "address_line_1": "North Road",
            "city": "London",
            "postcode": "N7 9DP",
            "country": "UK",
        }]
    
    def _extract_description(self, heading) -> str | None:
        """
        Extract description text from elements following a heading.
        
        Args:
            heading: BeautifulSoup heading element
            
        Returns:
            Cleaned description text or None
        """
        texts = []
        node = heading.find_next_sibling()
        
        while node and getattr(node, "name", None) not in ["h2", "h3"]:
            txt = self._clean_text(node.get_text(" ", strip=True))
            if txt and "SIGN ME UP" not in txt.upper():
                texts.append(txt)
            node = node.find_next_sibling()
        
        return " ".join(texts) if texts else None
    
    def _extract_image(self, heading) -> str | None:
        """
        Extract image URL from nearest img element before heading.
        
        Args:
            heading: BeautifulSoup heading element
            
        Returns:
            Absolute image URL or None
        """
        prev_img = heading.find_previous("img")
        if prev_img:
            src = prev_img.get("src") or prev_img.get("data-src")
            if src and not src.startswith("data:"):
                return self._absolute_url(BASE_URL, src)
        return None
    
    def _get_eventbrite_details(self, url: str) -> dict[str, Any]:
        """
        Extract price, location, and occurrences from an Eventbrite event.
        
        Tries JSON-LD on the event page first, then falls back to API.
        Eventbrite page structure is known to be fragile.
        
        Args:
            url: Eventbrite event URL
            
        Returns:
            Dictionary with price, location, and occurrences
        """
        info = {
            "price": None,
            "location": None,
            "occurrences": [],
        }
        
        # Attempt 1: JSON-LD on the event page
        try:
            response = self.fetch_url(url)
            soup = BeautifulSoup(response.text, "lxml")
        except Exception:
            soup = None
        
        if soup:
            for script in soup.find_all("script", type="application/ld+json"):
                raw = script.string or script.get_text()
                if not raw:
                    continue
                try:
                    data = json.loads(raw)
                except Exception:
                    continue
                if not isinstance(data, dict):
                    continue
                
                # Extract price from offers
                offers = data.get("offers")
                if offers:
                    if isinstance(offers, list):
                        offers = offers[0]
                    if isinstance(offers, dict):
                        low = offers.get("lowPrice") or offers.get("price")
                        currency = offers.get("priceCurrency", "GBP")
                        if low:
                            symbol = "£" if currency == "GBP" else currency + " "
                            info["price"] = f"{symbol}{float(low):.0f}"
                
                # Extract location from structured data
                loc = data.get("location")
                if isinstance(loc, dict):
                    addr = loc.get("address", {})
                    if isinstance(addr, dict):
                        parts = [
                            loc.get("name", ""),
                            addr.get("streetAddress", ""),
                            addr.get("addressLocality", ""),
                            addr.get("addressRegion", ""),
                        ]
                        location_str = ", ".join(p for p in parts if p)
                        if location_str:
                            info["location"] = {
                                "location_name": loc.get("name", ""),
                                "formatted_address": location_str,
                                "address_line_1": addr.get("streetAddress", ""),
                                "city": addr.get("addressLocality", ""),
                                "region": addr.get("addressRegion", ""),
                                "postcode": addr.get("postalCode", ""),
                                "country": "UK",
                            }
                
                # Extract event schedule (occurrences)
                start_date = data.get("startDate")
                end_date = data.get("endDate")
                if start_date:
                    occurrence = {
                        "start_at": start_date,
                        "end_at": end_date,
                        "booking_url": url,
                    }
                    info["occurrences"].append(occurrence)
        
        # Attempt 2: Eventbrite destination API (fallback for price)
        if not info["price"]:
            m = _EB_ID_RE.search(url)
            if m:
                api_price = self._eventbrite_price_from_api(m.group(1))
                if api_price:
                    info["price"] = api_price
        
        return info
    
    def _eventbrite_price_from_api(self, event_id: str) -> str | None:
        """
        Use the Eventbrite destination API to get the ticket price.
        
        Args:
            event_id: Numeric Eventbrite event ID
            
        Returns:
            Formatted price string or None
        """
        try:
            response = self.fetch_url(
                "https://www.eventbrite.com/api/v3/destination/events/",
                params={"event_ids": event_id, "expand": "ticket_availability"}
            )
            events = response.json().get("events", [])
            if not events:
                return None
            
            ta = events[0].get("ticket_availability", {})
            min_price = ta.get("minimum_ticket_price", {})
            value = min_price.get("major_value")
            currency = min_price.get("currency", "GBP")
            
            if value:
                symbol = "£" if currency == "GBP" else f"{currency} "
                return f"{symbol}{int(float(value))}"
        except Exception:
            pass
        
        return None
    
    def _clean_text(self, value: str | None) -> str | None:
        """Clean and normalize text."""
        if not value:
            return None
        return re.sub(r"\s+", " ", value).strip()
    
    def _absolute_url(self, base: str, href: str | None) -> str | None:
        """Convert relative URL to absolute."""
        if not href:
            return None
        return urljoin(base, href)
