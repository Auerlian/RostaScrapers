"""Comptoir Bakery scraper - extracts workshop experiences from Bookwhen events."""

import json
import re
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from src.extract.base_scraper import BaseScraper
from src.models.raw_provider_data import RawProviderData


WORKSHOPS_URL = "https://www.comptoirbakery.co.uk/pages/all-our-workshops"
BOOKWHEN_BASE = "https://bookwhen.com/comptoirbakeryschool"
SITE_BASE = "https://www.comptoirbakery.co.uk"
PROVIDER_NAME = "Comptoir Bakery"
CONTACT_EMAIL = "enquiries@comptoirbakery.co.uk"
LOCATION_ADDRESS = "Comptoir Bakery School and Workshop, 96 Druid Street, London, SE1 2HQ"


class ComptoirBakeryScraper(BaseScraper):
    """
    Scraper for Comptoir Bakery workshop experiences.
    
    Extracts workshop data from their website and Bookwhen event pages.
    Each workshop may have multiple ticket packages, which are treated as
    separate event templates. Event occurrences are extracted from Bookwhen
    event data when available.
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
            "source_name": "Comptoir Bakery Website + Bookwhen",
            "source_base_url": WORKSHOPS_URL
        }
    
    def scrape(self) -> RawProviderData:
        """
        Execute scraping logic and return raw structured data.
        
        Returns:
            RawProviderData containing provider info, location, and events
            
        Raises:
            Exception: If scraping fails (network error, parsing error, etc.)
        """
        # Fetch the main workshops page
        response = self.fetch_url(WORKSHOPS_URL)
        soup = BeautifulSoup(response.text, "lxml")
        
        # Extract location data
        raw_locations = self._extract_locations()
        
        # Extract workshop cards
        cards = soup.find_all(
            "div",
            class_=lambda c: c and "info-cols--image_and_text-column" in c,
        )
        
        raw_events = []
        raw_templates = []
        seen_names = set()
        
        for card in cards:
            # Find the title link (first <a> that isn't "BOOK NOW")
            title_link = None
            for a in card.find_all("a", href=True):
                text = a.get_text(strip=True)
                if text and "BOOK" not in text.upper():
                    title_link = a
                    break
            
            if not title_link:
                continue
            
            title = self._clean_text(title_link.get_text())
            href = title_link["href"]
            if not title:
                continue
            
            # Deduplicate by title
            name_key = title.upper().strip()
            if name_key in seen_names:
                continue
            seen_names.add(name_key)
            
            # Extract image from card
            image = None
            img = card.find("img")
            if img:
                src = img.get("src") or img.get("data-src")
                if src and not src.startswith("data:"):
                    image = self._absolute_url(WORKSHOPS_URL, src)
            
            # Extract card-level description
            card_description = None
            for tag in card.find_all(["p", "span", "div"]):
                txt = self._clean_text(tag.get_text())
                if (txt and len(txt) > 20 and 
                    title.upper() not in txt.upper() and 
                    "BOOK" not in txt.upper()):
                    card_description = txt
                    break
            
            # Resolve the bookwhen event URL
            event_url = None
            if href.startswith("http") and "bookwhen.com" in href:
                if "/e/" in href:
                    event_url = href
                elif "/vouchers" not in href:
                    # Tag-filtered URL - resolve to first event
                    event_url = self._resolve_first_event(href)
            elif href.startswith("/pages/"):
                detail_url = urljoin(SITE_BASE, href)
                event_url = self._find_bookwhen_event_url(detail_url)
            
            # Scrape the bookwhen event page for tickets and occurrences
            tickets = []
            bookwhen_description = None
            occurrences = []
            
            if event_url:
                event_data = self._scrape_bookwhen_event(event_url)
                tickets = event_data["tickets"]
                bookwhen_description = event_data["description"]
                occurrences = event_data["occurrences"]
            
            description = card_description or bookwhen_description
            
            if tickets:
                # Create one template per ticket package
                for ticket in tickets:
                    package_name = f"{title} — {ticket['ticket_name']}"
                    package_desc_parts = [description] if description else []
                    if ticket["ticket_description"]:
                        package_desc_parts.append(ticket["ticket_description"])
                    package_desc = " | ".join(package_desc_parts) if package_desc_parts else None
                    
                    template = {
                        "title": package_name,
                        "description": package_desc,
                        "price": ticket["price"],
                        "source_url": event_url,
                        "image_url": image,
                        "ticket_name": ticket["ticket_name"],
                        "ticket_description": ticket["ticket_description"],
                    }
                    raw_templates.append(template)
            else:
                # No tickets found - create single template
                website = event_url or urljoin(SITE_BASE, href)
                template = {
                    "title": title,
                    "description": description,
                    "price": None,
                    "source_url": website,
                    "image_url": image,
                }
                raw_templates.append(template)
            
            # Add occurrences if found
            for occurrence in occurrences:
                occurrence["title"] = title
                occurrence["description"] = description
                occurrence["image_url"] = image
                occurrence["source_url"] = event_url
                raw_events.append(occurrence)
        
        return RawProviderData(
            provider_name=PROVIDER_NAME,
            provider_website=SITE_BASE,
            provider_contact_email=CONTACT_EMAIL,
            source_name="Comptoir Bakery Website + Bookwhen",
            source_base_url=WORKSHOPS_URL,
            raw_locations=raw_locations,
            raw_templates=raw_templates,
            raw_events=raw_events
        )
    
    def _extract_locations(self) -> list[dict[str, Any]]:
        """
        Extract location data for Comptoir Bakery.
        
        Returns:
            List containing the single location record
        """
        return [{
            "location_name": "Comptoir Bakery School and Workshop",
            "formatted_address": LOCATION_ADDRESS,
            "address_line_1": "96 Druid Street",
            "city": "London",
            "postcode": "SE1 2HQ",
            "country": "UK",
        }]
    
    def _find_bookwhen_event_url(self, detail_page_url: str) -> str | None:
        """
        Visit a Comptoir detail page and return the first bookwhen event URL.
        
        Args:
            detail_page_url: URL of the detail page to scrape
            
        Returns:
            Bookwhen event URL or None if not found
        """
        try:
            response = self.fetch_url(detail_page_url)
            soup = BeautifulSoup(response.text, "lxml")
        except Exception:
            return None
        
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "bookwhen.com/comptoirbakeryschool" in href:
                if "/vouchers" not in href and href != BOOKWHEN_BASE:
                    # If it's a tag-filtered URL, resolve the first event from it
                    if "?tags=" in href:
                        return self._resolve_first_event(href)
                    # Direct event link
                    if "/e/" in href:
                        return href
        return None
    
    def _resolve_first_event(self, tag_url: str) -> str | None:
        """
        Given a bookwhen tag-filtered URL, find the first event's direct URL.
        
        Args:
            tag_url: Bookwhen URL with tag filter
            
        Returns:
            Direct event URL or None if not found
        """
        try:
            response = self.fetch_url(tag_url)
            soup = BeautifulSoup(response.text, "lxml")
        except Exception:
            return None
        
        el = soup.find(attrs={"data-event": True})
        if el:
            eid = el["data-event"]
            return f"{BOOKWHEN_BASE}/e/{eid}"
        return None
    
    def _scrape_bookwhen_event(self, event_url: str) -> dict[str, Any]:
        """
        Scrape a bookwhen event page for description, tickets, and occurrences.
        
        Args:
            event_url: Bookwhen event URL
            
        Returns:
            Dictionary with description, tickets list, and occurrences list
        """
        result = {
            "description": None,
            "tickets": [],
            "occurrences": [],
        }
        
        try:
            response = self.fetch_url(event_url)
            soup = BeautifulSoup(response.text, "lxml")
        except Exception:
            return result
        
        # Parse JSON-LD for description, prices, and event schedule
        offers = []
        events = []
        
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
            
            # Extract description
            if data.get("description"):
                desc = data["description"]
                result["description"] = self._clean_text(desc.split("\n")[0])
            
            # Extract offers (ticket prices)
            if data.get("offers"):
                offers = data["offers"]
                if not isinstance(offers, list):
                    offers = [offers]
            
            # Extract event schedule (occurrences)
            if data.get("@type") == "EventSeries" and data.get("subEvent"):
                sub_events = data["subEvent"]
                if not isinstance(sub_events, list):
                    sub_events = [sub_events]
                events.extend(sub_events)
        
        # Get ticket names/descriptions from the HTML
        ticket_divs = soup.find_all("div", class_="ticket_information")
        
        # Match tickets with JSON-LD offers by index
        for i, ticket_div in enumerate(ticket_divs):
            ticket_name_el = ticket_div.find("h4", class_="ticket-summary-title__title")
            ticket_name = self._clean_text(ticket_name_el.get_text()) if ticket_name_el else None
            
            ticket_desc_el = ticket_div.find("div", class_="summary_text")
            ticket_desc = self._clean_text(ticket_desc_el.get_text()) if ticket_desc_el else None
            
            # Price from JSON-LD offers
            price = None
            if i < len(offers):
                offer = offers[i]
                raw_price = offer.get("price")
                currency = offer.get("priceCurrency", "GBP")
                if raw_price:
                    symbol = "£" if currency == "GBP" else f"{currency} "
                    price = f"{symbol}{int(float(raw_price))}"
            
            if ticket_name:
                result["tickets"].append({
                    "ticket_name": ticket_name,
                    "ticket_description": ticket_desc,
                    "price": price,
                })
        
        # Extract occurrences from event schedule
        for event in events:
            occurrence = {
                "start_at": event.get("startDate"),
                "end_at": event.get("endDate"),
                "booking_url": event.get("url") or event_url,
                "location_data": event.get("location", {}),
            }
            result["occurrences"].append(occurrence)
        
        return result
    
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

