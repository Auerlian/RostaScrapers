"""Scrape Caravan Coffee School experiences."""
from __future__ import annotations

import json
import re

import requests

from scraper_utils import (
    absolute_url,
    clean_text,
    dedupe_preserve_order,
    extract_first_price,
    fetch,
    polite_pause,
    soup_from_url,
    write_json,
    HEADERS,
)

BASE_URL = "https://caravanandco.com/pages/coffee-school"
PROVIDER = "Caravan Coffee Roasters"
LOCATION = "Lambworks Roastery Brewbar, North Road, London, N7 9DP"

# Regex to pull the numeric event ID from an Eventbrite URL
_EB_ID_RE = re.compile(r"-(\d{10,})(?:\?|$)")


def _eventbrite_price_from_api(event_id: str) -> str | None:
    """Use the Eventbrite destination API to get the ticket price."""
    try:
        r = requests.get(
            "https://www.eventbrite.com/api/v3/destination/events/",
            headers=HEADERS,
            timeout=25,
            params={"event_ids": event_id, "expand": "ticket_availability"},
        )
        r.raise_for_status()
        events = r.json().get("events", [])
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


def get_eventbrite_details(url: str) -> dict:
    """Extract price from an Eventbrite event, trying JSON-LD first then API."""
    info = {"price": None, "location": None}

    # --- Attempt 1: JSON-LD on the event page ---
    try:
        soup = soup_from_url(url)
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
                    info["location"] = ", ".join(p for p in parts if p)

    # --- Attempt 2: Eventbrite destination API (fallback) ---
    if not info["price"]:
        m = _EB_ID_RE.search(url)
        if m:
            polite_pause(0.3)
            info["price"] = _eventbrite_price_from_api(m.group(1))

    return info


def main():
    print("Fetching Caravan Coffee School...")
    soup = soup_from_url(BASE_URL)

    # The page has 4 classes, each with a heading, description, image, and
    # a "SIGN ME UP" link to Eventbrite.
    # Structure: sections alternate image + text blocks.
    # We pair each heading with its Eventbrite link.

    # Collect all "SIGN ME UP" links in order
    eventbrite_links = []
    for a in soup.find_all("a", href=True):
        text = a.get_text(strip=True).upper()
        if text == "SIGN ME UP" and "eventbrite.com" in a["href"]:
            eventbrite_links.append(a["href"])

    # Collect class headings and their associated content
    class_names = [
        "LONDON ROASTERY TOUR & TASTING",
        "HOME FILTER CLASS",
        "HOME ESPRESSO CLASS",
        "MILK & LATTE ART CLASS",
    ]

    # Find each heading and its nearby image + description
    experiences = []
    headings_found = []

    for heading in soup.find_all(["h2", "h3"]):
        name = clean_text(heading.get_text())
        if not name:
            continue
        if name.upper() not in class_names:
            continue
        headings_found.append((heading, name))

    for idx, (heading, name) in enumerate(headings_found):
        # Get description from next sibling text
        description = None
        node = heading.find_next_sibling()
        texts = []
        while node and getattr(node, "name", None) not in ["h2", "h3"]:
            txt = clean_text(node.get_text(" ", strip=True))
            if txt and "SIGN ME UP" not in txt.upper():
                texts.append(txt)
            node = node.find_next_sibling()
        if texts:
            description = " ".join(texts)

        # Get image - look for the nearest img before this heading
        image = None
        prev_img = heading.find_previous("img")
        if prev_img:
            src = prev_img.get("src") or prev_img.get("data-src")
            if src and not src.startswith("data:"):
                image = absolute_url(BASE_URL, src)

        # Match with eventbrite link by index
        eventbrite_url = eventbrite_links[idx] if idx < len(eventbrite_links) else None

        # Fetch price from Eventbrite
        price = None
        email_contact = None
        if eventbrite_url:
            polite_pause(0.5)
            details = get_eventbrite_details(eventbrite_url)
            price = details["price"]

        experiences.append({
            "experience_name": name.title(),
            "experience_description": description,
            "experience_provider_name": PROVIDER,
            "detailed_location": LOCATION,
            "email_contact": email_contact,
            "price": price,
            "website": eventbrite_url or BASE_URL,
            "images": [image] if image else [],
        })

    write_json("caravan_coffee_school_experiences.json", dedupe_preserve_order(experiences))
    print(f"Saved caravan_coffee_school_experiences.json ({len(experiences)} experiences)")


if __name__ == "__main__":
    main()
