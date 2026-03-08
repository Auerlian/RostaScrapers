"""Scrape Pasta Evangelists experiences via their public API."""
from __future__ import annotations

import re

from scraper_utils import (
    clean_text,
    dedupe_preserve_order,
    polite_pause,
    write_json,
)

import requests

API_BASE = "https://pensa.pastaevangelists.com/api/v2"
SITE_BASE = "https://plan.pastaevangelists.com"
PROVIDER = "Pasta Evangelists"
CONTACT_EMAIL = "events@pastaevangelists.com"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}

HTML_TAG_RE = re.compile(r"<[^>]+>")


def strip_html(text: str | None) -> str | None:
    if not text:
        return None
    return clean_text(HTML_TAG_RE.sub(" ", text))


def fetch_json(endpoint: str, params: dict | None = None) -> dict:
    url = f"{API_BASE}/{endpoint}"
    resp = requests.get(url, headers=HEADERS, timeout=25, params=params or {})
    resp.raise_for_status()
    return resp.json()


def fetch_all_pages(endpoint: str) -> list[dict]:
    """Paginate through an API endpoint and return all items."""
    page = 1
    items = []
    while True:
        data = fetch_json(endpoint, {"page": page, "per_page": 50})
        items.extend(data.get("data", []))
        meta = data.get("meta", {})
        if meta.get("next_page") is None:
            break
        page += 1
        polite_pause(0.3)
    return items


def fetch_locations() -> dict[str, dict]:
    """Return a map of location id -> location attributes."""
    raw = fetch_all_pages("event_locations")
    locations = {}
    for loc in raw:
        attrs = loc["attributes"]
        parts = [attrs.get("name", "")]
        if attrs.get("address1"):
            parts.append(attrs["address1"])
        if attrs.get("address2"):
            parts.append(attrs["address2"])
        if attrs.get("city"):
            parts.append(attrs["city"])
        if attrs.get("zip"):
            parts.append(attrs["zip"])
        locations[loc["id"]] = {
            "address": ", ".join(p for p in parts if p),
            "image_url": attrs.get("image_url"),
        }
    return locations


def main():
    print("Fetching Pasta Evangelists data from API...")

    locations = fetch_locations()
    polite_pause(0.3)
    templates = fetch_all_pages("event_templates")

    # Build a combined location string from all locations
    all_locations = [loc["address"] for loc in locations.values()]

    experiences = []
    for tmpl in templates:
        attrs = tmpl["attributes"]
        name = clean_text(attrs.get("name", ""))
        if not name:
            continue

        # Build description from summary + activity
        desc_parts = []
        if attrs.get("summary"):
            desc_parts.append(strip_html(attrs["summary"]))
        if attrs.get("activity"):
            desc_parts.append(strip_html(attrs["activity"]))
        description = " ".join(p for p in desc_parts if p) or None

        price_raw = attrs.get("price")
        price = f"£{float(price_raw):.0f}" if price_raw else None

        # Booking URL: the site uses /events/themes then clicking into a template
        # The booking flow is at /events/booking?templateId=<id>
        booking_url = f"{SITE_BASE}/events/themes"

        image = attrs.get("product_image_url") or None
        images = [image] if image else []

        experiences.append({
            "experience_name": name,
            "experience_description": description,
            "experience_provider_name": PROVIDER,
            "detailed_location": all_locations,
            "email_contact": CONTACT_EMAIL,
            "price": price,
            "website": booking_url,
            "images": images,
        })

    write_json("pasta_evangelists_experiences.json", dedupe_preserve_order(experiences))
    print(f"Saved pasta_evangelists_experiences.json ({len(experiences)} experiences)")


if __name__ == "__main__":
    main()
