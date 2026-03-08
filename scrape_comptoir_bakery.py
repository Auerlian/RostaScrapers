"""Scrape Comptoir Bakery workshop experiences with ticket packages."""
from __future__ import annotations

import json
from urllib.parse import urljoin

from scraper_utils import (
    absolute_url,
    clean_text,
    dedupe_preserve_order,
    polite_pause,
    soup_from_url,
    write_json,
)

WORKSHOPS_URL = "https://www.comptoirbakery.co.uk/pages/all-our-workshops"
BOOKWHEN_BASE = "https://bookwhen.com/comptoirbakeryschool"
SITE_BASE = "https://www.comptoirbakery.co.uk"
PROVIDER = "Comptoir Bakery"
EMAIL_CONTACT = "enquiries@comptoirbakery.co.uk"
LOCATION = "Comptoir Bakery School and Workshop, 96 Druid Street, London, SE1 2HQ"


def find_bookwhen_event_url(detail_page_url: str) -> str | None:
    """Visit a Comptoir detail page and return the first bookwhen event URL."""
    try:
        soup = soup_from_url(detail_page_url)
    except Exception:
        return None

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "bookwhen.com/comptoirbakeryschool" in href:
            if "/vouchers" not in href and href != BOOKWHEN_BASE:
                # If it's a tag-filtered URL, resolve the first event from it
                if "?tags=" in href:
                    return resolve_first_event(href)
                # Direct event link
                if "/e/" in href:
                    return href
    return None


def resolve_first_event(tag_url: str) -> str | None:
    """Given a bookwhen tag-filtered URL, find the first event's direct URL."""
    try:
        soup = soup_from_url(tag_url)
    except Exception:
        return None

    el = soup.find(attrs={"data-event": True})
    if el:
        eid = el["data-event"]
        return f"{BOOKWHEN_BASE}/e/{eid}"
    return None


def scrape_bookwhen_event(event_url: str) -> dict:
    """Scrape a bookwhen event page for description and ticket packages."""
    result = {
        "description": None,
        "tickets": [],
    }
    try:
        soup = soup_from_url(event_url)
    except Exception:
        return result

    # Parse JSON-LD for description and prices (prices are JS-rendered,
    # but the JSON-LD schema.org data includes them server-side).
    offers = []
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
        if data.get("description"):
            desc = data["description"]
            result["description"] = clean_text(desc.split("\n")[0])
        if data.get("offers"):
            offers = data["offers"]
            if not isinstance(offers, list):
                offers = [offers]

    # Get ticket names/descriptions from the HTML (these render server-side)
    ticket_divs = soup.find_all("div", class_="ticket_information")

    # Match tickets with JSON-LD offers by index (they correspond 1:1)
    for i, ticket_div in enumerate(ticket_divs):
        ticket_name_el = ticket_div.find("h4", class_="ticket-summary-title__title")
        ticket_name = clean_text(ticket_name_el.get_text()) if ticket_name_el else None

        ticket_desc_el = ticket_div.find("div", class_="summary_text")
        ticket_desc = clean_text(ticket_desc_el.get_text()) if ticket_desc_el else None

        # Price from JSON-LD offers (includes VAT)
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

    return result


def main():
    print("Fetching Comptoir Bakery workshops...")
    soup = soup_from_url(WORKSHOPS_URL)

    # Workshop cards are in divs with class 'info-cols--image_and_text-column'
    cards = soup.find_all(
        "div",
        class_=lambda c: c and "info-cols--image_and_text-column" in c,
    )

    experiences = []
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

        title = clean_text(title_link.get_text())
        href = title_link["href"]
        if not title:
            continue

        # Deduplicate
        name_key = title.upper().strip()
        if name_key in seen_names:
            continue
        seen_names.add(name_key)

        # Image within this card
        image = None
        img = card.find("img")
        if img:
            src = img.get("src") or img.get("data-src")
            if src and not src.startswith("data:"):
                image = absolute_url(WORKSHOPS_URL, src)

        # Card-level description
        card_description = None
        for tag in card.find_all(["p", "span", "div"]):
            txt = clean_text(tag.get_text())
            if txt and len(txt) > 20 and title.upper() not in txt.upper() and "BOOK" not in txt.upper():
                card_description = txt
                break

        # Resolve the bookwhen event URL
        event_url = None
        if href.startswith("http") and "bookwhen.com" in href:
            if "/e/" in href:
                event_url = href
            elif "/vouchers" not in href:
                # Tag-filtered URL
                polite_pause(0.4)
                event_url = resolve_first_event(href)
        elif href.startswith("/pages/"):
            detail_url = urljoin(SITE_BASE, href)
            polite_pause(0.4)
            event_url = find_bookwhen_event_url(detail_url)

        # Scrape the bookwhen event page for tickets
        tickets = []
        bookwhen_description = None
        if event_url:
            polite_pause(0.4)
            event_data = scrape_bookwhen_event(event_url)
            tickets = event_data["tickets"]
            bookwhen_description = event_data["description"]

        description = card_description or bookwhen_description

        if tickets:
            # Create one experience per ticket package
            for ticket in tickets:
                package_name = f"{title} — {ticket['ticket_name']}"
                package_desc_parts = [description] if description else []
                if ticket["ticket_description"]:
                    package_desc_parts.append(ticket["ticket_description"])
                package_desc = " | ".join(package_desc_parts) if package_desc_parts else None

                experiences.append({
                    "experience_name": package_name,
                    "experience_description": package_desc,
                    "experience_provider_name": PROVIDER,
                    "detailed_location": LOCATION,
                    "email_contact": EMAIL_CONTACT,
                    "price": ticket["price"],
                    "website": event_url,
                    "images": [image] if image else [],
                })
        else:
            # No tickets found — still record the workshop
            website = event_url or urljoin(SITE_BASE, href)
            experiences.append({
                "experience_name": title,
                "experience_description": description,
                "experience_provider_name": PROVIDER,
                "detailed_location": LOCATION,
                "email_contact": EMAIL_CONTACT,
                "price": None,
                "website": website,
                "images": [image] if image else [],
            })

    write_json("comptoir_bakery_experiences.json", dedupe_preserve_order(experiences))
    print(f"Saved comptoir_bakery_experiences.json ({len(experiences)} experiences)")


if __name__ == "__main__":
    main()
