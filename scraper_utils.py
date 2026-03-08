from __future__ import annotations

import json
import re
import time
from typing import Iterable
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    )
}

EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)
PRICE_RE = re.compile(r"(£\s?\d+(?:\.\d{2})?(?:\s?(?:pp|per person))?)", re.I)


def fetch(url: str, timeout: int = 25) -> requests.Response:
    resp = requests.get(url, headers=HEADERS, timeout=timeout)
    resp.raise_for_status()
    return resp


def soup_from_url(url: str) -> BeautifulSoup:
    return BeautifulSoup(fetch(url).text, "lxml")


def clean_text(value: str | None) -> str | None:
    if not value:
        return None
    return re.sub(r"\s+", " ", value).strip()


def first_non_empty(*values):
    for v in values:
        if v:
            return v
    return None


def dedupe_preserve_order(items: Iterable):
    seen = set()
    out = []
    for item in items:
        key = json.dumps(item, sort_keys=True, ensure_ascii=False) if isinstance(item, dict) else item
        if key not in seen:
            seen.add(key)
            out.append(item)
    return out


def extract_emails(text: str) -> list[str]:
    return dedupe_preserve_order(EMAIL_RE.findall(text or ""))


def extract_first_price(text: str) -> str | None:
    if not text:
        return None
    m = PRICE_RE.search(text)
    return m.group(1).replace("  ", " ").strip() if m else None


def absolute_url(base: str, href: str | None) -> str | None:
    if not href:
        return None
    return urljoin(base, href)


def get_meta_content(soup: BeautifulSoup, *attrs: tuple[str, str]) -> str | None:
    for key, value in attrs:
        tag = soup.find("meta", attrs={key: value})
        if tag and tag.get("content"):
            return clean_text(tag["content"])
    return None


def get_json_ld(soup: BeautifulSoup) -> list[dict]:
    data = []
    for tag in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = tag.string or tag.get_text()
        if not raw:
            continue
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                data.extend([x for x in parsed if isinstance(x, dict)])
            elif isinstance(parsed, dict):
                data.append(parsed)
        except Exception:
            continue
    return data


def best_image_candidates(soup: BeautifulSoup, base_url: str) -> list[str]:
    images = []

    og = get_meta_content(soup, ("property", "og:image"), ("name", "og:image"))
    if og:
        images.append(absolute_url(base_url, og))

    for img in soup.select("img"):
        src = (
            img.get("src")
            or img.get("data-src")
            or img.get("data-original")
            or img.get("data-lazy-src")
        )
        if src:
            images.append(absolute_url(base_url, src))

    cleaned = []
    for img in images:
        if not img:
            continue
        if img.startswith("data:"):
            continue
        cleaned.append(img)

    return dedupe_preserve_order(cleaned)


def write_json(filename: str, payload: list[dict]) -> None:
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def polite_pause(seconds: float = 0.6) -> None:
    time.sleep(seconds)
