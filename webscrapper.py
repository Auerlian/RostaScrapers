from bs4 import BeautifulSoup
from urllib.parse import urljoin

BASE_URL = "https://www.virginexperiencedays.co.uk"

with open("page.html", "r", encoding="utf-8") as f:
    html = f.read()

soup = BeautifulSoup(html, "html.parser")

product_cards = soup.select('a[data-testid="product-card"][href]')

for i, card in enumerate(product_cards, start=1):
    href = card["href"].strip()
    full_url = urljoin(BASE_URL, href)

    title_el = card.select_one('[data-testid="product-card-title"]')
    title = title_el.get_text(strip=True) if title_el else "No title"

    print(f"{i}. {title}")
    print(full_url)
    print("-" * 60)