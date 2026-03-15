# Extractors

This directory contains provider-specific scrapers that fetch raw data from provider websites and APIs.

## Architecture

All scrapers inherit from `BaseScraper` and implement:
- `scrape()` - Main scraping logic that returns `RawProviderData`
- `provider_name` - Property returning the provider name
- `provider_metadata` - Property returning provider metadata

## Available Scrapers

### Pasta Evangelists

Scrapes event templates and locations from the Pasta Evangelists API.

**Usage:**
```python
from src.extract import PastaEvangelistsScraper

scraper = PastaEvangelistsScraper()
raw_data = scraper.scrape()

print(f"Provider: {raw_data.provider_name}")
print(f"Locations: {len(raw_data.raw_locations)}")
print(f"Templates: {len(raw_data.raw_templates)}")
```

**Key Features:**
- Fetches locations as separate entities (not embedded in events)
- Handles API pagination automatically
- Implements polite delays between requests
- Returns structured `RawProviderData` for normalization

**Migration Notes:**
- Replaces `scrape_pasta_evangelists.py`
- Fixes bug where all locations were assigned to all events
- Locations are now extracted separately for proper relationship mapping
- Returns raw API data without normalization (handled by transform stage)

## Creating a New Scraper

1. Create a new file in `src/extract/` (e.g., `my_provider.py`)
2. Inherit from `BaseScraper`
3. Implement required methods and properties
4. Add tests in `tests/test_my_provider_scraper.py`
5. Export in `src/extract/__init__.py`

**Example:**
```python
from src.extract.base_scraper import BaseScraper
from src.models.raw_provider_data import RawProviderData

class MyProviderScraper(BaseScraper):
    @property
    def provider_name(self) -> str:
        return "My Provider"
    
    @property
    def provider_metadata(self) -> dict:
        return {
            "website": "https://example.com",
            "contact_email": "info@example.com",
            "source_name": "My Provider API",
            "source_base_url": "https://api.example.com"
        }
    
    def scrape(self) -> RawProviderData:
        # Fetch data from provider
        response = self.fetch_url("https://api.example.com/events")
        data = response.json()
        
        return RawProviderData(
            provider_name=self.provider_name,
            provider_website="https://example.com",
            raw_locations=data.get("locations", []),
            raw_events=data.get("events", [])
        )
```

## Testing

Run tests for all scrapers:
```bash
pytest tests/test_*_scraper.py -v
```

Run tests for a specific scraper:
```bash
pytest tests/test_pasta_evangelists_scraper.py -v
```
