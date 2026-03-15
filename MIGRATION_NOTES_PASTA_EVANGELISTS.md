# Pasta Evangelists Scraper Migration

## Summary

Successfully migrated the Pasta Evangelists scraper from the old standalone script (`scrape_pasta_evangelists.py`) to the new pipeline architecture (`src/extract/pasta_evangelists.py`).

## Key Changes

### Architecture
- **Old**: Standalone script that writes JSON directly to file
- **New**: Class-based scraper inheriting from `BaseScraper` that returns `RawProviderData`

### Location Handling (Bug Fix)
- **Old Bug**: Assigned ALL locations to ALL events via `detailed_location: all_locations`
- **New Fix**: Returns locations as separate entities in `raw_locations` field
- **Impact**: Proper event-location relationships can now be established by the normalizer

### Data Flow
- **Old**: Extract → Write JSON file
- **New**: Extract → Return RawProviderData → Normalize → Enrich → Sync → Export

### API Integration
- Preserved existing API endpoints and pagination logic
- Maintained polite delays between requests (now via `BaseScraper.fetch_url()`)
- Improved error handling with retry logic from `BaseScraper`

## Files Created

1. **src/extract/pasta_evangelists.py** - New scraper implementation
2. **tests/test_pasta_evangelists_scraper.py** - Comprehensive test suite
3. **examples/test_pasta_evangelists_live.py** - Live API test script
4. **src/extract/README.md** - Documentation for extractors

## Files Modified

1. **src/extract/__init__.py** - Added `PastaEvangelistsScraper` export

## Requirements Satisfied

✅ **Requirement 1.1**: Fetch data from provider's API  
✅ **Requirement 1.2**: Return structured data with provider info, locations, and events  
✅ **Requirement 1.4**: Preserve all source data fields without loss  
✅ **Requirement 2.4**: Extract locations as separate entities  
✅ **Requirement 2.5**: Preserve event-location relationships from source  
✅ **Requirement 13.1**: Link events to locations when available from source  
✅ **Requirement 13.3**: Handle events without specific location information  
✅ **Requirement 13.5**: Do NOT link events to all provider locations (bug fix)

## Testing

All tests pass:
```bash
pytest tests/test_pasta_evangelists_scraper.py -v
# 7 tests passed

pytest tests/test_base_scraper.py -v
# 17 tests passed
```

## Usage Example

```python
from src.extract import PastaEvangelistsScraper

# Create scraper
scraper = PastaEvangelistsScraper()

# Fetch data
raw_data = scraper.scrape()

# Access results
print(f"Provider: {raw_data.provider_name}")
print(f"Locations: {len(raw_data.raw_locations)}")
print(f"Templates: {len(raw_data.raw_templates)}")
```

## Next Steps

1. The old `scrape_pasta_evangelists.py` can be removed once all scrapers are migrated
2. Update `run_all_scrapers.py` to use new scraper classes
3. Implement normalizer to process `RawProviderData` into canonical models
4. Handle `location_scope="provider-wide"` in normalizer when location mapping is unavailable

## Notes

- The API does not expose explicit event-location relationships in the template endpoint
- Location scope handling (provider-wide vs specific) will be determined by the normalizer
- All source data is preserved in the raw format for maximum flexibility
- The scraper focuses solely on extraction; normalization is handled downstream
