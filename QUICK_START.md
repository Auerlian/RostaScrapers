# RostaScraper Pipeline - Quick Start Guide

## Prerequisites

```bash
# Install dependencies
pip install -r requirements.txt

# Optional: Set API keys for enrichment
export MAPBOX_ACCESS_TOKEN="your_mapbox_token"
export ANTHROPIC_API_KEY="your_anthropic_key"
```

## Basic Commands

### Run Full Pipeline

```bash
# All providers (skip enrichment to avoid API keys)
python run_pipeline.py run --skip-geocoding --skip-ai

# Single provider
python run_pipeline.py run --provider pasta-evangelists --skip-geocoding --skip-ai
python run_pipeline.py run --provider caravan-coffee --skip-geocoding --skip-ai
python run_pipeline.py run --provider comptoir-bakery --skip-geocoding --skip-ai

# With enrichment (requires API keys)
python run_pipeline.py run
```

### Export Only

```bash
# Regenerate CSV exports from existing data
python run_pipeline.py export-only
```

### Validate Store

```bash
# Check data integrity and quality
python run_pipeline.py validate
```

## Output Files

### Canonical Store (JSON)
- `data/current/providers.json` - Provider records
- `data/current/locations.json` - Location records
- `data/current/event_templates.json` - Event template records
- `data/current/event_occurrences.json` - Event occurrence records

### CSV Exports
- `exports/events.csv` - All events (templates + occurrences)
- `exports/locations.csv` - All locations with event counts

## Pipeline Stages

1. **Extract** - Scrape data from provider websites/APIs
2. **Normalize** - Transform to canonical data models
3. **Enrich** - Geocode locations and AI enrich events (optional)
4. **Sync** - Merge with existing store using hash-based change detection
5. **Export** - Generate CSV exports

## Provider Status

| Provider | Status | Notes |
|----------|--------|-------|
| Pasta Evangelists | ✅ Working | Locations separate from events |
| Caravan Coffee | ✅ Working | Coffee school classes |
| Comptoir Bakery | ✅ Working | Baking classes |

## Common Issues

### API Key Missing
```
Error: MAPBOX_ACCESS_TOKEN not found
Solution: Use --skip-geocoding flag or set environment variable
```

### Scraper Failure
```
Error: Failed to scrape provider-name
Solution: Pipeline continues with other providers, check logs for details
```

### Empty Store
```
Warning: No providers to sync
Solution: Run extract stage first with 'run' command
```

## Testing

```bash
# Run all tests
python -m pytest tests/ -v

# Run specific test suites
python -m pytest tests/test_orchestrator.py -v
python -m pytest tests/test_pipeline_e2e.py -v

# Run with coverage
python -m pytest tests/ --cov=src --cov-report=html
```

## Development

### Adding a New Provider

1. Create scraper in `src/extract/your_provider.py`
2. Inherit from `BaseScraper`
3. Implement `scrape()`, `provider_name`, and `provider_metadata`
4. Add to scraper registry in `src/pipeline/orchestrator.py`

### Modifying Export Format

Edit `src/export/csv_exporter.py` to change CSV structure or add new export formats.

### Customizing Normalization

Edit `src/transform/normalizer.py` to adjust field mappings or add new transformations.

## Support

For issues or questions, check:
- `TASK_PIPELINE_WIRING_COMPLETION.md` - Detailed implementation notes
- `docs/` - Module-specific documentation
- Test files in `tests/` - Usage examples
