# Transform Module

This module handles the transformation of raw provider data into canonical data models.

## Components

### Normalizer (`normalizer.py`)

The `Normalizer` class transforms provider-specific raw data into canonical data models with deterministic IDs, normalized fields, and computed hashes for change detection.

#### Key Methods

- **`normalize_provider(raw_data)`**: Creates a canonical Provider record from raw data
- **`normalize_locations(raw_data, provider_id)`**: Creates canonical Location records with deterministic IDs
- **`normalize_events(raw_data, provider_id, location_map)`**: Creates canonical EventTemplate and EventOccurrence records with location links

#### Features

**Data Normalization:**
- Strips HTML tags from descriptions
- Normalizes whitespace in text fields
- Handles null values consistently
- Parses dates, prices, currencies into canonical formats

**ID Generation:**
- Uses deterministic ID generation based on normalized source data
- Ensures same source data produces same IDs across pipeline runs
- Leverages `id_generator` module for consistent ID formats

**Hash Computation:**
- Computes `source_hash` for each record (drives sync decisions)
- Computes `record_hash` for each record (tracks all canonical field changes)
- Computes `address_hash` for locations (used for geocoding cache)
- Uses `hash_computer` module for consistent hash algorithms

**Location Linking:**
- Links events to locations using location IDs
- Supports multiple strategies for resolving location references:
  - Direct location_id field
  - Embedded location data
  - Location reference via location_map

**Data Parsing:**
- Price parsing: Handles numeric values, currency symbols (£, $, €), currency codes (GBP, USD, EUR)
- Datetime parsing: Supports ISO format, common date formats, timezone handling
- Duration parsing: Handles numeric minutes, "X hours", "X minutes" formats
- List extraction: Handles arrays, comma-separated, semicolon-separated strings

#### Usage Example

```python
from src.transform.normalizer import Normalizer
from src.models.raw_provider_data import RawProviderData

# Create raw data from scraper
raw_data = RawProviderData(
    provider_name="Pasta Evangelists",
    provider_website="https://pastaevangelists.com",
    raw_locations=[{
        "location_name": "The Pasta Academy",
        "formatted_address": "123 Main St, London, EC1A 9EJ"
    }],
    raw_templates=[{
        "title": "Beginners Class",
        "description": "<p>Learn to make pasta</p>",
        "price": 68.0
    }]
)

# Normalize data
normalizer = Normalizer()

# 1. Normalize provider
provider = normalizer.normalize_provider(raw_data)

# 2. Normalize locations
locations = normalizer.normalize_locations(raw_data, provider.provider_id)

# 3. Build location map for event linking
location_map = {loc.formatted_address: loc.location_id for loc in locations}

# 4. Normalize events
events = normalizer.normalize_events(raw_data, provider.provider_id, location_map)
```

### ID Generator (`id_generator.py`)

Provides deterministic ID generation functions for all canonical models.

**Functions:**
- `generate_provider_id(provider_name)`: Generate provider ID from name
- `generate_location_id(provider_slug, address)`: Generate location ID from provider and address
- `generate_event_template_id(provider_slug, source_template_id, title)`: Generate template ID
- `generate_event_occurrence_id(provider_slug, source_event_id, title, location_id, start_at)`: Generate occurrence ID

### Hash Computer (`hash_computer.py`)

Provides hash computation functions for change detection and caching.

**Functions:**
- `compute_source_hash(record, source_fields)`: Compute hash of source fields only
- `compute_record_hash(record, exclude_fields)`: Compute hash of all canonical fields
- `compute_address_hash(location)`: Compute hash of location address fields

**Field Definitions:**
- `EVENT_TEMPLATE_SOURCE_FIELDS`: Fields included in template source hash
- `EVENT_OCCURRENCE_SOURCE_FIELDS`: Fields included in occurrence source hash
- `LOCATION_SOURCE_FIELDS`: Fields included in location source hash

## Testing

The module includes comprehensive unit tests and integration tests:

- **`tests/test_normalizer.py`**: Unit tests for all Normalizer methods
- **`tests/test_normalizer_integration.py`**: Integration tests with realistic scraper data patterns
- **`tests/test_id_generation.py`**: Tests for ID generation functions
- **`tests/test_hash_computer.py`**: Tests for hash computation functions

Run tests:
```bash
pytest tests/test_normalizer.py -v
pytest tests/test_normalizer_integration.py -v
```

## Design Principles

1. **Deterministic IDs**: Same source data always produces same IDs
2. **Graceful Degradation**: Invalid data is skipped, not rejected
3. **Flexible Parsing**: Handles multiple data formats for dates, prices, etc.
4. **Separation of Concerns**: ID generation, hashing, and normalization are separate
5. **Testability**: All functions are pure and easily testable
