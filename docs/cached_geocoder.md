# CachedGeocoder Documentation

## Overview

`CachedGeocoder` is a wrapper class that adds intelligent caching to any `Geocoder` implementation. It stores geocoding results in the filesystem to avoid redundant API calls, significantly improving performance and reducing API costs.

## Features

- **Transparent Caching**: Wraps any `Geocoder` implementation without changing its interface
- **Address Normalization**: Normalizes addresses before hashing to ensure cache hits for minor formatting differences
- **Smart Cache Invalidation**: Automatically detects when an address has changed and re-geocodes
- **Failed Result Handling**: Does not cache failed geocoding attempts, allowing retries
- **Filesystem-Based**: Uses simple JSON files for cache storage, no database required
- **Error Resilient**: Handles corrupted cache files and geocoding errors gracefully

## Usage

### Basic Usage

```python
from src.enrich.mapbox_geocoder import MapboxGeocoder
from src.enrich.cached_geocoder import CachedGeocoder
from src.models.location import Location

# Create underlying geocoder
mapbox = MapboxGeocoder(api_key="your_api_key")

# Wrap with caching
cached_geocoder = CachedGeocoder(mapbox)

# Geocode a location
location = Location(
    location_id="loc-1",
    provider_id="provider-1",
    provider_name="Test Provider",
    formatted_address="10 Downing Street, London, UK"
)

# First call - hits API and caches result
result = cached_geocoder.geocode_location(location)

# Second call with same address - uses cache
result2 = cached_geocoder.geocode_location(location)
```

### Custom Cache Directory

```python
# Use custom cache directory
cached_geocoder = CachedGeocoder(
    geocoder=mapbox,
    cache_dir="custom/cache/path"
)
```

## How It Works

### Cache Key Generation

1. The address is normalized using `normalize_address()` from `src.transform.id_generator`
2. A SHA-256 hash is computed from the normalized address
3. The first 12 characters of the hash are used as the cache key

### Cache Lookup Logic

When `geocode_location()` is called:

1. **Check if geocoding can be skipped**:
   - If `location.address_hash` matches the computed hash
   - AND `location.geocode_status` is "success"
   - Then return the location unchanged (no API call)

2. **Check cache**:
   - Compute address hash
   - Look for cache file: `cache/geocoding/{hash}.json`
   - If found and valid, apply cached result to location

3. **Call underlying geocoder**:
   - If cache miss, call `geocoder.geocode(address)`
   - If successful, save result to cache
   - Apply result to location

### Cache Storage Format

Cache files are stored as JSON with the following structure:

```json
{
  "latitude": 51.5074,
  "longitude": -0.1278,
  "status": "success",
  "precision": "rooftop",
  "metadata": {
    "provider": "mapbox",
    "place_name": "10 Downing Street, London, UK"
  },
  "cached_at": "2025-01-20T14:30:00.123456"
}
```

## Cache Behavior

### What Gets Cached

- ✅ Successful geocoding results (`status="success"`)
- ✅ Results with valid coordinates (latitude and longitude)

### What Doesn't Get Cached

- ❌ Failed geocoding attempts (`status="failed"`)
- ❌ Invalid address results (`status="invalid_address"`)
- ❌ Results with missing coordinates

This ensures that temporary failures (network issues, rate limits) can be retried on subsequent runs.

## Address Normalization

Addresses are normalized before hashing to ensure cache hits for minor formatting differences:

```python
# These addresses produce the same cache key:
"123 Main St., London"
"123  Main  St,  London"
"123 main st london"
```

Normalization process:
1. Convert to lowercase
2. Normalize whitespace (multiple spaces → single space)
3. Remove punctuation
4. Trim leading/trailing whitespace

## Cache Invalidation

The cache is automatically invalidated when:

1. **Address changes**: If `location.formatted_address` changes, the computed hash will differ from `location.address_hash`, triggering re-geocoding
2. **Manual invalidation**: Delete cache files from `cache/geocoding/` directory
3. **Corrupted cache**: If a cache file is corrupted, it's ignored and re-geocoding occurs

## Performance Considerations

### Cache Hit Performance

- **Cache hit**: ~0.001 seconds (file read + JSON parse)
- **Cache miss**: ~0.5-2 seconds (API call + cache write)

### Storage

- Each cache file: ~500 bytes
- 1000 locations: ~500 KB
- 10,000 locations: ~5 MB

### API Cost Savings

With caching enabled:
- First run: 100% API calls
- Subsequent runs with unchanged addresses: 0% API calls
- Typical savings: 90-95% reduction in API calls

## Error Handling

### Geocoding Errors

If the underlying geocoder raises an exception:
- Location is marked with `geocode_status="failed"`
- No cache file is created
- Location is returned with error status
- Pipeline continues processing other locations

### Cache File Errors

If a cache file is corrupted or unreadable:
- Cache is ignored
- Underlying geocoder is called
- New cache file is created if successful

### Cache Write Errors

If cache file cannot be written (disk full, permissions):
- Error is silently ignored
- Geocoding result is still returned
- Next run will attempt to geocode again

## Integration with Pipeline

The `CachedGeocoder` is designed to integrate seamlessly with the pipeline orchestrator:

```python
from src.enrich.mapbox_geocoder import MapboxGeocoder
from src.enrich.cached_geocoder import CachedGeocoder

# In pipeline orchestrator
mapbox = MapboxGeocoder()
geocoder = CachedGeocoder(mapbox)

# Geocode all locations
for location in locations:
    location = geocoder.geocode_location(location)
```

## Testing

### Unit Tests

Run unit tests with mocked geocoder:

```bash
pytest tests/test_cached_geocoder.py -v
```

### Integration Tests

Run integration tests with real Mapbox API (requires `MAPBOX_API_KEY`):

```bash
export MAPBOX_API_KEY="your_api_key"
pytest tests/test_cached_geocoder_integration.py -v
```

## Best Practices

1. **Use in production**: Always wrap geocoders with `CachedGeocoder` in production to reduce API costs
2. **Backup cache**: Consider backing up `cache/geocoding/` directory periodically
3. **Monitor cache size**: If cache grows too large, consider archiving old entries
4. **Clear cache selectively**: To re-geocode specific addresses, delete their cache files
5. **Version control**: Add `cache/` to `.gitignore` to avoid committing cache files

## Limitations

1. **No expiration**: Cache entries never expire automatically
2. **No size limits**: Cache can grow indefinitely
3. **No distributed caching**: Each machine maintains its own cache
4. **No cache warming**: Cache is populated on-demand only

## Future Enhancements

Potential improvements for future versions:

- Cache expiration based on age
- Cache size limits with LRU eviction
- Distributed cache support (Redis, Memcached)
- Cache statistics and monitoring
- Batch cache warming
- Cache compression for large datasets
