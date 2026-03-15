# MapboxGeocoder Documentation

## Overview

The `MapboxGeocoder` class provides geocoding functionality using the Mapbox Geocoding API. It implements the `Geocoder` interface and converts address strings into geographic coordinates (latitude/longitude) with metadata.

## Features

- **Geocoding**: Convert addresses to coordinates using Mapbox API
- **Error Handling**: Graceful handling of API errors, rate limits, and timeouts
- **Precision Mapping**: Maps Mapbox place types to precision levels (rooftop, street, city, etc.)
- **Rich Metadata**: Returns detailed information including place names, relevance scores, and context
- **Environment Variable Support**: Reads API key from `MAPBOX_API_KEY` environment variable
- **Configurable Timeout**: Customizable request timeout (default: 10 seconds)

## Installation

The MapboxGeocoder requires the `requests` library, which is included in the project's `requirements.txt`:

```bash
pip install -r requirements.txt
```

## Configuration

### API Key Setup

The MapboxGeocoder requires a Mapbox API key. You can obtain one from [Mapbox](https://www.mapbox.com/).

**Option 1: Environment Variable (Recommended)**

```bash
export MAPBOX_API_KEY="your_api_key_here"
```

**Option 2: Pass Directly to Constructor**

```python
from src.enrich import MapboxGeocoder

geocoder = MapboxGeocoder(api_key="your_api_key_here")
```

### Timeout Configuration

You can customize the request timeout:

```python
geocoder = MapboxGeocoder(api_key="your_key", timeout=30)  # 30 seconds
```

## Usage

### Basic Usage

```python
from src.enrich import MapboxGeocoder

# Initialize geocoder (reads from MAPBOX_API_KEY env var)
geocoder = MapboxGeocoder()

# Geocode an address
result = geocoder.geocode("10 Downing Street, London, UK")

if result.is_success():
    print(f"Latitude: {result.latitude}")
    print(f"Longitude: {result.longitude}")
    print(f"Precision: {result.precision}")
else:
    print(f"Geocoding failed: {result.status}")
    print(f"Error: {result.metadata.get('error')}")
```

### Polymorphic Usage

The MapboxGeocoder implements the `Geocoder` interface, allowing polymorphic usage:

```python
from src.enrich import Geocoder, MapboxGeocoder

def geocode_location(geocoder: Geocoder, address: str):
    """Function that accepts any Geocoder implementation."""
    return geocoder.geocode(address)

# Use with MapboxGeocoder
mapbox = MapboxGeocoder()
result = geocode_location(mapbox, "London, UK")
```

## API Response

### GeocodeResult

The `geocode()` method returns a `GeocodeResult` object with the following fields:

| Field | Type | Description |
|-------|------|-------------|
| `latitude` | `float \| None` | Latitude coordinate |
| `longitude` | `float \| None` | Longitude coordinate |
| `status` | `str` | Status: "success", "failed", or "invalid_address" |
| `precision` | `str \| None` | Precision level (see below) |
| `metadata` | `dict` | Additional provider-specific metadata |

### Status Values

- **success**: Geocoding succeeded, coordinates are available
- **failed**: Geocoding failed due to API error, network error, or timeout
- **invalid_address**: Address not found or invalid

### Precision Levels

The precision field indicates the accuracy of the geocoding result:

| Precision | Description | Mapbox Place Types |
|-----------|-------------|-------------------|
| `rooftop` | Exact address or POI | address, poi |
| `street` | Street-level accuracy | street |
| `neighborhood` | Neighborhood-level | neighborhood |
| `postcode` | Postcode/ZIP level | postcode |
| `city` | City-level accuracy | place, locality, district |
| `region` | Region/state level | region |
| `country` | Country level | country |
| `unknown` | Unknown precision | Other types |

### Metadata Fields

The metadata dictionary contains:

| Field | Type | Description |
|-------|------|-------------|
| `provider` | `str` | Always "mapbox" |
| `place_name` | `str` | Full formatted place name |
| `place_type` | `list[str]` | Mapbox place types |
| `relevance` | `float` | Relevance score (0-1) |
| `confidence` | `str \| None` | Accuracy level from API |
| `context` | `dict` | Context information (postcode, city, region, country) |
| `error` | `str` | Error message (only present on failure) |

## Error Handling

The MapboxGeocoder handles various error conditions gracefully:

### Empty or Invalid Address

```python
result = geocoder.geocode("")
# Returns: status="invalid_address", error="Empty address provided"
```

### No Results Found

```python
result = geocoder.geocode("Invalid Address XYZ123")
# Returns: status="invalid_address", error="No results found for address"
```

### Rate Limit Exceeded (HTTP 429)

```python
result = geocoder.geocode("Some Address")
# Returns: status="failed", error="Rate limit exceeded", status_code=429
```

### Network Timeout

```python
result = geocoder.geocode("Some Address")
# Returns: status="failed", error="Request timeout"
```

### Network Errors

```python
result = geocoder.geocode("Some Address")
# Returns: status="failed", error="Network error: ..."
```

## Examples

### Example 1: Geocode Multiple Addresses

```python
from src.enrich import MapboxGeocoder

geocoder = MapboxGeocoder()

addresses = [
    "10 Downing Street, London, UK",
    "Big Ben, London",
    "London",
]

for address in addresses:
    result = geocoder.geocode(address)
    if result.is_success():
        print(f"{address}: ({result.latitude}, {result.longitude})")
```

### Example 2: Extract Context Information

```python
from src.enrich import MapboxGeocoder

geocoder = MapboxGeocoder()
result = geocoder.geocode("123 Test Street, London, UK")

if result.is_success():
    context = result.metadata.get("context", {})
    print(f"Postcode: {context.get('postcode')}")
    print(f"City: {context.get('place')}")
    print(f"Region: {context.get('region')}")
    print(f"Country: {context.get('country')}")
```

### Example 3: Check Precision Level

```python
from src.enrich import MapboxGeocoder

geocoder = MapboxGeocoder()
result = geocoder.geocode("London")

if result.is_success():
    if result.precision == "rooftop":
        print("Exact address found")
    elif result.precision == "city":
        print("City-level accuracy only")
    else:
        print(f"Precision: {result.precision}")
```

## Testing

The MapboxGeocoder includes comprehensive unit and integration tests:

```bash
# Run all geocoder tests
python -m pytest tests/test_geocoder.py tests/test_mapbox_geocoder.py tests/test_mapbox_geocoder_integration.py -v

# Run only MapboxGeocoder tests
python -m pytest tests/test_mapbox_geocoder.py -v

# Run integration tests
python -m pytest tests/test_mapbox_geocoder_integration.py -v
```

## API Limits

Mapbox Geocoding API has the following limits:

- **Free Tier**: 100,000 requests/month
- **Rate Limit**: Varies by plan
- **Timeout**: Default 10 seconds (configurable)

For more information, see [Mapbox Pricing](https://www.mapbox.com/pricing/).

## Implementation Details

### API Endpoint

```
https://api.mapbox.com/geocoding/v5/mapbox.places/{address}.json
```

### Request Parameters

- `access_token`: Mapbox API key
- `limit`: 1 (only return best match)
- `types`: address,poi,place (focus on physical locations)

### Response Format

The Mapbox API returns GeoJSON with features containing:
- `geometry.coordinates`: [longitude, latitude] (note: longitude first!)
- `place_name`: Formatted address
- `place_type`: Array of place types
- `relevance`: Relevance score
- `context`: Additional location context

## Security Considerations

- **Never commit API keys**: Use environment variables
- **API key validation**: Fails fast if API key is missing
- **Error message sanitization**: API keys are not exposed in error messages
- **Timeout protection**: Prevents hanging requests

## Future Enhancements

Potential improvements for future versions:

- **Caching**: Add caching layer to reduce API calls (see `CachedGeocoder` in design)
- **Batch geocoding**: Support geocoding multiple addresses in one call
- **Fallback providers**: Support fallback to other geocoding services
- **Retry logic**: Automatic retry with exponential backoff
- **Address normalization**: Normalize addresses before geocoding for better cache hits

## Related Components

- **Geocoder Interface**: `src/enrich/geocoder.py`
- **GeocodeResult**: `src/enrich/geocoder.py`
- **CachedGeocoder**: (Planned for v1.5)

## References

- [Mapbox Geocoding API Documentation](https://docs.mapbox.com/api/search/geocoding/)
- [Mapbox API Pricing](https://www.mapbox.com/pricing/)
- [Mapbox Place Types](https://docs.mapbox.com/api/search/geocoding/#data-types)
