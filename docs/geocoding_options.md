# Geocoding Options

The pipeline supports two geocoding providers with automatic fallback:

## 1. Mapbox (Commercial - Recommended)

**Pros:**
- High accuracy
- Fast response times
- Detailed metadata
- Good coverage worldwide

**Cons:**
- Requires API key (free tier available)
- Requires signup at https://www.mapbox.com/

**Setup:**
```bash
export MAPBOX_ACCESS_TOKEN="your-token-here"
```

Or add to `.env` file:
```
MAPBOX_ACCESS_TOKEN=your-token-here
```

## 2. Nominatim (Free - Fallback)

**Pros:**
- Completely free
- No API key required
- No signup needed
- Open source data (OpenStreetMap)

**Cons:**
- Rate limited to 1 request/second
- Less accurate than commercial services
- Slower response times

**Setup:**
No setup required! The pipeline automatically uses Nominatim when Mapbox is not available.

## How It Works

The pipeline automatically selects the best available geocoder:

1. **Try Mapbox first** - If `MAPBOX_ACCESS_TOKEN` is set, uses Mapbox
2. **Fallback to Nominatim** - If no Mapbox token, uses free Nominatim
3. **Cache results** - Both geocoders cache results to avoid redundant API calls

## Caching

All geocoding results are cached in `cache/geocoding/` directory:
- Cache key: hash of normalized address
- Cached data: coordinates, precision, provider metadata
- Cache is persistent across pipeline runs
- Significantly reduces API calls and costs

## Usage in GitHub Actions

The workflow automatically detects which geocoder to use:

```yaml
env:
  MAPBOX_ACCESS_TOKEN: ${{ secrets.MAPBOX_ACCESS_TOKEN }}
```

- If secret is set → uses Mapbox
- If secret is not set → uses Nominatim (free)

## Skipping Geocoding

To skip geocoding entirely:

```bash
python run_pipeline.py run --skip-geocoding
```

This is useful for:
- Testing without API calls
- Running pipeline when geocoding is not needed
- Debugging other pipeline stages
