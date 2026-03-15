"""Example usage of CachedGeocoder with MapboxGeocoder."""

import os
from datetime import datetime

from src.enrich.mapbox_geocoder import MapboxGeocoder
from src.enrich.cached_geocoder import CachedGeocoder
from src.models.location import Location


def main():
    """Demonstrate CachedGeocoder usage."""
    
    # Check for API key
    api_key = os.getenv("MAPBOX_API_KEY")
    if not api_key:
        print("Error: MAPBOX_API_KEY environment variable not set")
        print("Set it with: export MAPBOX_API_KEY='your_api_key'")
        return
    
    # Create geocoder with caching
    mapbox = MapboxGeocoder(api_key=api_key)
    cached_geocoder = CachedGeocoder(mapbox, cache_dir="cache/geocoding")
    
    # Create sample locations
    locations = [
        Location(
            location_id="loc-1",
            provider_id="provider-test",
            provider_name="Test Provider",
            formatted_address="10 Downing Street, London, UK",
            first_seen_at=datetime.now(),
            last_seen_at=datetime.now()
        ),
        Location(
            location_id="loc-2",
            provider_id="provider-test",
            provider_name="Test Provider",
            formatted_address="Buckingham Palace, London, UK",
            first_seen_at=datetime.now(),
            last_seen_at=datetime.now()
        ),
        Location(
            location_id="loc-3",
            provider_id="provider-test",
            provider_name="Test Provider",
            formatted_address="Tower Bridge, London, UK",
            first_seen_at=datetime.now(),
            last_seen_at=datetime.now()
        ),
    ]
    
    print("=== First Run (Cache Miss) ===\n")
    
    # First run - will hit API and cache results
    for location in locations:
        print(f"Geocoding: {location.formatted_address}")
        result = cached_geocoder.geocode_location(location)
        
        if result.geocode_status == "success":
            print(f"  ✓ Success: ({result.latitude:.4f}, {result.longitude:.4f})")
            print(f"  Precision: {result.geocode_precision}")
            print(f"  Provider: {result.geocode_provider}")
            print(f"  Address Hash: {result.address_hash}")
        else:
            print(f"  ✗ Failed: {result.geocode_status}")
        print()
    
    print("\n=== Second Run (Cache Hit) ===\n")
    
    # Second run - will use cache
    for location in locations:
        print(f"Geocoding: {location.formatted_address}")
        result = cached_geocoder.geocode_location(location)
        
        if result.geocode_status == "success":
            print(f"  ✓ Success (from cache): ({result.latitude:.4f}, {result.longitude:.4f})")
        else:
            print(f"  ✗ Failed: {result.geocode_status}")
        print()
    
    print("\n=== Cache Statistics ===\n")
    
    # Show cache files
    from pathlib import Path
    cache_dir = Path("cache/geocoding")
    cache_files = list(cache_dir.glob("*.json"))
    
    print(f"Cache directory: {cache_dir}")
    print(f"Cache files: {len(cache_files)}")
    print(f"Total cache size: {sum(f.stat().st_size for f in cache_files)} bytes")
    
    print("\nCache files:")
    for cache_file in cache_files:
        print(f"  - {cache_file.name}")


if __name__ == "__main__":
    main()
