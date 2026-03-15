"""Integration tests for CachedGeocoder with MapboxGeocoder."""

import os
import pytest
from pathlib import Path

from src.enrich.mapbox_geocoder import MapboxGeocoder
from src.enrich.cached_geocoder import CachedGeocoder
from src.models.location import Location


@pytest.fixture
def temp_cache_dir(tmp_path):
    """Create temporary cache directory."""
    cache_dir = tmp_path / "cache" / "geocoding"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return str(cache_dir)


@pytest.fixture
def mapbox_api_key():
    """Get Mapbox API key from environment."""
    api_key = os.getenv("MAPBOX_API_KEY")
    if not api_key:
        pytest.skip("MAPBOX_API_KEY not set - skipping integration test")
    return api_key


class TestCachedGeocoderWithMapbox:
    """Test CachedGeocoder integration with MapboxGeocoder."""
    
    def test_geocode_with_mapbox_and_cache(self, temp_cache_dir, mapbox_api_key):
        """Test geocoding with Mapbox and caching."""
        mapbox = MapboxGeocoder(api_key=mapbox_api_key)
        cached = CachedGeocoder(mapbox, cache_dir=temp_cache_dir)
        
        location = Location(
            location_id="loc-1",
            provider_id="provider-test",
            provider_name="Test Provider",
            formatted_address="10 Downing Street, London, UK"
        )
        
        # First call - should hit Mapbox API
        result1 = cached.geocode_location(location)
        
        assert result1.geocode_status == "success"
        assert result1.latitude is not None
        assert result1.longitude is not None
        assert result1.geocode_provider == "mapbox"
        assert result1.address_hash is not None
        
        # Verify cache file was created
        cache_files = list(Path(temp_cache_dir).glob("*.json"))
        assert len(cache_files) == 1
        
        # Second call with same address - should use cache
        location2 = Location(
            location_id="loc-2",
            provider_id="provider-test",
            provider_name="Test Provider",
            formatted_address="10 Downing Street, London, UK"
        )
        
        result2 = cached.geocode_location(location2)
        
        # Should have same coordinates from cache
        assert result2.latitude == result1.latitude
        assert result2.longitude == result1.longitude
        assert result2.geocode_status == "success"
        
        # Still only one cache file
        cache_files = list(Path(temp_cache_dir).glob("*.json"))
        assert len(cache_files) == 1
    
    def test_skip_geocoding_for_unchanged_address(self, temp_cache_dir, mapbox_api_key):
        """Test that geocoding is skipped if address unchanged and already successful."""
        mapbox = MapboxGeocoder(api_key=mapbox_api_key)
        cached = CachedGeocoder(mapbox, cache_dir=temp_cache_dir)
        
        # First geocoding
        location = Location(
            location_id="loc-1",
            provider_id="provider-test",
            provider_name="Test Provider",
            formatted_address="Buckingham Palace, London, UK"
        )
        
        result1 = cached.geocode_location(location)
        assert result1.geocode_status == "success"
        
        # Update location with geocoding results and same address hash
        location.latitude = result1.latitude
        location.longitude = result1.longitude
        location.geocode_status = result1.geocode_status
        location.address_hash = result1.address_hash
        
        # Call again - should skip geocoding
        result2 = cached.geocode_location(location)
        
        # Should return same location without calling API
        assert result2.latitude == result1.latitude
        assert result2.longitude == result1.longitude
    
    def test_multiple_locations_separate_cache_files(self, temp_cache_dir, mapbox_api_key):
        """Test that different addresses create separate cache files."""
        mapbox = MapboxGeocoder(api_key=mapbox_api_key)
        cached = CachedGeocoder(mapbox, cache_dir=temp_cache_dir)
        
        location1 = Location(
            location_id="loc-1",
            provider_id="provider-test",
            provider_name="Test Provider",
            formatted_address="Tower Bridge, London, UK"
        )
        
        location2 = Location(
            location_id="loc-2",
            provider_id="provider-test",
            provider_name="Test Provider",
            formatted_address="Big Ben, London, UK"
        )
        
        result1 = cached.geocode_location(location1)
        result2 = cached.geocode_location(location2)
        
        assert result1.geocode_status == "success"
        assert result2.geocode_status == "success"
        
        # Should have two separate cache files
        cache_files = list(Path(temp_cache_dir).glob("*.json"))
        assert len(cache_files) == 2
        
        # Coordinates should be different
        assert result1.latitude != result2.latitude or result1.longitude != result2.longitude
