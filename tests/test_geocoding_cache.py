"""Integration tests for geocoding cache behavior.

Tests cache hit/miss scenarios, cache invalidation, and cache persistence
across geocoder instances. Uses temporary cache directories for test isolation.

**Validates: Requirements 4.2, 12.1, 12.3, 12.4, 12.7**
"""

import json
import pytest
from pathlib import Path
from datetime import datetime

from src.enrich.geocoder import Geocoder, GeocodeResult
from src.enrich.cached_geocoder import CachedGeocoder
from src.models.location import Location


class MockGeocoder(Geocoder):
    """Mock geocoder for testing cache behavior."""
    
    def __init__(self):
        self.call_count = 0
        self.called_addresses = []
    
    def geocode(self, address: str) -> GeocodeResult:
        """Return mock geocoding result and track calls."""
        self.call_count += 1
        self.called_addresses.append(address)
        
        # Return different coordinates based on address for testing
        if "London" in address:
            lat, lon = 51.5074, -0.1278
        elif "Manchester" in address:
            lat, lon = 53.4808, -2.2426
        elif "Edinburgh" in address:
            lat, lon = 55.9533, -3.1883
        else:
            lat, lon = 51.0, 0.0
        
        return GeocodeResult(
            latitude=lat,
            longitude=lon,
            status="success",
            precision="rooftop",
            metadata={"provider": "mock"}
        )



@pytest.fixture
def temp_cache_dir(tmp_path):
    """Create temporary cache directory for test isolation."""
    cache_dir = tmp_path / "cache" / "geocoding"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return str(cache_dir)


@pytest.fixture
def mock_geocoder():
    """Create mock geocoder instance."""
    return MockGeocoder()


class TestCacheHitMissBehavior:
    """Test cache hit and miss scenarios.
    
    **Validates: Requirements 4.2, 12.1, 12.3**
    """
    
    def test_cache_miss_on_first_geocode(self, temp_cache_dir, mock_geocoder):
        """Test that first geocode for an address is a cache miss."""
        cached = CachedGeocoder(mock_geocoder, cache_dir=temp_cache_dir)
        
        location = Location(
            location_id="loc-1",
            provider_id="provider-test",
            provider_name="Test Provider",
            formatted_address="10 Downing Street, London, UK"
        )
        
        result = cached.geocode_location(location)
        
        # Should call underlying geocoder (cache miss)
        assert mock_geocoder.call_count == 1
        assert result.geocode_status == "success"
        assert result.latitude == 51.5074
        assert result.longitude == -0.1278
        
        # Verify cache file was created
        cache_files = list(Path(temp_cache_dir).glob("*.json"))
        assert len(cache_files) == 1

    
    def test_cache_hit_on_second_geocode(self, temp_cache_dir, mock_geocoder):
        """Test that second geocode for same address is a cache hit."""
        cached = CachedGeocoder(mock_geocoder, cache_dir=temp_cache_dir)
        
        # First location - cache miss
        location1 = Location(
            location_id="loc-1",
            provider_id="provider-test",
            provider_name="Test Provider",
            formatted_address="Buckingham Palace, London, UK"
        )
        
        result1 = cached.geocode_location(location1)
        assert mock_geocoder.call_count == 1
        
        # Second location with same address - cache hit
        location2 = Location(
            location_id="loc-2",
            provider_id="provider-test",
            provider_name="Test Provider",
            formatted_address="Buckingham Palace, London, UK"
        )
        
        result2 = cached.geocode_location(location2)
        
        # Should NOT call geocoder again (cache hit)
        assert mock_geocoder.call_count == 1
        
        # Should have same coordinates from cache
        assert result2.latitude == result1.latitude
        assert result2.longitude == result1.longitude
        assert result2.geocode_status == "success"
    
    def test_multiple_addresses_create_separate_cache_entries(self, temp_cache_dir, mock_geocoder):
        """Test that different addresses create separate cache entries."""
        cached = CachedGeocoder(mock_geocoder, cache_dir=temp_cache_dir)
        
        locations = [
            Location(
                location_id=f"loc-{i}",
                provider_id="provider-test",
                provider_name="Test Provider",
                formatted_address=address
            )
            for i, address in enumerate([
                "Tower Bridge, London, UK",
                "Old Trafford, Manchester, UK",
                "Edinburgh Castle, Edinburgh, UK"
            ])
        ]
        
        results = [cached.geocode_location(loc) for loc in locations]
        
        # Should call geocoder for each unique address
        assert mock_geocoder.call_count == 3
        
        # Should have three separate cache files
        cache_files = list(Path(temp_cache_dir).glob("*.json"))
        assert len(cache_files) == 3
        
        # Each should have different coordinates
        assert results[0].latitude != results[1].latitude
        assert results[1].latitude != results[2].latitude

    
    def test_normalized_addresses_share_cache(self, temp_cache_dir, mock_geocoder):
        """Test that normalized addresses share the same cache entry."""
        cached = CachedGeocoder(mock_geocoder, cache_dir=temp_cache_dir)
        
        # Addresses with different formatting but same normalized form
        addresses = [
            "123  Test  Street,  London,  UK",  # Extra spaces
            "123 Test Street, London, UK",       # Normal
            "123 test street london uk",         # Lowercase, no punctuation
        ]
        
        for i, address in enumerate(addresses):
            location = Location(
                location_id=f"loc-{i}",
                provider_id="provider-test",
                provider_name="Test Provider",
                formatted_address=address
            )
            cached.geocode_location(location)
        
        # Should only call geocoder once (all share same normalized address)
        assert mock_geocoder.call_count == 1
        
        # Should only have one cache file
        cache_files = list(Path(temp_cache_dir).glob("*.json"))
        assert len(cache_files) == 1


class TestCacheInvalidation:
    """Test cache invalidation when address changes.
    
    **Validates: Requirements 12.4**
    """
    
    def test_cache_invalidated_when_address_changes(self, temp_cache_dir, mock_geocoder):
        """Test that changing address invalidates cache and triggers re-geocoding."""
        cached = CachedGeocoder(mock_geocoder, cache_dir=temp_cache_dir)
        
        # First geocoding with original address
        location = Location(
            location_id="loc-1",
            provider_id="provider-test",
            provider_name="Test Provider",
            formatted_address="Old Address, London, UK"
        )
        
        result1 = cached.geocode_location(location)
        assert mock_geocoder.call_count == 1
        original_hash = result1.address_hash
        
        # Update location with new address but keep old hash
        location.formatted_address = "New Address, Manchester, UK"
        location.latitude = result1.latitude
        location.longitude = result1.longitude
        location.geocode_status = result1.geocode_status
        location.address_hash = original_hash  # Old hash
        
        result2 = cached.geocode_location(location)
        
        # Should call geocoder again because address changed
        assert mock_geocoder.call_count == 2
        
        # Should have new coordinates
        assert result2.latitude == 53.4808  # Manchester coordinates
        assert result2.longitude == -2.2426
        
        # Should have new address hash
        assert result2.address_hash != original_hash

    
    def test_cache_used_after_address_change_if_matches_existing(self, temp_cache_dir, mock_geocoder):
        """Test that cache is used if new address matches an existing cached address."""
        cached = CachedGeocoder(mock_geocoder, cache_dir=temp_cache_dir)
        
        # Geocode first address
        location1 = Location(
            location_id="loc-1",
            provider_id="provider-test",
            provider_name="Test Provider",
            formatted_address="Tower Bridge, London, UK"
        )
        result1 = cached.geocode_location(location1)
        assert mock_geocoder.call_count == 1
        
        # Geocode second address
        location2 = Location(
            location_id="loc-2",
            provider_id="provider-test",
            provider_name="Test Provider",
            formatted_address="Big Ben, London, UK"
        )
        result2 = cached.geocode_location(location2)
        assert mock_geocoder.call_count == 2
        
        # Change location2's address back to first address
        location2.formatted_address = "Tower Bridge, London, UK"
        location2.address_hash = result2.address_hash  # Keep old hash
        
        result3 = cached.geocode_location(location2)
        
        # Should use cache from location1 (no new API call)
        assert mock_geocoder.call_count == 2
        
        # Should have same coordinates as location1
        assert result3.latitude == result1.latitude
        assert result3.longitude == result1.longitude


class TestUnchangedAddressCaching:
    """Test that unchanged addresses use cached results.
    
    **Validates: Requirements 4.2, 12.3, 12.7**
    """
    
    def test_unchanged_address_skips_geocoding(self, temp_cache_dir, mock_geocoder):
        """Test that unchanged address with successful geocode skips API call."""
        cached = CachedGeocoder(mock_geocoder, cache_dir=temp_cache_dir)
        
        # First geocoding
        location = Location(
            location_id="loc-1",
            provider_id="provider-test",
            provider_name="Test Provider",
            formatted_address="Westminster Abbey, London, UK"
        )
        
        result1 = cached.geocode_location(location)
        assert mock_geocoder.call_count == 1
        
        # Update location with geocoding results
        location.latitude = result1.latitude
        location.longitude = result1.longitude
        location.geocode_status = result1.geocode_status
        location.address_hash = result1.address_hash
        
        # Call again with same location
        result2 = cached.geocode_location(location)
        
        # Should NOT call geocoder (address unchanged and already successful)
        assert mock_geocoder.call_count == 1
        
        # Should return same location
        assert result2.latitude == result1.latitude
        assert result2.longitude == result1.longitude

    
    def test_unchanged_address_with_failed_status_retries(self, temp_cache_dir):
        """Test that unchanged address with failed status retries geocoding."""
        # Create geocoder that fails first time, succeeds second time
        class RetryGeocoder(Geocoder):
            def __init__(self):
                self.call_count = 0
            
            def geocode(self, address: str) -> GeocodeResult:
                self.call_count += 1
                if self.call_count == 1:
                    return GeocodeResult(None, None, "failed", None, {})
                return GeocodeResult(51.5, -0.1, "success", "rooftop", {"provider": "mock"})
        
        retry_geocoder = RetryGeocoder()
        cached = CachedGeocoder(retry_geocoder, cache_dir=temp_cache_dir)
        
        location = Location(
            location_id="loc-1",
            provider_id="provider-test",
            provider_name="Test Provider",
            formatted_address="Test Address, London, UK"
        )
        
        # First call - fails
        result1 = cached.geocode_location(location)
        assert retry_geocoder.call_count == 1
        assert result1.geocode_status == "failed"
        
        # Update location with failed result
        location.geocode_status = result1.geocode_status
        location.address_hash = result1.address_hash
        
        # Second call - should retry because previous was failed
        result2 = cached.geocode_location(location)
        
        # Should call geocoder again (failed results don't prevent retry)
        assert retry_geocoder.call_count == 2
        assert result2.geocode_status == "success"
    
    def test_multiple_locations_same_address_all_use_cache(self, temp_cache_dir, mock_geocoder):
        """Test that multiple locations with same address all use cache after first."""
        cached = CachedGeocoder(mock_geocoder, cache_dir=temp_cache_dir)
        
        address = "Trafalgar Square, London, UK"
        
        # Create 5 locations with same address
        locations = [
            Location(
                location_id=f"loc-{i}",
                provider_id="provider-test",
                provider_name="Test Provider",
                formatted_address=address
            )
            for i in range(5)
        ]
        
        results = [cached.geocode_location(loc) for loc in locations]
        
        # Should only call geocoder once
        assert mock_geocoder.call_count == 1
        
        # All should have same coordinates
        for result in results[1:]:
            assert result.latitude == results[0].latitude
            assert result.longitude == results[0].longitude



class TestCachePersistence:
    """Test cache persistence across geocoder instances.
    
    **Validates: Requirements 12.1, 12.7**
    """
    
    def test_cache_persists_across_geocoder_instances(self, temp_cache_dir):
        """Test that cache is reused when creating new geocoder instances."""
        # First geocoder instance
        geocoder1 = MockGeocoder()
        cached1 = CachedGeocoder(geocoder1, cache_dir=temp_cache_dir)
        
        location1 = Location(
            location_id="loc-1",
            provider_id="provider-test",
            provider_name="Test Provider",
            formatted_address="St Paul's Cathedral, London, UK"
        )
        
        result1 = cached1.geocode_location(location1)
        assert geocoder1.call_count == 1
        
        # Create new geocoder instance with same cache directory
        geocoder2 = MockGeocoder()
        cached2 = CachedGeocoder(geocoder2, cache_dir=temp_cache_dir)
        
        location2 = Location(
            location_id="loc-2",
            provider_id="provider-test",
            provider_name="Test Provider",
            formatted_address="St Paul's Cathedral, London, UK"
        )
        
        result2 = cached2.geocode_location(location2)
        
        # Second geocoder should NOT be called (uses persisted cache)
        assert geocoder2.call_count == 0
        
        # Should have same coordinates from cache
        assert result2.latitude == result1.latitude
        assert result2.longitude == result1.longitude
    
    def test_cache_files_readable_after_restart(self, temp_cache_dir, mock_geocoder):
        """Test that cache files are properly formatted and readable."""
        cached = CachedGeocoder(mock_geocoder, cache_dir=temp_cache_dir)
        
        location = Location(
            location_id="loc-1",
            provider_id="provider-test",
            provider_name="Test Provider",
            formatted_address="British Museum, London, UK"
        )
        
        result = cached.geocode_location(location)
        
        # Find and read cache file
        cache_files = list(Path(temp_cache_dir).glob("*.json"))
        assert len(cache_files) == 1
        
        with open(cache_files[0], 'r') as f:
            cache_data = json.load(f)
        
        # Verify cache file structure
        assert "latitude" in cache_data
        assert "longitude" in cache_data
        assert "status" in cache_data
        assert "precision" in cache_data
        assert "metadata" in cache_data
        assert "cached_at" in cache_data
        
        # Verify values match result
        assert cache_data["latitude"] == result.latitude
        assert cache_data["longitude"] == result.longitude
        assert cache_data["status"] == "success"

    
    def test_corrupted_cache_file_handled_gracefully(self, temp_cache_dir, mock_geocoder):
        """Test that corrupted cache files are handled gracefully."""
        cached = CachedGeocoder(mock_geocoder, cache_dir=temp_cache_dir)
        
        # Create corrupted cache file
        from src.transform.id_generator import normalize_address
        import hashlib
        
        address = "Corrupted Cache Test, London, UK"
        normalized = normalize_address(address)
        address_hash = hashlib.sha256(normalized.encode()).hexdigest()[:12]
        
        cache_path = Path(temp_cache_dir) / f"{address_hash}.json"
        with open(cache_path, 'w') as f:
            f.write("invalid json content {{{")
        
        location = Location(
            location_id="loc-1",
            provider_id="provider-test",
            provider_name="Test Provider",
            formatted_address=address
        )
        
        result = cached.geocode_location(location)
        
        # Should ignore corrupted cache and call geocoder
        assert mock_geocoder.call_count == 1
        assert result.geocode_status == "success"


class TestCacheDirectoryIsolation:
    """Test cache directory isolation for tests.
    
    **Validates: Requirements 12.1**
    """
    
    def test_separate_cache_directories_are_isolated(self, tmp_path):
        """Test that separate cache directories don't interfere with each other."""
        # Create two separate cache directories
        cache_dir1 = str(tmp_path / "cache1" / "geocoding")
        cache_dir2 = str(tmp_path / "cache2" / "geocoding")
        
        geocoder1 = MockGeocoder()
        geocoder2 = MockGeocoder()
        
        cached1 = CachedGeocoder(geocoder1, cache_dir=cache_dir1)
        cached2 = CachedGeocoder(geocoder2, cache_dir=cache_dir2)
        
        location1 = Location(
            location_id="loc-1",
            provider_id="provider-test",
            provider_name="Test Provider",
            formatted_address="Isolation Test, London, UK"
        )
        
        # Geocode with first cache
        cached1.geocode_location(location1)
        assert geocoder1.call_count == 1
        
        # Create separate location object for second cache
        location2 = Location(
            location_id="loc-2",
            provider_id="provider-test",
            provider_name="Test Provider",
            formatted_address="Isolation Test, London, UK"
        )
        
        # Geocode with second cache (should not use first cache)
        cached2.geocode_location(location2)
        assert geocoder2.call_count == 1
        
        # Verify separate cache files
        cache_files1 = list(Path(cache_dir1).glob("*.json"))
        cache_files2 = list(Path(cache_dir2).glob("*.json"))
        
        assert len(cache_files1) == 1
        assert len(cache_files2) == 1
        assert cache_files1[0] != cache_files2[0]

    
    def test_tmp_path_fixture_provides_clean_cache(self, tmp_path):
        """Test that tmp_path fixture provides clean isolated cache directory."""
        cache_dir = str(tmp_path / "cache" / "geocoding")
        
        # Verify directory doesn't exist yet
        assert not Path(cache_dir).exists()
        
        # Create geocoder with cache
        geocoder = MockGeocoder()
        cached = CachedGeocoder(geocoder, cache_dir=cache_dir)
        
        # Verify directory was created
        assert Path(cache_dir).exists()
        
        # Verify it's empty
        cache_files = list(Path(cache_dir).glob("*.json"))
        assert len(cache_files) == 0
        
        # Add some cache entries
        location = Location(
            location_id="loc-1",
            provider_id="provider-test",
            provider_name="Test Provider",
            formatted_address="Clean Cache Test, London, UK"
        )
        cached.geocode_location(location)
        
        # Verify cache file was created
        cache_files = list(Path(cache_dir).glob("*.json"))
        assert len(cache_files) == 1
    
    def test_cache_directory_created_if_not_exists(self, tmp_path):
        """Test that cache directory is created if it doesn't exist."""
        cache_dir = str(tmp_path / "nonexistent" / "cache" / "geocoding")
        
        assert not Path(cache_dir).exists()
        
        geocoder = MockGeocoder()
        cached = CachedGeocoder(geocoder, cache_dir=cache_dir)
        
        # Directory should be created
        assert Path(cache_dir).exists()
        assert Path(cache_dir).is_dir()
