"""Unit tests for CachedGeocoder."""

import json
import pytest
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock

from src.enrich.geocoder import Geocoder, GeocodeResult
from src.enrich.cached_geocoder import CachedGeocoder
from src.models.location import Location


class MockGeocoder(Geocoder):
    """Mock geocoder for testing."""
    
    def __init__(self, result: GeocodeResult):
        self.result = result
        self.call_count = 0
        self.last_address = None
    
    def geocode(self, address: str) -> GeocodeResult:
        """Return the configured result."""
        self.call_count += 1
        self.last_address = address
        return self.result


@pytest.fixture
def temp_cache_dir(tmp_path):
    """Create temporary cache directory."""
    cache_dir = tmp_path / "cache" / "geocoding"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return str(cache_dir)


@pytest.fixture
def success_result():
    """Create successful geocode result."""
    return GeocodeResult(
        latitude=51.5074,
        longitude=-0.1278,
        status="success",
        precision="rooftop",
        metadata={"provider": "test"}
    )


@pytest.fixture
def failed_result():
    """Create failed geocode result."""
    return GeocodeResult(
        latitude=None,
        longitude=None,
        status="failed",
        precision=None,
        metadata={"error": "API error"}
    )



class TestCachedGeocoderInit:
    """Test CachedGeocoder initialization."""
    
    def test_init_with_default_cache_dir(self):
        """Test initialization with default cache directory."""
        mock_geocoder = MockGeocoder(GeocodeResult(None, None, "failed", None, {}))
        cached = CachedGeocoder(mock_geocoder)
        
        assert cached.geocoder is mock_geocoder
        assert cached.cache_dir == Path("cache/geocoding")
    
    def test_init_with_custom_cache_dir(self, temp_cache_dir):
        """Test initialization with custom cache directory."""
        mock_geocoder = MockGeocoder(GeocodeResult(None, None, "failed", None, {}))
        cached = CachedGeocoder(mock_geocoder, cache_dir=temp_cache_dir)
        
        assert cached.geocoder is mock_geocoder
        assert cached.cache_dir == Path(temp_cache_dir)
    
    def test_creates_cache_directory_if_not_exists(self, tmp_path):
        """Test that cache directory is created if it doesn't exist."""
        cache_dir = tmp_path / "new_cache"
        mock_geocoder = MockGeocoder(GeocodeResult(None, None, "failed", None, {}))
        
        cached = CachedGeocoder(mock_geocoder, cache_dir=str(cache_dir))
        
        assert cache_dir.exists()
        assert cache_dir.is_dir()


class TestCachedGeocoderGeocodeLocation:
    """Test CachedGeocoder.geocode_location method."""
    
    def test_geocode_new_location(self, temp_cache_dir, success_result):
        """Test geocoding a new location (cache miss)."""
        mock_geocoder = MockGeocoder(success_result)
        cached = CachedGeocoder(mock_geocoder, cache_dir=temp_cache_dir)
        
        location = Location(
            location_id="loc-1",
            provider_id="provider-1",
            provider_name="Test Provider",
            formatted_address="123 Test St, London, UK"
        )
        
        result = cached.geocode_location(location)
        
        # Should call underlying geocoder
        assert mock_geocoder.call_count == 1
        assert mock_geocoder.last_address == "123 Test St, London, UK"
        
        # Should update location with geocoding data
        assert result.latitude == 51.5074
        assert result.longitude == -0.1278
        assert result.geocode_status == "success"
        assert result.geocode_precision == "rooftop"
        assert result.geocode_provider == "test"
        assert result.geocoded_at is not None
        assert result.address_hash is not None
    
    def test_skip_geocoding_if_address_unchanged_and_successful(self, temp_cache_dir, success_result):
        """Test that geocoding is skipped if address unchanged and already successful."""
        mock_geocoder = MockGeocoder(success_result)
        cached = CachedGeocoder(mock_geocoder, cache_dir=temp_cache_dir)
        
        # Create location that's already successfully geocoded
        location = Location(
            location_id="loc-1",
            provider_id="provider-1",
            provider_name="Test Provider",
            formatted_address="123 Test St, London, UK",
            latitude=51.5074,
            longitude=-0.1278,
            geocode_status="success",
            geocode_precision="rooftop",
            geocode_provider="test",
            address_hash="abc123def456"  # Simulate existing hash
        )
        
        # Compute what the hash should be
        from src.transform.id_generator import normalize_address
        import hashlib
        normalized = normalize_address(location.formatted_address)
        expected_hash = hashlib.sha256(normalized.encode()).hexdigest()[:12]
        location.address_hash = expected_hash
        
        result = cached.geocode_location(location)
        
        # Should NOT call underlying geocoder
        assert mock_geocoder.call_count == 0
        
        # Should return location unchanged
        assert result.latitude == 51.5074
        assert result.longitude == -0.1278
        assert result.geocode_status == "success"
    
    def test_re_geocode_if_address_changed(self, temp_cache_dir, success_result):
        """Test that geocoding happens if address has changed."""
        mock_geocoder = MockGeocoder(success_result)
        cached = CachedGeocoder(mock_geocoder, cache_dir=temp_cache_dir)
        
        # Create location with old address hash
        location = Location(
            location_id="loc-1",
            provider_id="provider-1",
            provider_name="Test Provider",
            formatted_address="456 New St, London, UK",  # Different address
            latitude=51.5074,
            longitude=-0.1278,
            geocode_status="success",
            address_hash="old_hash_123"  # Old hash from previous address
        )
        
        result = cached.geocode_location(location)
        
        # Should call underlying geocoder because address changed
        assert mock_geocoder.call_count == 1
        assert result.geocode_status == "success"
    
    def test_use_cache_on_second_call(self, temp_cache_dir, success_result):
        """Test that cache is used on second call for same address."""
        mock_geocoder = MockGeocoder(success_result)
        cached = CachedGeocoder(mock_geocoder, cache_dir=temp_cache_dir)
        
        location1 = Location(
            location_id="loc-1",
            provider_id="provider-1",
            provider_name="Test Provider",
            formatted_address="123 Test St, London, UK"
        )
        
        # First call - should geocode and cache
        result1 = cached.geocode_location(location1)
        assert mock_geocoder.call_count == 1
        
        # Second call with different location but same address
        location2 = Location(
            location_id="loc-2",
            provider_id="provider-1",
            provider_name="Test Provider",
            formatted_address="123 Test St, London, UK"
        )
        
        result2 = cached.geocode_location(location2)
        
        # Should NOT call geocoder again - should use cache
        assert mock_geocoder.call_count == 1
        
        # Should have same geocoding results
        assert result2.latitude == result1.latitude
        assert result2.longitude == result1.longitude
        assert result2.geocode_status == "success"
    
    def test_cache_normalized_addresses(self, temp_cache_dir, success_result):
        """Test that addresses are normalized before caching."""
        mock_geocoder = MockGeocoder(success_result)
        cached = CachedGeocoder(mock_geocoder, cache_dir=temp_cache_dir)
        
        # First location with punctuation and extra spaces
        location1 = Location(
            location_id="loc-1",
            provider_id="provider-1",
            provider_name="Test Provider",
            formatted_address="123  Test  St.,  London,  UK"
        )
        
        result1 = cached.geocode_location(location1)
        assert mock_geocoder.call_count == 1
        
        # Second location with normalized address
        location2 = Location(
            location_id="loc-2",
            provider_id="provider-1",
            provider_name="Test Provider",
            formatted_address="123 Test St London UK"
        )
        
        result2 = cached.geocode_location(location2)
        
        # Should use cache because normalized addresses are the same
        assert mock_geocoder.call_count == 1
    
    def test_failed_geocoding_not_cached(self, temp_cache_dir, failed_result):
        """Test that failed geocoding results are not cached."""
        mock_geocoder = MockGeocoder(failed_result)
        cached = CachedGeocoder(mock_geocoder, cache_dir=temp_cache_dir)
        
        location = Location(
            location_id="loc-1",
            provider_id="provider-1",
            provider_name="Test Provider",
            formatted_address="Invalid Address"
        )
        
        result = cached.geocode_location(location)
        
        # Should call geocoder
        assert mock_geocoder.call_count == 1
        assert result.geocode_status == "failed"
        
        # Try again with same address
        location2 = Location(
            location_id="loc-2",
            provider_id="provider-1",
            provider_name="Test Provider",
            formatted_address="Invalid Address"
        )
        
        result2 = cached.geocode_location(location2)
        
        # Should call geocoder again because failed results aren't cached
        assert mock_geocoder.call_count == 2
    
    def test_handle_geocoder_exception(self, temp_cache_dir):
        """Test handling of exceptions from underlying geocoder."""
        mock_geocoder = Mock(spec=Geocoder)
        mock_geocoder.geocode.side_effect = Exception("API error")
        
        cached = CachedGeocoder(mock_geocoder, cache_dir=temp_cache_dir)
        
        location = Location(
            location_id="loc-1",
            provider_id="provider-1",
            provider_name="Test Provider",
            formatted_address="123 Test St, London, UK"
        )
        
        result = cached.geocode_location(location)
        
        # Should handle exception gracefully
        assert result.geocode_status == "failed"
        assert result.latitude is None
        assert result.longitude is None
        assert result.geocoded_at is not None


class TestCachedGeocoderCacheOperations:
    """Test cache file operations."""
    
    def test_cache_file_created(self, temp_cache_dir, success_result):
        """Test that cache file is created after successful geocoding."""
        mock_geocoder = MockGeocoder(success_result)
        cached = CachedGeocoder(mock_geocoder, cache_dir=temp_cache_dir)
        
        location = Location(
            location_id="loc-1",
            provider_id="provider-1",
            provider_name="Test Provider",
            formatted_address="123 Test St, London, UK"
        )
        
        cached.geocode_location(location)
        
        # Check that cache file was created
        cache_files = list(Path(temp_cache_dir).glob("*.json"))
        assert len(cache_files) == 1
    
    def test_cache_file_content(self, temp_cache_dir, success_result):
        """Test that cache file contains correct data."""
        mock_geocoder = MockGeocoder(success_result)
        cached = CachedGeocoder(mock_geocoder, cache_dir=temp_cache_dir)
        
        location = Location(
            location_id="loc-1",
            provider_id="provider-1",
            provider_name="Test Provider",
            formatted_address="123 Test St, London, UK"
        )
        
        result = cached.geocode_location(location)
        
        # Read cache file
        cache_files = list(Path(temp_cache_dir).glob("*.json"))
        with open(cache_files[0], 'r') as f:
            cache_data = json.load(f)
        
        # Verify cache content
        assert cache_data["latitude"] == 51.5074
        assert cache_data["longitude"] == -0.1278
        assert cache_data["status"] == "success"
        assert cache_data["precision"] == "rooftop"
        assert cache_data["metadata"]["provider"] == "test"
        assert "cached_at" in cache_data
    
    def test_load_from_corrupted_cache(self, temp_cache_dir, success_result):
        """Test handling of corrupted cache files."""
        mock_geocoder = MockGeocoder(success_result)
        cached = CachedGeocoder(mock_geocoder, cache_dir=temp_cache_dir)
        
        # Create corrupted cache file
        from src.transform.id_generator import normalize_address
        import hashlib
        address = "123 Test St, London, UK"
        normalized = normalize_address(address)
        address_hash = hashlib.sha256(normalized.encode()).hexdigest()[:12]
        
        cache_path = Path(temp_cache_dir) / f"{address_hash}.json"
        with open(cache_path, 'w') as f:
            f.write("invalid json {{{")
        
        location = Location(
            location_id="loc-1",
            provider_id="provider-1",
            provider_name="Test Provider",
            formatted_address=address
        )
        
        result = cached.geocode_location(location)
        
        # Should ignore corrupted cache and call geocoder
        assert mock_geocoder.call_count == 1
        assert result.geocode_status == "success"


class TestCachedGeocoderAddressHash:
    """Test address hash computation."""
    
    def test_compute_address_hash(self, temp_cache_dir):
        """Test address hash computation."""
        mock_geocoder = MockGeocoder(GeocodeResult(None, None, "failed", None, {}))
        cached = CachedGeocoder(mock_geocoder, cache_dir=temp_cache_dir)
        
        hash1 = cached._compute_address_hash("123 Test St, London")
        hash2 = cached._compute_address_hash("123 Test St, London")
        
        # Same address should produce same hash
        assert hash1 == hash2
        assert len(hash1) == 12
    
    def test_different_addresses_different_hashes(self, temp_cache_dir):
        """Test that different addresses produce different hashes."""
        mock_geocoder = MockGeocoder(GeocodeResult(None, None, "failed", None, {}))
        cached = CachedGeocoder(mock_geocoder, cache_dir=temp_cache_dir)
        
        hash1 = cached._compute_address_hash("123 Test St, London")
        hash2 = cached._compute_address_hash("456 Other St, London")
        
        assert hash1 != hash2
