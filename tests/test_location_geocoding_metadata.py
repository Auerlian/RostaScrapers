"""Tests for Location model geocoding metadata fields (Task 7.6)."""

from datetime import datetime
import pytest

from src.models.location import Location


class TestLocationGeocodingMetadata:
    """Test Location model geocoding metadata fields."""
    
    def test_location_with_all_geocoding_metadata(self):
        """Test location with complete geocoding metadata."""
        now = datetime.now()
        location = Location(
            location_id="location-test-123",
            provider_id="provider-test",
            provider_name="Test Provider",
            formatted_address="123 Test St, London, UK",
            latitude=51.5074,
            longitude=-0.1278,
            geocode_provider="mapbox",
            geocode_status="success",
            geocode_precision="rooftop",
            geocoded_at=now,
            address_hash="abc123def456"
        )
        
        assert location.is_valid()
        assert location.geocode_provider == "mapbox"
        assert location.geocode_status == "success"
        assert location.geocode_precision == "rooftop"
        assert location.geocoded_at == now
        assert location.address_hash == "abc123def456"
    
    def test_location_geocode_status_success(self):
        """Test location with geocode_status='success'."""
        location = Location(
            location_id="location-test-123",
            provider_id="provider-test",
            provider_name="Test Provider",
            formatted_address="123 Test St",
            geocode_status="success"
        )
        
        assert location.is_valid()
        assert location.geocode_status == "success"
    
    def test_location_geocode_status_failed(self):
        """Test location with geocode_status='failed'."""
        location = Location(
            location_id="location-test-123",
            provider_id="provider-test",
            provider_name="Test Provider",
            formatted_address="123 Test St",
            geocode_status="failed"
        )
        
        assert location.is_valid()
        assert location.geocode_status == "failed"
    
    def test_location_geocode_status_invalid_address(self):
        """Test location with geocode_status='invalid_address'."""
        location = Location(
            location_id="location-test-123",
            provider_id="provider-test",
            provider_name="Test Provider",
            formatted_address="123 Test St",
            geocode_status="invalid_address"
        )
        
        assert location.is_valid()
        assert location.geocode_status == "invalid_address"
    
    def test_location_geocode_status_not_geocoded(self):
        """Test location with geocode_status='not_geocoded' (default)."""
        location = Location(
            location_id="location-test-123",
            provider_id="provider-test",
            provider_name="Test Provider",
            formatted_address="123 Test St"
        )
        
        assert location.is_valid()
        assert location.geocode_status == "not_geocoded"
    
    def test_location_geocode_status_invalid_value(self):
        """Test location validation rejects invalid geocode_status."""
        location = Location(
            location_id="location-test-123",
            provider_id="provider-test",
            provider_name="Test Provider",
            formatted_address="123 Test St",
            geocode_status="invalid_status"
        )
        
        errors = location.validate()
        assert not location.is_valid()
        assert any("geocode_status" in err for err in errors)
        assert any("not_geocoded" in err for err in errors)
        assert any("success" in err for err in errors)
        assert any("failed" in err for err in errors)
        assert any("invalid_address" in err for err in errors)
    
    def test_location_geocoding_metadata_optional_fields(self):
        """Test that geocoding metadata fields are optional."""
        location = Location(
            location_id="location-test-123",
            provider_id="provider-test",
            provider_name="Test Provider",
            formatted_address="123 Test St",
            geocode_provider=None,
            geocode_precision=None,
            geocoded_at=None,
            address_hash=None
        )
        
        assert location.is_valid()
        assert location.geocode_provider is None
        assert location.geocode_precision is None
        assert location.geocoded_at is None
        assert location.address_hash is None
    
    def test_location_geocoding_timestamp_stored(self):
        """Test that geocoded_at timestamp is properly stored."""
        timestamp = datetime(2025, 1, 20, 10, 30, 0)
        location = Location(
            location_id="location-test-123",
            provider_id="provider-test",
            provider_name="Test Provider",
            formatted_address="123 Test St",
            geocode_status="success",
            geocoded_at=timestamp
        )
        
        assert location.is_valid()
        assert location.geocoded_at == timestamp
        assert location.geocoded_at.year == 2025
        assert location.geocoded_at.month == 1
        assert location.geocoded_at.day == 20
    
    def test_location_address_hash_stored(self):
        """Test that address_hash is properly stored for cache key."""
        location = Location(
            location_id="location-test-123",
            provider_id="provider-test",
            provider_name="Test Provider",
            formatted_address="123 Test St",
            address_hash="abc123def456"
        )
        
        assert location.is_valid()
        assert location.address_hash == "abc123def456"
