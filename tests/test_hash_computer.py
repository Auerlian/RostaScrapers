"""Tests for hash computation functions."""

import pytest
from src.transform.hash_computer import (
    compute_source_hash,
    compute_record_hash,
    compute_address_hash,
    EVENT_TEMPLATE_SOURCE_FIELDS,
    EVENT_OCCURRENCE_SOURCE_FIELDS,
    LOCATION_SOURCE_FIELDS,
)


class TestComputeSourceHash:
    """Tests for compute_source_hash function."""
    
    def test_basic_hash_computation(self):
        """Test basic hash computation with simple fields."""
        record = {
            "title": "Pasta Making Class",
            "price": 50.0,
            "description": "Learn to make pasta"
        }
        source_fields = ["title", "price"]
        
        hash_value = compute_source_hash(record, source_fields)
        
        assert isinstance(hash_value, str)
        assert len(hash_value) == 12
        assert hash_value.isalnum()
    
    def test_deterministic_hashing(self):
        """Test that same input produces same hash."""
        record = {
            "title": "Pasta Making Class",
            "price": 50.0,
            "description": "Learn to make pasta"
        }
        source_fields = ["title", "price"]
        
        hash1 = compute_source_hash(record, source_fields)
        hash2 = compute_source_hash(record, source_fields)
        
        assert hash1 == hash2
    
    def test_field_order_independence(self):
        """Test that field order doesn't affect hash."""
        record1 = {"title": "Class", "price": 50.0}
        record2 = {"price": 50.0, "title": "Class"}
        source_fields = ["title", "price"]
        
        hash1 = compute_source_hash(record1, source_fields)
        hash2 = compute_source_hash(record2, source_fields)
        
        assert hash1 == hash2
    
    def test_ignores_non_source_fields(self):
        """Test that non-source fields don't affect hash."""
        record1 = {"title": "Class", "price": 50.0, "status": "active"}
        record2 = {"title": "Class", "price": 50.0, "status": "inactive"}
        source_fields = ["title", "price"]
        
        hash1 = compute_source_hash(record1, source_fields)
        hash2 = compute_source_hash(record2, source_fields)
        
        assert hash1 == hash2
    
    def test_different_values_produce_different_hashes(self):
        """Test that different field values produce different hashes."""
        record1 = {"title": "Pasta Making", "price": 50.0}
        record2 = {"title": "Pasta Making", "price": 60.0}
        source_fields = ["title", "price"]
        
        hash1 = compute_source_hash(record1, source_fields)
        hash2 = compute_source_hash(record2, source_fields)
        
        assert hash1 != hash2
    
    def test_handles_missing_fields(self):
        """Test that missing fields are handled gracefully."""
        record = {"title": "Pasta Making"}
        source_fields = ["title", "price", "description"]
        
        hash_value = compute_source_hash(record, source_fields)
        
        assert isinstance(hash_value, str)
        assert len(hash_value) == 12
    
    def test_handles_none_values(self):
        """Test that None values are included in hash."""
        record1 = {"title": "Class", "price": None}
        record2 = {"title": "Class", "price": 50.0}
        source_fields = ["title", "price"]
        
        hash1 = compute_source_hash(record1, source_fields)
        hash2 = compute_source_hash(record2, source_fields)
        
        assert hash1 != hash2
    
    def test_handles_list_fields(self):
        """Test that list fields are handled correctly."""
        record = {
            "title": "Class",
            "image_urls": ["http://example.com/1.jpg", "http://example.com/2.jpg"]
        }
        source_fields = ["title", "image_urls"]
        
        hash_value = compute_source_hash(record, source_fields)
        
        assert isinstance(hash_value, str)
        assert len(hash_value) == 12
    
    def test_list_order_matters(self):
        """Test that list order affects hash."""
        record1 = {"title": "Class", "image_urls": ["url1", "url2"]}
        record2 = {"title": "Class", "image_urls": ["url2", "url1"]}
        source_fields = ["title", "image_urls"]
        
        hash1 = compute_source_hash(record1, source_fields)
        hash2 = compute_source_hash(record2, source_fields)
        
        assert hash1 != hash2


class TestComputeRecordHash:
    """Tests for compute_record_hash function."""
    
    def test_basic_record_hash(self):
        """Test basic record hash computation."""
        record = {
            "title": "Pasta Making",
            "slug": "pasta-making",
            "price": 50.0,
            "status": "active"
        }
        
        hash_value = compute_record_hash(record)
        
        assert isinstance(hash_value, str)
        assert len(hash_value) == 12
        assert hash_value.isalnum()
    
    def test_excludes_lifecycle_fields_by_default(self):
        """Test that lifecycle fields are excluded by default."""
        record1 = {
            "title": "Class",
            "slug": "class",
            "status": "active",
            "first_seen_at": "2024-01-01",
            "last_seen_at": "2024-01-02"
        }
        record2 = {
            "title": "Class",
            "slug": "class",
            "status": "inactive",
            "first_seen_at": "2024-02-01",
            "last_seen_at": "2024-02-02"
        }
        
        hash1 = compute_record_hash(record1)
        hash2 = compute_record_hash(record2)
        
        assert hash1 == hash2
    
    def test_includes_enriched_fields(self):
        """Test that enriched fields affect the hash."""
        record1 = {
            "title": "Class",
            "description_clean": "Original description"
        }
        record2 = {
            "title": "Class",
            "description_clean": "Modified description"
        }
        
        hash1 = compute_record_hash(record1)
        hash2 = compute_record_hash(record2)
        
        assert hash1 != hash2
    
    def test_custom_exclusions(self):
        """Test that custom exclusions work."""
        record1 = {"title": "Class", "price": 50.0, "custom_field": "value1"}
        record2 = {"title": "Class", "price": 50.0, "custom_field": "value2"}
        
        hash1 = compute_record_hash(record1, exclude_fields=["custom_field"])
        hash2 = compute_record_hash(record2, exclude_fields=["custom_field"])
        
        assert hash1 == hash2
    
    def test_deterministic_hashing(self):
        """Test that same input produces same hash."""
        record = {
            "title": "Class",
            "slug": "class",
            "price": 50.0,
            "tags": ["beginner", "italian"]
        }
        
        hash1 = compute_record_hash(record)
        hash2 = compute_record_hash(record)
        
        assert hash1 == hash2
    
    def test_includes_computed_fields(self):
        """Test that computed fields like slug affect the hash."""
        record1 = {"title": "Pasta Making", "slug": "pasta-making"}
        record2 = {"title": "Pasta Making", "slug": "pasta-making-class"}
        
        hash1 = compute_record_hash(record1)
        hash2 = compute_record_hash(record2)
        
        assert hash1 != hash2


class TestComputeAddressHash:
    """Tests for compute_address_hash function."""
    
    def test_basic_address_hash(self):
        """Test basic address hash computation."""
        location = {
            "address_line_1": "123 Main Street",
            "city": "London",
            "postcode": "SW1A 1AA",
            "country": "UK"
        }
        
        hash_value = compute_address_hash(location)
        
        assert isinstance(hash_value, str)
        assert len(hash_value) == 12
        assert hash_value.isalnum()
    
    def test_deterministic_hashing(self):
        """Test that same address produces same hash."""
        location = {
            "address_line_1": "123 Main Street",
            "city": "London",
            "postcode": "SW1A 1AA"
        }
        
        hash1 = compute_address_hash(location)
        hash2 = compute_address_hash(location)
        
        assert hash1 == hash2
    
    def test_different_addresses_produce_different_hashes(self):
        """Test that different addresses produce different hashes."""
        location1 = {
            "address_line_1": "123 Main Street",
            "city": "London",
            "postcode": "SW1A 1AA"
        }
        location2 = {
            "address_line_1": "456 High Street",
            "city": "London",
            "postcode": "SW1A 1AA"
        }
        
        hash1 = compute_address_hash(location1)
        hash2 = compute_address_hash(location2)
        
        assert hash1 != hash2
    
    def test_ignores_non_address_fields(self):
        """Test that non-address fields don't affect hash."""
        location1 = {
            "address_line_1": "123 Main Street",
            "city": "London",
            "latitude": 51.5074,
            "longitude": -0.1278,
            "status": "active"
        }
        location2 = {
            "address_line_1": "123 Main Street",
            "city": "London",
            "latitude": 51.5075,
            "longitude": -0.1279,
            "status": "inactive"
        }
        
        hash1 = compute_address_hash(location1)
        hash2 = compute_address_hash(location2)
        
        assert hash1 == hash2
    
    def test_handles_partial_addresses(self):
        """Test that partial addresses are handled correctly."""
        location = {
            "address_line_1": "123 Main Street",
            "city": "London"
        }
        
        hash_value = compute_address_hash(location)
        
        assert isinstance(hash_value, str)
        assert len(hash_value) == 12
    
    def test_includes_all_address_components(self):
        """Test that all address components affect the hash."""
        location1 = {
            "address_line_1": "123 Main Street",
            "address_line_2": "Flat 4",
            "city": "London",
            "region": "Greater London",
            "postcode": "SW1A 1AA",
            "country": "UK"
        }
        location2 = {
            "address_line_1": "123 Main Street",
            "address_line_2": "Flat 5",  # Different
            "city": "London",
            "region": "Greater London",
            "postcode": "SW1A 1AA",
            "country": "UK"
        }
        
        hash1 = compute_address_hash(location1)
        hash2 = compute_address_hash(location2)
        
        assert hash1 != hash2
    
    def test_normalizes_address_fields_for_consistent_hashing(self):
        """Test that address fields are normalized before hashing.
        
        This ensures that minor formatting differences (punctuation, whitespace, case)
        don't produce different cache keys, satisfying Requirement 4.7.
        """
        # Same address with different formatting
        location1 = {
            "address_line_1": "123 Main St.",
            "city": "London",
            "postcode": "SW1A 1AA"
        }
        location2 = {
            "address_line_1": "123 MAIN ST",  # Different case, no punctuation
            "city": "  London  ",  # Extra whitespace
            "postcode": "SW1A 1AA"
        }
        location3 = {
            "address_line_1": "123  Main   St",  # Multiple spaces
            "city": "london",  # Lowercase
            "postcode": "SW1A 1AA"
        }
        
        hash1 = compute_address_hash(location1)
        hash2 = compute_address_hash(location2)
        hash3 = compute_address_hash(location3)
        
        # All should produce the same hash due to normalization
        assert hash1 == hash2 == hash3


class TestSourceFieldDefinitions:
    """Tests for source field definitions."""
    
    def test_event_template_source_fields_defined(self):
        """Test that event template source fields are defined."""
        assert isinstance(EVENT_TEMPLATE_SOURCE_FIELDS, list)
        assert len(EVENT_TEMPLATE_SOURCE_FIELDS) > 0
        assert "title" in EVENT_TEMPLATE_SOURCE_FIELDS
        assert "description_raw" in EVENT_TEMPLATE_SOURCE_FIELDS
    
    def test_event_occurrence_source_fields_defined(self):
        """Test that event occurrence source fields are defined."""
        assert isinstance(EVENT_OCCURRENCE_SOURCE_FIELDS, list)
        assert len(EVENT_OCCURRENCE_SOURCE_FIELDS) > 0
        assert "title" in EVENT_OCCURRENCE_SOURCE_FIELDS
        assert "start_at" in EVENT_OCCURRENCE_SOURCE_FIELDS
    
    def test_location_source_fields_defined(self):
        """Test that location source fields are defined."""
        assert isinstance(LOCATION_SOURCE_FIELDS, list)
        assert len(LOCATION_SOURCE_FIELDS) > 0
        assert "address_line_1" in LOCATION_SOURCE_FIELDS
        assert "city" in LOCATION_SOURCE_FIELDS


class TestIntegrationScenarios:
    """Integration tests for realistic scenarios."""
    
    def test_event_template_change_detection(self):
        """Test change detection for event templates."""
        # Original event
        event1 = {
            "title": "Pasta Making Class",
            "description_raw": "Learn to make fresh pasta",
            "price_from": 50.0,
            "source_url": "http://example.com/pasta",
            "status": "active",
            "first_seen_at": "2024-01-01"
        }
        
        # Same event, different timestamp (should have same source hash)
        event2 = {
            "title": "Pasta Making Class",
            "description_raw": "Learn to make fresh pasta",
            "price_from": 50.0,
            "source_url": "http://example.com/pasta",
            "status": "active",
            "first_seen_at": "2024-01-02"
        }
        
        # Event with changed price (should have different source hash)
        event3 = {
            "title": "Pasta Making Class",
            "description_raw": "Learn to make fresh pasta",
            "price_from": 60.0,
            "source_url": "http://example.com/pasta",
            "status": "active",
            "first_seen_at": "2024-01-01"
        }
        
        hash1 = compute_source_hash(event1, EVENT_TEMPLATE_SOURCE_FIELDS)
        hash2 = compute_source_hash(event2, EVENT_TEMPLATE_SOURCE_FIELDS)
        hash3 = compute_source_hash(event3, EVENT_TEMPLATE_SOURCE_FIELDS)
        
        assert hash1 == hash2  # Timestamps don't affect source hash
        assert hash1 != hash3  # Price change affects source hash
    
    def test_location_geocoding_cache_key(self):
        """Test that address hash works for geocoding cache."""
        # Same address, different geocoding results
        location1 = {
            "address_line_1": "123 Main Street",
            "city": "London",
            "postcode": "SW1A 1AA",
            "latitude": None,
            "longitude": None,
            "geocode_status": "not_geocoded"
        }
        
        location2 = {
            "address_line_1": "123 Main Street",
            "city": "London",
            "postcode": "SW1A 1AA",
            "latitude": 51.5074,
            "longitude": -0.1278,
            "geocode_status": "success"
        }
        
        # Different address
        location3 = {
            "address_line_1": "456 High Street",
            "city": "London",
            "postcode": "SW1A 2BB",
            "latitude": None,
            "longitude": None
        }
        
        hash1 = compute_address_hash(location1)
        hash2 = compute_address_hash(location2)
        hash3 = compute_address_hash(location3)
        
        assert hash1 == hash2  # Same address, different geocoding
        assert hash1 != hash3  # Different address
    
    def test_event_occurrence_with_all_fields(self):
        """Test event occurrence hash with all source fields."""
        occurrence = {
            "title": "Pasta Making - Evening Session",
            "description_raw": "Join us for an evening of pasta making",
            "price": 55.0,
            "booking_url": "http://example.com/book/123",
            "start_at": "2024-06-15T18:00:00",
            "end_at": "2024-06-15T20:00:00",
            "location_id": "location-pasta-abc123",
            "image_urls": ["http://example.com/img1.jpg"],
            "capacity": 12,
            "remaining_spaces": 5,
            "availability_status": "available",
            "source_event_id": "evt_123"
        }
        
        source_hash = compute_source_hash(occurrence, EVENT_OCCURRENCE_SOURCE_FIELDS)
        record_hash = compute_record_hash(occurrence)
        
        assert isinstance(source_hash, str)
        assert isinstance(record_hash, str)
        assert len(source_hash) == 12
        assert len(record_hash) == 12
