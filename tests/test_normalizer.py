"""Unit tests for Normalizer."""

import pytest
from datetime import datetime

from src.transform.normalizer import Normalizer
from src.models.raw_provider_data import RawProviderData
from src.models.provider import Provider
from src.models.location import Location
from src.models.event_template import EventTemplate
from src.models.event_occurrence import EventOccurrence


class TestNormalizeProvider:
    """Tests for normalize_provider method."""
    
    def test_basic_provider_normalization(self):
        """Test basic provider normalization."""
        raw_data = RawProviderData(
            provider_name="Pasta Evangelists",
            provider_website="https://pastaevangelists.com",
            provider_contact_email="info@pastaevangelists.com",
            source_name="Pasta Evangelists API",
            source_base_url="https://api.pastaevangelists.com"
        )
        
        normalizer = Normalizer()
        provider = normalizer.normalize_provider(raw_data)
        
        assert isinstance(provider, Provider)
        assert provider.provider_id == "provider-pasta-evangelists"
        assert provider.provider_name == "Pasta Evangelists"
        assert provider.provider_slug == "pasta-evangelists"
        assert provider.provider_website == "https://pastaevangelists.com"
        assert provider.provider_contact_email == "info@pastaevangelists.com"
        assert provider.source_name == "Pasta Evangelists API"
        assert provider.source_base_url == "https://api.pastaevangelists.com"
        assert provider.status == "active"
        assert provider.first_seen_at is not None
        assert provider.last_seen_at is not None
    
    def test_provider_with_minimal_data(self):
        """Test provider normalization with minimal data."""
        raw_data = RawProviderData(
            provider_name="Test Provider"
        )
        
        normalizer = Normalizer()
        provider = normalizer.normalize_provider(raw_data)
        
        assert provider.provider_id == "provider-test-provider"
        assert provider.provider_name == "Test Provider"
        assert provider.source_name == "Test Provider"  # Defaults to provider_name
        assert provider.source_base_url == ""
    
    def test_provider_with_metadata(self):
        """Test provider normalization with metadata."""
        raw_data = RawProviderData(
            provider_name="Test Provider",
            provider_metadata={"custom_field": "value", "count": 42}
        )
        
        normalizer = Normalizer()
        provider = normalizer.normalize_provider(raw_data)
        
        assert provider.metadata == {"custom_field": "value", "count": 42}


class TestNormalizeLocations:
    """Tests for normalize_locations method."""
    
    def test_basic_location_normalization(self):
        """Test basic location normalization."""
        raw_data = RawProviderData(
            provider_name="Pasta Evangelists",
            raw_locations=[{
                "location_name": "The Pasta Academy",
                "formatted_address": "123 Main St, London, EC1A 9EJ",
                "address_line_1": "123 Main St",
                "city": "London",
                "postcode": "EC1A 9EJ",
                "country": "UK"
            }]
        )
        
        normalizer = Normalizer()
        locations = normalizer.normalize_locations(raw_data, "provider-pasta-evangelists")
        
        assert len(locations) == 1
        location = locations[0]
        
        assert isinstance(location, Location)
        assert location.location_id.startswith("location-pasta-evangelists-")
        assert location.provider_id == "provider-pasta-evangelists"
        assert location.provider_name == "Pasta Evangelists"
        assert location.location_name == "The Pasta Academy"
        assert location.formatted_address == "123 Main St, London, EC1A 9EJ"
        assert location.address_line_1 == "123 Main St"
        assert location.city == "London"
        assert location.postcode == "EC1A 9EJ"
        assert location.country == "UK"
        assert location.geocode_status == "not_geocoded"
        assert location.status == "active"
        assert location.address_hash is not None
    
    def test_location_without_formatted_address(self):
        """Test location normalization builds formatted address from components."""
        raw_data = RawProviderData(
            provider_name="Test Provider",
            raw_locations=[{
                "location_name": "Test Venue",
                "address_line_1": "456 High St",
                "city": "London",
                "postcode": "SW1A 1AA"
            }]
        )
        
        normalizer = Normalizer()
        locations = normalizer.normalize_locations(raw_data, "provider-test")
        
        assert len(locations) == 1
        assert locations[0].formatted_address == "Test Venue, 456 High St, London, SW1A 1AA"
    
    def test_location_with_all_address_fields(self):
        """Test location normalization with all address fields."""
        raw_data = RawProviderData(
            provider_name="Test Provider",
            raw_locations=[{
                "location_name": "Test Venue",
                "address_line_1": "Building 1",
                "address_line_2": "456 High St",
                "city": "London",
                "region": "Greater London",
                "postcode": "SW1A 1AA",
                "country": "UK",
                "venue_phone": "020 1234 5678",
                "venue_email": "venue@example.com",
                "venue_website": "https://venue.example.com"
            }]
        )
        
        normalizer = Normalizer()
        locations = normalizer.normalize_locations(raw_data, "provider-test")
        
        assert len(locations) == 1
        location = locations[0]
        assert location.address_line_2 == "456 High St"
        assert location.region == "Greater London"
        assert location.venue_phone == "020 1234 5678"
        assert location.venue_email == "venue@example.com"
        assert location.venue_website == "https://venue.example.com"
    
    def test_multiple_locations(self):
        """Test normalization of multiple locations."""
        raw_data = RawProviderData(
            provider_name="Test Provider",
            raw_locations=[
                {"formatted_address": "123 Main St, London"},
                {"formatted_address": "456 High St, Manchester"},
                {"formatted_address": "789 Park Rd, Birmingham"}
            ]
        )
        
        normalizer = Normalizer()
        locations = normalizer.normalize_locations(raw_data, "provider-test")
        
        assert len(locations) == 3
        # Each should have unique ID
        ids = [loc.location_id for loc in locations]
        assert len(set(ids)) == 3
    
    def test_location_without_address_skipped(self):
        """Test that locations without address are skipped."""
        raw_data = RawProviderData(
            provider_name="Test Provider",
            raw_locations=[
                {"location_name": "Valid Location", "formatted_address": "123 Main St"},
                {"location_name": "Invalid Location"},  # No address
                {"formatted_address": ""}  # Empty address
            ]
        )
        
        normalizer = Normalizer()
        locations = normalizer.normalize_locations(raw_data, "provider-test")
        
        assert len(locations) == 1
        assert locations[0].location_name == "Valid Location"


class TestNormalizeEvents:
    """Tests for normalize_events method."""
    
    def test_normalize_template(self):
        """Test normalization of event template."""
        raw_data = RawProviderData(
            provider_name="Test Provider",
            raw_templates=[{
                "title": "Pasta Making Class",
                "description": "<p>Learn to make <b>fresh pasta</b></p>",
                "price": 50.0,
                "category": "Cooking",
                "duration": "2 hours",
                "tags": ["hands-on", "italian"],
                "source_url": "https://example.com/pasta"
            }]
        )
        
        normalizer = Normalizer()
        events = normalizer.normalize_events(raw_data, "provider-test", {})
        
        assert len(events) == 1
        template = events[0]
        
        assert isinstance(template, EventTemplate)
        assert template.event_template_id.startswith("event-template-test-")
        assert template.provider_id == "provider-test"
        assert template.title == "Pasta Making Class"
        assert template.slug == "pasta-making-class"
        assert template.description_raw == "<p>Learn to make <b>fresh pasta</b></p>"
        assert template.description_clean == "Learn to make fresh pasta"
        assert template.category == "Cooking"
        assert template.price_from == 50.0
        assert template.duration_minutes == 120
        assert template.tags == ["hands-on", "italian"]
        assert template.source_url == "https://example.com/pasta"
        assert template.source_hash is not None
        assert template.record_hash is not None
    
    def test_normalize_occurrence(self):
        """Test normalization of event occurrence."""
        raw_data = RawProviderData(
            provider_name="Test Provider",
            raw_events=[{
                "title": "Pasta Class - Evening Session",
                "description": "Join us for pasta making",
                "start_at": "2024-06-15T18:00:00",
                "end_at": "2024-06-15T20:00:00",
                "price": "£55",
                "booking_url": "https://example.com/book/123",
                "capacity": 12,
                "remaining_spaces": 5,
                "availability_status": "available"
            }]
        )
        
        normalizer = Normalizer()
        events = normalizer.normalize_events(raw_data, "provider-test", {})
        
        assert len(events) == 1
        occurrence = events[0]
        
        assert isinstance(occurrence, EventOccurrence)
        assert occurrence.event_id.startswith("event-test-")
        assert occurrence.provider_id == "provider-test"
        assert occurrence.title == "Pasta Class - Evening Session"
        assert occurrence.start_at == datetime(2024, 6, 15, 18, 0)
        assert occurrence.end_at == datetime(2024, 6, 15, 20, 0)
        assert occurrence.price == 55.0
        assert occurrence.booking_url == "https://example.com/book/123"
        assert occurrence.capacity == 12
        assert occurrence.remaining_spaces == 5
        assert occurrence.availability_status == "available"
        assert occurrence.source_hash is not None
        assert occurrence.record_hash is not None
    
    def test_normalize_mixed_templates_and_occurrences(self):
        """Test normalization of both templates and occurrences."""
        raw_data = RawProviderData(
            provider_name="Test Provider",
            raw_templates=[
                {"title": "Template 1", "price": 50.0},
                {"title": "Template 2", "price": 60.0}
            ],
            raw_events=[
                {"title": "Event 1", "start_at": "2024-06-15T18:00:00"},
                {"title": "Event 2", "start_at": "2024-06-16T18:00:00"}
            ]
        )
        
        normalizer = Normalizer()
        events = normalizer.normalize_events(raw_data, "provider-test", {})
        
        assert len(events) == 4
        templates = [e for e in events if isinstance(e, EventTemplate)]
        occurrences = [e for e in events if isinstance(e, EventOccurrence)]
        assert len(templates) == 2
        assert len(occurrences) == 2
    
    def test_event_without_title_skipped(self):
        """Test that events without title are skipped."""
        raw_data = RawProviderData(
            provider_name="Test Provider",
            raw_templates=[
                {"title": "Valid Template", "price": 50.0},
                {"description": "No title"},  # Missing title
                {"title": ""},  # Empty title
            ]
        )
        
        normalizer = Normalizer()
        events = normalizer.normalize_events(raw_data, "provider-test", {})
        
        assert len(events) == 1
        assert events[0].title == "Valid Template"
    
    def test_event_with_location_mapping(self):
        """Test event normalization with location mapping."""
        location_map = {
            "123 Main St, London": "location-test-abc123"
        }
        
        raw_data = RawProviderData(
            provider_name="Test Provider",
            raw_events=[{
                "title": "Test Event",
                "start_at": "2024-06-15T18:00:00",
                "location_data": {
                    "formatted_address": "123 Main St, London"
                }
            }]
        )
        
        normalizer = Normalizer()
        events = normalizer.normalize_events(raw_data, "provider-test", location_map)
        
        assert len(events) == 1
        assert events[0].location_id == "location-test-abc123"


class TestHelperMethods:
    """Tests for helper methods."""
    
    def test_normalize_text(self):
        """Test text normalization."""
        normalizer = Normalizer()
        
        assert normalizer._normalize_text("  Hello  World  ") == "Hello World"
        assert normalizer._normalize_text("Multiple\n\nLines") == "Multiple Lines"
        assert normalizer._normalize_text("") is None
        assert normalizer._normalize_text(None) is None
        assert normalizer._normalize_text("   ") is None
    
    def test_strip_html(self):
        """Test HTML stripping with structure preservation."""
        normalizer = Normalizer()
        
        # Basic HTML stripping
        assert normalizer._strip_html("<p>Hello <b>World</b></p>") == "Hello World"
        assert normalizer._strip_html("<div>Test</div>") == "Test"
        assert normalizer._strip_html("Plain text") == "Plain text"
        assert normalizer._strip_html("") is None
        assert normalizer._strip_html(None) is None
        
        # Preserve paragraph structure
        html_with_paragraphs = "<p>First paragraph.</p><p>Second paragraph.</p>"
        result = normalizer._strip_html(html_with_paragraphs)
        assert "First paragraph." in result
        assert "Second paragraph." in result
        assert "\n" in result  # Should have newline between paragraphs
        
        # Preserve list structure
        html_with_list = "<ul><li>Item 1</li><li>Item 2</li><li>Item 3</li></ul>"
        result = normalizer._strip_html(html_with_list)
        assert "• Item 1" in result
        assert "• Item 2" in result
        assert "• Item 3" in result
        
        # Handle line breaks
        html_with_br = "Line 1<br>Line 2<br/>Line 3"
        result = normalizer._strip_html(html_with_br)
        assert "Line 1" in result
        assert "Line 2" in result
        assert "Line 3" in result
        assert "\n" in result
    
    def test_parse_price(self):
        """Test price parsing."""
        normalizer = Normalizer()
        
        assert normalizer._parse_price(50) == 50.0
        assert normalizer._parse_price(50.5) == 50.5
        assert normalizer._parse_price("50") == 50.0
        assert normalizer._parse_price("£50") == 50.0
        assert normalizer._parse_price("$50.00") == 50.0
        assert normalizer._parse_price("GBP 50") == 50.0
        assert normalizer._parse_price("50.99") == 50.99
        assert normalizer._parse_price(None) is None
        assert normalizer._parse_price("invalid") is None
    
    def test_parse_datetime(self):
        """Test datetime parsing."""
        normalizer = Normalizer()
        
        # ISO format
        result = normalizer._parse_datetime("2024-06-15T18:00:00")
        assert result == datetime(2024, 6, 15, 18, 0)
        
        # ISO with Z
        result = normalizer._parse_datetime("2024-06-15T18:00:00Z")
        assert result is not None
        
        # Common formats
        result = normalizer._parse_datetime("2024-06-15 18:00:00")
        assert result == datetime(2024, 6, 15, 18, 0)
        
        result = normalizer._parse_datetime("15/06/2024 18:00")
        assert result == datetime(2024, 6, 15, 18, 0)
        
        # Already datetime
        dt = datetime(2024, 6, 15, 18, 0)
        assert normalizer._parse_datetime(dt) == dt
        
        # Invalid
        assert normalizer._parse_datetime(None) is None
        assert normalizer._parse_datetime("invalid") is None
    
    def test_parse_int(self):
        """Test integer parsing."""
        normalizer = Normalizer()
        
        assert normalizer._parse_int(42) == 42
        assert normalizer._parse_int("42") == 42
        assert normalizer._parse_int(None) is None
        assert normalizer._parse_int("invalid") is None
    
    def test_parse_duration(self):
        """Test duration parsing."""
        normalizer = Normalizer()
        
        assert normalizer._parse_duration(120) == 120
        assert normalizer._parse_duration("120") == 120
        assert normalizer._parse_duration("2 hours") == 120
        assert normalizer._parse_duration("2h") == 120
        assert normalizer._parse_duration("1.5 hours") == 90
        assert normalizer._parse_duration("90 minutes") == 90
        assert normalizer._parse_duration("90min") == 90
        assert normalizer._parse_duration(None) is None
        assert normalizer._parse_duration("invalid") is None
    
    def test_extract_list(self):
        """Test list extraction."""
        normalizer = Normalizer()
        
        assert normalizer._extract_list(["tag1", "tag2"]) == ["tag1", "tag2"]
        assert normalizer._extract_list("tag1, tag2") == ["tag1", "tag2"]
        assert normalizer._extract_list("tag1; tag2") == ["tag1", "tag2"]
        assert normalizer._extract_list("single") == ["single"]
        assert normalizer._extract_list("") == []
        assert normalizer._extract_list(None) == []
    
    def test_extract_image_urls(self):
        """Test image URL extraction."""
        normalizer = Normalizer()
        
        # List of URLs
        data = {"image_urls": ["http://example.com/1.jpg", "http://example.com/2.jpg"]}
        assert normalizer._extract_image_urls(data) == ["http://example.com/1.jpg", "http://example.com/2.jpg"]
        
        # Single URL
        data = {"image_url": "http://example.com/image.jpg"}
        assert normalizer._extract_image_urls(data) == ["http://example.com/image.jpg"]
        
        # Alternative field names
        data = {"images": ["http://example.com/1.jpg"]}
        assert normalizer._extract_image_urls(data) == ["http://example.com/1.jpg"]
        
        data = {"image": "http://example.com/image.jpg"}
        assert normalizer._extract_image_urls(data) == ["http://example.com/image.jpg"]
        
        # No images
        assert normalizer._extract_image_urls({}) == []


class TestLocationResolution:
    """Tests for location ID resolution."""
    
    def test_resolve_location_direct_id(self):
        """Test location resolution with direct location_id."""
        normalizer = Normalizer()
        raw_event = {"location_id": "location-test-123"}
        
        location_id = normalizer._resolve_location_id(raw_event, {})
        assert location_id == "location-test-123"
    
    def test_resolve_location_embedded_data(self):
        """Test location resolution with embedded location data."""
        normalizer = Normalizer()
        raw_event = {
            "location_data": {
                "formatted_address": "123 Main St, London"
            }
        }
        location_map = {"123 Main St, London": "location-test-abc"}
        
        location_id = normalizer._resolve_location_id(raw_event, location_map)
        assert location_id == "location-test-abc"
    
    def test_resolve_location_by_reference(self):
        """Test location resolution by reference field."""
        normalizer = Normalizer()
        raw_event = {"location_ref": "main-venue"}
        location_map = {"main-venue": "location-test-xyz"}
        
        location_id = normalizer._resolve_location_id(raw_event, location_map)
        assert location_id == "location-test-xyz"
    
    def test_resolve_location_not_found(self):
        """Test location resolution when not found."""
        normalizer = Normalizer()
        raw_event = {"location_ref": "unknown"}
        
        location_id = normalizer._resolve_location_id(raw_event, {})
        assert location_id is None


class TestIntegrationScenarios:
    """Integration tests for realistic scenarios."""
    
    def test_complete_provider_normalization(self):
        """Test complete normalization of provider with locations and events."""
        raw_data = RawProviderData(
            provider_name="Pasta Evangelists",
            provider_website="https://pastaevangelists.com",
            source_name="Pasta Evangelists API",
            source_base_url="https://api.pastaevangelists.com",
            raw_locations=[
                {
                    "location_name": "The Pasta Academy Farringdon",
                    "formatted_address": "62-63 Long Lane, London, EC1A 9EJ",
                    "address_line_1": "62-63 Long Lane",
                    "city": "London",
                    "postcode": "EC1A 9EJ"
                }
            ],
            raw_templates=[
                {
                    "title": "Beginners Class",
                    "description": "<p>Join us to master the techniques of pasta fatta a mano</p>",
                    "price": 68.0,
                    "category": "Cooking",
                    "sub_category": "Pasta Making",
                    "duration": "2 hours",
                    "tags": ["hands-on", "italian", "beginner-friendly"],
                    "source_url": "https://plan.pastaevangelists.com/events/themes",
                    "image_url": "https://cdn.example.com/pasta.jpg"
                }
            ]
        )
        
        normalizer = Normalizer()
        
        # Normalize provider
        provider = normalizer.normalize_provider(raw_data)
        assert provider.provider_id == "provider-pasta-evangelists"
        assert provider.is_valid()
        
        # Normalize locations
        locations = normalizer.normalize_locations(raw_data, provider.provider_id)
        assert len(locations) == 1
        assert locations[0].is_valid()
        
        # Build location map
        location_map = {
            loc.formatted_address: loc.location_id for loc in locations
        }
        
        # Normalize events
        events = normalizer.normalize_events(raw_data, provider.provider_id, location_map)
        assert len(events) == 1
        assert isinstance(events[0], EventTemplate)
        assert events[0].is_valid()
        assert events[0].title == "Beginners Class"
        assert events[0].description_clean == "Join us to master the techniques of pasta fatta a mano"
        assert events[0].price_from == 68.0
        assert events[0].duration_minutes == 120
        assert events[0].tags == ["hands-on", "italian", "beginner-friendly"]
    
    def test_event_with_location_linking(self):
        """Test event normalization with location linking."""
        raw_data = RawProviderData(
            provider_name="Test Provider",
            raw_locations=[
                {"formatted_address": "123 Main St, London"}
            ],
            raw_events=[
                {
                    "title": "Test Event",
                    "start_at": "2024-06-15T18:00:00",
                    "location_data": {
                        "formatted_address": "123 Main St, London"
                    }
                }
            ]
        )
        
        normalizer = Normalizer()
        
        # Normalize locations first
        locations = normalizer.normalize_locations(raw_data, "provider-test")
        location_map = {loc.formatted_address: loc.location_id for loc in locations}
        
        # Normalize events with location map
        events = normalizer.normalize_events(raw_data, "provider-test", location_map)
        
        assert len(events) == 1
        assert events[0].location_id == locations[0].location_id
