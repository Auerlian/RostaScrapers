"""Unit tests for canonical data models."""

from datetime import datetime
import pytest

from src.models import Provider, Location, EventTemplate, EventOccurrence


class TestProvider:
    """Tests for Provider model."""
    
    def test_valid_provider(self):
        """Test creating a valid provider."""
        provider = Provider(
            provider_id="provider-test",
            provider_name="Test Provider",
            provider_slug="test-provider",
            source_name="Test Source",
            source_base_url="https://example.com",
            status="active"
        )
        
        assert provider.is_valid()
        assert len(provider.validate()) == 0
    
    def test_provider_missing_required_fields(self):
        """Test provider validation with missing required fields."""
        provider = Provider(
            provider_id="",
            provider_name="",
            provider_slug="",
            source_name="Test",
            source_base_url="https://example.com"
        )
        
        errors = provider.validate()
        assert not provider.is_valid()
        assert any("provider_id" in err for err in errors)
        assert any("provider_name" in err for err in errors)
        assert any("provider_slug" in err for err in errors)
    
    def test_provider_invalid_slug_format(self):
        """Test provider validation with invalid slug format."""
        provider = Provider(
            provider_id="provider-test",
            provider_name="Test Provider",
            provider_slug="Test_Provider!",
            source_name="Test",
            source_base_url="https://example.com"
        )
        
        errors = provider.validate()
        assert not provider.is_valid()
        assert any("kebab-case" in err for err in errors)
    
    def test_provider_invalid_status(self):
        """Test provider validation with invalid status."""
        provider = Provider(
            provider_id="provider-test",
            provider_name="Test Provider",
            provider_slug="test-provider",
            source_name="Test",
            source_base_url="https://example.com",
            status="invalid"
        )
        
        errors = provider.validate()
        assert not provider.is_valid()
        assert any("status" in err for err in errors)
    
    def test_provider_invalid_timestamps(self):
        """Test provider validation with invalid timestamps."""
        provider = Provider(
            provider_id="provider-test",
            provider_name="Test Provider",
            provider_slug="test-provider",
            source_name="Test",
            source_base_url="https://example.com",
            first_seen_at=datetime(2025, 1, 20),
            last_seen_at=datetime(2025, 1, 15)
        )
        
        errors = provider.validate()
        assert not provider.is_valid()
        assert any("first_seen_at" in err and "last_seen_at" in err for err in errors)


class TestLocation:
    """Tests for Location model."""
    
    def test_valid_location(self):
        """Test creating a valid location."""
        location = Location(
            location_id="location-test-123",
            provider_id="provider-test",
            provider_name="Test Provider",
            formatted_address="123 Test St, London, UK",
            latitude=51.5074,
            longitude=-0.1278,
            geocode_status="success"
        )
        
        assert location.is_valid()
        assert len(location.validate()) == 0
    
    def test_location_missing_required_fields(self):
        """Test location validation with missing required fields."""
        location = Location(
            location_id="",
            provider_id="",
            provider_name="Test",
            formatted_address=""
        )
        
        errors = location.validate()
        assert not location.is_valid()
        assert any("location_id" in err for err in errors)
        assert any("provider_id" in err for err in errors)
        assert any("formatted_address" in err for err in errors)
    
    def test_location_invalid_coordinates(self):
        """Test location validation with invalid coordinates."""
        location = Location(
            location_id="location-test-123",
            provider_id="provider-test",
            provider_name="Test Provider",
            formatted_address="123 Test St",
            latitude=100.0,  # Invalid
            longitude=-200.0  # Invalid
        )
        
        errors = location.validate()
        assert not location.is_valid()
        assert any("latitude" in err for err in errors)
        assert any("longitude" in err for err in errors)
    
    def test_location_valid_coordinate_boundaries(self):
        """Test location validation with boundary coordinates."""
        location = Location(
            location_id="location-test-123",
            provider_id="provider-test",
            provider_name="Test Provider",
            formatted_address="123 Test St",
            latitude=90.0,
            longitude=-180.0
        )
        
        assert location.is_valid()
    
    def test_location_invalid_geocode_status(self):
        """Test location validation with invalid geocode status."""
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


class TestEventTemplate:
    """Tests for EventTemplate model."""
    
    def test_valid_event_template(self):
        """Test creating a valid event template."""
        template = EventTemplate(
            event_template_id="event-template-test-123",
            provider_id="provider-test",
            title="Test Event",
            slug="test-event",
            price_from=50.0,
            age_min=18,
            age_max=65
        )
        
        assert template.is_valid()
        assert len(template.validate()) == 0
    
    def test_event_template_missing_required_fields(self):
        """Test event template validation with missing required fields."""
        template = EventTemplate(
            event_template_id="",
            provider_id="",
            title="",
            slug=""
        )
        
        errors = template.validate()
        assert not template.is_valid()
        assert any("event_template_id" in err for err in errors)
        assert any("provider_id" in err for err in errors)
        assert any("title" in err for err in errors)
        assert any("slug" in err for err in errors)
    
    def test_event_template_invalid_slug_format(self):
        """Test event template validation with invalid slug format."""
        template = EventTemplate(
            event_template_id="event-template-test-123",
            provider_id="provider-test",
            title="Test Event",
            slug="Test Event!"
        )
        
        errors = template.validate()
        assert not template.is_valid()
        assert any("kebab-case" in err for err in errors)
    
    def test_event_template_invalid_price(self):
        """Test event template validation with invalid price."""
        template = EventTemplate(
            event_template_id="event-template-test-123",
            provider_id="provider-test",
            title="Test Event",
            slug="test-event",
            price_from=-10.0
        )
        
        errors = template.validate()
        assert not template.is_valid()
        assert any("price_from" in err for err in errors)
    
    def test_event_template_invalid_age_range(self):
        """Test event template validation with invalid age range."""
        template = EventTemplate(
            event_template_id="event-template-test-123",
            provider_id="provider-test",
            title="Test Event",
            slug="test-event",
            age_min=65,
            age_max=18
        )
        
        errors = template.validate()
        assert not template.is_valid()
        assert any("age_min" in err and "age_max" in err for err in errors)
    
    def test_event_template_invalid_location_scope(self):
        """Test event template validation with invalid location scope."""
        template = EventTemplate(
            event_template_id="event-template-test-123",
            provider_id="provider-test",
            title="Test Event",
            slug="test-event",
            location_scope="invalid"
        )
        
        errors = template.validate()
        assert not template.is_valid()
        assert any("location_scope" in err for err in errors)


class TestEventOccurrence:
    """Tests for EventOccurrence model."""
    
    def test_valid_event_occurrence(self):
        """Test creating a valid event occurrence."""
        occurrence = EventOccurrence(
            event_id="event-test-123",
            provider_id="provider-test",
            title="Test Event",
            start_at=datetime(2025, 6, 1, 10, 0),
            end_at=datetime(2025, 6, 1, 12, 0),
            price=50.0,
            availability_status="available"
        )
        
        assert occurrence.is_valid()
        assert len(occurrence.validate()) == 0
    
    def test_event_occurrence_missing_required_fields(self):
        """Test event occurrence validation with missing required fields."""
        occurrence = EventOccurrence(
            event_id="",
            provider_id="",
            title=""
        )
        
        errors = occurrence.validate()
        assert not occurrence.is_valid()
        assert any("event_id" in err for err in errors)
        assert any("provider_id" in err for err in errors)
        assert any("title" in err for err in errors)
    
    def test_event_occurrence_invalid_datetime_range(self):
        """Test event occurrence validation with invalid datetime range."""
        occurrence = EventOccurrence(
            event_id="event-test-123",
            provider_id="provider-test",
            title="Test Event",
            start_at=datetime(2025, 6, 1, 12, 0),
            end_at=datetime(2025, 6, 1, 10, 0)
        )
        
        errors = occurrence.validate()
        assert not occurrence.is_valid()
        assert any("start_at" in err and "end_at" in err for err in errors)
    
    def test_event_occurrence_invalid_price(self):
        """Test event occurrence validation with invalid price."""
        occurrence = EventOccurrence(
            event_id="event-test-123",
            provider_id="provider-test",
            title="Test Event",
            price=-10.0
        )
        
        errors = occurrence.validate()
        assert not occurrence.is_valid()
        assert any("price" in err for err in errors)
    
    def test_event_occurrence_invalid_availability_status(self):
        """Test event occurrence validation with invalid availability status."""
        occurrence = EventOccurrence(
            event_id="event-test-123",
            provider_id="provider-test",
            title="Test Event",
            availability_status="invalid"
        )
        
        errors = occurrence.validate()
        assert not occurrence.is_valid()
        assert any("availability_status" in err for err in errors)
    
    def test_event_occurrence_invalid_status(self):
        """Test event occurrence validation with invalid status."""
        occurrence = EventOccurrence(
            event_id="event-test-123",
            provider_id="provider-test",
            title="Test Event",
            status="invalid"
        )
        
        errors = occurrence.validate()
        assert not occurrence.is_valid()
        assert any("status" in err for err in errors)
