"""Unit tests for CSV exporter."""

import csv
import tempfile
from pathlib import Path
from datetime import datetime

import pytest

from src.models.event_template import EventTemplate
from src.models.event_occurrence import EventOccurrence
from src.models.location import Location
from src.models.provider import Provider
from src.storage.store import CanonicalStore
from src.export.csv_exporter import CSVExporter


@pytest.fixture
def temp_store():
    """Create a temporary canonical store for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        store = CanonicalStore(base_path=tmpdir)
        yield store


@pytest.fixture
def sample_provider():
    """Create a sample provider for testing."""
    return Provider(
        provider_id="provider-test-provider",
        provider_name="Test Provider",
        provider_slug="test-provider",
        source_name="Test Provider API",
        source_base_url="https://example.com",
        status="active",
        first_seen_at=datetime(2025, 1, 15, 10, 0, 0),
        last_seen_at=datetime(2025, 1, 20, 14, 0, 0)
    )


@pytest.fixture
def sample_location():
    """Create a sample location for testing."""
    return Location(
        location_id="location-test-provider-abc123",
        provider_id="provider-test-provider",
        provider_name="Test Provider",
        formatted_address="123 Test Street, London, EC1A 1AA",
        location_name="Test Venue",
        address_line_1="123 Test Street",
        city="London",
        postcode="EC1A 1AA",
        country="UK",
        latitude=51.5074,
        longitude=-0.1278,
        geocode_status="success",
        status="active",
        first_seen_at=datetime(2025, 1, 15, 10, 0, 0),
        last_seen_at=datetime(2025, 1, 20, 14, 0, 0)
    )


@pytest.fixture
def sample_template():
    """Create a sample event template for testing."""
    return EventTemplate(
        event_template_id="event-template-test-provider-cooking-class",
        provider_id="provider-test-provider",
        title="Cooking Class",
        slug="cooking-class",
        category="Cooking",
        sub_category="Italian",
        description_raw="<p>Learn to cook Italian food</p>",
        description_clean="Learn to cook Italian food",
        description_ai="Master Italian cooking techniques",
        summary_short="Italian cooking class",
        summary_medium="Learn authentic Italian cooking in this hands-on class",
        tags=["hands-on", "italian", "beginner-friendly"],
        occasion_tags=["date-night", "team-building"],
        skills_required=[],
        skills_created=["pasta-making", "italian-cooking"],
        age_min=18,
        age_max=None,
        audience="adults",
        family_friendly=False,
        beginner_friendly=True,
        duration_minutes=120,
        price_from=68.0,
        currency="GBP",
        source_url="https://example.com/classes",
        image_urls=["https://example.com/img1.jpg", "https://example.com/img2.jpg"],
        status="active",
        first_seen_at=datetime(2025, 1, 15, 10, 0, 0),
        last_seen_at=datetime(2025, 1, 20, 14, 0, 0)
    )


@pytest.fixture
def sample_occurrence(sample_location):
    """Create a sample event occurrence for testing."""
    return EventOccurrence(
        event_id="event-test-provider-session-123",
        event_template_id="event-template-test-provider-cooking-class",
        provider_id="provider-test-provider",
        location_id=sample_location.location_id,
        title="Cooking Class",
        start_at=datetime(2025, 2, 15, 18, 0, 0),
        end_at=datetime(2025, 2, 15, 20, 0, 0),
        timezone="Europe/London",
        booking_url="https://example.com/book/123",
        price=68.0,
        currency="GBP",
        capacity=12,
        remaining_spaces=5,
        availability_status="available",
        description_clean="Learn to cook Italian food",
        tags=["hands-on", "italian"],
        skills_required=[],
        skills_created=["pasta-making"],
        age_min=18,
        status="active",
        first_seen_at=datetime(2025, 1, 18, 9, 0, 0),
        last_seen_at=datetime(2025, 1, 20, 14, 0, 0)
    )


def test_export_events_creates_csv_file(temp_store, sample_template, sample_location, sample_provider):
    """Test that export_events creates a CSV file."""
    # Save data to store
    temp_store.save_providers([sample_provider])
    temp_store.save_events([sample_template])
    temp_store.save_locations([sample_location])
    
    # Export
    exporter = CSVExporter(temp_store)
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "events.csv"
        result = exporter.export_events(str(output_path))
        
        # Verify file exists
        assert output_path.exists()
        
        # Verify result statistics
        assert result["total_records"] == 1
        assert result["template_count"] == 1
        assert result["occurrence_count"] == 0


def test_export_events_template_row_format(temp_store, sample_template, sample_location, sample_provider):
    """Test that template rows are formatted correctly."""
    temp_store.save_providers([sample_provider])
    temp_store.save_events([sample_template])
    temp_store.save_locations([sample_location])
    
    exporter = CSVExporter(temp_store)
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "events.csv"
        exporter.export_events(str(output_path))
        
        # Read CSV
        with open(output_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        assert len(rows) == 1
        row = rows[0]
        
        # Verify record type and IDs
        assert row["record_type"] == "template"
        assert row["record_id"] == "event-template-test-provider-cooking-class"
        assert row["event_template_id"] == ""  # Empty for template rows
        
        # Verify core fields
        assert row["provider_id"] == "provider-test-provider"
        assert row["provider_name"] == "Test Provider"
        assert row["title"] == "Cooking Class"
        assert row["slug"] == "cooking-class"
        assert row["category"] == "Cooking"
        assert row["sub_category"] == "Italian"
        
        # Verify list fields use semicolons
        assert row["tags"] == "hands-on;italian;beginner-friendly"
        assert row["occasion_tags"] == "date-night;team-building"
        assert row["skills_required"] == ""
        assert row["skills_created"] == "pasta-making;italian-cooking"
        assert row["image_urls"] == "https://example.com/img1.jpg;https://example.com/img2.jpg"
        
        # Verify boolean fields
        assert row["family_friendly"] == "false"
        assert row["beginner_friendly"] == "true"
        
        # Verify numeric fields
        assert row["age_min"] == "18"
        assert row["age_max"] == ""
        assert row["duration_minutes"] == "120"
        assert row["price"] == "68.0"
        assert row["currency"] == "GBP"
        
        # Verify template-specific empty fields
        assert row["location_id"] == ""
        assert row["location_name"] == ""
        assert row["formatted_address"] == ""
        assert row["start_at"] == ""
        assert row["end_at"] == ""
        assert row["timezone"] == ""
        assert row["capacity"] == ""
        assert row["remaining_spaces"] == ""
        assert row["availability_status"] == ""
        
        # Verify timestamps
        assert row["first_seen_at"] == "2025-01-15T10:00:00Z"
        assert row["last_seen_at"] == "2025-01-20T14:00:00Z"


def test_export_events_occurrence_row_format(temp_store, sample_occurrence, sample_location, sample_provider):
    """Test that occurrence rows are formatted correctly."""
    temp_store.save_providers([sample_provider])
    temp_store.save_events([sample_occurrence])
    temp_store.save_locations([sample_location])
    
    exporter = CSVExporter(temp_store)
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "events.csv"
        exporter.export_events(str(output_path))
        
        # Read CSV
        with open(output_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        assert len(rows) == 1
        row = rows[0]
        
        # Verify record type and IDs
        assert row["record_type"] == "occurrence"
        assert row["record_id"] == "event-test-provider-session-123"
        assert row["event_template_id"] == "event-template-test-provider-cooking-class"
        
        # Verify core fields
        assert row["provider_id"] == "provider-test-provider"
        assert row["provider_name"] == "Test Provider"
        assert row["title"] == "Cooking Class"
        
        # Verify location denormalization
        assert row["location_id"] == "location-test-provider-abc123"
        assert row["location_name"] == "Test Venue"
        assert row["formatted_address"] == "123 Test Street, London, EC1A 1AA"
        
        # Verify datetime fields
        assert row["start_at"] == "2025-02-15T18:00:00Z"
        assert row["end_at"] == "2025-02-15T20:00:00Z"
        assert row["timezone"] == "Europe/London"
        
        # Verify booking fields
        assert row["booking_url"] == "https://example.com/book/123"
        assert row["price"] == "68.0"
        assert row["capacity"] == "12"
        assert row["remaining_spaces"] == "5"
        assert row["availability_status"] == "available"
        
        # Verify occurrence-specific empty fields
        assert row["slug"] == ""
        assert row["category"] == ""
        assert row["sub_category"] == ""
        assert row["summary_short"] == ""
        assert row["summary_medium"] == ""
        assert row["occasion_tags"] == ""
        assert row["audience"] == ""
        assert row["family_friendly"] == ""
        assert row["beginner_friendly"] == ""
        assert row["duration_minutes"] == ""
        assert row["source_url"] == ""
        assert row["image_urls"] == ""


def test_export_events_mixed_records(temp_store, sample_template, sample_occurrence, sample_location, sample_provider):
    """Test exporting both templates and occurrences together."""
    temp_store.save_providers([sample_provider])
    temp_store.save_events([sample_template, sample_occurrence])
    temp_store.save_locations([sample_location])
    
    exporter = CSVExporter(temp_store)
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "events.csv"
        result = exporter.export_events(str(output_path))
        
        # Verify statistics
        assert result["total_records"] == 2
        assert result["template_count"] == 1
        assert result["occurrence_count"] == 1
        
        # Read CSV
        with open(output_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        assert len(rows) == 2
        
        # Verify template row comes first
        assert rows[0]["record_type"] == "template"
        assert rows[0]["record_id"] == "event-template-test-provider-cooking-class"
        
        # Verify occurrence row comes second
        assert rows[1]["record_type"] == "occurrence"
        assert rows[1]["record_id"] == "event-test-provider-session-123"


def test_export_events_filters_by_status(temp_store, sample_template, sample_provider):
    """Test that export filters by status correctly."""
    # Save provider
    temp_store.save_providers([sample_provider])
    
    # Create inactive template
    inactive_template = EventTemplate(
        event_template_id="event-template-test-provider-inactive",
        provider_id="provider-test-provider",
        title="Inactive Class",
        slug="inactive-class",
        status="inactive",
        first_seen_at=datetime(2025, 1, 15, 10, 0, 0),
        last_seen_at=datetime(2025, 1, 20, 14, 0, 0)
    )
    
    temp_store.save_events([sample_template, inactive_template])
    
    exporter = CSVExporter(temp_store)
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "events.csv"
        
        # Export with default filter (active only)
        result = exporter.export_events(str(output_path))
        assert result["total_records"] == 1
        
        # Export with no filter
        result = exporter.export_events(str(output_path), filters={})
        assert result["total_records"] == 2


def test_export_events_handles_null_values(temp_store, sample_provider):
    """Test that null values are formatted as empty strings."""
    # Save provider
    temp_store.save_providers([sample_provider])
    
    minimal_template = EventTemplate(
        event_template_id="event-template-minimal",
        provider_id="provider-test",
        title="Minimal Event",
        slug="minimal-event",
        # All optional fields left as None/empty
        status="active",
        first_seen_at=datetime(2025, 1, 15, 10, 0, 0),
        last_seen_at=datetime(2025, 1, 20, 14, 0, 0)
    )
    
    temp_store.save_events([minimal_template])
    
    exporter = CSVExporter(temp_store)
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "events.csv"
        exporter.export_events(str(output_path))
        
        # Read CSV
        with open(output_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        row = rows[0]
        
        # Verify null fields are empty strings
        assert row["category"] == ""
        assert row["sub_category"] == ""
        assert row["description_raw"] == ""
        assert row["description_clean"] == ""
        assert row["description_ai"] == ""
        assert row["age_min"] == ""
        assert row["age_max"] == ""
        assert row["price"] == ""
        assert row["tags"] == ""
        assert row["image_urls"] == ""


def test_export_events_handles_empty_lists(temp_store, sample_provider):
    """Test that empty lists are formatted as empty strings."""
    # Save provider
    temp_store.save_providers([sample_provider])
    
    template = EventTemplate(
        event_template_id="event-template-no-lists",
        provider_id="provider-test",
        title="No Lists Event",
        slug="no-lists-event",
        tags=[],  # Empty list
        occasion_tags=[],
        skills_required=[],
        skills_created=[],
        image_urls=[],
        status="active",
        first_seen_at=datetime(2025, 1, 15, 10, 0, 0),
        last_seen_at=datetime(2025, 1, 20, 14, 0, 0)
    )
    
    temp_store.save_events([template])
    
    exporter = CSVExporter(temp_store)
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "events.csv"
        exporter.export_events(str(output_path))
        
        # Read CSV
        with open(output_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        row = rows[0]
        
        # Verify empty lists are empty strings
        assert row["tags"] == ""
        assert row["occasion_tags"] == ""
        assert row["skills_required"] == ""
        assert row["skills_created"] == ""
        assert row["image_urls"] == ""


def test_export_events_occurrence_without_location(temp_store, sample_provider):
    """Test that occurrences without location_id handle gracefully."""
    # Save provider
    temp_store.save_providers([sample_provider])
    
    occurrence = EventOccurrence(
        event_id="event-no-location",
        provider_id="provider-test",
        title="No Location Event",
        location_id=None,  # No location
        start_at=datetime(2025, 2, 15, 18, 0, 0),
        status="active",
        first_seen_at=datetime(2025, 1, 18, 9, 0, 0),
        last_seen_at=datetime(2025, 1, 20, 14, 0, 0)
    )
    
    temp_store.save_events([occurrence])
    
    exporter = CSVExporter(temp_store)
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "events.csv"
        exporter.export_events(str(output_path))
        
        # Read CSV
        with open(output_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        row = rows[0]
        
        # Verify location fields are empty
        assert row["location_id"] == ""
        assert row["location_name"] == ""
        assert row["formatted_address"] == ""


def test_export_events_creates_parent_directory(temp_store, sample_template, sample_provider):
    """Test that export creates parent directories if they don't exist."""
    temp_store.save_providers([sample_provider])
    temp_store.save_events([sample_template])
    
    exporter = CSVExporter(temp_store)
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "nested" / "dir" / "events.csv"
        exporter.export_events(str(output_path))
        
        # Verify file exists
        assert output_path.exists()
        assert output_path.parent.exists()



def test_export_locations_creates_csv_file(temp_store, sample_location, sample_provider):
    """Test that export_locations creates a CSV file."""
    # Save data to store
    temp_store.save_providers([sample_provider])
    temp_store.save_locations([sample_location])
    
    # Export
    exporter = CSVExporter(temp_store)
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "locations.csv"
        result = exporter.export_locations(str(output_path))
        
        # Verify file exists
        assert output_path.exists()
        
        # Verify result statistics
        assert result["total_records"] == 1
        assert result["with_coordinates"] == 1


def test_export_locations_row_format(temp_store, sample_location, sample_provider, sample_template, sample_occurrence):
    """Test that location rows are formatted correctly."""
    temp_store.save_providers([sample_provider])
    temp_store.save_locations([sample_location])
    temp_store.save_events([sample_template, sample_occurrence])
    
    exporter = CSVExporter(temp_store)
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "locations.csv"
        exporter.export_locations(str(output_path))
        
        # Read CSV
        with open(output_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        assert len(rows) == 1
        row = rows[0]
        
        # Verify core fields
        assert row["location_id"] == "location-test-provider-abc123"
        assert row["provider_id"] == "provider-test-provider"
        assert row["provider_name"] == "Test Provider"
        assert row["location_name"] == "Test Venue"
        assert row["formatted_address"] == "123 Test Street, London, EC1A 1AA"
        
        # Verify address fields
        assert row["address_line_1"] == "123 Test Street"
        assert row["address_line_2"] == ""
        assert row["city"] == "London"
        assert row["region"] == ""
        assert row["postcode"] == "EC1A 1AA"
        assert row["country"] == "UK"
        
        # Verify geocoding fields
        assert row["latitude"] == "51.5074"
        assert row["longitude"] == "-0.1278"
        assert row["geocode_status"] == "success"
        assert row["geocode_precision"] == ""
        assert row["geocode_provider"] == ""
        assert row["geocoded_at"] == ""
        
        # Verify contact fields
        assert row["venue_phone"] == ""
        assert row["venue_email"] == ""
        assert row["venue_website"] == ""
        
        # Verify event summary fields
        assert row["event_count"] == "1"  # Only occurrence is linked to location
        assert row["active_event_count"] == "1"
        assert row["event_names"] == "Cooking Class"
        assert row["active_event_ids"] == "event-test-provider-session-123"
        
        # Verify lifecycle fields
        assert row["status"] == "active"
        assert row["first_seen_at"] == "2025-01-15T10:00:00Z"
        assert row["last_seen_at"] == "2025-01-20T14:00:00Z"


def test_export_locations_with_multiple_events(temp_store, sample_location, sample_provider):
    """Test location export with multiple events at the same location."""
    temp_store.save_providers([sample_provider])
    temp_store.save_locations([sample_location])
    
    # Create multiple occurrences at the same location
    occurrences = [
        EventOccurrence(
            event_id=f"event-test-provider-session-{i}",
            provider_id="provider-test-provider",
            location_id=sample_location.location_id,
            title=f"Event {i}",
            start_at=datetime(2025, 2, 15 + i, 18, 0, 0),
            status="active",
            first_seen_at=datetime(2025, 1, 18, 9, 0, 0),
            last_seen_at=datetime(2025, 1, 20, 14, 0, 0)
        )
        for i in range(3)
    ]
    temp_store.save_events(occurrences)
    
    exporter = CSVExporter(temp_store)
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "locations.csv"
        exporter.export_locations(str(output_path))
        
        # Read CSV
        with open(output_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        row = rows[0]
        
        # Verify event counts
        assert row["event_count"] == "3"
        assert row["active_event_count"] == "3"
        
        # Verify event names are semicolon-separated
        event_names = row["event_names"].split(";")
        assert len(event_names) == 3
        assert "Event 0" in event_names
        assert "Event 1" in event_names
        assert "Event 2" in event_names
        
        # Verify event IDs are semicolon-separated
        event_ids = row["active_event_ids"].split(";")
        assert len(event_ids) == 3


def test_export_locations_truncates_long_event_list(temp_store, sample_location, sample_provider):
    """Test that event names are truncated if too long."""
    temp_store.save_providers([sample_provider])
    temp_store.save_locations([sample_location])
    
    # Create 15 occurrences (more than the 10 limit)
    occurrences = [
        EventOccurrence(
            event_id=f"event-test-provider-session-{i}",
            provider_id="provider-test-provider",
            location_id=sample_location.location_id,
            title=f"Event {i}",
            start_at=datetime(2025, 2, 15, 18, 0, 0),
            status="active",
            first_seen_at=datetime(2025, 1, 18, 9, 0, 0),
            last_seen_at=datetime(2025, 1, 20, 14, 0, 0)
        )
        for i in range(15)
    ]
    temp_store.save_events(occurrences)
    
    exporter = CSVExporter(temp_store)
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "locations.csv"
        exporter.export_locations(str(output_path))
        
        # Read CSV
        with open(output_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        row = rows[0]
        
        # Verify event counts show all events
        assert row["event_count"] == "15"
        assert row["active_event_count"] == "15"
        
        # Verify event names are truncated to 10
        event_names = row["event_names"].split(";")
        assert len(event_names) == 10
        
        # Verify all event IDs are included (not truncated)
        event_ids = row["active_event_ids"].split(";")
        assert len(event_ids) == 15


def test_export_locations_without_coordinates(temp_store, sample_provider):
    """Test location export without geocoded coordinates."""
    # Create location without coordinates
    location = Location(
        location_id="location-test-provider-no-coords",
        provider_id="provider-test-provider",
        provider_name="Test Provider",
        formatted_address="Unknown Address",
        latitude=None,
        longitude=None,
        geocode_status="not_geocoded",
        status="active",
        first_seen_at=datetime(2025, 1, 15, 10, 0, 0),
        last_seen_at=datetime(2025, 1, 20, 14, 0, 0)
    )
    
    temp_store.save_providers([sample_provider])
    temp_store.save_locations([location])
    
    exporter = CSVExporter(temp_store)
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "locations.csv"
        result = exporter.export_locations(str(output_path))
        
        # Verify statistics
        assert result["total_records"] == 1
        assert result["with_coordinates"] == 0
        
        # Read CSV
        with open(output_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        row = rows[0]
        
        # Verify coordinates are empty
        assert row["latitude"] == ""
        assert row["longitude"] == ""
        assert row["geocode_status"] == "not_geocoded"


def test_export_locations_filters_by_status(temp_store, sample_location, sample_provider):
    """Test that export filters by status correctly."""
    temp_store.save_providers([sample_provider])
    
    # Create inactive location
    inactive_location = Location(
        location_id="location-test-provider-inactive",
        provider_id="provider-test-provider",
        provider_name="Test Provider",
        formatted_address="Inactive Address",
        status="inactive",
        first_seen_at=datetime(2025, 1, 15, 10, 0, 0),
        last_seen_at=datetime(2025, 1, 20, 14, 0, 0)
    )
    
    temp_store.save_locations([sample_location, inactive_location])
    
    exporter = CSVExporter(temp_store)
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "locations.csv"
        
        # Export with default filter (active only)
        result = exporter.export_locations(str(output_path))
        assert result["total_records"] == 1
        
        # Export with no filter
        result = exporter.export_locations(str(output_path), filters={})
        assert result["total_records"] == 2


def test_export_locations_without_events(temp_store, sample_location, sample_provider):
    """Test location export when location has no events."""
    temp_store.save_providers([sample_provider])
    temp_store.save_locations([sample_location])
    # Don't save any events
    
    exporter = CSVExporter(temp_store)
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "locations.csv"
        exporter.export_locations(str(output_path))
        
        # Read CSV
        with open(output_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        row = rows[0]
        
        # Verify event counts are zero
        assert row["event_count"] == "0"
        assert row["active_event_count"] == "0"
        assert row["event_names"] == ""
        assert row["active_event_ids"] == ""


def test_export_locations_creates_parent_directory(temp_store, sample_location, sample_provider):
    """Test that export creates parent directories if they don't exist."""
    temp_store.save_providers([sample_provider])
    temp_store.save_locations([sample_location])
    
    exporter = CSVExporter(temp_store)
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "nested" / "dir" / "locations.csv"
        exporter.export_locations(str(output_path))
        
        # Verify file exists
        assert output_path.exists()
        assert output_path.parent.exists()


def test_export_locations_handles_null_values(temp_store, sample_provider):
    """Test that null values are formatted as empty strings."""
    # Create minimal location with many null fields
    minimal_location = Location(
        location_id="location-minimal",
        provider_id="provider-test",
        provider_name="Test Provider",
        formatted_address="Minimal Address",
        location_name=None,
        address_line_1=None,
        address_line_2=None,
        city=None,
        region=None,
        postcode=None,
        country="UK",
        latitude=None,
        longitude=None,
        geocode_status="not_geocoded",
        geocode_precision=None,
        geocode_provider=None,
        geocoded_at=None,
        venue_phone=None,
        venue_email=None,
        venue_website=None,
        status="active",
        first_seen_at=datetime(2025, 1, 15, 10, 0, 0),
        last_seen_at=datetime(2025, 1, 20, 14, 0, 0)
    )
    
    temp_store.save_providers([sample_provider])
    temp_store.save_locations([minimal_location])
    
    exporter = CSVExporter(temp_store)
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "locations.csv"
        exporter.export_locations(str(output_path))
        
        # Read CSV
        with open(output_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        row = rows[0]
        
        # Verify null fields are empty strings
        assert row["location_name"] == ""
        assert row["address_line_1"] == ""
        assert row["address_line_2"] == ""
        assert row["city"] == ""
        assert row["region"] == ""
        assert row["postcode"] == ""
        assert row["latitude"] == ""
        assert row["longitude"] == ""
        assert row["geocode_precision"] == ""
        assert row["geocode_provider"] == ""
        assert row["geocoded_at"] == ""
        assert row["venue_phone"] == ""
        assert row["venue_email"] == ""
        assert row["venue_website"] == ""



def test_validate_export_success(temp_store, sample_template, sample_occurrence, sample_location, sample_provider):
    """Test that validate_export passes when all records are present."""
    # Save data to store
    temp_store.save_providers([sample_provider])
    temp_store.save_events([sample_template, sample_occurrence])
    temp_store.save_locations([sample_location])

    # Export
    exporter = CSVExporter(temp_store)
    with tempfile.TemporaryDirectory() as tmpdir:
        events_path = Path(tmpdir) / "events.csv"
        locations_path = Path(tmpdir) / "locations.csv"

        exporter.export_events(str(events_path))
        exporter.export_locations(str(locations_path))

        # Validate
        result = exporter.validate_export(str(events_path), str(locations_path))

        # Verify validation passed
        assert result["valid"] is True
        assert len(result["errors"]) == 0

        # Verify events validation
        assert result["events_validation"]["parseable"] is True
        assert result["events_validation"]["active_records_count"] == 2
        assert result["events_validation"]["csv_records_count"] == 2
        assert len(result["events_validation"]["missing_records"]) == 0
        assert len(result["events_validation"]["duplicate_records"]) == 0

        # Verify locations validation
        assert result["locations_validation"]["parseable"] is True
        assert result["locations_validation"]["active_records_count"] == 1
        assert result["locations_validation"]["csv_records_count"] == 1
        assert len(result["locations_validation"]["missing_records"]) == 0
        assert len(result["locations_validation"]["duplicate_records"]) == 0


def test_validate_export_missing_events_file(temp_store, sample_location, sample_provider):
    """Test validation when events CSV file is missing."""
    temp_store.save_providers([sample_provider])
    temp_store.save_locations([sample_location])

    exporter = CSVExporter(temp_store)
    with tempfile.TemporaryDirectory() as tmpdir:
        events_path = Path(tmpdir) / "events.csv"
        locations_path = Path(tmpdir) / "locations.csv"

        # Only export locations
        exporter.export_locations(str(locations_path))

        # Validate (events file missing)
        result = exporter.validate_export(str(events_path), str(locations_path))

        # Verify validation failed
        assert result["valid"] is False
        assert len(result["errors"]) > 0
        assert any("Events CSV file not found" in err for err in result["errors"])
        assert result["events_validation"]["parseable"] is False


def test_validate_export_missing_locations_file(temp_store, sample_template, sample_provider):
    """Test validation when locations CSV file is missing."""
    temp_store.save_providers([sample_provider])
    temp_store.save_events([sample_template])

    exporter = CSVExporter(temp_store)
    with tempfile.TemporaryDirectory() as tmpdir:
        events_path = Path(tmpdir) / "events.csv"
        locations_path = Path(tmpdir) / "locations.csv"

        # Only export events
        exporter.export_events(str(events_path))

        # Validate (locations file missing)
        result = exporter.validate_export(str(events_path), str(locations_path))

        # Verify validation failed
        assert result["valid"] is False
        assert len(result["errors"]) > 0
        assert any("Locations CSV file not found" in err for err in result["errors"])
        assert result["locations_validation"]["parseable"] is False


def test_validate_export_missing_records(temp_store, sample_template, sample_occurrence, sample_location, sample_provider):
    """Test validation when CSV is missing some active records."""
    # Save data to store
    temp_store.save_providers([sample_provider])
    temp_store.save_events([sample_template, sample_occurrence])
    temp_store.save_locations([sample_location])

    exporter = CSVExporter(temp_store)
    with tempfile.TemporaryDirectory() as tmpdir:
        events_path = Path(tmpdir) / "events.csv"
        locations_path = Path(tmpdir) / "locations.csv"

        # Export
        exporter.export_events(str(events_path))
        exporter.export_locations(str(locations_path))

        # Manually remove a row from events CSV
        with open(events_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        # Keep only first row (remove second)
        with open(events_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerow(rows[0])

        # Validate
        result = exporter.validate_export(str(events_path), str(locations_path))

        # Verify validation failed
        assert result["valid"] is False
        assert len(result["errors"]) > 0
        assert any("missing" in err.lower() for err in result["errors"])

        # Verify missing records detected
        assert result["events_validation"]["active_records_count"] == 2
        assert result["events_validation"]["csv_records_count"] == 1
        assert len(result["events_validation"]["missing_records"]) == 1


def test_validate_export_duplicate_records(temp_store, sample_template, sample_location, sample_provider):
    """Test validation when CSV contains duplicate records."""
    # Save data to store
    temp_store.save_providers([sample_provider])
    temp_store.save_events([sample_template])
    temp_store.save_locations([sample_location])

    exporter = CSVExporter(temp_store)
    with tempfile.TemporaryDirectory() as tmpdir:
        events_path = Path(tmpdir) / "events.csv"
        locations_path = Path(tmpdir) / "locations.csv"

        # Export
        exporter.export_events(str(events_path))
        exporter.export_locations(str(locations_path))

        # Manually duplicate a row in events CSV
        with open(events_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        # Write rows with duplicate
        with open(events_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerow(rows[0])
            writer.writerow(rows[0])  # Duplicate

        # Validate
        result = exporter.validate_export(str(events_path), str(locations_path))

        # Verify validation failed
        assert result["valid"] is False
        assert len(result["errors"]) > 0
        assert any("duplicate" in err.lower() for err in result["errors"])

        # Verify duplicate records detected
        assert len(result["events_validation"]["duplicate_records"]) == 1
        assert result["events_validation"]["duplicate_records"][0] == sample_template.event_template_id


def test_validate_export_invalid_csv_format(temp_store, sample_location, sample_provider):
    """Test validation when CSV has invalid format."""
    # Save data to store
    temp_store.save_providers([sample_provider])
    temp_store.save_locations([sample_location])

    exporter = CSVExporter(temp_store)
    with tempfile.TemporaryDirectory() as tmpdir:
        events_path = Path(tmpdir) / "events.csv"
        locations_path = Path(tmpdir) / "locations.csv"

        # Create valid locations CSV
        exporter.export_locations(str(locations_path))

        # Create invalid events CSV (malformed)
        with open(events_path, 'w', encoding='utf-8') as f:
            f.write("invalid,csv,format\n")
            f.write("missing,quotes,\"and\n")
            f.write("broken,lines\n")

        # Validate
        result = exporter.validate_export(str(events_path), str(locations_path))

        # Verify validation failed
        assert result["valid"] is False
        assert len(result["errors"]) > 0

        # Should detect missing record_id column
        assert any("missing required 'record_id' column" in err.lower() for err in result["errors"])


def test_validate_export_empty_csv(temp_store, sample_template, sample_location, sample_provider):
    """Test validation when CSV is empty but store has records."""
    # Save data to store
    temp_store.save_providers([sample_provider])
    temp_store.save_events([sample_template])
    temp_store.save_locations([sample_location])

    exporter = CSVExporter(temp_store)
    with tempfile.TemporaryDirectory() as tmpdir:
        events_path = Path(tmpdir) / "events.csv"
        locations_path = Path(tmpdir) / "locations.csv"

        # Create empty CSVs with headers only
        with open(events_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['record_id', 'title'])
            writer.writeheader()

        with open(locations_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['location_id', 'formatted_address'])
            writer.writeheader()

        # Validate
        result = exporter.validate_export(str(events_path), str(locations_path))

        # Verify validation failed
        assert result["valid"] is False
        assert len(result["errors"]) > 0

        # Verify missing records detected
        assert result["events_validation"]["active_records_count"] == 1
        assert result["events_validation"]["csv_records_count"] == 0
        assert len(result["events_validation"]["missing_records"]) == 1

        assert result["locations_validation"]["active_records_count"] == 1
        assert result["locations_validation"]["csv_records_count"] == 0
        assert len(result["locations_validation"]["missing_records"]) == 1


def test_validate_export_no_active_records(temp_store, sample_provider):
    """Test validation when store has no active records."""
    # Save only provider (no events or locations)
    temp_store.save_providers([sample_provider])

    exporter = CSVExporter(temp_store)
    with tempfile.TemporaryDirectory() as tmpdir:
        events_path = Path(tmpdir) / "events.csv"
        locations_path = Path(tmpdir) / "locations.csv"

        # Export (should create empty CSVs)
        exporter.export_events(str(events_path))
        exporter.export_locations(str(locations_path))

        # Validate
        result = exporter.validate_export(str(events_path), str(locations_path))

        # Verify validation passed (no records to validate)
        assert result["valid"] is True
        assert len(result["errors"]) == 0
        assert result["events_validation"]["active_records_count"] == 0
        assert result["events_validation"]["csv_records_count"] == 0
        assert result["locations_validation"]["active_records_count"] == 0
        assert result["locations_validation"]["csv_records_count"] == 0


def test_export_locations_with_complete_geocoding_data(temp_store, sample_provider):
    """Test that locations with complete geocoding data export all fields correctly."""
    # Create location with complete geocoding data
    location = Location(
        location_id="location-test-provider-geocoded",
        provider_id="provider-test-provider",
        provider_name="Test Provider",
        formatted_address="123 Test Street, London, EC1A 1AA",
        location_name="Geocoded Venue",
        address_line_1="123 Test Street",
        city="London",
        postcode="EC1A 1AA",
        country="UK",
        latitude=51.5074,
        longitude=-0.1278,
        geocode_status="success",
        geocode_precision="rooftop",
        geocode_provider="mapbox",
        geocoded_at=datetime(2025, 1, 15, 11, 0, 0),
        status="active",
        first_seen_at=datetime(2025, 1, 15, 10, 0, 0),
        last_seen_at=datetime(2025, 1, 20, 14, 0, 0)
    )
    
    temp_store.save_providers([sample_provider])
    temp_store.save_locations([location])
    
    exporter = CSVExporter(temp_store)
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "locations.csv"
        result = exporter.export_locations(str(output_path))
        
        # Verify statistics show location has coordinates
        assert result["total_records"] == 1
        assert result["with_coordinates"] == 1
        
        # Read CSV
        with open(output_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        assert len(rows) == 1
        row = rows[0]
        
        # Verify all geocoding fields are exported correctly
        assert row["latitude"] == "51.5074"
        assert row["longitude"] == "-0.1278"
        assert row["geocode_status"] == "success"
        assert row["geocode_precision"] == "rooftop"
        assert row["geocode_provider"] == "mapbox"
        assert row["geocoded_at"] == "2025-01-15T11:00:00Z"


def test_export_locations_with_failed_geocoding(temp_store, sample_provider):
    """Test that locations with failed geocoding export status correctly."""
    # Create location with failed geocoding
    location = Location(
        location_id="location-test-provider-failed",
        provider_id="provider-test-provider",
        provider_name="Test Provider",
        formatted_address="Invalid Address XYZ",
        location_name="Failed Geocode Venue",
        latitude=None,
        longitude=None,
        geocode_status="failed",
        geocode_precision=None,
        geocode_provider="mapbox",
        geocoded_at=datetime(2025, 1, 15, 11, 0, 0),
        status="active",
        first_seen_at=datetime(2025, 1, 15, 10, 0, 0),
        last_seen_at=datetime(2025, 1, 20, 14, 0, 0)
    )
    
    temp_store.save_providers([sample_provider])
    temp_store.save_locations([location])
    
    exporter = CSVExporter(temp_store)
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "locations.csv"
        result = exporter.export_locations(str(output_path))
        
        # Verify statistics show no coordinates
        assert result["total_records"] == 1
        assert result["with_coordinates"] == 0
        
        # Read CSV
        with open(output_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        row = rows[0]
        
        # Verify geocoding fields show failed status
        assert row["latitude"] == ""
        assert row["longitude"] == ""
        assert row["geocode_status"] == "failed"
        assert row["geocode_precision"] == ""
        assert row["geocode_provider"] == "mapbox"
        assert row["geocoded_at"] == "2025-01-15T11:00:00Z"


def test_export_locations_with_not_geocoded_status(temp_store, sample_provider):
    """Test that locations not yet geocoded export with not_geocoded status."""
    # Create location that hasn't been geocoded yet
    location = Location(
        location_id="location-test-provider-not-geocoded",
        provider_id="provider-test-provider",
        provider_name="Test Provider",
        formatted_address="123 New Street, London",
        location_name="Not Yet Geocoded",
        latitude=None,
        longitude=None,
        geocode_status="not_geocoded",
        geocode_precision=None,
        geocode_provider=None,
        geocoded_at=None,
        status="active",
        first_seen_at=datetime(2025, 1, 15, 10, 0, 0),
        last_seen_at=datetime(2025, 1, 20, 14, 0, 0)
    )
    
    temp_store.save_providers([sample_provider])
    temp_store.save_locations([location])
    
    exporter = CSVExporter(temp_store)
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "locations.csv"
        result = exporter.export_locations(str(output_path))
        
        # Verify statistics
        assert result["total_records"] == 1
        assert result["with_coordinates"] == 0
        
        # Read CSV
        with open(output_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        row = rows[0]
        
        # Verify all geocoding fields are empty except status
        assert row["latitude"] == ""
        assert row["longitude"] == ""
        assert row["geocode_status"] == "not_geocoded"
        assert row["geocode_precision"] == ""
        assert row["geocode_provider"] == ""
        assert row["geocoded_at"] == ""

