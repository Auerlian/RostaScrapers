"""Unit tests for CanonicalStore."""

import json
import pytest
from datetime import datetime
from pathlib import Path
import tempfile
import shutil

from src.storage.store import CanonicalStore
from src.models.provider import Provider
from src.models.location import Location
from src.models.event_template import EventTemplate
from src.models.event_occurrence import EventOccurrence


@pytest.fixture
def temp_store():
    """Create a temporary store for testing."""
    temp_dir = tempfile.mkdtemp()
    store = CanonicalStore(base_path=temp_dir)
    yield store
    # Cleanup both temp_dir and archive
    shutil.rmtree(temp_dir, ignore_errors=True)
    archive_dir = Path(temp_dir).parent / "archive"
    if archive_dir.exists():
        shutil.rmtree(archive_dir, ignore_errors=True)


@pytest.fixture
def sample_provider():
    """Create a sample provider for testing."""
    return Provider(
        provider_id="provider-test",
        provider_name="Test Provider",
        provider_slug="test-provider",
        source_name="Test Source",
        source_base_url="https://test.com",
        provider_website="https://test.com",
        status="active",
        first_seen_at=datetime(2024, 1, 1, 12, 0, 0),
        last_seen_at=datetime(2024, 1, 2, 12, 0, 0),
        metadata={"key": "value"}
    )


@pytest.fixture
def sample_location():
    """Create a sample location for testing."""
    return Location(
        location_id="location-test-123",
        provider_id="provider-test",
        provider_name="Test Provider",
        location_name="Test Venue",
        formatted_address="123 Test St, London, UK",
        address_line_1="123 Test St",
        city="London",
        country="UK",
        postcode="SW1A 1AA",
        latitude=51.5074,
        longitude=-0.1278,
        geocode_status="success",
        status="active",
        first_seen_at=datetime(2024, 1, 1, 12, 0, 0),
        last_seen_at=datetime(2024, 1, 2, 12, 0, 0)
    )


@pytest.fixture
def sample_event_template():
    """Create a sample event template for testing."""
    return EventTemplate(
        event_template_id="event-template-test-123",
        provider_id="provider-test",
        title="Test Event",
        slug="test-event",
        description_raw="Test description",
        tags=["test", "sample"],
        price_from=50.0,
        currency="GBP",
        status="active",
        first_seen_at=datetime(2024, 1, 1, 12, 0, 0),
        last_seen_at=datetime(2024, 1, 2, 12, 0, 0)
    )


@pytest.fixture
def sample_event_occurrence():
    """Create a sample event occurrence for testing."""
    return EventOccurrence(
        event_id="event-occurrence-test-123",
        provider_id="provider-test",
        title="Test Event Session",
        start_at=datetime(2024, 6, 1, 14, 0, 0),
        end_at=datetime(2024, 6, 1, 16, 0, 0),
        price=50.0,
        currency="GBP",
        availability_status="available",
        status="active",
        first_seen_at=datetime(2024, 1, 1, 12, 0, 0),
        last_seen_at=datetime(2024, 1, 2, 12, 0, 0)
    )


class TestCanonicalStoreProviders:
    """Tests for provider save/load operations."""
    
    def test_save_and_load_providers(self, temp_store, sample_provider):
        """Test saving and loading providers."""
        # Save
        temp_store.save_providers([sample_provider])
        
        # Verify file exists and is dict-keyed
        assert temp_store.providers_file.exists()
        with open(temp_store.providers_file) as f:
            data = json.load(f)
            assert isinstance(data, dict)
            assert "provider-test" in data
        
        # Load
        loaded = temp_store.load_providers()
        assert len(loaded) == 1
        assert loaded[0].provider_id == sample_provider.provider_id
        assert loaded[0].provider_name == sample_provider.provider_name
        assert loaded[0].first_seen_at == sample_provider.first_seen_at
    
    def test_save_multiple_providers(self, temp_store):
        """Test saving multiple providers."""
        providers = [
            Provider(
                provider_id=f"provider-{i}",
                provider_name=f"Provider {i}",
                provider_slug=f"provider-{i}",
                source_name="Test",
                source_base_url="https://test.com"
            )
            for i in range(3)
        ]
        
        temp_store.save_providers(providers)
        loaded = temp_store.load_providers()
        
        assert len(loaded) == 3
        assert {p.provider_id for p in loaded} == {f"provider-{i}" for i in range(3)}
    
    def test_load_providers_empty_file(self, temp_store):
        """Test loading providers when file doesn't exist."""
        loaded = temp_store.load_providers()
        assert loaded == []


class TestCanonicalStoreLocations:
    """Tests for location save/load operations."""
    
    def test_save_and_load_locations(self, temp_store, sample_location):
        """Test saving and loading locations."""
        # Save
        temp_store.save_locations([sample_location])
        
        # Verify file exists and is dict-keyed
        assert temp_store.locations_file.exists()
        with open(temp_store.locations_file) as f:
            data = json.load(f)
            assert isinstance(data, dict)
            assert "location-test-123" in data
        
        # Load
        loaded = temp_store.load_locations()
        assert len(loaded) == 1
        assert loaded[0].location_id == sample_location.location_id
        assert loaded[0].latitude == sample_location.latitude
        assert loaded[0].longitude == sample_location.longitude
    
    def test_load_locations_with_status_filter(self, temp_store):
        """Test loading locations with status filter."""
        locations = [
            Location(
                location_id="loc-1",
                provider_id="provider-test",
                provider_name="Test",
                formatted_address="Address 1",
                status="active"
            ),
            Location(
                location_id="loc-2",
                provider_id="provider-test",
                provider_name="Test",
                formatted_address="Address 2",
                status="removed"
            )
        ]
        
        temp_store.save_locations(locations)
        
        # Filter by active status
        active = temp_store.load_locations(filters={"status": "active"})
        assert len(active) == 1
        assert active[0].location_id == "loc-1"
    
    def test_load_locations_with_provider_filter(self, temp_store):
        """Test loading locations with provider filter."""
        locations = [
            Location(
                location_id="loc-1",
                provider_id="provider-a",
                provider_name="Provider A",
                formatted_address="Address 1"
            ),
            Location(
                location_id="loc-2",
                provider_id="provider-b",
                provider_name="Provider B",
                formatted_address="Address 2"
            )
        ]
        
        temp_store.save_locations(locations)
        
        # Filter by provider
        filtered = temp_store.load_locations(filters={"provider_id": "provider-a"})
        assert len(filtered) == 1
        assert filtered[0].location_id == "loc-1"


class TestCanonicalStoreEvents:
    """Tests for event save/load operations."""
    
    def test_save_and_load_event_templates(self, temp_store, sample_event_template):
        """Test saving and loading event templates."""
        # Save
        temp_store.save_events([sample_event_template])
        
        # Verify file exists and is dict-keyed
        assert temp_store.event_templates_file.exists()
        with open(temp_store.event_templates_file) as f:
            data = json.load(f)
            assert isinstance(data, dict)
            assert "event-template-test-123" in data
        
        # Load
        loaded = temp_store.load_events()
        assert len(loaded) == 1
        assert isinstance(loaded[0], EventTemplate)
        assert loaded[0].event_template_id == sample_event_template.event_template_id
    
    def test_save_and_load_event_occurrences(self, temp_store, sample_event_occurrence):
        """Test saving and loading event occurrences."""
        # Save
        temp_store.save_events([sample_event_occurrence])
        
        # Verify file exists and is dict-keyed
        assert temp_store.event_occurrences_file.exists()
        with open(temp_store.event_occurrences_file) as f:
            data = json.load(f)
            assert isinstance(data, dict)
            assert "event-occurrence-test-123" in data
        
        # Load
        loaded = temp_store.load_events()
        assert len(loaded) == 1
        assert isinstance(loaded[0], EventOccurrence)
        assert loaded[0].event_id == sample_event_occurrence.event_id
    
    def test_save_mixed_events(self, temp_store, sample_event_template, sample_event_occurrence):
        """Test saving both templates and occurrences together."""
        temp_store.save_events([sample_event_template, sample_event_occurrence])
        
        # Both files should exist
        assert temp_store.event_templates_file.exists()
        assert temp_store.event_occurrences_file.exists()
        
        # Load should return both
        loaded = temp_store.load_events()
        assert len(loaded) == 2
        
        templates = [e for e in loaded if isinstance(e, EventTemplate)]
        occurrences = [e for e in loaded if isinstance(e, EventOccurrence)]
        
        assert len(templates) == 1
        assert len(occurrences) == 1
    
    def test_load_events_with_status_filter(self, temp_store):
        """Test loading events with status filter."""
        events = [
            EventTemplate(
                event_template_id="template-1",
                provider_id="provider-test",
                title="Active Event",
                slug="active-event",
                status="active"
            ),
            EventTemplate(
                event_template_id="template-2",
                provider_id="provider-test",
                title="Removed Event",
                slug="removed-event",
                status="removed"
            )
        ]
        
        temp_store.save_events(events)
        
        # Filter by active status
        active = temp_store.load_events(filters={"status": "active"})
        assert len(active) == 1
        assert active[0].event_template_id == "template-1"
    
    def test_load_events_with_date_filter(self, temp_store):
        """Test loading events with date range filter."""
        occurrences = [
            EventOccurrence(
                event_id="occ-1",
                provider_id="provider-test",
                title="Early Event",
                start_at=datetime(2024, 1, 1, 10, 0, 0)
            ),
            EventOccurrence(
                event_id="occ-2",
                provider_id="provider-test",
                title="Late Event",
                start_at=datetime(2024, 12, 31, 10, 0, 0)
            )
        ]
        
        temp_store.save_events(occurrences)
        
        # Filter by date range (end_date is inclusive up to end of day)
        filtered = temp_store.load_events(filters={
            "start_date": datetime(2024, 6, 1),
            "end_date": datetime(2024, 12, 31, 23, 59, 59)
        })
        
        assert len(filtered) == 1
        assert filtered[0].event_id == "occ-2"


class TestCanonicalStoreAtomicWrites:
    """Tests for atomic write operations."""
    
    def test_atomic_write_creates_temp_file(self, temp_store, sample_provider):
        """Test that atomic write uses temp file."""
        temp_store.save_providers([sample_provider])
        
        # Temp file should not exist after successful write
        temp_file = temp_store.providers_file.with_suffix('.tmp')
        assert not temp_file.exists()
        
        # Actual file should exist
        assert temp_store.providers_file.exists()
    
    def test_overwrite_existing_data(self, temp_store, sample_provider):
        """Test that saving overwrites existing data."""
        # Save initial data
        temp_store.save_providers([sample_provider])
        
        # Save new data
        new_provider = Provider(
            provider_id="provider-new",
            provider_name="New Provider",
            provider_slug="new-provider",
            source_name="New Source",
            source_base_url="https://new.com"
        )
        temp_store.save_providers([new_provider])
        
        # Load should return only new data
        loaded = temp_store.load_providers()
        assert len(loaded) == 1
        assert loaded[0].provider_id == "provider-new"


class TestCanonicalStoreErrorHandling:
    """Tests for error handling and graceful degradation."""
    
    def test_load_missing_file_returns_empty(self, temp_store):
        """Test that loading missing file returns empty list."""
        loaded = temp_store.load_providers()
        assert loaded == []
        
        loaded = temp_store.load_locations()
        assert loaded == []
        
        loaded = temp_store.load_events()
        assert loaded == []
    
    def test_load_corrupted_json_returns_empty(self, temp_store):
        """Test that loading corrupted JSON returns empty list."""
        # Write invalid JSON
        temp_store.providers_file.write_text("{ invalid json }")
        
        # Should return empty list, not raise exception
        loaded = temp_store.load_providers()
        assert loaded == []
    
    def test_load_non_dict_json_returns_empty(self, temp_store):
        """Test that loading non-dict JSON returns empty list."""
        # Write array instead of dict
        temp_store.providers_file.write_text('[{"key": "value"}]')
        
        # Should return empty list
        loaded = temp_store.load_providers()
        assert loaded == []


class TestCanonicalStoreArchive:
    """Tests for archive snapshot functionality."""
    
    def test_archive_snapshot(self, temp_store, sample_provider, sample_location):
        """Test creating archive snapshot."""
        # Save some data
        temp_store.save_providers([sample_provider])
        temp_store.save_locations([sample_location])
        
        # Create archive
        timestamp = "2024-01-01T12-00-00"
        temp_store.archive_snapshot(timestamp)
        
        # Verify archive directory and files
        archive_dir = Path(temp_store.base_path).parent / "archive" / timestamp
        assert archive_dir.exists()
        assert (archive_dir / "providers.json").exists()
        assert (archive_dir / "locations.json").exists()
    
    def test_archive_with_missing_files(self, temp_store):
        """Test archiving when some files don't exist."""
        # Only save providers (not locations or events)
        provider = Provider(
            provider_id="provider-test",
            provider_name="Test",
            provider_slug="test",
            source_name="Test",
            source_base_url="https://test.com"
        )
        temp_store.save_providers([provider])
        
        # Archive should not fail even with missing files
        timestamp = "2024-01-01T12-00-00"
        temp_store.archive_snapshot(timestamp)
        
        # Only providers.json should be archived (locations and events not saved)
        archive_dir = Path(temp_store.base_path).parent / "archive" / timestamp
        assert (archive_dir / "providers.json").exists()
        # These files were never created, so they shouldn't be in archive
        assert not (archive_dir / "locations.json").exists()
        assert not (archive_dir / "event_templates.json").exists()
        assert not (archive_dir / "event_occurrences.json").exists()


class TestCanonicalStoreDataIntegrity:
    """Tests for data integrity and serialization."""
    
    def test_datetime_serialization(self, temp_store):
        """Test that datetime fields are properly serialized and deserialized."""
        provider = Provider(
            provider_id="provider-test",
            provider_name="Test",
            provider_slug="test",
            source_name="Test",
            source_base_url="https://test.com",
            first_seen_at=datetime(2024, 1, 1, 12, 30, 45),
            last_seen_at=datetime(2024, 1, 2, 14, 15, 30)
        )
        
        temp_store.save_providers([provider])
        loaded = temp_store.load_providers()
        
        assert loaded[0].first_seen_at == provider.first_seen_at
        assert loaded[0].last_seen_at == provider.last_seen_at
    
    def test_list_field_serialization(self, temp_store):
        """Test that list fields are properly serialized."""
        template = EventTemplate(
            event_template_id="template-test",
            provider_id="provider-test",
            title="Test",
            slug="test",
            tags=["tag1", "tag2", "tag3"],
            skills_required=["skill1", "skill2"],
            image_urls=["https://example.com/img1.jpg"]
        )
        
        temp_store.save_events([template])
        loaded = temp_store.load_events()
        
        assert loaded[0].tags == template.tags
        assert loaded[0].skills_required == template.skills_required
        assert loaded[0].image_urls == template.image_urls
    
    def test_null_field_handling(self, temp_store):
        """Test that null/None fields are handled correctly."""
        location = Location(
            location_id="loc-test",
            provider_id="provider-test",
            provider_name="Test",
            formatted_address="Test Address",
            location_name=None,
            latitude=None,
            longitude=None,
            geocoded_at=None
        )
        
        temp_store.save_locations([location])
        loaded = temp_store.load_locations()
        
        assert loaded[0].location_name is None
        assert loaded[0].latitude is None
        assert loaded[0].longitude is None
        assert loaded[0].geocoded_at is None
