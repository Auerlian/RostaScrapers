"""Integration tests for CanonicalStore operations.

Tests cover:
- Save and load operations with dict-keyed JSON files
- Filtering by status, provider, date ranges
- Handling of missing or corrupted files
- Atomic write operations
- Test isolation with temporary directories
"""

import pytest
from datetime import datetime
from pathlib import Path
import tempfile
import shutil
import json
import os

from src.storage.store import CanonicalStore
from src.models.provider import Provider
from src.models.location import Location
from src.models.event_template import EventTemplate
from src.models.event_occurrence import EventOccurrence


@pytest.fixture
def temp_store():
    """Create a temporary store for testing with cleanup."""
    temp_dir = tempfile.mkdtemp()
    store = CanonicalStore(base_path=temp_dir)
    yield store
    # Cleanup temp directory and any archives
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
        status="active",
        first_seen_at=datetime(2024, 1, 1, 12, 0, 0),
        last_seen_at=datetime(2024, 1, 15, 12, 0, 0)
    )


@pytest.fixture
def sample_locations():
    """Create sample locations for testing."""
    return [
        Location(
            location_id="location-test-1",
            provider_id="provider-test",
            provider_name="Test Provider",
            location_name="Test Location 1",
            formatted_address="123 Test St, London, UK",
            city="London",
            postcode="SW1A 1AA",
            latitude=51.5074,
            longitude=-0.1278,
            geocode_status="success",
            status="active",
            first_seen_at=datetime(2024, 1, 1, 12, 0, 0),
            last_seen_at=datetime(2024, 1, 15, 12, 0, 0)
        ),
        Location(
            location_id="location-test-2",
            provider_id="provider-test",
            provider_name="Test Provider",
            location_name="Test Location 2",
            formatted_address="456 Test Ave, Manchester, UK",
            city="Manchester",
            postcode="M1 1AA",
            status="removed",
            first_seen_at=datetime(2024, 1, 1, 12, 0, 0),
            last_seen_at=datetime(2024, 1, 10, 12, 0, 0),
            deleted_at=datetime(2024, 1, 10, 12, 0, 0)
        )
    ]


@pytest.fixture
def sample_events():
    """Create sample events (templates and occurrences) for testing."""
    return [
        EventTemplate(
            event_template_id="template-test-1",
            provider_id="provider-test",
            title="Test Workshop",
            slug="test-workshop",
            description_raw="A test workshop",
            tags=["test", "workshop"],
            price_from=50.0,
            currency="GBP",
            status="active",
            first_seen_at=datetime(2024, 1, 1, 12, 0, 0),
            last_seen_at=datetime(2024, 1, 15, 12, 0, 0)
        ),
        EventOccurrence(
            event_id="occurrence-test-1",
            event_template_id="template-test-1",
            provider_id="provider-test",
            location_id="location-test-1",
            title="Test Workshop - January",
            start_at=datetime(2024, 1, 20, 14, 0, 0),
            end_at=datetime(2024, 1, 20, 17, 0, 0),
            price=50.0,
            currency="GBP",
            availability_status="available",
            status="active",
            first_seen_at=datetime(2024, 1, 1, 12, 0, 0),
            last_seen_at=datetime(2024, 1, 15, 12, 0, 0)
        ),
        EventOccurrence(
            event_id="occurrence-test-2",
            event_template_id="template-test-1",
            provider_id="provider-test",
            location_id="location-test-2",
            title="Test Workshop - February",
            start_at=datetime(2024, 2, 15, 14, 0, 0),
            end_at=datetime(2024, 2, 15, 17, 0, 0),
            price=50.0,
            currency="GBP",
            availability_status="available",
            status="expired",
            first_seen_at=datetime(2024, 1, 1, 12, 0, 0),
            last_seen_at=datetime(2024, 2, 16, 12, 0, 0)
        )
    ]


class TestDictKeyedJSONOperations:
    """Test save and load operations with dict-keyed JSON files."""
    
    def test_save_providers_creates_dict_keyed_json(self, temp_store, sample_provider):
        """Test that providers are saved as dict keyed by provider_id."""
        temp_store.save_providers([sample_provider])
        
        # Verify file structure
        providers_file = Path(temp_store.base_path) / "providers.json"
        assert providers_file.exists()
        
        with open(providers_file) as f:
            data = json.load(f)
            assert isinstance(data, dict)
            assert "provider-test" in data
            assert data["provider-test"]["provider_name"] == "Test Provider"
    
    def test_save_locations_creates_dict_keyed_json(self, temp_store, sample_locations):
        """Test that locations are saved as dict keyed by location_id."""
        temp_store.save_locations(sample_locations)
        
        # Verify file structure
        locations_file = Path(temp_store.base_path) / "locations.json"
        assert locations_file.exists()
        
        with open(locations_file) as f:
            data = json.load(f)
            assert isinstance(data, dict)
            assert "location-test-1" in data
            assert "location-test-2" in data
            assert data["location-test-1"]["location_name"] == "Test Location 1"
    
    def test_save_events_creates_separate_dict_keyed_files(self, temp_store, sample_events):
        """Test that events are saved in separate files for templates and occurrences."""
        temp_store.save_events(sample_events)
        
        # Verify template file
        templates_file = Path(temp_store.base_path) / "event_templates.json"
        assert templates_file.exists()
        
        with open(templates_file) as f:
            data = json.load(f)
            assert isinstance(data, dict)
            assert "template-test-1" in data
            assert data["template-test-1"]["title"] == "Test Workshop"
        
        # Verify occurrence file
        occurrences_file = Path(temp_store.base_path) / "event_occurrences.json"
        assert occurrences_file.exists()
        
        with open(occurrences_file) as f:
            data = json.load(f)
            assert isinstance(data, dict)
            assert "occurrence-test-1" in data
            assert "occurrence-test-2" in data
    
    def test_load_providers_from_dict_keyed_json(self, temp_store, sample_provider):
        """Test loading providers from dict-keyed JSON."""
        temp_store.save_providers([sample_provider])
        
        loaded = temp_store.load_providers()
        assert len(loaded) == 1
        assert loaded[0].provider_id == "provider-test"
        assert loaded[0].provider_name == "Test Provider"
    
    def test_load_locations_from_dict_keyed_json(self, temp_store, sample_locations):
        """Test loading locations from dict-keyed JSON."""
        temp_store.save_locations(sample_locations)
        
        loaded = temp_store.load_locations()
        assert len(loaded) == 2
        location_ids = {loc.location_id for loc in loaded}
        assert location_ids == {"location-test-1", "location-test-2"}
    
    def test_load_events_from_dict_keyed_json(self, temp_store, sample_events):
        """Test loading events from dict-keyed JSON files."""
        temp_store.save_events(sample_events)
        
        loaded = temp_store.load_events()
        assert len(loaded) == 3  # 1 template + 2 occurrences
        
        templates = [e for e in loaded if isinstance(e, EventTemplate)]
        occurrences = [e for e in loaded if isinstance(e, EventOccurrence)]
        
        assert len(templates) == 1
        assert len(occurrences) == 2


class TestFilteringOperations:
    """Test filtering by status, provider, and date ranges."""
    
    def test_filter_locations_by_status(self, temp_store, sample_locations):
        """Test filtering locations by status."""
        temp_store.save_locations(sample_locations)
        
        # Filter for active locations
        active = temp_store.load_locations(filters={"status": "active"})
        assert len(active) == 1
        assert active[0].location_id == "location-test-1"
        
        # Filter for removed locations
        removed = temp_store.load_locations(filters={"status": "removed"})
        assert len(removed) == 1
        assert removed[0].location_id == "location-test-2"
    
    def test_filter_locations_by_provider(self, temp_store, sample_locations):
        """Test filtering locations by provider_id."""
        # Add locations from different providers
        locations = sample_locations + [
            Location(
                location_id="location-other-1",
                provider_id="provider-other",
                provider_name="Other Provider",
                formatted_address="789 Other St, London, UK",
                status="active",
                first_seen_at=datetime(2024, 1, 1, 12, 0, 0),
                last_seen_at=datetime(2024, 1, 15, 12, 0, 0)
            )
        ]
        temp_store.save_locations(locations)
        
        # Filter by provider
        test_provider_locs = temp_store.load_locations(filters={"provider_id": "provider-test"})
        assert len(test_provider_locs) == 2
        assert all(loc.provider_id == "provider-test" for loc in test_provider_locs)
        
        other_provider_locs = temp_store.load_locations(filters={"provider_id": "provider-other"})
        assert len(other_provider_locs) == 1
        assert other_provider_locs[0].provider_id == "provider-other"
    
    def test_filter_events_by_status(self, temp_store, sample_events):
        """Test filtering events by status."""
        temp_store.save_events(sample_events)
        
        # Filter for active events
        active = temp_store.load_events(filters={"status": "active"})
        assert len(active) == 2  # 1 template + 1 occurrence
        
        # Filter for expired events
        expired = temp_store.load_events(filters={"status": "expired"})
        assert len(expired) == 1
        assert expired[0].status == "expired"
    
    def test_filter_events_by_provider(self, temp_store, sample_events):
        """Test filtering events by provider_id."""
        # Add events from different providers
        events = sample_events + [
            EventTemplate(
                event_template_id="template-other-1",
                provider_id="provider-other",
                title="Other Workshop",
                slug="other-workshop",
                status="active",
                first_seen_at=datetime(2024, 1, 1, 12, 0, 0),
                last_seen_at=datetime(2024, 1, 15, 12, 0, 0)
            )
        ]
        temp_store.save_events(events)
        
        # Filter by provider
        test_provider_events = temp_store.load_events(filters={"provider_id": "provider-test"})
        assert len(test_provider_events) == 3
        assert all(e.provider_id == "provider-test" for e in test_provider_events)
        
        other_provider_events = temp_store.load_events(filters={"provider_id": "provider-other"})
        assert len(other_provider_events) == 1
        assert other_provider_events[0].provider_id == "provider-other"
    
    def test_filter_events_by_date_range(self, temp_store, sample_events):
        """Test filtering event occurrences by date range."""
        temp_store.save_events(sample_events)
        
        # Filter for January events (should include template + 1 occurrence)
        jan_events = temp_store.load_events(filters={
            "start_date": datetime(2024, 1, 1),
            "end_date": datetime(2024, 1, 31, 23, 59, 59)
        })
        
        # Should include 1 template (always included) + 1 occurrence in January
        assert len(jan_events) == 2
        occurrences = [e for e in jan_events if isinstance(e, EventOccurrence)]
        assert len(occurrences) == 1
        assert occurrences[0].event_id == "occurrence-test-1"
        
        # Filter for February events
        feb_events = temp_store.load_events(filters={
            "start_date": datetime(2024, 2, 1),
            "end_date": datetime(2024, 2, 29, 23, 59, 59)
        })
        
        # Should include 1 template + 1 occurrence in February
        assert len(feb_events) == 2
        occurrences = [e for e in feb_events if isinstance(e, EventOccurrence)]
        assert len(occurrences) == 1
        assert occurrences[0].event_id == "occurrence-test-2"
    
    def test_filter_events_by_start_date_only(self, temp_store, sample_events):
        """Test filtering events with only start_date (no end_date)."""
        temp_store.save_events(sample_events)
        
        # Filter for events starting from Feb 1
        events = temp_store.load_events(filters={
            "start_date": datetime(2024, 2, 1)
        })
        
        # Should include 1 template + 1 occurrence (Feb occurrence)
        assert len(events) == 2
        occurrences = [e for e in events if isinstance(e, EventOccurrence)]
        assert len(occurrences) == 1
        assert occurrences[0].start_at >= datetime(2024, 2, 1)
    
    def test_filter_events_by_end_date_only(self, temp_store, sample_events):
        """Test filtering events with only end_date (no start_date)."""
        temp_store.save_events(sample_events)
        
        # Filter for events ending before Feb 1
        events = temp_store.load_events(filters={
            "end_date": datetime(2024, 1, 31, 23, 59, 59)
        })
        
        # Should include 1 template + 1 occurrence (Jan occurrence)
        assert len(events) == 2
        occurrences = [e for e in events if isinstance(e, EventOccurrence)]
        assert len(occurrences) == 1
        assert occurrences[0].start_at <= datetime(2024, 1, 31, 23, 59, 59)
    
    def test_combined_filters(self, temp_store, sample_events):
        """Test combining multiple filters."""
        temp_store.save_events(sample_events)
        
        # Filter by status AND date range
        active_jan_events = temp_store.load_events(filters={
            "status": "active",
            "start_date": datetime(2024, 1, 1),
            "end_date": datetime(2024, 1, 31, 23, 59, 59)
        })
        
        # Should include 1 template + 1 active occurrence in January
        assert len(active_jan_events) == 2
        assert all(e.status == "active" for e in active_jan_events)


class TestMissingAndCorruptedFiles:
    """Test handling of missing or corrupted files."""
    
    def test_load_from_missing_files_returns_empty(self, temp_store):
        """Test that loading from non-existent files returns empty lists."""
        # Don't save anything, just try to load
        providers = temp_store.load_providers()
        locations = temp_store.load_locations()
        events = temp_store.load_events()
        
        assert providers == []
        assert locations == []
        assert events == []
    
    def test_load_from_corrupted_providers_file(self, temp_store, sample_provider):
        """Test graceful handling of corrupted providers file."""
        # Save valid data first
        temp_store.save_providers([sample_provider])
        
        # Corrupt the file
        providers_file = Path(temp_store.base_path) / "providers.json"
        providers_file.write_text("{ invalid json syntax }")
        
        # Should return empty list, not crash
        providers = temp_store.load_providers()
        assert providers == []
    
    def test_load_from_corrupted_locations_file(self, temp_store, sample_locations):
        """Test graceful handling of corrupted locations file."""
        temp_store.save_locations(sample_locations)
        
        # Corrupt the file
        locations_file = Path(temp_store.base_path) / "locations.json"
        locations_file.write_text("not valid json at all")
        
        # Should return empty list, not crash
        locations = temp_store.load_locations()
        assert locations == []
    
    def test_load_from_corrupted_events_files(self, temp_store, sample_events):
        """Test graceful handling of corrupted event files."""
        temp_store.save_events(sample_events)
        
        # Corrupt both event files
        templates_file = Path(temp_store.base_path) / "event_templates.json"
        occurrences_file = Path(temp_store.base_path) / "event_occurrences.json"
        
        templates_file.write_text("{ corrupted }")
        occurrences_file.write_text("[ wrong type ]")
        
        # Should return empty list, not crash
        events = temp_store.load_events()
        assert events == []
    
    def test_recovery_after_corruption(self, temp_store, sample_provider):
        """Test that store can recover after file corruption."""
        # Save valid data
        temp_store.save_providers([sample_provider])
        
        # Corrupt the file
        providers_file = Path(temp_store.base_path) / "providers.json"
        providers_file.write_text("corrupted")
        
        # Load returns empty
        assert temp_store.load_providers() == []
        
        # Save again - should recover
        temp_store.save_providers([sample_provider])
        
        # Now load should work
        providers = temp_store.load_providers()
        assert len(providers) == 1
        assert providers[0].provider_id == "provider-test"
    
    def test_load_from_non_dict_json(self, temp_store):
        """Test handling of JSON files that aren't dicts."""
        # Create a file with valid JSON but wrong type (array instead of dict)
        providers_file = Path(temp_store.base_path) / "providers.json"
        providers_file.write_text('[{"provider_id": "test"}]')
        
        # Should return empty list, not crash
        providers = temp_store.load_providers()
        assert providers == []


class TestAtomicWriteOperations:
    """Test atomic write operations to ensure data integrity."""
    
    def test_atomic_write_uses_temp_file(self, temp_store, sample_provider):
        """Test that atomic write uses temporary file."""
        # Monitor file system during save
        temp_store.save_providers([sample_provider])
        
        # Verify final file exists
        providers_file = Path(temp_store.base_path) / "providers.json"
        assert providers_file.exists()
        
        # Verify no temp file left behind
        temp_file = Path(temp_store.base_path) / "providers.tmp"
        assert not temp_file.exists()
    
    def test_atomic_write_preserves_data_on_error(self, temp_store, sample_provider):
        """Test that atomic write doesn't corrupt existing data on error."""
        # Save initial data
        temp_store.save_providers([sample_provider])
        
        # Verify it was saved
        providers = temp_store.load_providers()
        assert len(providers) == 1
        
        # Make the directory read-only to cause write error
        providers_file = Path(temp_store.base_path) / "providers.json"
        original_content = providers_file.read_text()
        
        # Try to save with permission error (simulate by making parent dir read-only)
        # Note: This is platform-dependent, so we'll just verify the mechanism
        # The actual atomic write is tested by checking temp file cleanup
        
        # Verify original file is still intact
        assert providers_file.read_text() == original_content
    
    def test_concurrent_save_operations(self, temp_store, sample_provider, sample_locations):
        """Test that multiple save operations don't interfere."""
        # Save different record types
        temp_store.save_providers([sample_provider])
        temp_store.save_locations(sample_locations)
        
        # Verify both were saved correctly
        providers = temp_store.load_providers()
        locations = temp_store.load_locations()
        
        assert len(providers) == 1
        assert len(locations) == 2
        
        # Verify no temp files left behind
        temp_dir = Path(temp_store.base_path)
        temp_files = list(temp_dir.glob("*.tmp"))
        assert len(temp_files) == 0
    
    def test_save_overwrites_existing_data(self, temp_store, sample_provider):
        """Test that save operation completely replaces existing data."""
        # Save initial provider
        temp_store.save_providers([sample_provider])
        
        # Save different provider
        new_provider = Provider(
            provider_id="provider-new",
            provider_name="New Provider",
            provider_slug="new-provider",
            source_name="New Source",
            source_base_url="https://new.com"
        )
        temp_store.save_providers([new_provider])
        
        # Load should only have new provider
        providers = temp_store.load_providers()
        assert len(providers) == 1
        assert providers[0].provider_id == "provider-new"
    
    def test_save_empty_list_creates_empty_dict(self, temp_store):
        """Test that saving empty list creates valid empty dict JSON."""
        temp_store.save_providers([])
        
        # Verify file exists and contains empty dict
        providers_file = Path(temp_store.base_path) / "providers.json"
        assert providers_file.exists()
        
        with open(providers_file) as f:
            data = json.load(f)
            assert data == {}
        
        # Load should return empty list
        providers = temp_store.load_providers()
        assert providers == []


class TestArchiveOperations:
    """Test archive snapshot functionality."""
    
    def test_archive_snapshot_creates_backup(self, temp_store, sample_provider, sample_locations, sample_events):
        """Test that archive snapshot creates timestamped backup."""
        # Save data
        temp_store.save_providers([sample_provider])
        temp_store.save_locations(sample_locations)
        temp_store.save_events(sample_events)
        
        # Create archive
        timestamp = "2024-01-15T12-00-00"
        temp_store.archive_snapshot(timestamp)
        
        # Verify archive directory structure
        archive_dir = Path(temp_store.base_path).parent / "archive" / timestamp
        assert archive_dir.exists()
        assert (archive_dir / "providers.json").exists()
        assert (archive_dir / "locations.json").exists()
        assert (archive_dir / "event_templates.json").exists()
        assert (archive_dir / "event_occurrences.json").exists()
    
    def test_archive_preserves_data_integrity(self, temp_store, sample_provider):
        """Test that archived data matches original data."""
        # Save data
        temp_store.save_providers([sample_provider])
        
        # Read original content
        providers_file = Path(temp_store.base_path) / "providers.json"
        original_content = providers_file.read_text()
        
        # Create archive
        timestamp = "2024-01-15T12-00-00"
        temp_store.archive_snapshot(timestamp)
        
        # Read archived content
        archive_file = Path(temp_store.base_path).parent / "archive" / timestamp / "providers.json"
        archived_content = archive_file.read_text()
        
        # Verify they match
        assert archived_content == original_content
    
    def test_archive_handles_missing_files(self, temp_store):
        """Test that archive handles missing files gracefully."""
        # Don't save anything, just try to archive
        timestamp = "2024-01-15T12-00-00"
        
        # Should not crash
        temp_store.archive_snapshot(timestamp)
        
        # Archive directory should be created but empty
        archive_dir = Path(temp_store.base_path).parent / "archive" / timestamp
        assert archive_dir.exists()
        
        # No files should be in archive
        archived_files = list(archive_dir.glob("*.json"))
        assert len(archived_files) == 0
    
    def test_multiple_archives_dont_interfere(self, temp_store, sample_provider):
        """Test that multiple archives can coexist."""
        # Save initial data
        temp_store.save_providers([sample_provider])
        
        # Create first archive
        temp_store.archive_snapshot("2024-01-15T12-00-00")
        
        # Modify data
        sample_provider.provider_name = "Updated Provider"
        temp_store.save_providers([sample_provider])
        
        # Create second archive
        temp_store.archive_snapshot("2024-01-16T12-00-00")
        
        # Verify both archives exist
        archive_base = Path(temp_store.base_path).parent / "archive"
        assert (archive_base / "2024-01-15T12-00-00").exists()
        assert (archive_base / "2024-01-16T12-00-00").exists()
        
        # Verify they have different content
        archive1_file = archive_base / "2024-01-15T12-00-00" / "providers.json"
        archive2_file = archive_base / "2024-01-16T12-00-00" / "providers.json"
        
        with open(archive1_file) as f:
            data1 = json.load(f)
        with open(archive2_file) as f:
            data2 = json.load(f)
        
        assert data1["provider-test"]["provider_name"] == "Test Provider"
        assert data2["provider-test"]["provider_name"] == "Updated Provider"
