"""Integration tests for CanonicalStore demonstrating full workflow."""

import pytest
from datetime import datetime
from pathlib import Path
import tempfile
import shutil
import json

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
    # Cleanup
    shutil.rmtree(temp_dir, ignore_errors=True)
    archive_dir = Path(temp_dir).parent / "archive"
    if archive_dir.exists():
        shutil.rmtree(archive_dir, ignore_errors=True)


def test_full_pipeline_workflow(temp_store):
    """Test a complete pipeline workflow: save, load, filter, archive."""
    
    # Step 1: Create and save provider
    provider = Provider(
        provider_id="provider-pasta",
        provider_name="Pasta Evangelists",
        provider_slug="pasta-evangelists",
        source_name="Pasta Evangelists API",
        source_base_url="https://pastaevangelists.com",
        status="active",
        first_seen_at=datetime(2024, 1, 1, 12, 0, 0),
        last_seen_at=datetime(2024, 1, 15, 12, 0, 0)
    )
    temp_store.save_providers([provider])
    
    # Step 2: Create and save locations
    locations = [
        Location(
            location_id="location-pasta-london",
            provider_id="provider-pasta",
            provider_name="Pasta Evangelists",
            location_name="London Studio",
            formatted_address="123 Pasta St, London, UK",
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
            location_id="location-pasta-manchester",
            provider_id="provider-pasta",
            provider_name="Pasta Evangelists",
            location_name="Manchester Studio",
            formatted_address="456 Pasta Ave, Manchester, UK",
            city="Manchester",
            postcode="M1 1AA",
            status="active",
            first_seen_at=datetime(2024, 1, 1, 12, 0, 0),
            last_seen_at=datetime(2024, 1, 15, 12, 0, 0)
        )
    ]
    temp_store.save_locations(locations)
    
    # Step 3: Create and save events (mix of templates and occurrences)
    events = [
        EventTemplate(
            event_template_id="template-pasta-making",
            provider_id="provider-pasta",
            title="Pasta Making Workshop",
            slug="pasta-making-workshop",
            description_raw="Learn to make fresh pasta",
            tags=["cooking", "italian", "hands-on"],
            price_from=75.0,
            currency="GBP",
            status="active",
            first_seen_at=datetime(2024, 1, 1, 12, 0, 0),
            last_seen_at=datetime(2024, 1, 15, 12, 0, 0)
        ),
        EventOccurrence(
            event_id="occurrence-pasta-jan",
            event_template_id="template-pasta-making",
            provider_id="provider-pasta",
            location_id="location-pasta-london",
            title="Pasta Making Workshop - January",
            start_at=datetime(2024, 1, 20, 14, 0, 0),
            end_at=datetime(2024, 1, 20, 17, 0, 0),
            price=75.0,
            currency="GBP",
            availability_status="available",
            status="active",
            first_seen_at=datetime(2024, 1, 1, 12, 0, 0),
            last_seen_at=datetime(2024, 1, 15, 12, 0, 0)
        ),
        EventOccurrence(
            event_id="occurrence-pasta-feb",
            event_template_id="template-pasta-making",
            provider_id="provider-pasta",
            location_id="location-pasta-manchester",
            title="Pasta Making Workshop - February",
            start_at=datetime(2024, 2, 15, 14, 0, 0),
            end_at=datetime(2024, 2, 15, 17, 0, 0),
            price=75.0,
            currency="GBP",
            availability_status="available",
            status="active",
            first_seen_at=datetime(2024, 1, 1, 12, 0, 0),
            last_seen_at=datetime(2024, 1, 15, 12, 0, 0)
        )
    ]
    temp_store.save_events(events)
    
    # Step 4: Verify files are dict-keyed JSON
    providers_file = Path(temp_store.base_path) / "providers.json"
    with open(providers_file) as f:
        providers_data = json.load(f)
        assert isinstance(providers_data, dict)
        assert "provider-pasta" in providers_data
    
    locations_file = Path(temp_store.base_path) / "locations.json"
    with open(locations_file) as f:
        locations_data = json.load(f)
        assert isinstance(locations_data, dict)
        assert "location-pasta-london" in locations_data
        assert "location-pasta-manchester" in locations_data
    
    # Step 5: Load and verify all data
    loaded_providers = temp_store.load_providers()
    assert len(loaded_providers) == 1
    assert loaded_providers[0].provider_id == "provider-pasta"
    
    loaded_locations = temp_store.load_locations()
    assert len(loaded_locations) == 2
    
    loaded_events = temp_store.load_events()
    assert len(loaded_events) == 3  # 1 template + 2 occurrences
    
    # Step 6: Test filtering
    # Filter locations by status
    active_locations = temp_store.load_locations(filters={"status": "active"})
    assert len(active_locations) == 2
    
    # Filter events by provider
    provider_events = temp_store.load_events(filters={"provider_id": "provider-pasta"})
    assert len(provider_events) == 3
    
    # Filter occurrences by date range
    jan_events = temp_store.load_events(filters={
        "start_date": datetime(2024, 1, 1),
        "end_date": datetime(2024, 1, 31, 23, 59, 59)
    })
    # Should include 1 template + 1 occurrence in January
    assert len(jan_events) == 2
    occurrences = [e for e in jan_events if isinstance(e, EventOccurrence)]
    assert len(occurrences) == 1
    assert occurrences[0].event_id == "occurrence-pasta-jan"
    
    # Step 7: Create archive snapshot
    timestamp = "2024-01-15T12-00-00"
    temp_store.archive_snapshot(timestamp)
    
    # Verify archive was created
    archive_dir = Path(temp_store.base_path).parent / "archive" / timestamp
    assert archive_dir.exists()
    assert (archive_dir / "providers.json").exists()
    assert (archive_dir / "locations.json").exists()
    assert (archive_dir / "event_templates.json").exists()
    assert (archive_dir / "event_occurrences.json").exists()
    
    # Step 8: Modify data and save again
    # Mark one location as removed
    locations[1].status = "removed"
    locations[1].deleted_at = datetime(2024, 1, 16, 12, 0, 0)
    temp_store.save_locations(locations)
    
    # Verify the change
    active_locations = temp_store.load_locations(filters={"status": "active"})
    assert len(active_locations) == 1
    assert active_locations[0].location_id == "location-pasta-london"
    
    removed_locations = temp_store.load_locations(filters={"status": "removed"})
    assert len(removed_locations) == 1
    assert removed_locations[0].location_id == "location-pasta-manchester"


def test_error_recovery_workflow(temp_store):
    """Test that store handles errors gracefully and can recover."""
    
    # Step 1: Try to load from empty store
    providers = temp_store.load_providers()
    locations = temp_store.load_locations()
    events = temp_store.load_events()
    
    assert providers == []
    assert locations == []
    assert events == []
    
    # Step 2: Save some data
    provider = Provider(
        provider_id="provider-test",
        provider_name="Test Provider",
        provider_slug="test-provider",
        source_name="Test",
        source_base_url="https://test.com"
    )
    temp_store.save_providers([provider])
    
    # Step 3: Corrupt the file
    providers_file = Path(temp_store.base_path) / "providers.json"
    providers_file.write_text("{ corrupted json }")
    
    # Step 4: Try to load - should return empty list, not crash
    providers = temp_store.load_providers()
    assert providers == []
    
    # Step 5: Save again - should recover
    temp_store.save_providers([provider])
    providers = temp_store.load_providers()
    assert len(providers) == 1
    assert providers[0].provider_id == "provider-test"


def test_incremental_update_workflow(temp_store):
    """Test incremental updates to the store."""
    
    # Initial save with 2 providers
    providers = [
        Provider(
            provider_id="provider-a",
            provider_name="Provider A",
            provider_slug="provider-a",
            source_name="Source A",
            source_base_url="https://a.com"
        ),
        Provider(
            provider_id="provider-b",
            provider_name="Provider B",
            provider_slug="provider-b",
            source_name="Source B",
            source_base_url="https://b.com"
        )
    ]
    temp_store.save_providers(providers)
    
    # Load and verify
    loaded = temp_store.load_providers()
    assert len(loaded) == 2
    
    # Update with 3 providers (add one, keep one, remove one)
    new_providers = [
        Provider(
            provider_id="provider-b",
            provider_name="Provider B Updated",
            provider_slug="provider-b",
            source_name="Source B",
            source_base_url="https://b.com"
        ),
        Provider(
            provider_id="provider-c",
            provider_name="Provider C",
            provider_slug="provider-c",
            source_name="Source C",
            source_base_url="https://c.com"
        )
    ]
    temp_store.save_providers(new_providers)
    
    # Load and verify - should have only the new set
    loaded = temp_store.load_providers()
    assert len(loaded) == 2
    assert {p.provider_id for p in loaded} == {"provider-b", "provider-c"}
    
    # Verify provider-b was updated
    provider_b = [p for p in loaded if p.provider_id == "provider-b"][0]
    assert provider_b.provider_name == "Provider B Updated"
