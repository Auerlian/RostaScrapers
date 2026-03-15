"""Integration tests for MergeEngine with CanonicalStore."""

import pytest
import tempfile
import shutil
from pathlib import Path
from datetime import datetime, timezone, timedelta

from src.sync.merge_engine import MergeEngine
from src.storage.store import CanonicalStore
from src.models.event_template import EventTemplate
from src.models.event_occurrence import EventOccurrence
from src.models.location import Location
from src.models.provider import Provider


class TestMergeEngineIntegration:
    """Integration tests for MergeEngine with CanonicalStore."""
    
    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for test data."""
        temp_path = tempfile.mkdtemp()
        yield temp_path
        shutil.rmtree(temp_path)
    
    @pytest.fixture
    def store(self, temp_dir):
        """Create a CanonicalStore instance with temp directory."""
        return CanonicalStore(base_path=temp_dir)
    
    @pytest.fixture
    def engine(self):
        """Create a MergeEngine instance."""
        return MergeEngine()
    
    @pytest.fixture
    def base_timestamp(self):
        """Base timestamp for testing."""
        return datetime(2025, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
    
    def test_merge_and_save_new_templates(self, engine, store, base_timestamp):
        """Test merging new templates and saving to store."""
        # Create new templates
        template1 = EventTemplate(
            event_template_id="event-template-pasta",
            provider_id="provider-test",
            title="Pasta Making Class",
            slug="pasta-making-class",
            currency="GBP",
            status="active",
            first_seen_at=base_timestamp,
            last_seen_at=base_timestamp,
            tags=["italian", "hands-on"],
            occasion_tags=[],
            skills_required=[],
            skills_created=["pasta-making"],
            image_urls=[],
            family_friendly=False,
            beginner_friendly=True,
            source_hash="abc123",
            record_hash="xyz789"
        )
        
        template2 = EventTemplate(
            event_template_id="event-template-croissant",
            provider_id="provider-test",
            title="Croissant Baking",
            slug="croissant-baking",
            currency="GBP",
            status="active",
            first_seen_at=base_timestamp,
            last_seen_at=base_timestamp,
            tags=["french", "baking"],
            occasion_tags=[],
            skills_required=[],
            skills_created=["croissant-making"],
            image_urls=[],
            family_friendly=False,
            beginner_friendly=True,
            source_hash="def456",
            record_hash="uvw012"
        )
        
        # Merge with empty existing records
        merged, result = engine.merge_records([template1, template2], [], "event_template")
        
        # Verify merge result
        assert result.inserted == 2
        assert result.updated == 0
        assert result.unchanged == 0
        
        # Save to store
        store.save_events(merged)
        
        # Load from store and verify
        loaded = store.load_events()
        assert len(loaded) == 2
        
        loaded_ids = {e.event_template_id for e in loaded}
        assert "event-template-pasta" in loaded_ids
        assert "event-template-croissant" in loaded_ids
    
    def test_merge_update_existing_templates(self, engine, store, base_timestamp):
        """Test merging updated templates with existing store data."""
        later_timestamp = base_timestamp + timedelta(days=5)
        
        # Create and save initial templates
        initial_template = EventTemplate(
            event_template_id="event-template-pasta",
            provider_id="provider-test",
            title="Pasta Making Class",
            slug="pasta-making-class",
            description_raw="Learn to make pasta",
            currency="GBP",
            status="active",
            first_seen_at=base_timestamp,
            last_seen_at=base_timestamp,
            tags=["italian"],
            occasion_tags=[],
            skills_required=[],
            skills_created=["pasta-making"],
            image_urls=[],
            family_friendly=False,
            beginner_friendly=True,
            source_hash="abc123",
            record_hash="xyz789"
        )
        
        store.save_events([initial_template])
        
        # Load existing records
        existing = store.load_events()
        assert len(existing) == 1
        
        # Create updated template (changed description)
        updated_template = EventTemplate(
            event_template_id="event-template-pasta",
            provider_id="provider-test",
            title="Pasta Making Class",
            slug="pasta-making-class",
            description_raw="Learn to make authentic Italian pasta",  # Changed
            currency="GBP",
            status="active",
            first_seen_at=later_timestamp,
            last_seen_at=later_timestamp,
            tags=["italian", "hands-on"],  # Added tag
            occasion_tags=[],
            skills_required=[],
            skills_created=["pasta-making"],
            image_urls=[],
            family_friendly=False,
            beginner_friendly=True,
            source_hash="abc999",  # Different hash
            record_hash="xyz999"
        )
        
        # Merge
        merged, result = engine.merge_records([updated_template], existing, "event_template")
        
        # Verify merge result
        assert result.inserted == 0
        assert result.updated == 1
        assert result.unchanged == 0
        
        # Verify first_seen_at preserved
        assert merged[0].first_seen_at == base_timestamp
        assert merged[0].description_raw == "Learn to make authentic Italian pasta"
        assert "hands-on" in merged[0].tags
        
        # Save and reload
        store.save_events(merged)
        reloaded = store.load_events()
        assert len(reloaded) == 1
        assert reloaded[0].first_seen_at == base_timestamp
        assert reloaded[0].description_raw == "Learn to make authentic Italian pasta"
    
    def test_merge_unchanged_templates_preserve(self, engine, store, base_timestamp):
        """Test that unchanged templates are preserved with updated last_seen_at."""
        later_timestamp = base_timestamp + timedelta(days=5)
        
        # Create and save initial template
        initial_template = EventTemplate(
            event_template_id="event-template-pasta",
            provider_id="provider-test",
            title="Pasta Making Class",
            slug="pasta-making-class",
            currency="GBP",
            status="active",
            first_seen_at=base_timestamp,
            last_seen_at=base_timestamp,
            tags=["italian"],
            occasion_tags=[],
            skills_required=[],
            skills_created=["pasta-making"],
            image_urls=[],
            family_friendly=False,
            beginner_friendly=True,
            source_hash="abc123",
            record_hash="xyz789"
        )
        
        store.save_events([initial_template])
        
        # Load existing
        existing = store.load_events()
        
        # Create "new" template with same source_hash (unchanged)
        unchanged_template = EventTemplate(
            event_template_id="event-template-pasta",
            provider_id="provider-test",
            title="Pasta Making Class",
            slug="pasta-making-class",
            currency="GBP",
            status="active",
            first_seen_at=later_timestamp,
            last_seen_at=later_timestamp,
            tags=["italian"],
            occasion_tags=[],
            skills_required=[],
            skills_created=["pasta-making"],
            image_urls=[],
            family_friendly=False,
            beginner_friendly=True,
            source_hash="abc123",  # Same hash
            record_hash="xyz789"
        )
        
        # Merge
        merged, result = engine.merge_records([unchanged_template], existing, "event_template")
        
        # Verify merge result
        assert result.inserted == 0
        assert result.updated == 0
        assert result.unchanged == 1
        
        # Verify first_seen_at preserved and last_seen_at updated
        assert merged[0].first_seen_at == base_timestamp
        assert merged[0].last_seen_at > base_timestamp
        
        # Save and reload
        store.save_events(merged)
        reloaded = store.load_events()
        assert len(reloaded) == 1
        assert reloaded[0].first_seen_at == base_timestamp
    
    def test_merge_mixed_operations_with_store(self, engine, store, base_timestamp):
        """Test mixed insert/update/unchanged operations with store."""
        later_timestamp = base_timestamp + timedelta(days=5)
        
        # Create and save initial templates
        template1 = EventTemplate(
            event_template_id="event-template-1",
            provider_id="provider-test",
            title="Template 1",
            slug="template-1",
            currency="GBP",
            status="active",
            first_seen_at=base_timestamp,
            last_seen_at=base_timestamp,
            tags=[],
            occasion_tags=[],
            skills_required=[],
            skills_created=[],
            image_urls=[],
            family_friendly=False,
            beginner_friendly=True,
            source_hash="hash1",
            record_hash="rec1"
        )
        
        template2 = EventTemplate(
            event_template_id="event-template-2",
            provider_id="provider-test",
            title="Template 2",
            slug="template-2",
            currency="GBP",
            status="active",
            first_seen_at=base_timestamp,
            last_seen_at=base_timestamp,
            tags=[],
            occasion_tags=[],
            skills_required=[],
            skills_created=[],
            image_urls=[],
            family_friendly=False,
            beginner_friendly=True,
            source_hash="hash2",
            record_hash="rec2"
        )
        
        store.save_events([template1, template2])
        
        # Load existing
        existing = store.load_events()
        assert len(existing) == 2
        
        # Create new batch: 1 unchanged, 1 updated, 1 new
        new_template1 = EventTemplate(  # Unchanged
            event_template_id="event-template-1",
            provider_id="provider-test",
            title="Template 1",
            slug="template-1",
            currency="GBP",
            status="active",
            first_seen_at=later_timestamp,
            last_seen_at=later_timestamp,
            tags=[],
            occasion_tags=[],
            skills_required=[],
            skills_created=[],
            image_urls=[],
            family_friendly=False,
            beginner_friendly=True,
            source_hash="hash1",  # Same
            record_hash="rec1"
        )
        
        new_template2 = EventTemplate(  # Updated
            event_template_id="event-template-2",
            provider_id="provider-test",
            title="Template 2 Updated",
            slug="template-2",
            currency="GBP",
            status="active",
            first_seen_at=later_timestamp,
            last_seen_at=later_timestamp,
            tags=[],
            occasion_tags=[],
            skills_required=[],
            skills_created=[],
            image_urls=[],
            family_friendly=False,
            beginner_friendly=True,
            source_hash="hash2-new",  # Different
            record_hash="rec2-new"
        )
        
        new_template3 = EventTemplate(  # New
            event_template_id="event-template-3",
            provider_id="provider-test",
            title="Template 3",
            slug="template-3",
            currency="GBP",
            status="active",
            first_seen_at=later_timestamp,
            last_seen_at=later_timestamp,
            tags=[],
            occasion_tags=[],
            skills_required=[],
            skills_created=[],
            image_urls=[],
            family_friendly=False,
            beginner_friendly=True,
            source_hash="hash3",
            record_hash="rec3"
        )
        
        # Merge
        merged, result = engine.merge_records(
            [new_template1, new_template2, new_template3],
            existing,
            "event_template"
        )
        
        # Verify merge result
        assert result.inserted == 1
        assert result.updated == 1
        assert result.unchanged == 1
        assert len(merged) == 3
        
        # Save and reload
        store.save_events(merged)
        reloaded = store.load_events()
        assert len(reloaded) == 3
        
        # Verify each record
        reloaded_by_id = {e.event_template_id: e for e in reloaded}
        
        # Template 1: unchanged, first_seen_at preserved
        assert reloaded_by_id["event-template-1"].first_seen_at == base_timestamp
        assert reloaded_by_id["event-template-1"].title == "Template 1"
        
        # Template 2: updated, first_seen_at preserved, title changed
        assert reloaded_by_id["event-template-2"].first_seen_at == base_timestamp
        assert reloaded_by_id["event-template-2"].title == "Template 2 Updated"
        
        # Template 3: new, first_seen_at is later_timestamp
        assert reloaded_by_id["event-template-3"].first_seen_at >= later_timestamp
        assert reloaded_by_id["event-template-3"].title == "Template 3"
    
    def test_merge_locations_with_store(self, engine, store, base_timestamp):
        """Test merging locations with store."""
        # Create and save initial location
        initial_location = Location(
            location_id="location-test-abc123",
            provider_id="provider-test",
            provider_name="Test Provider",
            formatted_address="123 Test St, London, EC1A 9EJ",
            country="UK",
            geocode_status="not_geocoded",
            status="active",
            first_seen_at=base_timestamp,
            last_seen_at=base_timestamp,
            address_hash="abc123"
        )
        
        store.save_locations([initial_location])
        
        # Load and verify
        existing = store.load_locations()
        assert len(existing) == 1
        
        # Create new location (same ID, will be treated as unchanged since no source_hash)
        new_location = Location(
            location_id="location-test-abc123",
            provider_id="provider-test",
            provider_name="Test Provider",
            formatted_address="123 Test St, London, EC1A 9EJ",
            country="UK",
            geocode_status="not_geocoded",
            status="active",
            first_seen_at=base_timestamp + timedelta(days=5),
            last_seen_at=base_timestamp + timedelta(days=5),
            address_hash="abc123"
        )
        
        # Merge (Note: Location doesn't have source_hash, so _detect_change returns True)
        merged, result = engine.merge_records([new_location], existing, "location")
        
        # Since Location doesn't have source_hash, it will be detected as changed
        assert result.updated == 1
        assert result.inserted == 0
        
        # Save and reload
        store.save_locations(merged)
        reloaded = store.load_locations()
        assert len(reloaded) == 1
        assert reloaded[0].first_seen_at == base_timestamp
    
    def test_merge_occurrences_with_store(self, engine, store, base_timestamp):
        """Test merging event occurrences with store."""
        event_start = base_timestamp + timedelta(days=7)
        
        # Create and save initial occurrence
        initial_occurrence = EventOccurrence(
            event_id="event-test-abc123",
            provider_id="provider-test",
            title="Pasta Making Class",
            start_at=event_start,
            timezone="Europe/London",
            currency="GBP",
            availability_status="available",
            remaining_spaces=10,
            status="active",
            first_seen_at=base_timestamp,
            last_seen_at=base_timestamp,
            tags=[],
            skills_required=[],
            skills_created=[],
            source_hash="abc123",
            record_hash="xyz789"
        )
        
        store.save_events([initial_occurrence])
        
        # Load existing
        existing = store.load_events()
        assert len(existing) == 1
        
        # Create updated occurrence (spaces reduced)
        updated_occurrence = EventOccurrence(
            event_id="event-test-abc123",
            provider_id="provider-test",
            title="Pasta Making Class",
            start_at=event_start,
            timezone="Europe/London",
            currency="GBP",
            availability_status="limited",
            remaining_spaces=2,  # Changed
            status="active",
            first_seen_at=base_timestamp + timedelta(days=5),
            last_seen_at=base_timestamp + timedelta(days=5),
            tags=[],
            skills_required=[],
            skills_created=[],
            source_hash="def456",  # Different
            record_hash="uvw012"
        )
        
        # Merge
        merged, result = engine.merge_records([updated_occurrence], existing, "event_occurrence")
        
        # Verify
        assert result.updated == 1
        assert result.inserted == 0
        assert merged[0].first_seen_at == base_timestamp
        assert merged[0].remaining_spaces == 2
        assert merged[0].availability_status == "limited"
        
        # Save and reload
        store.save_events(merged)
        reloaded = store.load_events()
        assert len(reloaded) == 1
        assert reloaded[0].remaining_spaces == 2
