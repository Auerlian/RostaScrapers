"""Unit tests for merge logic and lifecycle rules.

This test module validates the core merge and lifecycle functionality as specified
in task 3.9:
- New record insertion
- Existing record update detection via hash comparison
- Unchanged record preservation
- Soft delete for missing records
- Lifecycle state transitions (active -> expired, active -> removed)
- Timestamp updates (first_seen_at preservation, last_seen_at updates)
"""

import pytest
from datetime import datetime, timezone, timedelta

from src.sync.merge_engine import MergeEngine, MergeResult
from src.sync.lifecycle import mark_expired, mark_removed
from src.models.event_template import EventTemplate
from src.models.event_occurrence import EventOccurrence
from src.models.location import Location


@pytest.fixture
def engine():
    """Create a MergeEngine instance."""
    return MergeEngine()


@pytest.fixture
def base_timestamp():
    """Base timestamp for testing."""
    return datetime(2025, 1, 15, 10, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def later_timestamp():
    """Later timestamp for testing updates."""
    return datetime(2025, 1, 20, 14, 0, 0, tzinfo=timezone.utc)


class TestNewRecordInsertion:
    """Test new record insertion (Requirement 3.4)."""
    
    def test_insert_new_event_template(self, engine, base_timestamp):
        """Test that new event templates are inserted with first_seen_at timestamp."""
        new_template = EventTemplate(
            event_template_id="event-template-test-pasta",
            provider_id="provider-test",
            title="Pasta Making Class",
            slug="pasta-making-class",
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
            source_hash="abc123",
            record_hash="xyz789"
        )
        
        merged, result = engine.merge_records([new_template], [], "event_template")
        
        assert result.inserted == 1
        assert result.updated == 0
        assert result.unchanged == 0
        assert result.total == 1
        assert len(merged) == 1
        assert merged[0].event_template_id == "event-template-test-pasta"
        assert merged[0].first_seen_at == base_timestamp
    
    def test_insert_new_event_occurrence(self, engine, base_timestamp):
        """Test that new event occurrences are inserted."""
        new_occurrence = EventOccurrence(
            event_id="event-test-abc123",
            provider_id="provider-test",
            title="Pasta Making Class",
            start_at=base_timestamp + timedelta(days=7),
            timezone="Europe/London",
            currency="GBP",
            availability_status="available",
            status="active",
            first_seen_at=base_timestamp,
            last_seen_at=base_timestamp,
            tags=[],
            skills_required=[],
            skills_created=[],
            source_hash="abc123",
            record_hash="xyz789"
        )
        
        merged, result = engine.merge_records([new_occurrence], [], "event_occurrence")
        
        assert result.inserted == 1
        assert len(merged) == 1
        assert merged[0].event_id == "event-test-abc123"
    
    def test_insert_new_location(self, engine, base_timestamp):
        """Test that new locations are inserted."""
        new_location = Location(
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
        
        merged, result = engine.merge_records([new_location], [], "location")
        
        assert result.inserted == 1
        assert len(merged) == 1
        assert merged[0].location_id == "location-test-abc123"


class TestExistingRecordUpdateDetection:
    """Test existing record update detection via hash comparison (Requirement 3.3)."""
    
    def test_detect_changed_template_via_hash(self, engine, base_timestamp, later_timestamp):
        """Test that changed templates are detected via source_hash comparison."""
        existing_template = EventTemplate(
            event_template_id="event-template-test-pasta",
            provider_id="provider-test",
            title="Pasta Making Class",
            slug="pasta-making-class",
            description_raw="Old description",
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
            source_hash="abc123",  # Old hash
            record_hash="xyz789"
        )
        
        new_template = EventTemplate(
            event_template_id="event-template-test-pasta",
            provider_id="provider-test",
            title="Pasta Making Class",
            slug="pasta-making-class",
            description_raw="New updated description",  # Changed
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
            source_hash="def456",  # Different hash - indicates change
            record_hash="uvw012"
        )
        
        merged, result = engine.merge_records([new_template], [existing_template], "event_template")
        
        assert result.updated == 1
        assert result.inserted == 0
        assert result.unchanged == 0
        # Should preserve first_seen_at from existing
        assert merged[0].first_seen_at == base_timestamp
        # Should have new description
        assert merged[0].description_raw == "New updated description"
        # Should have new hash
        assert merged[0].source_hash == "def456"
    
    def test_detect_changed_occurrence_via_hash(self, engine, base_timestamp, later_timestamp):
        """Test that changed occurrences are detected via source_hash."""
        event_start = base_timestamp + timedelta(days=7)
        
        existing_occurrence = EventOccurrence(
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
        
        new_occurrence = EventOccurrence(
            event_id="event-test-abc123",
            provider_id="provider-test",
            title="Pasta Making Class",
            start_at=event_start,
            timezone="Europe/London",
            currency="GBP",
            availability_status="limited",  # Changed
            remaining_spaces=2,  # Changed
            status="active",
            first_seen_at=later_timestamp,
            last_seen_at=later_timestamp,
            tags=[],
            skills_required=[],
            skills_created=[],
            source_hash="def456",  # Different hash
            record_hash="uvw012"
        )
        
        merged, result = engine.merge_records([new_occurrence], [existing_occurrence], "event_occurrence")
        
        assert result.updated == 1
        assert merged[0].first_seen_at == base_timestamp
        assert merged[0].availability_status == "limited"
        assert merged[0].remaining_spaces == 2


class TestUnchangedRecordPreservation:
    """Test unchanged record preservation (Requirement 3.2)."""
    
    def test_preserve_unchanged_template(self, engine, base_timestamp, later_timestamp):
        """Test that unchanged templates are preserved with only last_seen_at updated."""
        existing_template = EventTemplate(
            event_template_id="event-template-test-pasta",
            provider_id="provider-test",
            title="Pasta Making Class",
            slug="pasta-making-class",
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
            source_hash="abc123",  # Same hash
            record_hash="xyz789"
        )
        
        new_template = EventTemplate(
            event_template_id="event-template-test-pasta",
            provider_id="provider-test",
            title="Pasta Making Class",
            slug="pasta-making-class",
            currency="GBP",
            status="active",
            first_seen_at=later_timestamp,  # Different timestamp
            last_seen_at=later_timestamp,
            tags=[],
            occasion_tags=[],
            skills_required=[],
            skills_created=[],
            image_urls=[],
            family_friendly=False,
            beginner_friendly=True,
            source_hash="abc123",  # Same hash - no change
            record_hash="xyz789"
        )
        
        merged, result = engine.merge_records([new_template], [existing_template], "event_template")
        
        assert result.unchanged == 1
        assert result.inserted == 0
        assert result.updated == 0
        # Should preserve first_seen_at from existing
        assert merged[0].first_seen_at == base_timestamp
        # Should update last_seen_at to current time
        assert merged[0].last_seen_at > base_timestamp
    
    def test_preserve_unchanged_occurrence(self, engine, base_timestamp, later_timestamp):
        """Test that unchanged occurrences are preserved."""
        event_start = base_timestamp + timedelta(days=7)
        
        existing_occurrence = EventOccurrence(
            event_id="event-test-abc123",
            provider_id="provider-test",
            title="Pasta Making Class",
            start_at=event_start,
            timezone="Europe/London",
            currency="GBP",
            availability_status="available",
            status="active",
            first_seen_at=base_timestamp,
            last_seen_at=base_timestamp,
            tags=[],
            skills_required=[],
            skills_created=[],
            source_hash="abc123",
            record_hash="xyz789"
        )
        
        new_occurrence = EventOccurrence(
            event_id="event-test-abc123",
            provider_id="provider-test",
            title="Pasta Making Class",
            start_at=event_start,
            timezone="Europe/London",
            currency="GBP",
            availability_status="available",
            status="active",
            first_seen_at=later_timestamp,
            last_seen_at=later_timestamp,
            tags=[],
            skills_required=[],
            skills_created=[],
            source_hash="abc123",  # Same hash
            record_hash="xyz789"
        )
        
        merged, result = engine.merge_records([new_occurrence], [existing_occurrence], "event_occurrence")
        
        assert result.unchanged == 1
        assert merged[0].first_seen_at == base_timestamp


class TestSoftDeleteForMissingRecords:
    """Test soft delete for missing records (Requirement 3.5)."""
    
    def test_mark_missing_future_event_as_removed(self):
        """Test that missing future events are marked as removed (soft delete)."""
        now = datetime.now(timezone.utc)
        future_time = now + timedelta(days=1)
        
        event = EventOccurrence(
            event_id="event-1",
            provider_id="provider-1",
            title="Missing Future Event",
            start_at=future_time,
            status="active",
            first_seen_at=now - timedelta(days=10),
            last_seen_at=now - timedelta(days=5)
        )
        
        # Event not in new_record_ids (missing from scrape)
        result = mark_removed([event], set(), "provider-1", now)
        
        assert len(result) == 1
        assert result[0].status == "removed"
        assert result[0].deleted_at == now
        assert result[0].first_seen_at == event.first_seen_at  # Preserved
        assert result[0].last_seen_at == now  # Updated
    
    def test_mark_missing_template_as_removed(self):
        """Test that missing templates are marked as removed."""
        now = datetime.now(timezone.utc)
        
        template = EventTemplate(
            event_template_id="template-1",
            provider_id="provider-1",
            title="Missing Template",
            slug="missing-template",
            status="active",
            first_seen_at=now - timedelta(days=10)
        )
        
        result = mark_removed([template], set(), "provider-1", now)
        
        assert len(result) == 1
        assert result[0].status == "removed"
        assert result[0].deleted_at == now
    
    def test_mark_missing_location_as_removed(self):
        """Test that missing locations are marked as removed."""
        now = datetime.now(timezone.utc)
        
        location = Location(
            location_id="location-1",
            provider_id="provider-1",
            provider_name="Provider 1",
            formatted_address="123 Main St",
            status="active",
            first_seen_at=now - timedelta(days=10)
        )
        
        result = mark_removed([location], set(), "provider-1", now)
        
        assert len(result) == 1
        assert result[0].status == "removed"
        assert result[0].deleted_at == now
    
    def test_preserve_present_records(self):
        """Test that records present in new data are not marked as removed."""
        now = datetime.now(timezone.utc)
        future_time = now + timedelta(days=1)
        
        event = EventOccurrence(
            event_id="event-1",
            provider_id="provider-1",
            title="Present Event",
            start_at=future_time,
            status="active"
        )
        
        # Event is in new_record_ids (present in scrape)
        result = mark_removed([event], {"event-1"}, "provider-1", now)
        
        assert len(result) == 1
        assert result[0].status == "active"
        assert result[0].deleted_at is None


class TestLifecycleStateTransitions:
    """Test lifecycle state transitions (Requirement 3.6, 14.2)."""
    
    def test_transition_active_to_expired(self):
        """Test transition from active to expired for past events."""
        now = datetime.now(timezone.utc)
        past_time = now - timedelta(days=1)
        
        event = EventOccurrence(
            event_id="event-1",
            provider_id="provider-1",
            title="Past Event",
            start_at=past_time,
            status="active",
            first_seen_at=now - timedelta(days=10),
            last_seen_at=now - timedelta(days=5)
        )
        
        result = mark_expired([event], now)
        
        assert len(result) == 1
        assert result[0].status == "expired"
        assert result[0].first_seen_at == event.first_seen_at  # Preserved
        assert result[0].last_seen_at == now  # Updated
    
    def test_transition_active_to_removed(self):
        """Test transition from active to removed for missing future events."""
        now = datetime.now(timezone.utc)
        future_time = now + timedelta(days=1)
        
        event = EventOccurrence(
            event_id="event-1",
            provider_id="provider-1",
            title="Missing Future Event",
            start_at=future_time,
            status="active"
        )
        
        result = mark_removed([event], set(), "provider-1", now)
        
        assert len(result) == 1
        assert result[0].status == "removed"
    
    def test_preserve_future_active_events(self):
        """Test that future active events remain active."""
        now = datetime.now(timezone.utc)
        future_time = now + timedelta(days=1)
        
        event = EventOccurrence(
            event_id="event-1",
            provider_id="provider-1",
            title="Future Event",
            start_at=future_time,
            status="active"
        )
        
        result = mark_expired([event], now)
        
        assert len(result) == 1
        assert result[0].status == "active"
    
    def test_do_not_mark_past_events_as_removed(self):
        """Test that past events are not marked as removed (should be expired instead)."""
        now = datetime.now(timezone.utc)
        past_time = now - timedelta(days=1)
        
        event = EventOccurrence(
            event_id="event-1",
            provider_id="provider-1",
            title="Past Event",
            start_at=past_time,
            status="active"
        )
        
        # Event not in new_record_ids but is in the past
        result = mark_removed([event], set(), "provider-1", now)
        
        assert len(result) == 1
        assert result[0].status == "active"  # Not marked as removed
        assert result[0].deleted_at is None
    
    def test_preserve_already_removed_events(self):
        """Test that already removed events are not modified."""
        now = datetime.now(timezone.utc)
        future_time = now + timedelta(days=1)
        deleted_time = now - timedelta(days=2)
        
        event = EventOccurrence(
            event_id="event-1",
            provider_id="provider-1",
            title="Already Removed",
            start_at=future_time,
            status="removed",
            deleted_at=deleted_time
        )
        
        result = mark_removed([event], set(), "provider-1", now)
        
        assert len(result) == 1
        assert result[0].status == "removed"
        assert result[0].deleted_at == deleted_time  # Not updated
    
    def test_preserve_expired_events(self):
        """Test that expired events are not marked as removed."""
        now = datetime.now(timezone.utc)
        past_time = now - timedelta(days=1)
        
        event = EventOccurrence(
            event_id="event-1",
            provider_id="provider-1",
            title="Expired Event",
            start_at=past_time,
            status="expired"
        )
        
        result = mark_removed([event], set(), "provider-1", now)
        
        assert len(result) == 1
        assert result[0].status == "expired"


class TestTimestampUpdates:
    """Test timestamp updates (Requirement 3.2, 3.3, 3.8)."""
    
    def test_preserve_first_seen_at_on_update(self, engine, base_timestamp, later_timestamp):
        """Test that first_seen_at is preserved when updating a record."""
        existing_template = EventTemplate(
            event_template_id="event-template-1",
            provider_id="provider-test",
            title="Template",
            slug="template",
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
            source_hash="old_hash",
            record_hash="old_rec"
        )
        
        new_template = EventTemplate(
            event_template_id="event-template-1",
            provider_id="provider-test",
            title="Template Updated",
            slug="template",
            currency="GBP",
            status="active",
            first_seen_at=later_timestamp,  # Should be ignored
            last_seen_at=later_timestamp,
            tags=[],
            occasion_tags=[],
            skills_required=[],
            skills_created=[],
            image_urls=[],
            family_friendly=False,
            beginner_friendly=True,
            source_hash="new_hash",  # Changed
            record_hash="new_rec"
        )
        
        merged, result = engine.merge_records([new_template], [existing_template], "event_template")
        
        assert result.updated == 1
        # first_seen_at should be preserved from existing
        assert merged[0].first_seen_at == base_timestamp
        # last_seen_at should be updated
        assert merged[0].last_seen_at > base_timestamp
    
    def test_update_last_seen_at_on_unchanged_record(self, engine, base_timestamp, later_timestamp):
        """Test that last_seen_at is updated even for unchanged records."""
        existing_template = EventTemplate(
            event_template_id="event-template-1",
            provider_id="provider-test",
            title="Template",
            slug="template",
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
            source_hash="same_hash",
            record_hash="same_rec"
        )
        
        new_template = EventTemplate(
            event_template_id="event-template-1",
            provider_id="provider-test",
            title="Template",
            slug="template",
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
            source_hash="same_hash",  # Unchanged
            record_hash="same_rec"
        )
        
        merged, result = engine.merge_records([new_template], [existing_template], "event_template")
        
        assert result.unchanged == 1
        # first_seen_at should be preserved
        assert merged[0].first_seen_at == base_timestamp
        # last_seen_at should be updated
        assert merged[0].last_seen_at > base_timestamp
    
    def test_preserve_first_seen_at_through_lifecycle_changes(self):
        """Test that first_seen_at is preserved through lifecycle transitions."""
        now = datetime.now(timezone.utc)
        first_seen = now - timedelta(days=30)
        
        event = EventOccurrence(
            event_id="event-1",
            provider_id="provider-1",
            title="Event",
            start_at=now - timedelta(days=1),
            status="active",
            first_seen_at=first_seen,
            last_seen_at=now - timedelta(days=5)
        )
        
        # Mark as expired
        expired = mark_expired([event], now)
        assert expired[0].first_seen_at == first_seen
        
        # If we then mark as removed (shouldn't happen but test it)
        removed = mark_removed(expired, set(), "provider-1", now)
        assert removed[0].first_seen_at == first_seen
    
    def test_set_first_seen_at_on_new_record(self, engine, base_timestamp):
        """Test that first_seen_at is set when inserting a new record."""
        new_template = EventTemplate(
            event_template_id="event-template-new",
            provider_id="provider-test",
            title="New Template",
            slug="new-template",
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
            source_hash="hash",
            record_hash="rec"
        )
        
        merged, result = engine.merge_records([new_template], [], "event_template")
        
        assert result.inserted == 1
        assert merged[0].first_seen_at == base_timestamp
        assert merged[0].last_seen_at == base_timestamp


class TestIntegratedMergeAndLifecycle:
    """Test integrated merge and lifecycle workflows."""
    
    def test_complete_sync_workflow(self, engine):
        """Test complete sync workflow: merge, expire, remove."""
        now = datetime.now(timezone.utc)
        base_time = now - timedelta(days=10)
        
        # Existing records
        existing_events = [
            EventOccurrence(
                event_id="event-1",
                provider_id="provider-1",
                title="Past Event",
                start_at=now - timedelta(days=1),
                status="active",
                first_seen_at=base_time,
                last_seen_at=base_time,
                source_hash="hash1"
            ),
            EventOccurrence(
                event_id="event-2",
                provider_id="provider-1",
                title="Future Missing",
                start_at=now + timedelta(days=1),
                status="active",
                first_seen_at=base_time,
                last_seen_at=base_time,
                source_hash="hash2"
            ),
            EventOccurrence(
                event_id="event-3",
                provider_id="provider-1",
                title="Future Present",
                start_at=now + timedelta(days=1),
                status="active",
                first_seen_at=base_time,
                last_seen_at=base_time,
                source_hash="hash3"
            ),
        ]
        
        # New records (only event-3 is present)
        new_events = [
            EventOccurrence(
                event_id="event-3",
                provider_id="provider-1",
                title="Future Present",
                start_at=now + timedelta(days=1),
                status="active",
                first_seen_at=now,
                last_seen_at=now,
                source_hash="hash3"  # Unchanged
            ),
        ]
        
        # Step 1: Merge
        merged, result = engine.merge_records(new_events, existing_events, "event_occurrence")
        assert result.unchanged == 1  # event-3
        assert len(merged) == 3  # All existing records preserved
        
        # Step 2: Mark expired
        after_expired = mark_expired(merged, now)
        
        # Step 3: Mark removed
        new_ids = {e.event_id for e in new_events}
        after_removed = mark_removed(after_expired, new_ids, "provider-1", now)
        
        # Verify final states
        assert len(after_removed) == 3
        # Find each event by ID
        event_states = {e.event_id: e.status for e in after_removed}
        assert event_states["event-1"] == "expired"  # Past event
        assert event_states["event-2"] == "removed"  # Future missing
        assert event_states["event-3"] == "active"   # Future present
    
    def test_provider_isolation(self, engine):
        """Test that lifecycle rules only affect specified provider."""
        now = datetime.now(timezone.utc)
        future_time = now + timedelta(days=1)
        
        events = [
            EventOccurrence(
                event_id="event-1",
                provider_id="provider-1",
                title="Provider 1 Event",
                start_at=future_time,
                status="active"
            ),
            EventOccurrence(
                event_id="event-2",
                provider_id="provider-2",
                title="Provider 2 Event",
                start_at=future_time,
                status="active"
            ),
        ]
        
        # Neither event in new_record_ids, but only process provider-1
        result = mark_removed(events, set(), "provider-1", now)
        
        assert len(result) == 2
        assert result[0].status == "removed"  # provider-1 event marked removed
        assert result[1].status == "active"   # provider-2 event unchanged
