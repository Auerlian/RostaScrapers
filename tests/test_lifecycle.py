"""Unit tests for lifecycle management functions."""

import pytest
from datetime import datetime, timezone, timedelta

from src.sync.lifecycle import mark_expired, mark_removed
from src.models.event_occurrence import EventOccurrence
from src.models.event_template import EventTemplate
from src.models.location import Location


class TestMarkExpired:
    """Tests for mark_expired function."""
    
    def test_marks_past_event_as_expired(self):
        """Test that past events are marked as expired."""
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
    
    def test_preserves_future_event_status(self):
        """Test that future events remain active."""
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
    
    def test_preserves_already_expired_events(self):
        """Test that already expired events are not modified."""
        now = datetime.now(timezone.utc)
        past_time = now - timedelta(days=1)
        
        event = EventOccurrence(
            event_id="event-1",
            provider_id="provider-1",
            title="Already Expired",
            start_at=past_time,
            status="expired",
            first_seen_at=now - timedelta(days=10),
            last_seen_at=now - timedelta(days=5)
        )
        
        result = mark_expired([event], now)
        
        assert len(result) == 1
        assert result[0].status == "expired"
        assert result[0].last_seen_at == event.last_seen_at  # Not updated
    
    def test_preserves_removed_events(self):
        """Test that removed events are not marked as expired."""
        now = datetime.now(timezone.utc)
        past_time = now - timedelta(days=1)
        
        event = EventOccurrence(
            event_id="event-1",
            provider_id="provider-1",
            title="Removed Event",
            start_at=past_time,
            status="removed"
        )
        
        result = mark_expired([event], now)
        
        assert len(result) == 1
        assert result[0].status == "removed"
    
    def test_preserves_cancelled_events(self):
        """Test that cancelled events are not marked as expired."""
        now = datetime.now(timezone.utc)
        past_time = now - timedelta(days=1)
        
        event = EventOccurrence(
            event_id="event-1",
            provider_id="provider-1",
            title="Cancelled Event",
            start_at=past_time,
            status="cancelled"
        )
        
        result = mark_expired([event], now)
        
        assert len(result) == 1
        assert result[0].status == "cancelled"
    
    def test_handles_events_without_start_at(self):
        """Test that events without start_at are preserved as-is."""
        event = EventOccurrence(
            event_id="event-1",
            provider_id="provider-1",
            title="No Start Time",
            start_at=None,
            status="active"
        )
        
        result = mark_expired([event])
        
        assert len(result) == 1
        assert result[0].status == "active"
    
    def test_handles_empty_list(self):
        """Test that empty list returns empty list."""
        result = mark_expired([])
        assert result == []
    
    def test_handles_multiple_events(self):
        """Test processing multiple events with mixed statuses."""
        now = datetime.now(timezone.utc)
        
        events = [
            EventOccurrence(
                event_id="event-1",
                provider_id="provider-1",
                title="Past Active",
                start_at=now - timedelta(days=1),
                status="active"
            ),
            EventOccurrence(
                event_id="event-2",
                provider_id="provider-1",
                title="Future Active",
                start_at=now + timedelta(days=1),
                status="active"
            ),
            EventOccurrence(
                event_id="event-3",
                provider_id="provider-1",
                title="Past Expired",
                start_at=now - timedelta(days=2),
                status="expired"
            ),
        ]
        
        result = mark_expired(events, now)
        
        assert len(result) == 3
        assert result[0].status == "expired"  # Past active -> expired
        assert result[1].status == "active"   # Future active -> stays active
        assert result[2].status == "expired"  # Past expired -> stays expired
    
    def test_uses_current_time_when_now_not_provided(self):
        """Test that function uses current time when now parameter is None."""
        past_time = datetime.now(timezone.utc) - timedelta(days=1)
        
        event = EventOccurrence(
            event_id="event-1",
            provider_id="provider-1",
            title="Past Event",
            start_at=past_time,
            status="active"
        )
        
        result = mark_expired([event])  # No now parameter
        
        assert len(result) == 1
        assert result[0].status == "expired"


class TestMarkRemoved:
    """Tests for mark_removed function."""
    
    def test_marks_missing_future_event_as_removed(self):
        """Test that missing future events are marked as removed."""
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
    
    def test_preserves_event_present_in_new_data(self):
        """Test that events present in new data are not marked as removed."""
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
    
    def test_does_not_mark_past_events_as_removed(self):
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
    
    def test_preserves_already_removed_events(self):
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
    
    def test_preserves_expired_events(self):
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
    
    def test_preserves_cancelled_events(self):
        """Test that cancelled events are not marked as removed."""
        now = datetime.now(timezone.utc)
        future_time = now + timedelta(days=1)
        
        event = EventOccurrence(
            event_id="event-1",
            provider_id="provider-1",
            title="Cancelled Event",
            start_at=future_time,
            status="cancelled"
        )
        
        result = mark_removed([event], set(), "provider-1", now)
        
        assert len(result) == 1
        assert result[0].status == "cancelled"
    
    def test_only_affects_specified_provider(self):
        """Test that only records for specified provider are affected."""
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
    
    def test_works_with_event_templates(self):
        """Test that mark_removed works with EventTemplate records."""
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
    
    def test_works_with_locations(self):
        """Test that mark_removed works with Location records."""
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
    
    def test_handles_empty_list(self):
        """Test that empty list returns empty list."""
        result = mark_removed([], set(), "provider-1")
        assert result == []
    
    def test_handles_multiple_records_mixed_providers(self):
        """Test processing multiple records from different providers."""
        now = datetime.now(timezone.utc)
        future_time = now + timedelta(days=1)
        
        events = [
            EventOccurrence(
                event_id="event-1",
                provider_id="provider-1",
                title="P1 Missing",
                start_at=future_time,
                status="active"
            ),
            EventOccurrence(
                event_id="event-2",
                provider_id="provider-1",
                title="P1 Present",
                start_at=future_time,
                status="active"
            ),
            EventOccurrence(
                event_id="event-3",
                provider_id="provider-2",
                title="P2 Missing",
                start_at=future_time,
                status="active"
            ),
        ]
        
        # Only event-2 is in new data, processing provider-1
        result = mark_removed(events, {"event-2"}, "provider-1", now)
        
        assert len(result) == 3
        assert result[0].status == "removed"  # P1 missing -> removed
        assert result[1].status == "active"   # P1 present -> active
        assert result[2].status == "active"   # P2 not affected
    
    def test_uses_current_time_when_now_not_provided(self):
        """Test that function uses current time when now parameter is None."""
        future_time = datetime.now(timezone.utc) + timedelta(days=1)
        
        event = EventOccurrence(
            event_id="event-1",
            provider_id="provider-1",
            title="Missing Event",
            start_at=future_time,
            status="active"
        )
        
        result = mark_removed([event], set(), "provider-1")  # No now parameter
        
        assert len(result) == 1
        assert result[0].status == "removed"
        assert result[0].deleted_at is not None
    
    def test_handles_event_without_start_at(self):
        """Test that events without start_at are treated as future events."""
        now = datetime.now(timezone.utc)
        
        event = EventOccurrence(
            event_id="event-1",
            provider_id="provider-1",
            title="No Start Time",
            start_at=None,
            status="active"
        )
        
        # Event not in new_record_ids and has no start_at
        result = mark_removed([event], set(), "provider-1", now)
        
        assert len(result) == 1
        # Without start_at, we can't determine if it's past, so it gets marked removed
        assert result[0].status == "removed"


class TestLifecycleIntegration:
    """Integration tests for lifecycle functions working together."""
    
    def test_expired_and_removed_workflow(self):
        """Test typical workflow: mark expired first, then mark removed."""
        now = datetime.now(timezone.utc)
        
        events = [
            EventOccurrence(
                event_id="event-1",
                provider_id="provider-1",
                title="Past Event",
                start_at=now - timedelta(days=1),
                status="active"
            ),
            EventOccurrence(
                event_id="event-2",
                provider_id="provider-1",
                title="Future Missing",
                start_at=now + timedelta(days=1),
                status="active"
            ),
            EventOccurrence(
                event_id="event-3",
                provider_id="provider-1",
                title="Future Present",
                start_at=now + timedelta(days=1),
                status="active"
            ),
        ]
        
        # Step 1: Mark expired
        after_expired = mark_expired(events, now)
        
        # Step 2: Mark removed (only event-3 is in new data)
        after_removed = mark_removed(after_expired, {"event-3"}, "provider-1", now)
        
        assert len(after_removed) == 3
        assert after_removed[0].status == "expired"  # Past event
        assert after_removed[1].status == "removed"  # Future missing
        assert after_removed[2].status == "active"   # Future present
    
    def test_preserves_first_seen_at_through_lifecycle_changes(self):
        """Test that first_seen_at is preserved through all lifecycle changes."""
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
