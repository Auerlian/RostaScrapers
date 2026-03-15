"""Lifecycle management functions for canonical records.

This module handles lifecycle status transitions for records:
- mark_expired(): Set status=expired for past events
- mark_removed(): Set status=removed and deleted_at for missing future events

Lifecycle rules are applied per provider to ensure failed scrapes don't affect
other providers' records.
"""

from dataclasses import replace
from datetime import datetime, timezone
from typing import TypeVar

from src.models.event_template import EventTemplate
from src.models.event_occurrence import EventOccurrence
from src.models.location import Location


T = TypeVar('T', EventTemplate, EventOccurrence, Location)


def mark_expired(
    records: list[EventOccurrence],
    now: datetime | None = None
) -> list[EventOccurrence]:
    """Mark past event occurrences as expired.
    
    Sets status=expired for event occurrences where start_at is in the past.
    Preserves first_seen_at timestamps and updates last_seen_at.
    
    Args:
        records: List of event occurrence records
        now: Current timestamp (defaults to UTC now)
    
    Returns:
        List of records with expired status applied where appropriate
    
    Example:
        >>> from datetime import datetime, timezone, timedelta
        >>> past_event = EventOccurrence(
        ...     event_id="event-1",
        ...     provider_id="provider-1",
        ...     title="Past Event",
        ...     start_at=datetime.now(timezone.utc) - timedelta(days=1),
        ...     status="active"
        ... )
        >>> result = mark_expired([past_event])
        >>> result[0].status
        'expired'
    """
    if now is None:
        now = datetime.now(timezone.utc)
    
    updated_records = []
    
    for record in records:
        # Only process event occurrences with start_at
        if record.start_at is None:
            updated_records.append(record)
            continue
        
        # Check if event is in the past and currently active
        if record.start_at < now and record.status == "active":
            # Mark as expired, preserve first_seen_at
            updated_record = replace(
                record,
                status="expired",
                last_seen_at=now
            )
            updated_records.append(updated_record)
        else:
            # Keep record as-is
            updated_records.append(record)
    
    return updated_records


def mark_removed(
    existing_records: list[T],
    new_record_ids: set[str],
    provider_id: str,
    now: datetime | None = None
) -> list[T]:
    """Mark missing future events as removed.
    
    Sets status=removed and deleted_at for records that:
    - Belong to the specified provider
    - Do not appear in new_record_ids
    - Are currently active (not already removed/expired)
    - Are future events (for EventOccurrence, start_at >= now)
    
    This function only affects records for the specified provider, ensuring
    failed scrapes don't affect other providers' records.
    
    Args:
        existing_records: List of existing records from canonical store
        new_record_ids: Set of record IDs from new scraped data
        provider_id: Provider ID to filter records (only this provider's records are affected)
        now: Current timestamp (defaults to UTC now)
    
    Returns:
        List of records with removed status applied where appropriate
    
    Example:
        >>> from datetime import datetime, timezone, timedelta
        >>> future_event = EventOccurrence(
        ...     event_id="event-1",
        ...     provider_id="provider-1",
        ...     title="Future Event",
        ...     start_at=datetime.now(timezone.utc) + timedelta(days=1),
        ...     status="active"
        ... )
        >>> result = mark_removed([future_event], set(), "provider-1")
        >>> result[0].status
        'removed'
    """
    if now is None:
        now = datetime.now(timezone.utc)
    
    updated_records = []
    
    for record in existing_records:
        # Get record ID based on type
        record_id = _get_record_id(record)
        
        # Only process records for the specified provider
        if record.provider_id != provider_id:
            updated_records.append(record)
            continue
        
        # If record appears in new data, keep as-is
        if record_id in new_record_ids:
            updated_records.append(record)
            continue
        
        # If record is not active, keep as-is (already removed/expired/cancelled)
        if record.status != "active":
            updated_records.append(record)
            continue
        
        # For EventOccurrence, only mark future events as removed
        if isinstance(record, EventOccurrence):
            # If event is in the past, don't mark as removed (should be expired instead)
            if record.start_at is not None and record.start_at < now:
                updated_records.append(record)
                continue
        
        # Mark as removed, preserve first_seen_at
        updated_record = replace(
            record,
            status="removed",
            deleted_at=now,
            last_seen_at=now
        )
        updated_records.append(updated_record)
    
    return updated_records


def _get_record_id(record: T) -> str:
    """Extract record ID from a canonical record.
    
    Args:
        record: Canonical record
    
    Returns:
        Record ID string
    """
    if isinstance(record, EventTemplate):
        return record.event_template_id
    elif isinstance(record, EventOccurrence):
        return record.event_id
    elif isinstance(record, Location):
        return record.location_id
    else:
        raise ValueError(f"Unknown record type: {type(record)}")
