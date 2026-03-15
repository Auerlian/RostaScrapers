"""Merge engine for incremental sync and record lifecycle management.

This module handles merging new scraped data with existing canonical store data,
implementing hash-based change detection and lifecycle management.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from src.models.provider import Provider
from src.models.location import Location
from src.models.event_template import EventTemplate
from src.models.event_occurrence import EventOccurrence


@dataclass
class MergeResult:
    """Result of a merge operation with statistics."""
    
    inserted: int  # New records added
    updated: int   # Existing records modified
    unchanged: int # Records preserved without modification
    total: int     # Total records processed
    
    def __str__(self) -> str:
        """String representation of merge result."""
        return (
            f"MergeResult(inserted={self.inserted}, updated={self.updated}, "
            f"unchanged={self.unchanged}, total={self.total})"
        )


CanonicalRecord = Provider | Location | EventTemplate | EventOccurrence


class MergeEngine:
    """Handles incremental sync and record lifecycle."""
    
    def merge_records(
        self,
        new_records: list[CanonicalRecord],
        existing_records: list[CanonicalRecord],
        record_type: str
    ) -> tuple[list[CanonicalRecord], MergeResult]:
        """Merge new records with existing, detecting changes via hash comparison.
        
        This method implements the core incremental sync logic:
        - New records (not in existing): Insert with first_seen_at timestamp
        - Changed records (source_hash differs): Update, preserve first_seen_at
        - Unchanged records (source_hash matches): Preserve exactly, update last_seen_at only
        
        Args:
            new_records: List of newly scraped canonical records
            existing_records: List of existing records from canonical store
            record_type: Type of records being merged (for logging/debugging)
        
        Returns:
            Tuple of (merged_records, merge_result)
            - merged_records: Complete list of records after merge
            - merge_result: Statistics about the merge operation
        
        Example:
            >>> engine = MergeEngine()
            >>> new_recs = [template1, template2]
            >>> existing_recs = [template1_old]
            >>> merged, result = engine.merge_records(new_recs, existing_recs, "event_template")
            >>> print(result)
            MergeResult(inserted=1, updated=1, unchanged=0, total=2)
        """
        # Build index of existing records by ID for O(1) lookup
        existing_by_id = self._build_id_index(existing_records)
        
        # Track statistics
        inserted = 0
        updated = 0
        unchanged = 0
        
        # Result list
        merged_records = []
        
        # Current timestamp for updates
        now = datetime.now(timezone.utc)
        
        # Process each new record
        for new_record in new_records:
            record_id = self._get_record_id(new_record)
            
            if record_id not in existing_by_id:
                # New record: insert with first_seen_at
                merged_records.append(new_record)
                inserted += 1
            else:
                # Existing record: check for changes
                existing_record = existing_by_id[record_id]
                
                if self._detect_change(new_record, existing_record):
                    # Changed: update record, preserve first_seen_at
                    updated_record = self._update_record(
                        new_record, existing_record, now
                    )
                    merged_records.append(updated_record)
                    updated += 1
                else:
                    # Unchanged: preserve exactly, update last_seen_at only
                    preserved_record = self._preserve_record(existing_record, now)
                    merged_records.append(preserved_record)
                    unchanged += 1
                
                # Mark as processed
                del existing_by_id[record_id]
        
        # Remaining existing records not in new data are preserved as-is
        # (lifecycle management like marking removed/expired is handled separately)
        for remaining_record in existing_by_id.values():
            merged_records.append(remaining_record)
        
        # Create result summary
        result = MergeResult(
            inserted=inserted,
            updated=updated,
            unchanged=unchanged,
            total=len(new_records)
        )
        
        return merged_records, result
    
    def _detect_change(
        self,
        new_record: CanonicalRecord,
        existing_record: CanonicalRecord
    ) -> bool:
        """Compare record hashes to detect changes.
        
        Uses source_hash comparison to determine if source data has changed.
        If source_hash is identical, the record is considered unchanged.
        
        Args:
            new_record: Newly scraped record
            existing_record: Existing record from store
        
        Returns:
            True if source data has changed, False otherwise
        
        Example:
            >>> engine = MergeEngine()
            >>> new_rec = EventTemplate(..., source_hash="abc123")
            >>> old_rec = EventTemplate(..., source_hash="abc123")
            >>> engine._detect_change(new_rec, old_rec)
            False
        """
        new_hash = getattr(new_record, 'source_hash', None)
        existing_hash = getattr(existing_record, 'source_hash', None)
        
        # If either hash is missing, consider it changed
        if new_hash is None or existing_hash is None:
            return True
        
        # Compare hashes
        return new_hash != existing_hash
    
    def _update_record(
        self,
        new_record: CanonicalRecord,
        existing_record: CanonicalRecord,
        timestamp: datetime
    ) -> CanonicalRecord:
        """Update changed record, preserving first_seen_at.
        
        Creates a new record with updated data from new_record but preserves
        the first_seen_at timestamp from existing_record.
        
        Args:
            new_record: Newly scraped record with updated data
            existing_record: Existing record with historical timestamps
            timestamp: Current timestamp for last_seen_at
        
        Returns:
            Updated record with preserved first_seen_at
        """
        # Create a copy of the new record
        if isinstance(new_record, Provider):
            updated = Provider(
                provider_id=new_record.provider_id,
                provider_name=new_record.provider_name,
                provider_slug=new_record.provider_slug,
                provider_website=new_record.provider_website,
                provider_contact_email=new_record.provider_contact_email,
                source_name=new_record.source_name,
                source_base_url=new_record.source_base_url,
                status=new_record.status,
                first_seen_at=existing_record.first_seen_at,  # Preserve
                last_seen_at=timestamp,  # Update
                metadata=new_record.metadata
            )
        elif isinstance(new_record, Location):
            updated = Location(
                location_id=new_record.location_id,
                provider_id=new_record.provider_id,
                provider_name=new_record.provider_name,
                location_name=new_record.location_name,
                address_line_1=new_record.address_line_1,
                address_line_2=new_record.address_line_2,
                city=new_record.city,
                region=new_record.region,
                postcode=new_record.postcode,
                country=new_record.country,
                formatted_address=new_record.formatted_address,
                latitude=new_record.latitude,
                longitude=new_record.longitude,
                geocode_provider=new_record.geocode_provider,
                geocode_status=new_record.geocode_status,
                geocode_precision=new_record.geocode_precision,
                geocoded_at=new_record.geocoded_at,
                venue_phone=new_record.venue_phone,
                venue_email=new_record.venue_email,
                venue_website=new_record.venue_website,
                status=new_record.status,
                first_seen_at=existing_record.first_seen_at,  # Preserve
                last_seen_at=timestamp,  # Update
                deleted_at=new_record.deleted_at,
                address_hash=new_record.address_hash
            )
        elif isinstance(new_record, EventTemplate):
            updated = EventTemplate(
                event_template_id=new_record.event_template_id,
                provider_id=new_record.provider_id,
                source_template_id=new_record.source_template_id,
                title=new_record.title,
                slug=new_record.slug,
                category=new_record.category,
                sub_category=new_record.sub_category,
                description_raw=new_record.description_raw,
                description_clean=new_record.description_clean,
                description_ai=new_record.description_ai,
                summary_short=new_record.summary_short,
                summary_medium=new_record.summary_medium,
                tags=new_record.tags,
                occasion_tags=new_record.occasion_tags,
                skills_required=new_record.skills_required,
                skills_created=new_record.skills_created,
                age_min=new_record.age_min,
                age_max=new_record.age_max,
                audience=new_record.audience,
                family_friendly=new_record.family_friendly,
                beginner_friendly=new_record.beginner_friendly,
                duration_minutes=new_record.duration_minutes,
                price_from=new_record.price_from,
                currency=new_record.currency,
                source_url=new_record.source_url,
                image_urls=new_record.image_urls,
                location_scope=new_record.location_scope,
                status=new_record.status,
                first_seen_at=existing_record.first_seen_at,  # Preserve
                last_seen_at=timestamp,  # Update
                deleted_at=new_record.deleted_at,
                source_hash=new_record.source_hash,
                record_hash=new_record.record_hash
            )
        elif isinstance(new_record, EventOccurrence):
            updated = EventOccurrence(
                event_id=new_record.event_id,
                event_template_id=new_record.event_template_id,
                provider_id=new_record.provider_id,
                location_id=new_record.location_id,
                source_event_id=new_record.source_event_id,
                title=new_record.title,
                start_at=new_record.start_at,
                end_at=new_record.end_at,
                timezone=new_record.timezone,
                booking_url=new_record.booking_url,
                price=new_record.price,
                currency=new_record.currency,
                capacity=new_record.capacity,
                remaining_spaces=new_record.remaining_spaces,
                availability_status=new_record.availability_status,
                description_raw=new_record.description_raw,
                description_clean=new_record.description_clean,
                description_ai=new_record.description_ai,
                tags=new_record.tags,
                skills_required=new_record.skills_required,
                skills_created=new_record.skills_created,
                age_min=new_record.age_min,
                age_max=new_record.age_max,
                status=new_record.status,
                first_seen_at=existing_record.first_seen_at,  # Preserve
                last_seen_at=timestamp,  # Update
                deleted_at=new_record.deleted_at,
                source_hash=new_record.source_hash,
                record_hash=new_record.record_hash
            )
        else:
            # Fallback: return new record as-is
            updated = new_record
        
        return updated
    
    def _preserve_record(
        self,
        existing_record: CanonicalRecord,
        timestamp: datetime
    ) -> CanonicalRecord:
        """Preserve unchanged record, updating last_seen_at only.
        
        Creates a copy of the existing record with only the last_seen_at
        timestamp updated. All other fields remain identical.
        
        Args:
            existing_record: Existing record from store
            timestamp: Current timestamp for last_seen_at
        
        Returns:
            Record with updated last_seen_at
        """
        # Create a copy with updated last_seen_at
        if isinstance(existing_record, Provider):
            preserved = Provider(
                provider_id=existing_record.provider_id,
                provider_name=existing_record.provider_name,
                provider_slug=existing_record.provider_slug,
                provider_website=existing_record.provider_website,
                provider_contact_email=existing_record.provider_contact_email,
                source_name=existing_record.source_name,
                source_base_url=existing_record.source_base_url,
                status=existing_record.status,
                first_seen_at=existing_record.first_seen_at,
                last_seen_at=timestamp,  # Update
                metadata=existing_record.metadata
            )
        elif isinstance(existing_record, Location):
            preserved = Location(
                location_id=existing_record.location_id,
                provider_id=existing_record.provider_id,
                provider_name=existing_record.provider_name,
                location_name=existing_record.location_name,
                address_line_1=existing_record.address_line_1,
                address_line_2=existing_record.address_line_2,
                city=existing_record.city,
                region=existing_record.region,
                postcode=existing_record.postcode,
                country=existing_record.country,
                formatted_address=existing_record.formatted_address,
                latitude=existing_record.latitude,
                longitude=existing_record.longitude,
                geocode_provider=existing_record.geocode_provider,
                geocode_status=existing_record.geocode_status,
                geocode_precision=existing_record.geocode_precision,
                geocoded_at=existing_record.geocoded_at,
                venue_phone=existing_record.venue_phone,
                venue_email=existing_record.venue_email,
                venue_website=existing_record.venue_website,
                status=existing_record.status,
                first_seen_at=existing_record.first_seen_at,
                last_seen_at=timestamp,  # Update
                deleted_at=existing_record.deleted_at,
                address_hash=existing_record.address_hash
            )
        elif isinstance(existing_record, EventTemplate):
            preserved = EventTemplate(
                event_template_id=existing_record.event_template_id,
                provider_id=existing_record.provider_id,
                source_template_id=existing_record.source_template_id,
                title=existing_record.title,
                slug=existing_record.slug,
                category=existing_record.category,
                sub_category=existing_record.sub_category,
                description_raw=existing_record.description_raw,
                description_clean=existing_record.description_clean,
                description_ai=existing_record.description_ai,
                summary_short=existing_record.summary_short,
                summary_medium=existing_record.summary_medium,
                tags=existing_record.tags,
                occasion_tags=existing_record.occasion_tags,
                skills_required=existing_record.skills_required,
                skills_created=existing_record.skills_created,
                age_min=existing_record.age_min,
                age_max=existing_record.age_max,
                audience=existing_record.audience,
                family_friendly=existing_record.family_friendly,
                beginner_friendly=existing_record.beginner_friendly,
                duration_minutes=existing_record.duration_minutes,
                price_from=existing_record.price_from,
                currency=existing_record.currency,
                source_url=existing_record.source_url,
                image_urls=existing_record.image_urls,
                location_scope=existing_record.location_scope,
                status=existing_record.status,
                first_seen_at=existing_record.first_seen_at,
                last_seen_at=timestamp,  # Update
                deleted_at=existing_record.deleted_at,
                source_hash=existing_record.source_hash,
                record_hash=existing_record.record_hash
            )
        elif isinstance(existing_record, EventOccurrence):
            preserved = EventOccurrence(
                event_id=existing_record.event_id,
                event_template_id=existing_record.event_template_id,
                provider_id=existing_record.provider_id,
                location_id=existing_record.location_id,
                source_event_id=existing_record.source_event_id,
                title=existing_record.title,
                start_at=existing_record.start_at,
                end_at=existing_record.end_at,
                timezone=existing_record.timezone,
                booking_url=existing_record.booking_url,
                price=existing_record.price,
                currency=existing_record.currency,
                capacity=existing_record.capacity,
                remaining_spaces=existing_record.remaining_spaces,
                availability_status=existing_record.availability_status,
                description_raw=existing_record.description_raw,
                description_clean=existing_record.description_clean,
                description_ai=existing_record.description_ai,
                tags=existing_record.tags,
                skills_required=existing_record.skills_required,
                skills_created=existing_record.skills_created,
                age_min=existing_record.age_min,
                age_max=existing_record.age_max,
                status=existing_record.status,
                first_seen_at=existing_record.first_seen_at,
                last_seen_at=timestamp,  # Update
                deleted_at=existing_record.deleted_at,
                source_hash=existing_record.source_hash,
                record_hash=existing_record.record_hash
            )
        else:
            # Fallback: return existing record as-is
            preserved = existing_record
        
        return preserved
    
    def _build_id_index(
        self,
        records: list[CanonicalRecord]
    ) -> dict[str, CanonicalRecord]:
        """Build index of records by ID for fast lookup.
        
        Args:
            records: List of canonical records
        
        Returns:
            Dictionary mapping record ID to record
        """
        index = {}
        for record in records:
            record_id = self._get_record_id(record)
            index[record_id] = record
        return index
    
    def _get_record_id(self, record: CanonicalRecord) -> str:
        """Extract record ID from a canonical record.
        
        Args:
            record: Canonical record
        
        Returns:
            Record ID string
        """
        if isinstance(record, Provider):
            return record.provider_id
        elif isinstance(record, Location):
            return record.location_id
        elif isinstance(record, EventTemplate):
            return record.event_template_id
        elif isinstance(record, EventOccurrence):
            return record.event_id
        else:
            raise ValueError(f"Unknown record type: {type(record)}")
