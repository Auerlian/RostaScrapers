"""Canonical data store implementation with dict-keyed JSON backend."""

import json
import os
from pathlib import Path
from typing import Any
from datetime import datetime

from src.models.provider import Provider
from src.models.location import Location
from src.models.event_template import EventTemplate
from src.models.event_occurrence import EventOccurrence


class CanonicalStore:
    """Storage interface for canonical data using dict-keyed JSON files."""
    
    def __init__(self, base_path: str = "data/current"):
        """Initialize store with base directory path.
        
        Args:
            base_path: Directory path for storing JSON files
        """
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        
        # File paths for each record type
        self.providers_file = self.base_path / "providers.json"
        self.locations_file = self.base_path / "locations.json"
        self.event_templates_file = self.base_path / "event_templates.json"
        self.event_occurrences_file = self.base_path / "event_occurrences.json"
    
    def save_providers(self, providers: list[Provider]) -> None:
        """Save provider records as dict keyed by provider_id.
        
        Args:
            providers: List of Provider records to save
        """
        data = {p.provider_id: self._serialize_provider(p) for p in providers}
        self._write_json_atomic(self.providers_file, data)
    
    def save_locations(self, locations: list[Location]) -> None:
        """Save location records as dict keyed by location_id.
        
        Args:
            locations: List of Location records to save
        """
        data = {loc.location_id: self._serialize_location(loc) for loc in locations}
        self._write_json_atomic(self.locations_file, data)
    
    def save_events(self, events: list[EventTemplate | EventOccurrence]) -> None:
        """Save event records, separating templates and occurrences.
        
        Args:
            events: List of EventTemplate and/or EventOccurrence records to save
        """
        templates = {}
        occurrences = {}
        
        for event in events:
            if isinstance(event, EventTemplate):
                templates[event.event_template_id] = self._serialize_event_template(event)
            elif isinstance(event, EventOccurrence):
                occurrences[event.event_id] = self._serialize_event_occurrence(event)
        
        # Write both files atomically
        self._write_json_atomic(self.event_templates_file, templates)
        self._write_json_atomic(self.event_occurrences_file, occurrences)
    
    def load_providers(self) -> list[Provider]:
        """Load all provider records.
        
        Returns:
            List of Provider records
        """
        data = self._read_json_safe(self.providers_file)
        return [self._deserialize_provider(record) for record in data.values()]
    
    def load_locations(self, filters: dict = None) -> list[Location]:
        """Load location records with optional filtering.
        
        Args:
            filters: Optional dict with filter criteria:
                - status: Filter by status (e.g., "active")
                - provider_id: Filter by provider
        
        Returns:
            List of Location records matching filters
        """
        data = self._read_json_safe(self.locations_file)
        locations = [self._deserialize_location(record) for record in data.values()]
        
        if filters:
            if "status" in filters:
                locations = [loc for loc in locations if loc.status == filters["status"]]
            if "provider_id" in filters:
                locations = [loc for loc in locations if loc.provider_id == filters["provider_id"]]
        
        return locations
    
    def load_events(self, filters: dict = None) -> list[EventTemplate | EventOccurrence]:
        """Load event records (both templates and occurrences) with optional filtering.
        
        Args:
            filters: Optional dict with filter criteria:
                - status: Filter by status (e.g., "active")
                - provider_id: Filter by provider
                - start_date: Filter occurrences with start_at >= this date
                - end_date: Filter occurrences with start_at <= this date
        
        Returns:
            List of EventTemplate and EventOccurrence records matching filters
        """
        # Load templates
        templates_data = self._read_json_safe(self.event_templates_file)
        templates = [self._deserialize_event_template(record) for record in templates_data.values()]
        
        # Load occurrences
        occurrences_data = self._read_json_safe(self.event_occurrences_file)
        occurrences = [self._deserialize_event_occurrence(record) for record in occurrences_data.values()]
        
        # Combine and filter
        events = templates + occurrences
        
        if filters:
            if "status" in filters:
                events = [e for e in events if e.status == filters["status"]]
            if "provider_id" in filters:
                events = [e for e in events if e.provider_id == filters["provider_id"]]
            
            # Date filtering only applies to occurrences
            if "start_date" in filters or "end_date" in filters:
                start_date = filters.get("start_date")
                end_date = filters.get("end_date")
                
                filtered_events = []
                for e in events:
                    # Always keep templates
                    if isinstance(e, EventTemplate):
                        filtered_events.append(e)
                    # Filter occurrences by date range
                    elif isinstance(e, EventOccurrence) and e.start_at:
                        keep = True
                        if start_date and e.start_at < start_date:
                            keep = False
                        if end_date and e.start_at > end_date:
                            keep = False
                        if keep:
                            filtered_events.append(e)
                
                events = filtered_events
        
        return events
    
    def archive_snapshot(self, timestamp: str) -> None:
        """Archive current state to timestamped backup.
        
        Args:
            timestamp: Timestamp string for archive directory name
        """
        # Archive directory is sibling to base_path (data/current -> data/archive)
        archive_base = self.base_path.parent / "archive"
        archive_dir = archive_base / timestamp
        archive_dir.mkdir(parents=True, exist_ok=True)
        
        # Copy current files to archive
        for file_path in [
            self.providers_file,
            self.locations_file,
            self.event_templates_file,
            self.event_occurrences_file
        ]:
            if file_path.exists():
                archive_file = archive_dir / file_path.name
                archive_file.write_text(file_path.read_text())
    
    def _write_json_atomic(self, file_path: Path, data: dict) -> None:
        """Write JSON data atomically using temp file and rename.
        
        Args:
            file_path: Target file path
            data: Dictionary to write as JSON
        """
        temp_path = file_path.with_suffix('.tmp')
        
        try:
            # Write to temp file
            with open(temp_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, default=str, ensure_ascii=False)
            
            # Atomic rename
            temp_path.replace(file_path)
        except Exception as e:
            # Clean up temp file on error
            if temp_path.exists():
                temp_path.unlink()
            raise IOError(f"Failed to write {file_path}: {e}")
    
    def _read_json_safe(self, file_path: Path) -> dict:
        """Read JSON file with graceful handling of missing/corrupted files.
        
        Args:
            file_path: Path to JSON file
        
        Returns:
            Dictionary from JSON file, or empty dict if file missing/corrupted
        """
        if not file_path.exists():
            return {}
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if not isinstance(data, dict):
                    return {}
                return data
        except (json.JSONDecodeError, IOError):
            # Return empty dict for corrupted files
            return {}
    
    def _serialize_provider(self, provider: Provider) -> dict:
        """Convert Provider to JSON-serializable dict."""
        return {
            "provider_id": provider.provider_id,
            "provider_name": provider.provider_name,
            "provider_slug": provider.provider_slug,
            "provider_website": provider.provider_website,
            "provider_contact_email": provider.provider_contact_email,
            "source_name": provider.source_name,
            "source_base_url": provider.source_base_url,
            "status": provider.status,
            "first_seen_at": provider.first_seen_at.isoformat() if provider.first_seen_at else None,
            "last_seen_at": provider.last_seen_at.isoformat() if provider.last_seen_at else None,
            "metadata": provider.metadata
        }
    
    def _deserialize_provider(self, data: dict) -> Provider:
        """Convert dict to Provider instance."""
        return Provider(
            provider_id=data["provider_id"],
            provider_name=data["provider_name"],
            provider_slug=data["provider_slug"],
            provider_website=data.get("provider_website"),
            provider_contact_email=data.get("provider_contact_email"),
            source_name=data["source_name"],
            source_base_url=data["source_base_url"],
            status=data.get("status", "active"),
            first_seen_at=datetime.fromisoformat(data["first_seen_at"]) if data.get("first_seen_at") else None,
            last_seen_at=datetime.fromisoformat(data["last_seen_at"]) if data.get("last_seen_at") else None,
            metadata=data.get("metadata", {})
        )
    
    def _serialize_location(self, location: Location) -> dict:
        """Convert Location to JSON-serializable dict."""
        return {
            "location_id": location.location_id,
            "provider_id": location.provider_id,
            "provider_name": location.provider_name,
            "location_name": location.location_name,
            "address_line_1": location.address_line_1,
            "address_line_2": location.address_line_2,
            "city": location.city,
            "region": location.region,
            "postcode": location.postcode,
            "country": location.country,
            "formatted_address": location.formatted_address,
            "latitude": location.latitude,
            "longitude": location.longitude,
            "geocode_provider": location.geocode_provider,
            "geocode_status": location.geocode_status,
            "geocode_precision": location.geocode_precision,
            "geocoded_at": location.geocoded_at.isoformat() if location.geocoded_at else None,
            "venue_phone": location.venue_phone,
            "venue_email": location.venue_email,
            "venue_website": location.venue_website,
            "status": location.status,
            "first_seen_at": location.first_seen_at.isoformat() if location.first_seen_at else None,
            "last_seen_at": location.last_seen_at.isoformat() if location.last_seen_at else None,
            "deleted_at": location.deleted_at.isoformat() if location.deleted_at else None,
            "address_hash": location.address_hash
        }
    
    def _deserialize_location(self, data: dict) -> Location:
        """Convert dict to Location instance."""
        return Location(
            location_id=data["location_id"],
            provider_id=data["provider_id"],
            provider_name=data["provider_name"],
            formatted_address=data["formatted_address"],
            country=data.get("country", "UK"),
            location_name=data.get("location_name"),
            address_line_1=data.get("address_line_1"),
            address_line_2=data.get("address_line_2"),
            city=data.get("city"),
            region=data.get("region"),
            postcode=data.get("postcode"),
            latitude=data.get("latitude"),
            longitude=data.get("longitude"),
            geocode_provider=data.get("geocode_provider"),
            geocode_status=data.get("geocode_status", "not_geocoded"),
            geocode_precision=data.get("geocode_precision"),
            geocoded_at=datetime.fromisoformat(data["geocoded_at"]) if data.get("geocoded_at") else None,
            venue_phone=data.get("venue_phone"),
            venue_email=data.get("venue_email"),
            venue_website=data.get("venue_website"),
            status=data.get("status", "active"),
            first_seen_at=datetime.fromisoformat(data["first_seen_at"]) if data.get("first_seen_at") else None,
            last_seen_at=datetime.fromisoformat(data["last_seen_at"]) if data.get("last_seen_at") else None,
            deleted_at=datetime.fromisoformat(data["deleted_at"]) if data.get("deleted_at") else None,
            address_hash=data.get("address_hash")
        )
    
    def _serialize_event_template(self, template: EventTemplate) -> dict:
        """Convert EventTemplate to JSON-serializable dict."""
        return {
            "event_template_id": template.event_template_id,
            "provider_id": template.provider_id,
            "source_template_id": template.source_template_id,
            "title": template.title,
            "slug": template.slug,
            "category": template.category,
            "sub_category": template.sub_category,
            "description_raw": template.description_raw,
            "description_clean": template.description_clean,
            "description_ai": template.description_ai,
            "summary_short": template.summary_short,
            "summary_medium": template.summary_medium,
            "tags": template.tags,
            "occasion_tags": template.occasion_tags,
            "skills_required": template.skills_required,
            "skills_created": template.skills_created,
            "age_min": template.age_min,
            "age_max": template.age_max,
            "audience": template.audience,
            "family_friendly": template.family_friendly,
            "beginner_friendly": template.beginner_friendly,
            "duration_minutes": template.duration_minutes,
            "price_from": template.price_from,
            "currency": template.currency,
            "source_url": template.source_url,
            "image_urls": template.image_urls,
            "location_scope": template.location_scope,
            "status": template.status,
            "first_seen_at": template.first_seen_at.isoformat() if template.first_seen_at else None,
            "last_seen_at": template.last_seen_at.isoformat() if template.last_seen_at else None,
            "deleted_at": template.deleted_at.isoformat() if template.deleted_at else None,
            "source_hash": template.source_hash,
            "record_hash": template.record_hash
        }
    
    def _deserialize_event_template(self, data: dict) -> EventTemplate:
        """Convert dict to EventTemplate instance."""
        return EventTemplate(
            event_template_id=data["event_template_id"],
            provider_id=data["provider_id"],
            title=data["title"],
            slug=data["slug"],
            currency=data.get("currency", "GBP"),
            source_template_id=data.get("source_template_id"),
            category=data.get("category"),
            sub_category=data.get("sub_category"),
            description_raw=data.get("description_raw"),
            description_clean=data.get("description_clean"),
            description_ai=data.get("description_ai"),
            summary_short=data.get("summary_short"),
            summary_medium=data.get("summary_medium"),
            tags=data.get("tags", []),
            occasion_tags=data.get("occasion_tags", []),
            skills_required=data.get("skills_required", []),
            skills_created=data.get("skills_created", []),
            age_min=data.get("age_min"),
            age_max=data.get("age_max"),
            audience=data.get("audience"),
            family_friendly=data.get("family_friendly", False),
            beginner_friendly=data.get("beginner_friendly", False),
            duration_minutes=data.get("duration_minutes"),
            price_from=data.get("price_from"),
            source_url=data.get("source_url"),
            image_urls=data.get("image_urls", []),
            location_scope=data.get("location_scope"),
            status=data.get("status", "active"),
            first_seen_at=datetime.fromisoformat(data["first_seen_at"]) if data.get("first_seen_at") else None,
            last_seen_at=datetime.fromisoformat(data["last_seen_at"]) if data.get("last_seen_at") else None,
            deleted_at=datetime.fromisoformat(data["deleted_at"]) if data.get("deleted_at") else None,
            source_hash=data.get("source_hash"),
            record_hash=data.get("record_hash")
        )
    
    def _serialize_event_occurrence(self, occurrence: EventOccurrence) -> dict:
        """Convert EventOccurrence to JSON-serializable dict."""
        return {
            "event_id": occurrence.event_id,
            "event_template_id": occurrence.event_template_id,
            "provider_id": occurrence.provider_id,
            "location_id": occurrence.location_id,
            "source_event_id": occurrence.source_event_id,
            "title": occurrence.title,
            "start_at": occurrence.start_at.isoformat() if occurrence.start_at else None,
            "end_at": occurrence.end_at.isoformat() if occurrence.end_at else None,
            "timezone": occurrence.timezone,
            "booking_url": occurrence.booking_url,
            "price": occurrence.price,
            "currency": occurrence.currency,
            "capacity": occurrence.capacity,
            "remaining_spaces": occurrence.remaining_spaces,
            "availability_status": occurrence.availability_status,
            "description_raw": occurrence.description_raw,
            "description_clean": occurrence.description_clean,
            "description_ai": occurrence.description_ai,
            "tags": occurrence.tags,
            "skills_required": occurrence.skills_required,
            "skills_created": occurrence.skills_created,
            "age_min": occurrence.age_min,
            "age_max": occurrence.age_max,
            "status": occurrence.status,
            "first_seen_at": occurrence.first_seen_at.isoformat() if occurrence.first_seen_at else None,
            "last_seen_at": occurrence.last_seen_at.isoformat() if occurrence.last_seen_at else None,
            "deleted_at": occurrence.deleted_at.isoformat() if occurrence.deleted_at else None,
            "source_hash": occurrence.source_hash,
            "record_hash": occurrence.record_hash
        }
    
    def _deserialize_event_occurrence(self, data: dict) -> EventOccurrence:
        """Convert dict to EventOccurrence instance."""
        return EventOccurrence(
            event_id=data["event_id"],
            provider_id=data["provider_id"],
            title=data["title"],
            timezone=data.get("timezone", "Europe/London"),
            currency=data.get("currency", "GBP"),
            availability_status=data.get("availability_status", "unknown"),
            event_template_id=data.get("event_template_id"),
            location_id=data.get("location_id"),
            source_event_id=data.get("source_event_id"),
            start_at=datetime.fromisoformat(data["start_at"]) if data.get("start_at") else None,
            end_at=datetime.fromisoformat(data["end_at"]) if data.get("end_at") else None,
            booking_url=data.get("booking_url"),
            price=data.get("price"),
            capacity=data.get("capacity"),
            remaining_spaces=data.get("remaining_spaces"),
            description_raw=data.get("description_raw"),
            description_clean=data.get("description_clean"),
            description_ai=data.get("description_ai"),
            tags=data.get("tags", []),
            skills_required=data.get("skills_required", []),
            skills_created=data.get("skills_created", []),
            age_min=data.get("age_min"),
            age_max=data.get("age_max"),
            status=data.get("status", "active"),
            first_seen_at=datetime.fromisoformat(data["first_seen_at"]) if data.get("first_seen_at") else None,
            last_seen_at=datetime.fromisoformat(data["last_seen_at"]) if data.get("last_seen_at") else None,
            deleted_at=datetime.fromisoformat(data["deleted_at"]) if data.get("deleted_at") else None,
            source_hash=data.get("source_hash"),
            record_hash=data.get("record_hash")
        )
