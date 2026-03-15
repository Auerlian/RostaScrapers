"""CSV export functionality for events and locations."""

import csv
from pathlib import Path
from typing import Any

from src.models.event_template import EventTemplate
from src.models.event_occurrence import EventOccurrence
from src.models.location import Location
from src.storage.store import CanonicalStore
from src.export.formatters import (
    format_list,
    format_null,
    format_boolean,
    format_datetime
)


class CSVExporter:
    """Exports canonical data to CSV files."""
    
    def __init__(self, store: CanonicalStore):
        """Initialize CSV exporter with canonical store.
        
        Args:
            store: CanonicalStore instance for loading data
        """
        self.store = store
    
    def export_events(
        self,
        output_path: str,
        filters: dict = None
    ) -> dict:
        """Export events to CSV with optional filtering.
        
        Generates events.csv containing both event templates and event occurrences.
        Each row has a record_type field ("template" or "occurrence") and record_id
        field containing the canonical ID for that row.
        
        Args:
            output_path: Path to output CSV file
            filters: Optional dict with filter criteria (default: {"status": "active"})
        
        Returns:
            Dict with export statistics:
                - total_records: Total records exported
                - template_count: Number of template records
                - occurrence_count: Number of occurrence records
                - output_file: Path to generated CSV file
        """
        # Default to active records only
        if filters is None:
            filters = {"status": "active"}
        
        # Load events from store
        events = self.store.load_events(filters=filters)
        
        # Load locations for denormalization
        locations = self.store.load_locations()
        location_map = {loc.location_id: loc for loc in locations}
        
        # Load providers for denormalization
        providers = self.store.load_providers()
        provider_map = {p.provider_id: p for p in providers}
        
        # Separate templates and occurrences
        templates = [e for e in events if isinstance(e, EventTemplate)]
        occurrences = [e for e in events if isinstance(e, EventOccurrence)]
        
        # Write CSV
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=self._get_event_columns())
            writer.writeheader()
            
            # Write template rows
            for template in templates:
                row = self._format_template_row(template, location_map, provider_map)
                writer.writerow(row)
            
            # Write occurrence rows
            for occurrence in occurrences:
                row = self._format_occurrence_row(occurrence, location_map, provider_map)
                writer.writerow(row)
        
        return {
            "total_records": len(events),
            "template_count": len(templates),
            "occurrence_count": len(occurrences),
            "output_file": str(output_file)
        }
    
    def _get_event_columns(self) -> list[str]:
        """Get ordered list of column names for events CSV.
        
        Returns:
            List of column names in export order
        """
        return [
            "record_type",
            "record_id",
            "event_template_id",
            "provider_id",
            "provider_name",
            "title",
            "slug",
            "category",
            "sub_category",
            "description_raw",
            "description_clean",
            "description_ai",
            "summary_short",
            "summary_medium",
            "tags",
            "occasion_tags",
            "skills_required",
            "skills_created",
            "age_min",
            "age_max",
            "audience",
            "family_friendly",
            "beginner_friendly",
            "duration_minutes",
            "price",
            "currency",
            "location_id",
            "location_name",
            "formatted_address",
            "start_at",
            "end_at",
            "timezone",
            "booking_url",
            "source_url",
            "image_urls",
            "capacity",
            "remaining_spaces",
            "availability_status",
            "status",
            "first_seen_at",
            "last_seen_at"
        ]
    
    def _format_template_row(
        self,
        template: EventTemplate,
        location_map: dict[str, Location],
        provider_map: dict[str, Any]
    ) -> dict[str, Any]:
        """Format EventTemplate as CSV row.
        
        Args:
            template: EventTemplate instance
            location_map: Dict mapping location_id to Location instances
            provider_map: Dict mapping provider_id to Provider instances
        
        Returns:
            Dict with CSV column values
        """
        # Get provider name for denormalization
        provider = provider_map.get(template.provider_id)
        provider_name = provider.provider_name if provider else ""
        
        # Templates don't have location_id field, so location fields are empty
        return {
            "record_type": "template",
            "record_id": template.event_template_id,
            "event_template_id": "",  # Empty for template rows (not self-referential)
            "provider_id": template.provider_id,
            "provider_name": provider_name,
            "title": template.title,
            "slug": template.slug,
            "category": format_null(template.category),
            "sub_category": format_null(template.sub_category),
            "description_raw": format_null(template.description_raw),
            "description_clean": format_null(template.description_clean),
            "description_ai": format_null(template.description_ai),
            "summary_short": format_null(template.summary_short),
            "summary_medium": format_null(template.summary_medium),
            "tags": format_list(template.tags),
            "occasion_tags": format_list(template.occasion_tags),
            "skills_required": format_list(template.skills_required),
            "skills_created": format_list(template.skills_created),
            "age_min": format_null(template.age_min),
            "age_max": format_null(template.age_max),
            "audience": format_null(template.audience),
            "family_friendly": format_boolean(template.family_friendly),
            "beginner_friendly": format_boolean(template.beginner_friendly),
            "duration_minutes": format_null(template.duration_minutes),
            "price": format_null(template.price_from),
            "currency": template.currency,
            "location_id": "",  # Templates don't have specific locations
            "location_name": "",
            "formatted_address": "",
            "start_at": "",  # Templates don't have dates
            "end_at": "",
            "timezone": "",
            "booking_url": "",  # Templates typically don't have booking URLs
            "source_url": format_null(template.source_url),
            "image_urls": format_list(template.image_urls),
            "capacity": "",  # Templates don't have capacity
            "remaining_spaces": "",
            "availability_status": "",  # Templates don't have availability status
            "status": template.status,
            "first_seen_at": format_datetime(template.first_seen_at),
            "last_seen_at": format_datetime(template.last_seen_at)
        }
    
    def _format_occurrence_row(
        self,
        occurrence: EventOccurrence,
        location_map: dict[str, Location],
        provider_map: dict[str, Any]
    ) -> dict[str, Any]:
        """Format EventOccurrence as CSV row.
        
        Args:
            occurrence: EventOccurrence instance
            location_map: Dict mapping location_id to Location instances
            provider_map: Dict mapping provider_id to Provider instances
        
        Returns:
            Dict with CSV column values
        """
        # Get location details for denormalization
        location = location_map.get(occurrence.location_id) if occurrence.location_id else None
        
        # Get provider name for denormalization
        provider = provider_map.get(occurrence.provider_id)
        provider_name = provider.provider_name if provider else ""
        
        return {
            "record_type": "occurrence",
            "record_id": occurrence.event_id,
            "event_template_id": format_null(occurrence.event_template_id),
            "provider_id": occurrence.provider_id,
            "provider_name": provider_name,
            "title": occurrence.title,
            "slug": "",  # Occurrences don't have slugs
            "category": "",  # Occurrences don't have category (inherited from template)
            "sub_category": "",
            "description_raw": format_null(occurrence.description_raw),
            "description_clean": format_null(occurrence.description_clean),
            "description_ai": format_null(occurrence.description_ai),
            "summary_short": "",  # Occurrences don't have summaries
            "summary_medium": "",
            "tags": format_list(occurrence.tags),
            "occasion_tags": "",  # Occurrences don't have occasion_tags
            "skills_required": format_list(occurrence.skills_required),
            "skills_created": format_list(occurrence.skills_created),
            "age_min": format_null(occurrence.age_min),
            "age_max": format_null(occurrence.age_max),
            "audience": "",  # Occurrences don't have audience field
            "family_friendly": "",
            "beginner_friendly": "",
            "duration_minutes": "",  # Occurrences don't have duration (use start_at/end_at)
            "price": format_null(occurrence.price),
            "currency": occurrence.currency,
            "location_id": format_null(occurrence.location_id),
            "location_name": format_null(location.location_name if location else None),
            "formatted_address": format_null(location.formatted_address if location else None),
            "start_at": format_datetime(occurrence.start_at),
            "end_at": format_datetime(occurrence.end_at),
            "timezone": occurrence.timezone,
            "booking_url": format_null(occurrence.booking_url),
            "source_url": "",  # Occurrences don't have source_url
            "image_urls": "",  # Occurrences don't have image_urls
            "capacity": format_null(occurrence.capacity),
            "remaining_spaces": format_null(occurrence.remaining_spaces),
            "availability_status": occurrence.availability_status,
            "status": occurrence.status,
            "first_seen_at": format_datetime(occurrence.first_seen_at),
            "last_seen_at": format_datetime(occurrence.last_seen_at)
        }


    def export_locations(
        self,
        output_path: str,
        filters: dict = None
    ) -> dict:
        """Export locations to CSV with event summaries.

        Generates locations.csv containing location data optimized for map display,
        including geocoded coordinates, event counts, and event summaries.

        Args:
            output_path: Path to output CSV file
            filters: Optional dict with filter criteria (default: {"status": "active"})

        Returns:
            Dict with export statistics:
                - total_records: Total location records exported
                - with_coordinates: Number of locations with valid coordinates
                - output_file: Path to generated CSV file
        """
        # Default to active records only
        if filters is None:
            filters = {"status": "active"}

        # Load locations from store
        locations = self.store.load_locations(filters=filters)

        # Load all events (active only) for event summaries
        events = self.store.load_events(filters={"status": "active"})

        # Load providers for denormalization
        providers = self.store.load_providers()
        provider_map = {p.provider_id: p for p in providers}

        # Build event summaries per location
        location_events = self._build_location_event_summaries(locations, events)

        # Write CSV
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=self._get_location_columns())
            writer.writeheader()

            for location in locations:
                row = self._format_location_row(
                    location,
                    provider_map,
                    location_events.get(location.location_id, {})
                )
                writer.writerow(row)

        # Count locations with valid coordinates
        with_coordinates = sum(
            1 for loc in locations
            if loc.latitude is not None and loc.longitude is not None
        )

        return {
            "total_records": len(locations),
            "with_coordinates": with_coordinates,
            "output_file": str(output_file)
        }

    def _get_location_columns(self) -> list[str]:
        """Get ordered list of column names for locations CSV.

        Returns:
            List of column names in export order
        """
        return [
            "location_id",
            "provider_id",
            "provider_name",
            "location_name",
            "formatted_address",
            "address_line_1",
            "address_line_2",
            "city",
            "region",
            "postcode",
            "country",
            "latitude",
            "longitude",
            "geocode_status",
            "geocode_precision",
            "geocode_provider",
            "geocoded_at",
            "venue_phone",
            "venue_email",
            "venue_website",
            "event_count",
            "active_event_count",
            "event_names",
            "active_event_ids",
            "status",
            "first_seen_at",
            "last_seen_at"
        ]

    def _build_location_event_summaries(
        self,
        locations: list[Location],
        events: list[EventTemplate | EventOccurrence]
    ) -> dict[str, dict]:
        """Build event summaries for each location.

        Args:
            locations: List of Location instances
            events: List of active event instances (templates and occurrences)

        Returns:
            Dict mapping location_id to event summary dict with keys:
                - event_count: Total events at this location
                - active_event_count: Active events at this location
                - event_names: List of event titles (truncated)
                - active_event_ids: List of active event IDs
        """
        location_summaries = {}

        for location in locations:
            # Find events linked to this location
            location_events = []
            for event in events:
                # Check if event is linked to this location
                event_location_id = None
                if isinstance(event, EventOccurrence):
                    event_location_id = event.location_id
                # EventTemplates don't have location_id field

                if event_location_id == location.location_id:
                    location_events.append(event)

            # Build summary
            event_names = []
            active_event_ids = []

            for event in location_events:
                # Add event title
                event_names.append(event.title)

                # Add event ID
                if isinstance(event, EventTemplate):
                    active_event_ids.append(event.event_template_id)
                elif isinstance(event, EventOccurrence):
                    active_event_ids.append(event.event_id)

            # Truncate event names if too long (keep first 10)
            if len(event_names) > 10:
                event_names = event_names[:10]

            location_summaries[location.location_id] = {
                "event_count": len(location_events),
                "active_event_count": len(location_events),  # All events are active (filtered)
                "event_names": event_names,
                "active_event_ids": active_event_ids
            }

        return location_summaries

    def _format_location_row(
        self,
        location: Location,
        provider_map: dict[str, Any],
        event_summary: dict
    ) -> dict[str, Any]:
        """Format Location as CSV row.

        Args:
            location: Location instance
            provider_map: Dict mapping provider_id to Provider instances
            event_summary: Dict with event count and summary data

        Returns:
            Dict with CSV column values
        """
        # Get provider name for denormalization
        provider = provider_map.get(location.provider_id)
        provider_name = provider.provider_name if provider else ""

        return {
            "location_id": location.location_id,
            "provider_id": location.provider_id,
            "provider_name": provider_name,
            "location_name": format_null(location.location_name),
            "formatted_address": location.formatted_address,
            "address_line_1": format_null(location.address_line_1),
            "address_line_2": format_null(location.address_line_2),
            "city": format_null(location.city),
            "region": format_null(location.region),
            "postcode": format_null(location.postcode),
            "country": location.country,
            "latitude": format_null(location.latitude),
            "longitude": format_null(location.longitude),
            "geocode_status": location.geocode_status,
            "geocode_precision": format_null(location.geocode_precision),
            "geocode_provider": format_null(location.geocode_provider),
            "geocoded_at": format_datetime(location.geocoded_at),
            "venue_phone": format_null(location.venue_phone),
            "venue_email": format_null(location.venue_email),
            "venue_website": format_null(location.venue_website),
            "event_count": str(event_summary.get("event_count", 0)),
            "active_event_count": str(event_summary.get("active_event_count", 0)),
            "event_names": format_list(event_summary.get("event_names", [])),
            "active_event_ids": format_list(event_summary.get("active_event_ids", [])),
            "status": location.status,
            "first_seen_at": format_datetime(location.first_seen_at),
            "last_seen_at": format_datetime(location.last_seen_at)
        }

    def validate_export(
        self,
        events_csv_path: str,
        locations_csv_path: str
    ) -> dict:
        """Validate export completeness and correctness.
        
        Checks that:
        - All active records from the store appear in export files
        - No duplicate records exist in exports
        - CSV files are valid and parseable
        
        Args:
            events_csv_path: Path to events.csv file
            locations_csv_path: Path to locations.csv file
        
        Returns:
            Dict with validation results:
                - valid: Boolean indicating if validation passed
                - errors: List of error messages (empty if valid)
                - events_validation: Dict with events-specific validation results
                - locations_validation: Dict with locations-specific validation results
        
        Raises:
            FileNotFoundError: If CSV files don't exist
        """
        errors = []
        
        # Validate events export
        events_validation = self._validate_events_export(events_csv_path)
        errors.extend(events_validation["errors"])
        
        # Validate locations export
        locations_validation = self._validate_locations_export(locations_csv_path)
        errors.extend(locations_validation["errors"])
        
        return {
            "valid": len(errors) == 0,
            "errors": errors,
            "events_validation": events_validation,
            "locations_validation": locations_validation
        }
    
    def _validate_events_export(self, csv_path: str) -> dict:
        """Validate events CSV export.
        
        Args:
            csv_path: Path to events.csv file
        
        Returns:
            Dict with validation results:
                - parseable: Boolean indicating if CSV is parseable
                - active_records_count: Number of active records in store
                - csv_records_count: Number of records in CSV
                - missing_records: List of record IDs missing from CSV
                - duplicate_records: List of duplicate record IDs in CSV
                - errors: List of error messages
        """
        errors = []
        csv_path_obj = Path(csv_path)
        
        # Check if file exists
        if not csv_path_obj.exists():
            errors.append(f"Events CSV file not found: {csv_path}")
            return {
                "parseable": False,
                "active_records_count": 0,
                "csv_records_count": 0,
                "missing_records": [],
                "duplicate_records": [],
                "errors": errors
            }
        
        # Load active records from store
        active_events = self.store.load_events(filters={"status": "active"})
        active_ids = set()
        for event in active_events:
            if isinstance(event, EventTemplate):
                active_ids.add(event.event_template_id)
            elif isinstance(event, EventOccurrence):
                active_ids.add(event.event_id)
        
        # Parse CSV and collect record IDs
        csv_ids = []
        parseable = True
        
        try:
            with open(csv_path_obj, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                
                # Validate required columns exist
                if 'record_id' not in reader.fieldnames:
                    errors.append("Events CSV missing required 'record_id' column")
                    parseable = False
                else:
                    for row in reader:
                        record_id = row.get('record_id', '').strip()
                        if record_id:
                            csv_ids.append(record_id)
        except csv.Error as e:
            errors.append(f"Events CSV parsing error: {str(e)}")
            parseable = False
        except Exception as e:
            errors.append(f"Events CSV read error: {str(e)}")
            parseable = False
        
        # Check for missing records
        csv_ids_set = set(csv_ids)
        missing_records = list(active_ids - csv_ids_set)
        if missing_records:
            errors.append(
                f"Events CSV missing {len(missing_records)} active records: "
                f"{', '.join(missing_records[:5])}"
                + ("..." if len(missing_records) > 5 else "")
            )
        
        # Check for duplicate records
        duplicate_records = []
        seen = set()
        for record_id in csv_ids:
            if record_id in seen:
                duplicate_records.append(record_id)
            seen.add(record_id)
        
        if duplicate_records:
            unique_duplicates = list(set(duplicate_records))
            errors.append(
                f"Events CSV contains {len(duplicate_records)} duplicate records: "
                f"{', '.join(unique_duplicates[:5])}"
                + ("..." if len(unique_duplicates) > 5 else "")
            )
        
        return {
            "parseable": parseable,
            "active_records_count": len(active_ids),
            "csv_records_count": len(csv_ids),
            "missing_records": missing_records,
            "duplicate_records": list(set(duplicate_records)),
            "errors": errors
        }
    
    def _validate_locations_export(self, csv_path: str) -> dict:
        """Validate locations CSV export.
        
        Args:
            csv_path: Path to locations.csv file
        
        Returns:
            Dict with validation results:
                - parseable: Boolean indicating if CSV is parseable
                - active_records_count: Number of active records in store
                - csv_records_count: Number of records in CSV
                - missing_records: List of record IDs missing from CSV
                - duplicate_records: List of duplicate record IDs in CSV
                - errors: List of error messages
        """
        errors = []
        csv_path_obj = Path(csv_path)
        
        # Check if file exists
        if not csv_path_obj.exists():
            errors.append(f"Locations CSV file not found: {csv_path}")
            return {
                "parseable": False,
                "active_records_count": 0,
                "csv_records_count": 0,
                "missing_records": [],
                "duplicate_records": [],
                "errors": errors
            }
        
        # Load active records from store
        active_locations = self.store.load_locations(filters={"status": "active"})
        active_ids = {loc.location_id for loc in active_locations}
        
        # Parse CSV and collect record IDs
        csv_ids = []
        parseable = True
        
        try:
            with open(csv_path_obj, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                
                # Validate required columns exist
                if 'location_id' not in reader.fieldnames:
                    errors.append("Locations CSV missing required 'location_id' column")
                    parseable = False
                else:
                    for row in reader:
                        location_id = row.get('location_id', '').strip()
                        if location_id:
                            csv_ids.append(location_id)
        except csv.Error as e:
            errors.append(f"Locations CSV parsing error: {str(e)}")
            parseable = False
        except Exception as e:
            errors.append(f"Locations CSV read error: {str(e)}")
            parseable = False
        
        # Check for missing records
        csv_ids_set = set(csv_ids)
        missing_records = list(active_ids - csv_ids_set)
        if missing_records:
            errors.append(
                f"Locations CSV missing {len(missing_records)} active records: "
                f"{', '.join(missing_records[:5])}"
                + ("..." if len(missing_records) > 5 else "")
            )
        
        # Check for duplicate records
        duplicate_records = []
        seen = set()
        for location_id in csv_ids:
            if location_id in seen:
                duplicate_records.append(location_id)
            seen.add(location_id)
        
        if duplicate_records:
            unique_duplicates = list(set(duplicate_records))
            errors.append(
                f"Locations CSV contains {len(duplicate_records)} duplicate records: "
                f"{', '.join(unique_duplicates[:5])}"
                + ("..." if len(unique_duplicates) > 5 else "")
            )
        
        return {
            "parseable": parseable,
            "active_records_count": len(active_ids),
            "csv_records_count": len(csv_ids),
            "missing_records": missing_records,
            "duplicate_records": list(set(duplicate_records)),
            "errors": errors
        }
