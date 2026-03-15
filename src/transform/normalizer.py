"""
Normalizer for transforming raw provider data into canonical models.

This module transforms provider-specific raw data into canonical data models
with deterministic IDs, normalized fields, and computed hashes for change detection.
"""

import re
from datetime import datetime, timezone
from typing import Any
from bs4 import BeautifulSoup

from src.models.provider import Provider
from src.models.location import Location
from src.models.event_template import EventTemplate
from src.models.event_occurrence import EventOccurrence
from src.models.raw_provider_data import RawProviderData
from src.transform.id_generator import (
    generate_provider_id,
    generate_location_id,
    generate_event_template_id,
    generate_event_occurrence_id,
    slugify,
)
from src.transform.hash_computer import (
    compute_source_hash,
    compute_record_hash,
    compute_address_hash,
    EVENT_TEMPLATE_SOURCE_FIELDS,
    EVENT_OCCURRENCE_SOURCE_FIELDS,
    LOCATION_SOURCE_FIELDS,
)


class Normalizer:
    """Transforms raw provider data into canonical models."""
    
    def normalize_provider(self, raw_data: RawProviderData) -> Provider:
        """
        Create canonical Provider record from raw data.
        
        Args:
            raw_data: Raw provider data from scraper
            
        Returns:
            Canonical Provider record with deterministic ID
            
        Example:
            >>> raw_data = RawProviderData(
            ...     provider_name="Pasta Evangelists",
            ...     provider_website="https://pastaevangelists.com",
            ...     source_name="Pasta Evangelists API",
            ...     source_base_url="https://api.pastaevangelists.com"
            ... )
            >>> normalizer = Normalizer()
            >>> provider = normalizer.normalize_provider(raw_data)
            >>> provider.provider_id
            'provider-pasta-evangelists'
        """
        provider_id = generate_provider_id(raw_data.provider_name)
        provider_slug = provider_id.replace("provider-", "")
        
        now = datetime.now(timezone.utc)
        
        return Provider(
            provider_id=provider_id,
            provider_name=raw_data.provider_name,
            provider_slug=provider_slug,
            provider_website=raw_data.provider_website,
            provider_contact_email=raw_data.provider_contact_email,
            source_name=raw_data.source_name or raw_data.provider_name,
            source_base_url=raw_data.source_base_url or "",
            status="active",
            first_seen_at=now,
            last_seen_at=now,
            metadata=raw_data.provider_metadata,
        )
    
    def normalize_locations(
        self, 
        raw_data: RawProviderData, 
        provider_id: str
    ) -> list[Location]:
        """
        Create canonical Location records from raw data.
        
        Generates deterministic location IDs based on provider slug and
        normalized address. Computes address hash for geocoding cache lookups.
        
        Args:
            raw_data: Raw provider data from scraper
            provider_id: Canonical provider ID
            
        Returns:
            List of canonical Location records
            
        Example:
            >>> raw_data = RawProviderData(
            ...     provider_name="Pasta Evangelists",
            ...     raw_locations=[{
            ...         "location_name": "The Pasta Academy",
            ...         "formatted_address": "123 Main St, London, EC1A 9EJ",
            ...         "address_line_1": "123 Main St",
            ...         "city": "London",
            ...         "postcode": "EC1A 9EJ"
            ...     }]
            ... )
            >>> normalizer = Normalizer()
            >>> locations = normalizer.normalize_locations(raw_data, "provider-pasta-evangelists")
            >>> len(locations)
            1
        """
        locations = []
        provider_slug = provider_id.replace("provider-", "")
        now = datetime.now(timezone.utc)
        
        for raw_loc in raw_data.raw_locations:
            # Extract and normalize address fields
            formatted_address = self._normalize_text(
                raw_loc.get("formatted_address") or ""
            )
            
            if not formatted_address:
                # Try to build formatted address from components
                # Note: location_name alone is not sufficient - need actual address
                parts = [
                    raw_loc.get("location_name"),
                    raw_loc.get("address_line_1"),
                    raw_loc.get("address_line_2"),
                    raw_loc.get("city"),
                    raw_loc.get("postcode"),
                ]
                formatted_address = ", ".join(p for p in parts if p)
                
                # If we only have location_name and no actual address components, skip
                has_address_components = any([
                    raw_loc.get("address_line_1"),
                    raw_loc.get("city"),
                    raw_loc.get("postcode")
                ])
                if not has_address_components:
                    continue
            
            if not formatted_address:
                continue  # Skip locations without address
            
            # Generate deterministic location ID
            location_id = generate_location_id(provider_slug, formatted_address)
            
            # Create location record
            location = Location(
                location_id=location_id,
                provider_id=provider_id,
                provider_name=raw_data.provider_name,
                location_name=self._normalize_text(raw_loc.get("location_name")),
                address_line_1=self._normalize_text(raw_loc.get("address_line_1")),
                address_line_2=self._normalize_text(raw_loc.get("address_line_2")),
                city=self._normalize_text(raw_loc.get("city")),
                region=self._normalize_text(raw_loc.get("region")),
                postcode=self._normalize_text(raw_loc.get("postcode")),
                country=raw_loc.get("country", "UK"),
                formatted_address=formatted_address,
                venue_phone=self._normalize_text(raw_loc.get("venue_phone")),
                venue_email=self._normalize_text(raw_loc.get("venue_email")),
                venue_website=self._normalize_text(raw_loc.get("venue_website")),
                geocode_status="not_geocoded",
                status="active",
                first_seen_at=now,
                last_seen_at=now,
            )
            
            # Compute address hash for geocoding cache
            location_dict = {
                "address_line_1": location.address_line_1,
                "address_line_2": location.address_line_2,
                "city": location.city,
                "region": location.region,
                "postcode": location.postcode,
                "country": location.country,
            }
            location.address_hash = compute_address_hash(location_dict)
            
            locations.append(location)
        
        return locations
    
    def normalize_events(
        self, 
        raw_data: RawProviderData, 
        provider_id: str,
        location_map: dict[str, str]
    ) -> list[EventTemplate | EventOccurrence]:
        """
        Create canonical Event records with location links.
        
        Processes both templates and occurrences from raw data. Links events
        to locations using the location_map. Computes source and record hashes
        for change detection.
        
        Args:
            raw_data: Raw provider data from scraper
            provider_id: Canonical provider ID
            location_map: Mapping from raw location identifiers to canonical location IDs
            
        Returns:
            List of canonical EventTemplate and EventOccurrence records
            
        Example:
            >>> raw_data = RawProviderData(
            ...     provider_name="Pasta Evangelists",
            ...     raw_templates=[{
            ...         "title": "Pasta Making Class",
            ...         "description": "Learn to make pasta",
            ...         "price": 50.0
            ...     }]
            ... )
            >>> normalizer = Normalizer()
            >>> events = normalizer.normalize_events(raw_data, "provider-pasta-evangelists", {})
            >>> len(events)
            1
        """
        events = []
        provider_slug = provider_id.replace("provider-", "")
        
        # Process templates
        for raw_template in raw_data.raw_templates:
            template = self._normalize_template(
                raw_template, provider_id, provider_slug, location_map
            )
            if template:
                events.append(template)
        
        # Process occurrences
        for raw_event in raw_data.raw_events:
            occurrence = self._normalize_occurrence(
                raw_event, provider_id, provider_slug, location_map
            )
            if occurrence:
                events.append(occurrence)
        
        return events
    
    def _normalize_template(
        self,
        raw_template: dict[str, Any],
        provider_id: str,
        provider_slug: str,
        location_map: dict[str, str]
    ) -> EventTemplate | None:
        """Normalize a raw template into an EventTemplate record."""
        # Extract and normalize title
        title = self._normalize_text(raw_template.get("title"))
        if not title:
            return None  # Skip templates without title
        
        # Generate deterministic ID
        source_template_id = raw_template.get("source_template_id") or raw_template.get("template_id")
        event_template_id = generate_event_template_id(
            provider_slug, source_template_id, title
        )
        
        # Generate slug
        slug = slugify(title)
        
        # Process descriptions
        description_raw = raw_template.get("description")
        description_clean = self._strip_html(description_raw) if description_raw else None
        
        # Parse price
        price_from = self._parse_price(raw_template.get("price"))
        
        # Extract image URLs
        image_urls = self._extract_image_urls(raw_template)
        
        # Parse duration
        duration_minutes = self._parse_duration(raw_template.get("duration"))
        
        # Parse age restrictions
        age_min = self._parse_int(raw_template.get("age_min"))
        age_max = self._parse_int(raw_template.get("age_max"))
        
        # Extract tags
        tags = self._extract_list(raw_template.get("tags"))
        
        now = datetime.now(timezone.utc)
        
        # Create template record
        template = EventTemplate(
            event_template_id=event_template_id,
            provider_id=provider_id,
            source_template_id=source_template_id,
            title=title,
            slug=slug,
            category=self._normalize_text(raw_template.get("category")),
            sub_category=self._normalize_text(raw_template.get("sub_category")),
            description_raw=description_raw,
            description_clean=description_clean,
            tags=tags,
            occasion_tags=self._extract_list(raw_template.get("occasion_tags")),
            skills_required=self._extract_list(raw_template.get("skills_required")),
            skills_created=self._extract_list(raw_template.get("skills_created")),
            age_min=age_min,
            age_max=age_max,
            audience=self._normalize_text(raw_template.get("audience")),
            family_friendly=bool(raw_template.get("family_friendly")),
            beginner_friendly=bool(raw_template.get("beginner_friendly")),
            duration_minutes=duration_minutes,
            price_from=price_from,
            currency=raw_template.get("currency", "GBP"),
            source_url=self._normalize_text(raw_template.get("source_url")),
            image_urls=image_urls,
            location_scope=self._normalize_text(raw_template.get("location_scope")),
            status="active",
            first_seen_at=now,
            last_seen_at=now,
        )
        
        # Compute hashes
        template_dict = self._template_to_dict(template)
        template.source_hash = compute_source_hash(
            template_dict, EVENT_TEMPLATE_SOURCE_FIELDS
        )
        template.record_hash = compute_record_hash(template_dict)
        
        return template
    
    def _normalize_occurrence(
        self,
        raw_event: dict[str, Any],
        provider_id: str,
        provider_slug: str,
        location_map: dict[str, str]
    ) -> EventOccurrence | None:
        """Normalize a raw event into an EventOccurrence record."""
        # Extract and normalize title
        title = self._normalize_text(raw_event.get("title"))
        if not title:
            return None  # Skip events without title
        
        # Parse dates
        start_at = self._parse_datetime(raw_event.get("start_at"))
        end_at = self._parse_datetime(raw_event.get("end_at"))
        
        # Determine location ID
        location_id = self._resolve_location_id(raw_event, location_map)
        
        # Generate deterministic ID
        source_event_id = raw_event.get("source_event_id") or raw_event.get("event_id")
        event_id = generate_event_occurrence_id(
            provider_slug, source_event_id, title, location_id, start_at
        )
        
        # Process descriptions
        description_raw = raw_event.get("description")
        description_clean = self._strip_html(description_raw) if description_raw else None
        
        # Parse price
        price = self._parse_price(raw_event.get("price"))
        
        # Parse capacity fields
        capacity = self._parse_int(raw_event.get("capacity"))
        remaining_spaces = self._parse_int(raw_event.get("remaining_spaces"))
        
        # Extract tags
        tags = self._extract_list(raw_event.get("tags"))
        
        # Parse age restrictions
        age_min = self._parse_int(raw_event.get("age_min"))
        age_max = self._parse_int(raw_event.get("age_max"))
        
        now = datetime.now(timezone.utc)
        
        # Create occurrence record
        occurrence = EventOccurrence(
            event_id=event_id,
            provider_id=provider_id,
            event_template_id=self._normalize_text(raw_event.get("event_template_id")),
            location_id=location_id,
            source_event_id=source_event_id,
            title=title,
            start_at=start_at,
            end_at=end_at,
            timezone=raw_event.get("timezone", "Europe/London"),
            booking_url=self._normalize_text(raw_event.get("booking_url")),
            price=price,
            currency=raw_event.get("currency", "GBP"),
            capacity=capacity,
            remaining_spaces=remaining_spaces,
            availability_status=raw_event.get("availability_status", "unknown"),
            description_raw=description_raw,
            description_clean=description_clean,
            tags=tags,
            skills_required=self._extract_list(raw_event.get("skills_required")),
            skills_created=self._extract_list(raw_event.get("skills_created")),
            age_min=age_min,
            age_max=age_max,
            status="active",
            first_seen_at=now,
            last_seen_at=now,
        )
        
        # Compute hashes
        occurrence_dict = self._occurrence_to_dict(occurrence)
        occurrence.source_hash = compute_source_hash(
            occurrence_dict, EVENT_OCCURRENCE_SOURCE_FIELDS
        )
        occurrence.record_hash = compute_record_hash(occurrence_dict)
        
        return occurrence
    
    def _resolve_location_id(
        self,
        raw_event: dict[str, Any],
        location_map: dict[str, str]
    ) -> str | None:
        """
        Resolve location ID from raw event data.
        
        Tries multiple strategies:
        1. Direct location_id field
        2. Location data embedded in event
        3. Location reference via location_map
        
        Args:
            raw_event: Raw event data
            location_map: Mapping from raw location identifiers to canonical IDs
            
        Returns:
            Canonical location ID or None
        """
        # Strategy 1: Direct location_id
        if "location_id" in raw_event and raw_event["location_id"]:
            return raw_event["location_id"]
        
        # Strategy 2: Embedded location data
        if "location_data" in raw_event:
            loc_data = raw_event["location_data"]
            if isinstance(loc_data, dict):
                formatted_address = loc_data.get("formatted_address")
                if formatted_address:
                    # Look up in location_map by formatted address
                    return location_map.get(formatted_address)
        
        # Strategy 3: Location reference field
        location_ref = raw_event.get("location_ref") or raw_event.get("location_name")
        if location_ref:
            return location_map.get(location_ref)
        
        return None
    
    def _normalize_text(self, value: Any) -> str | None:
        """
        Normalize text field: strip HTML, normalize whitespace, handle nulls.
        
        Args:
            value: Input value (string or None)
            
        Returns:
            Normalized string or None
        """
        if value is None:
            return None
        
        text = str(value).strip()
        if not text:
            return None
        
        # Normalize whitespace
        text = re.sub(r'\s+', ' ', text)
        
        return text
    
    def _strip_html(self, html: str | None) -> str | None:
        """
        Strip HTML tags and normalize whitespace while preserving meaningful structure.
        
        Preserves:
        - Paragraph breaks (p, div, br tags become newlines)
        - List structure (li tags become newlines with bullets)
        
        Args:
            html: HTML string
            
        Returns:
            Plain text with preserved structure or None
        """
        if not html:
            return None
        
        # Use BeautifulSoup to parse HTML
        soup = BeautifulSoup(html, "html.parser")
        
        # Replace block-level elements with newlines to preserve structure
        for tag in soup.find_all(['p', 'div', 'br', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6']):
            tag.insert_after('\n')
        
        # Replace list items with newlines
        for tag in soup.find_all('li'):
            tag.insert_before('\n• ')
        
        # Extract text with newline separator
        text = soup.get_text(separator=' ')
        
        # Normalize whitespace within lines (but preserve newlines)
        lines = text.split('\n')
        lines = [re.sub(r'\s+', ' ', line.strip()) for line in lines]
        
        # Remove empty lines and join
        lines = [line for line in lines if line]
        text = '\n'.join(lines)
        
        return text if text else None
    
    def _parse_price(self, value: Any) -> float | None:
        """
        Parse price from various formats.
        
        Handles:
        - Numeric values: 50, 50.0
        - String with currency: "£50", "$50", "GBP 50"
        - String with decimals: "50.00"
        
        Args:
            value: Price value in various formats
            
        Returns:
            Float price or None
        """
        if value is None:
            return None
        
        # Already a number
        if isinstance(value, (int, float)):
            return float(value)
        
        # Parse from string
        if isinstance(value, str):
            # Remove currency symbols and whitespace
            cleaned = re.sub(r'[£$€\s,]', '', value)
            # Remove currency codes
            cleaned = re.sub(r'(GBP|USD|EUR)', '', cleaned, flags=re.IGNORECASE)
            cleaned = cleaned.strip()
            
            try:
                return float(cleaned)
            except ValueError:
                return None
        
        return None
    
    def _parse_datetime(self, value: Any) -> datetime | None:
        """
        Parse datetime from various formats.
        
        Args:
            value: Datetime value (string, datetime, or None)
            
        Returns:
            Datetime object or None
        """
        if value is None:
            return None
        
        if isinstance(value, datetime):
            return value
        
        if isinstance(value, str):
            # Try ISO format
            try:
                return datetime.fromisoformat(value.replace('Z', '+00:00'))
            except ValueError:
                pass
            
            # Try common formats
            formats = [
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%dT%H:%M",
                "%d/%m/%Y %H:%M",
                "%d-%m-%Y %H:%M",
            ]
            
            for fmt in formats:
                try:
                    return datetime.strptime(value, fmt)
                except ValueError:
                    continue
        
        return None
    
    def _parse_int(self, value: Any) -> int | None:
        """
        Parse integer from various formats.
        
        Args:
            value: Integer value (int, string, or None)
            
        Returns:
            Integer or None
        """
        if value is None:
            return None
        
        if isinstance(value, int):
            return value
        
        if isinstance(value, str):
            try:
                return int(value)
            except ValueError:
                return None
        
        return None
    
    def _parse_duration(self, value: Any) -> int | None:
        """
        Parse duration in minutes from various formats.
        
        Handles:
        - Numeric values: 120
        - String with units: "2 hours", "90 minutes", "1.5h"
        
        Args:
            value: Duration value
            
        Returns:
            Duration in minutes or None
        """
        if value is None:
            return None
        
        if isinstance(value, (int, float)):
            return int(value)
        
        if isinstance(value, str):
            value = value.lower().strip()
            
            # Try direct integer
            try:
                return int(value)
            except ValueError:
                pass
            
            # Parse "X hours"
            match = re.search(r'(\d+(?:\.\d+)?)\s*h(?:ours?)?', value)
            if match:
                return int(float(match.group(1)) * 60)
            
            # Parse "X minutes"
            match = re.search(r'(\d+)\s*m(?:in(?:utes?)?)?', value)
            if match:
                return int(match.group(1))
        
        return None
    
    def _extract_list(self, value: Any) -> list[str]:
        """
        Extract list of strings from various formats.
        
        Handles:
        - Already a list: ["tag1", "tag2"]
        - Comma-separated string: "tag1, tag2"
        - Semicolon-separated string: "tag1; tag2"
        
        Args:
            value: List value in various formats
            
        Returns:
            List of strings (empty list if None)
        """
        if value is None:
            return []
        
        if isinstance(value, list):
            return [str(item).strip() for item in value if item]
        
        if isinstance(value, str):
            # Try semicolon separator first
            if ';' in value:
                return [item.strip() for item in value.split(';') if item.strip()]
            # Try comma separator
            if ',' in value:
                return [item.strip() for item in value.split(',') if item.strip()]
            # Single value
            return [value.strip()] if value.strip() else []
        
        return []
    
    def _extract_image_urls(self, raw_data: dict[str, Any]) -> list[str]:
        """
        Extract image URLs from various field names.
        
        Args:
            raw_data: Raw data dictionary
            
        Returns:
            List of image URLs
        """
        # Try different field names
        for field in ["image_urls", "images", "image_url", "image"]:
            if field in raw_data:
                value = raw_data[field]
                if isinstance(value, list):
                    return [str(url) for url in value if url]
                elif isinstance(value, str) and value:
                    return [value]
        
        return []
    
    def _template_to_dict(self, template: EventTemplate) -> dict[str, Any]:
        """Convert EventTemplate to dictionary for hashing."""
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
        }
    
    def _occurrence_to_dict(self, occurrence: EventOccurrence) -> dict[str, Any]:
        """Convert EventOccurrence to dictionary for hashing."""
        return {
            "event_id": occurrence.event_id,
            "provider_id": occurrence.provider_id,
            "event_template_id": occurrence.event_template_id,
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
        }
