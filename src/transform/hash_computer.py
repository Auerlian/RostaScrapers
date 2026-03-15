"""Hash computation functions for change detection.

This module provides hash computation for different record types to support
incremental sync and cache invalidation. Hashes are computed using SHA256
with 12-character truncation for compact identifiers.
"""

import hashlib
import json
from typing import Any

from src.transform.id_generator import normalize_address


def compute_source_hash(record: dict[str, Any], source_fields: list[str]) -> str:
    """Compute hash of source fields only.
    
    The source hash drives sync decisions and cache invalidation. It includes
    only fields from the original source data, excluding computed, enriched,
    and lifecycle fields.
    
    Args:
        record: The record dictionary to hash
        source_fields: List of field names to include in the hash
    
    Returns:
        12-character SHA256 hash of the source fields
    
    Example:
        >>> record = {"title": "Pasta Making", "price": 50, "status": "active"}
        >>> compute_source_hash(record, ["title", "price"])
        'a1b2c3d4e5f6'
    """
    source_data = {k: record.get(k) for k in source_fields if k in record}
    canonical_json = json.dumps(source_data, sort_keys=True, default=str)
    return hashlib.sha256(canonical_json.encode()).hexdigest()[:12]


def compute_record_hash(record: dict[str, Any], exclude_fields: list[str] | None = None) -> str:
    """Compute hash of all canonical fields.
    
    The record hash tracks all canonical field changes including normalized,
    computed, and enriched fields. It excludes operational/lifecycle fields
    like timestamps and status.
    
    Args:
        record: The record dictionary to hash
        exclude_fields: Optional list of field names to exclude from the hash
    
    Returns:
        12-character SHA256 hash of the canonical fields
    
    Example:
        >>> record = {"title": "Pasta Making", "slug": "pasta-making", "status": "active"}
        >>> compute_record_hash(record, exclude_fields=["status"])
        'x9y8z7w6v5u4'
    """
    if exclude_fields is None:
        exclude_fields = []
    
    # Default exclusions for lifecycle/operational fields
    default_exclusions = [
        "first_seen_at",
        "last_seen_at",
        "deleted_at",
        "status",
    ]
    
    all_exclusions = set(default_exclusions + exclude_fields)
    
    # Filter out excluded fields
    filtered_data = {k: v for k, v in record.items() if k not in all_exclusions}
    
    canonical_json = json.dumps(filtered_data, sort_keys=True, default=str)
    return hashlib.sha256(canonical_json.encode()).hexdigest()[:12]


def compute_address_hash(location: dict[str, Any]) -> str:
    """Compute hash of location address fields.
    
    The address hash is used for geocoding cache lookups. It includes only
    the address components that affect geocoding results. Each address field
    is normalized before hashing to ensure consistent cache keys regardless
    of minor formatting differences.
    
    Args:
        location: The location record dictionary
    
    Returns:
        12-character SHA256 hash of the normalized address fields
    
    Example:
        >>> location = {
        ...     "address_line_1": "123 Main St.",
        ...     "city": "London",
        ...     "postcode": "SW1A 1AA"
        ... }
        >>> compute_address_hash(location)
        'p0o9i8u7y6t5'
    """
    address_fields = [
        "address_line_1",
        "address_line_2",
        "city",
        "region",
        "postcode",
        "country",
    ]
    
    # Normalize each address field before hashing
    normalized_data = {}
    for field in address_fields:
        value = location.get(field)
        if value:
            # Normalize the address component for consistent hashing
            normalized_data[field] = normalize_address(str(value))
    
    canonical_json = json.dumps(normalized_data, sort_keys=True, default=str)
    return hashlib.sha256(canonical_json.encode()).hexdigest()[:12]


# Field definitions for different record types
EVENT_TEMPLATE_SOURCE_FIELDS = [
    "title",
    "description_raw",
    "price_from",
    "source_url",
    "image_urls",
    "source_template_id",
    "category",
    "sub_category",
    "duration_minutes",
]

EVENT_OCCURRENCE_SOURCE_FIELDS = [
    "title",
    "description_raw",
    "price",
    "booking_url",
    "start_at",
    "end_at",
    "location_id",
    "image_urls",
    "capacity",
    "remaining_spaces",
    "availability_status",
    "source_event_id",
]

LOCATION_SOURCE_FIELDS = [
    "location_name",
    "address_line_1",
    "address_line_2",
    "city",
    "region",
    "postcode",
    "country",
    "venue_phone",
    "venue_email",
    "venue_website",
]
