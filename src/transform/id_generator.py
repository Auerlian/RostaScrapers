"""
ID generation functions for deterministic record identifiers.

This module provides functions to generate stable, deterministic IDs for all
canonical data models (Provider, Location, EventTemplate, EventOccurrence).
IDs are generated from normalized source data to ensure the same source data
always produces the same ID across pipeline runs.
"""

import hashlib
import re
from datetime import datetime


def slugify(text: str) -> str:
    """
    Convert text to URL-friendly slug.
    
    Args:
        text: Input text to slugify
        
    Returns:
        Lowercase slug with hyphens, limited to 50 characters
        
    Examples:
        >>> slugify("Pasta Making Workshop")
        'pasta-making-workshop'
        >>> slugify("Coffee & Latte Art!")
        'coffee-latte-art'
    """
    slug = text.lower().strip()
    slug = re.sub(r'[^a-z0-9]+', '-', slug)
    slug = slug.strip('-')
    return slug[:50]  # Limit length


def normalize_address(address: str) -> str:
    """
    Normalize address for consistent hashing.
    
    Removes punctuation, normalizes whitespace, and converts to lowercase
    to ensure minor formatting differences don't produce different hashes.
    
    Args:
        address: Full address string
        
    Returns:
        Normalized address string
        
    Examples:
        >>> normalize_address("123 Main St., London")
        '123 main st london'
        >>> normalize_address("  456   High  Street  ")
        '456 high street'
    """
    addr = address.lower().strip()
    addr = re.sub(r'\s+', ' ', addr)
    addr = re.sub(r'[^\w\s]', '', addr)
    return addr


def generate_provider_id(provider_name: str) -> str:
    """
    Generate deterministic provider ID from name.
    
    Args:
        provider_name: Provider name (e.g., "Pasta Evangelists")
        
    Returns:
        Provider ID in format "provider-{slug}"
        
    Examples:
        >>> generate_provider_id("Pasta Evangelists")
        'provider-pasta-evangelists'
        >>> generate_provider_id("Comptoir Bakery")
        'provider-comptoir-bakery'
    """
    slug = provider_name.lower().strip()
    slug = re.sub(r'[^a-z0-9]+', '-', slug)
    slug = slug.strip('-')
    return f"provider-{slug}"


def generate_location_id(provider_slug: str, address: str) -> str:
    """
    Generate deterministic location ID using provider slug and normalized address hash.
    
    Args:
        provider_slug: Provider slug (e.g., "pasta-evangelists")
        address: Full address string
        
    Returns:
        Location ID in format "location-{provider_slug}-{address_hash}"
        
    Examples:
        >>> generate_location_id("pasta-evangelists", "123 Main St, London")
        'location-pasta-evangelists-...'  # 12-char hash suffix
    """
    normalized = normalize_address(address)
    address_key = hashlib.sha256(normalized.encode()).hexdigest()[:12]
    return f"location-{provider_slug}-{address_key}"


def generate_event_template_id(
    provider_slug: str,
    source_template_id: str | None,
    title: str
) -> str:
    """
    Generate deterministic event template ID.
    
    Uses source_template_id if available, otherwise falls back to title slug.
    
    Args:
        provider_slug: Provider slug (e.g., "pasta-evangelists")
        source_template_id: Provider's internal template ID if available
        title: Event title
        
    Returns:
        Event template ID in format "event-template-{provider_slug}-{id_or_slug}"
        
    Examples:
        >>> generate_event_template_id("pasta-evangelists", "tmpl-123", "Pasta Making")
        'event-template-pasta-evangelists-tmpl-123'
        >>> generate_event_template_id("pasta-evangelists", None, "Pasta Making Workshop")
        'event-template-pasta-evangelists-pasta-making-workshop'
    """
    if source_template_id:
        return f"event-template-{provider_slug}-{source_template_id}"
    
    title_slug = slugify(title)
    return f"event-template-{provider_slug}-{title_slug}"


def generate_event_occurrence_id(
    provider_slug: str,
    source_event_id: str | None,
    title: str,
    location_id: str | None,
    start_at: datetime | None
) -> str:
    """
    Generate deterministic event occurrence ID.
    
    Uses source_event_id if available, otherwise generates composite hash
    from title, location, and start time.
    
    Args:
        provider_slug: Provider slug (e.g., "pasta-evangelists")
        source_event_id: Provider's internal event ID if available
        title: Event title
        location_id: Location ID if known
        start_at: Event start datetime if known
        
    Returns:
        Event occurrence ID in format "event-{provider_slug}-{id_or_hash}"
        
    Examples:
        >>> generate_event_occurrence_id("pasta-evangelists", "evt-456", "Pasta Class", None, None)
        'event-pasta-evangelists-evt-456'
        >>> from datetime import datetime
        >>> dt = datetime(2024, 3, 15, 18, 0)
        >>> generate_event_occurrence_id("pasta-evangelists", None, "Pasta Class", "location-123", dt)
        'event-pasta-evangelists-...'  # 8-char hash suffix
    """
    if source_event_id:
        return f"event-{provider_slug}-{source_event_id}"
    
    # Fallback: hash of title + location + datetime
    components = [
        slugify(title),
        location_id or "no-location",
        start_at.isoformat() if start_at else "no-date"
    ]
    composite = "-".join(components)
    hash_suffix = hashlib.sha256(composite.encode()).hexdigest()[:8]
    return f"event-{provider_slug}-{hash_suffix}"
