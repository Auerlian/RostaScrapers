"""Raw provider data structure for scrapers."""

from dataclasses import dataclass, field
from typing import Any


@dataclass
class RawProviderData:
    """
    Intermediate data structure returned by scrapers.
    
    Contains raw, unprocessed data from provider sources before normalization.
    Preserves all source data without transformation.
    """
    
    # Provider information
    provider_name: str
    provider_website: str | None = None
    provider_contact_email: str | None = None
    source_name: str | None = None
    source_base_url: str | None = None
    provider_metadata: dict[str, Any] = field(default_factory=dict)
    
    # Raw location data (list of dicts with provider-specific fields)
    raw_locations: list[dict[str, Any]] = field(default_factory=list)
    
    # Raw event data (list of dicts with provider-specific fields)
    # Can contain templates, occurrences, or mixed data
    raw_events: list[dict[str, Any]] = field(default_factory=list)
    
    # Optional: raw templates if provider separates them
    raw_templates: list[dict[str, Any]] = field(default_factory=list)
