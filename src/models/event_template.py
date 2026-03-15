"""EventTemplate canonical data model."""

from dataclasses import dataclass, field
from datetime import datetime
import re


@dataclass
class EventTemplate:
    """Canonical event template/type record (recurring or undated events)."""
    
    event_template_id: str
    provider_id: str
    title: str
    slug: str
    currency: str = "GBP"
    
    # Source reference
    source_template_id: str | None = None
    
    # Category fields
    category: str | None = None
    sub_category: str | None = None
    
    # Description fields
    description_raw: str | None = None
    description_clean: str | None = None
    description_ai: str | None = None
    summary_short: str | None = None
    summary_medium: str | None = None
    
    # Structured metadata
    tags: list[str] = field(default_factory=list)
    occasion_tags: list[str] = field(default_factory=list)
    skills_required: list[str] = field(default_factory=list)
    skills_created: list[str] = field(default_factory=list)
    
    # Audience fields
    age_min: int | None = None
    age_max: int | None = None
    audience: str | None = None
    family_friendly: bool = False
    beginner_friendly: bool = False
    
    # Logistics
    duration_minutes: int | None = None
    price_from: float | None = None
    
    # Media
    source_url: str | None = None
    image_urls: list[str] = field(default_factory=list)
    
    # Location scope
    location_scope: str | None = None
    
    # Lifecycle fields
    status: str = "active"
    first_seen_at: datetime | None = None
    last_seen_at: datetime | None = None
    deleted_at: datetime | None = None
    
    # Hashing for change detection
    source_hash: str | None = None
    record_hash: str | None = None
    
    def validate(self) -> list[str]:
        """Validate event template record and return list of validation errors."""
        errors = []
        
        # Required fields
        if not self.event_template_id or not self.event_template_id.strip():
            errors.append("event_template_id must be non-empty")
        
        if not self.provider_id or not self.provider_id.strip():
            errors.append("provider_id must be non-empty")
        
        if not self.title or not self.title.strip():
            errors.append("title must be non-empty")
        
        if not self.slug or not self.slug.strip():
            errors.append("slug must be non-empty")
        
        # Slug format validation
        if self.slug and not re.match(r'^[a-z0-9]+(-[a-z0-9]+)*$', self.slug):
            errors.append("slug must match kebab-case pattern")
        
        # Price validation
        if self.price_from is not None and self.price_from < 0:
            errors.append("price_from must be >= 0")
        
        # Age validation
        if self.age_min is not None and self.age_min <= 0:
            errors.append("age_min must be > 0")
        
        if self.age_max is not None and self.age_max <= 0:
            errors.append("age_max must be > 0")
        
        if self.age_min is not None and self.age_max is not None:
            if self.age_min > self.age_max:
                errors.append("age_min must be <= age_max")
        
        # Status validation
        valid_statuses = ["active", "inactive", "removed"]
        if self.status not in valid_statuses:
            errors.append(f"status must be one of: {', '.join(valid_statuses)}")
        
        # Location scope validation
        if self.location_scope is not None:
            valid_scopes = ["provider-wide", "unknown"]
            if self.location_scope not in valid_scopes:
                errors.append(f"location_scope must be one of: {', '.join(valid_scopes)}")
        
        # Timestamp validation
        if self.first_seen_at and self.last_seen_at:
            if self.first_seen_at > self.last_seen_at:
                errors.append("first_seen_at must be <= last_seen_at")
        
        return errors
    
    def is_valid(self) -> bool:
        """Check if event template record is valid."""
        return len(self.validate()) == 0
