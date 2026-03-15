"""EventOccurrence canonical data model."""

from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class EventOccurrence:
    """Canonical event occurrence record (specific dated sessions)."""
    
    event_id: str
    provider_id: str
    title: str
    timezone: str = "Europe/London"
    currency: str = "GBP"
    availability_status: str = "unknown"
    
    # References
    event_template_id: str | None = None
    location_id: str | None = None
    source_event_id: str | None = None
    
    # Core fields
    start_at: datetime | None = None
    end_at: datetime | None = None
    
    # Booking fields
    booking_url: str | None = None
    price: float | None = None
    capacity: int | None = None
    remaining_spaces: int | None = None
    
    # Description fields (can override template)
    description_raw: str | None = None
    description_clean: str | None = None
    description_ai: str | None = None
    
    # Metadata (can override template)
    tags: list[str] = field(default_factory=list)
    skills_required: list[str] = field(default_factory=list)
    skills_created: list[str] = field(default_factory=list)
    age_min: int | None = None
    age_max: int | None = None
    
    # Lifecycle fields
    status: str = "active"
    first_seen_at: datetime | None = None
    last_seen_at: datetime | None = None
    deleted_at: datetime | None = None
    
    # Hashing for change detection
    source_hash: str | None = None
    record_hash: str | None = None
    
    def validate(self) -> list[str]:
        """Validate event occurrence record and return list of validation errors."""
        errors = []
        
        # Required fields
        if not self.event_id or not self.event_id.strip():
            errors.append("event_id must be non-empty")
        
        if not self.provider_id or not self.provider_id.strip():
            errors.append("provider_id must be non-empty")
        
        if not self.title or not self.title.strip():
            errors.append("title must be non-empty")
        
        # Datetime validation
        if self.start_at and self.end_at:
            if self.start_at >= self.end_at:
                errors.append("start_at must be < end_at")
        
        # Price validation
        if self.price is not None and self.price < 0:
            errors.append("price must be >= 0")
        
        # Age validation
        if self.age_min is not None and self.age_min <= 0:
            errors.append("age_min must be > 0")
        
        if self.age_max is not None and self.age_max <= 0:
            errors.append("age_max must be > 0")
        
        if self.age_min is not None and self.age_max is not None:
            if self.age_min > self.age_max:
                errors.append("age_min must be <= age_max")
        
        # Availability status validation
        valid_availability_statuses = ["available", "sold_out", "limited", "unknown"]
        if self.availability_status not in valid_availability_statuses:
            errors.append(f"availability_status must be one of: {', '.join(valid_availability_statuses)}")
        
        # Status validation
        valid_statuses = ["active", "expired", "removed", "cancelled"]
        if self.status not in valid_statuses:
            errors.append(f"status must be one of: {', '.join(valid_statuses)}")
        
        # Timestamp validation
        if self.first_seen_at and self.last_seen_at:
            if self.first_seen_at > self.last_seen_at:
                errors.append("first_seen_at must be <= last_seen_at")
        
        return errors
    
    def is_valid(self) -> bool:
        """Check if event occurrence record is valid."""
        return len(self.validate()) == 0
