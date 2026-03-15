"""Provider canonical data model."""

from dataclasses import dataclass, field
from datetime import datetime
import re


@dataclass
class Provider:
    """Canonical provider/organization record."""
    
    provider_id: str
    provider_name: str
    provider_slug: str
    source_name: str
    source_base_url: str
    provider_website: str | None = None
    provider_contact_email: str | None = None
    
    # Lifecycle fields
    status: str = "active"
    first_seen_at: datetime | None = None
    last_seen_at: datetime | None = None
    
    # Metadata
    metadata: dict = field(default_factory=dict)
    
    def validate(self) -> list[str]:
        """Validate provider record and return list of validation errors."""
        errors = []
        
        # Required fields
        if not self.provider_id or not self.provider_id.strip():
            errors.append("provider_id must be non-empty")
        
        if not self.provider_name or not self.provider_name.strip():
            errors.append("provider_name must be non-empty")
        
        if not self.provider_slug or not self.provider_slug.strip():
            errors.append("provider_slug must be non-empty")
        
        # Slug format validation
        if self.provider_slug and not re.match(r'^[a-z0-9]+(-[a-z0-9]+)*$', self.provider_slug):
            errors.append("provider_slug must match kebab-case pattern")
        
        # Status validation
        valid_statuses = ["active", "inactive"]
        if self.status not in valid_statuses:
            errors.append(f"status must be one of: {', '.join(valid_statuses)}")
        
        # Timestamp validation
        if self.first_seen_at and self.last_seen_at:
            if self.first_seen_at > self.last_seen_at:
                errors.append("first_seen_at must be <= last_seen_at")
        
        return errors
    
    def is_valid(self) -> bool:
        """Check if provider record is valid."""
        return len(self.validate()) == 0
