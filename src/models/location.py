"""Location canonical data model."""

from dataclasses import dataclass
from datetime import datetime


@dataclass
class Location:
    """Canonical location/venue record."""
    
    location_id: str
    provider_id: str
    provider_name: str
    formatted_address: str
    country: str = "UK"
    
    # Address fields
    location_name: str | None = None
    address_line_1: str | None = None
    address_line_2: str | None = None
    city: str | None = None
    region: str | None = None
    postcode: str | None = None
    
    # Geocoding fields
    latitude: float | None = None
    longitude: float | None = None
    geocode_provider: str | None = None
    geocode_status: str = "not_geocoded"
    geocode_precision: str | None = None
    geocoded_at: datetime | None = None
    
    # Contact fields
    venue_phone: str | None = None
    venue_email: str | None = None
    venue_website: str | None = None
    
    # Lifecycle fields
    status: str = "active"
    first_seen_at: datetime | None = None
    last_seen_at: datetime | None = None
    deleted_at: datetime | None = None
    
    # Hashing for change detection
    address_hash: str | None = None
    
    def validate(self) -> list[str]:
        """Validate location record and return list of validation errors."""
        errors = []
        
        # Required fields
        if not self.location_id or not self.location_id.strip():
            errors.append("location_id must be non-empty")
        
        if not self.provider_id or not self.provider_id.strip():
            errors.append("provider_id must be non-empty")
        
        if not self.formatted_address or not self.formatted_address.strip():
            errors.append("formatted_address must be non-empty")
        
        # Geocoding validation
        if self.latitude is not None:
            if not (-90 <= self.latitude <= 90):
                errors.append("latitude must be between -90 and 90")
        
        if self.longitude is not None:
            if not (-180 <= self.longitude <= 180):
                errors.append("longitude must be between -180 and 180")
        
        # Geocode status validation
        valid_geocode_statuses = ["not_geocoded", "success", "failed", "invalid_address"]
        if self.geocode_status not in valid_geocode_statuses:
            errors.append(f"geocode_status must be one of: {', '.join(valid_geocode_statuses)}")
        
        # Status validation
        valid_statuses = ["active", "inactive", "removed"]
        if self.status not in valid_statuses:
            errors.append(f"status must be one of: {', '.join(valid_statuses)}")
        
        # Timestamp validation
        if self.first_seen_at and self.last_seen_at:
            if self.first_seen_at > self.last_seen_at:
                errors.append("first_seen_at must be <= last_seen_at")
        
        return errors
    
    def is_valid(self) -> bool:
        """Check if location record is valid."""
        return len(self.validate()) == 0
