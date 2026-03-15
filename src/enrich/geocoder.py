"""Geocoding interface and result types."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime


@dataclass
class GeocodeResult:
    """Result from geocoding an address."""
    
    latitude: float | None
    longitude: float | None
    status: str  # success, failed, invalid_address
    precision: str | None  # e.g., "rooftop", "street", "city"
    metadata: dict  # Additional provider-specific metadata
    
    def is_success(self) -> bool:
        """Check if geocoding was successful."""
        return self.status == "success" and self.latitude is not None and self.longitude is not None


class Geocoder(ABC):
    """Abstract geocoding interface.
    
    Implementations should geocode addresses and return coordinates with metadata.
    This abstraction allows for multiple geocoding providers (Mapbox, Google, Nominatim, etc.)
    and enables testing with mock geocoders.
    """
    
    @abstractmethod
    def geocode(self, address: str) -> GeocodeResult:
        """Geocode an address and return coordinates with metadata.
        
        Args:
            address: The address string to geocode
            
        Returns:
            GeocodeResult with coordinates, status, precision, and metadata
            
        Raises:
            May raise provider-specific exceptions for network errors, API errors, etc.
            Implementations should handle errors gracefully and return failed status when appropriate.
        """
        pass
