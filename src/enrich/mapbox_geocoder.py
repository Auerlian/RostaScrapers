"""Mapbox Geocoding API implementation."""

import os
import requests
from typing import Optional

from src.enrich.geocoder import Geocoder, GeocodeResult


class MapboxGeocoder(Geocoder):
    """Mapbox implementation of geocoding interface.
    
    Uses the Mapbox Geocoding API to convert addresses to coordinates.
    Requires MAPBOX_API_KEY environment variable to be set.
    
    API Documentation: https://docs.mapbox.com/api/search/geocoding/
    """
    
    BASE_URL = "https://api.mapbox.com/geocoding/v5/mapbox.places"
    DEFAULT_TIMEOUT = 10  # seconds
    
    def __init__(self, api_key: Optional[str] = None, timeout: int = DEFAULT_TIMEOUT):
        """Initialize MapboxGeocoder.
        
        Args:
            api_key: Mapbox API key. If None, reads from MAPBOX_API_KEY environment variable.
            timeout: Request timeout in seconds (default: 10)
            
        Raises:
            ValueError: If API key is not provided and MAPBOX_API_KEY env var is not set
        """
        self.api_key = api_key or os.getenv("MAPBOX_API_KEY")
        if not self.api_key:
            raise ValueError(
                "Mapbox API key is required. Set MAPBOX_API_KEY environment variable "
                "or pass api_key parameter."
            )
        
        self.timeout = timeout
    
    def geocode(self, address: str) -> GeocodeResult:
        """Geocode an address using Mapbox Geocoding API.
        
        Args:
            address: The address string to geocode
            
        Returns:
            GeocodeResult with coordinates, status, precision, and metadata
        """
        if not address or not address.strip():
            return GeocodeResult(
                latitude=None,
                longitude=None,
                status="invalid_address",
                precision=None,
                metadata={"error": "Empty address provided"}
            )
        
        try:
            # Build API request
            url = f"{self.BASE_URL}/{address}.json"
            params = {
                "access_token": self.api_key,
                "limit": 1,  # Only need the best match
                "types": "address,poi,place",  # Focus on physical locations
            }
            
            # Make API request
            response = requests.get(url, params=params, timeout=self.timeout)
            
            # Handle rate limiting
            if response.status_code == 429:
                return GeocodeResult(
                    latitude=None,
                    longitude=None,
                    status="failed",
                    precision=None,
                    metadata={
                        "error": "Rate limit exceeded",
                        "status_code": 429
                    }
                )
            
            # Handle other HTTP errors
            if response.status_code != 200:
                return GeocodeResult(
                    latitude=None,
                    longitude=None,
                    status="failed",
                    precision=None,
                    metadata={
                        "error": f"HTTP {response.status_code}",
                        "status_code": response.status_code
                    }
                )
            
            # Parse response
            data = response.json()
            features = data.get("features", [])
            
            # No results found
            if not features:
                return GeocodeResult(
                    latitude=None,
                    longitude=None,
                    status="invalid_address",
                    precision=None,
                    metadata={"error": "No results found for address"}
                )
            
            # Extract first result
            feature = features[0]
            coordinates = feature.get("geometry", {}).get("coordinates", [])
            
            if len(coordinates) != 2:
                return GeocodeResult(
                    latitude=None,
                    longitude=None,
                    status="failed",
                    precision=None,
                    metadata={"error": "Invalid coordinates in response"}
                )
            
            # Mapbox returns [longitude, latitude]
            longitude, latitude = coordinates
            
            # Extract precision from place_type
            place_type = feature.get("place_type", [])
            precision = self._map_precision(place_type)
            
            # Build metadata
            metadata = {
                "provider": "mapbox",
                "place_name": feature.get("place_name"),
                "place_type": place_type,
                "relevance": feature.get("relevance"),
                "confidence": feature.get("properties", {}).get("accuracy"),
            }
            
            # Add context information if available
            context = feature.get("context", [])
            if context:
                metadata["context"] = {
                    item.get("id", "").split(".")[0]: item.get("text")
                    for item in context
                }
            
            return GeocodeResult(
                latitude=latitude,
                longitude=longitude,
                status="success",
                precision=precision,
                metadata=metadata
            )
            
        except requests.exceptions.Timeout:
            return GeocodeResult(
                latitude=None,
                longitude=None,
                status="failed",
                precision=None,
                metadata={"error": "Request timeout"}
            )
        
        except requests.exceptions.RequestException as e:
            return GeocodeResult(
                latitude=None,
                longitude=None,
                status="failed",
                precision=None,
                metadata={"error": f"Network error: {str(e)}"}
            )
        
        except Exception as e:
            return GeocodeResult(
                latitude=None,
                longitude=None,
                status="failed",
                precision=None,
                metadata={"error": f"Unexpected error: {str(e)}"}
            )
    
    def _map_precision(self, place_type: list[str]) -> str:
        """Map Mapbox place_type to precision level.
        
        Args:
            place_type: List of place types from Mapbox response
            
        Returns:
            Precision string: "rooftop", "street", "city", or "region"
        """
        if not place_type:
            return "unknown"
        
        # Use the first (most specific) place type
        primary_type = place_type[0]
        
        # Map Mapbox types to precision levels
        precision_map = {
            "address": "rooftop",
            "poi": "rooftop",
            "street": "street",
            "neighborhood": "neighborhood",
            "locality": "city",
            "place": "city",
            "district": "city",
            "postcode": "postcode",
            "region": "region",
            "country": "country",
        }
        
        return precision_map.get(primary_type, "unknown")
