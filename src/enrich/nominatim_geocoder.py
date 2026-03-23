"""Nominatim (OpenStreetMap) geocoder - free, no API key required."""

import time
from typing import Tuple

import requests

from src.enrich.geocoder import Geocoder, GeocodeResult


class NominatimGeocoder(Geocoder):
    """Free geocoder using OpenStreetMap's Nominatim service.
    
    Advantages:
    - Completely free
    - No API key required
    - No signup needed
    - Open source data
    
    Limitations:
    - Rate limited to 1 request/second
    - Less accurate than commercial services
    - Must include User-Agent header
    - Usage policy: https://operations.osmfoundation.org/policies/nominatim/
    """
    
    def __init__(
        self,
        user_agent: str = "RostaScrapers/1.0",
        timeout: int = 10,
        rate_limit_delay: float = 1.0
    ):
        """Initialize Nominatim geocoder.
        
        Args:
            user_agent: User agent string (required by Nominatim)
            timeout: Request timeout in seconds
            rate_limit_delay: Delay between requests in seconds (min 1.0)
        """
        self.user_agent = user_agent
        self.timeout = timeout
        self.rate_limit_delay = max(1.0, rate_limit_delay)  # Enforce 1 req/sec minimum
        self.base_url = "https://nominatim.openstreetmap.org/search"
        self.last_request_time = 0
    
    def geocode(self, address: str) -> GeocodeResult:
        """Geocode an address using Nominatim.
        
        Args:
            address: Address string to geocode
            
        Returns:
            GeocodeResult with coordinates and metadata
        """
        # Enforce rate limit
        self._enforce_rate_limit()
        
        try:
            # Make request to Nominatim
            response = requests.get(
                self.base_url,
                params={
                    "q": address,
                    "format": "json",
                    "limit": 1,
                    "addressdetails": 1
                },
                headers={
                    "User-Agent": self.user_agent
                },
                timeout=self.timeout
            )
            
            response.raise_for_status()
            results = response.json()
            
            if not results:
                return GeocodeResult(
                    latitude=None,
                    longitude=None,
                    status="invalid_address",
                    precision=None,
                    metadata={"error": "No results found"}
                )
            
            # Parse first result
            result = results[0]
            
            return GeocodeResult(
                latitude=float(result["lat"]),
                longitude=float(result["lon"]),
                status="success",
                precision=self._determine_precision(result),
                metadata={
                    "provider": "nominatim",
                    "formatted_address": result.get("display_name"),
                    "place_id": result.get("place_id"),
                    "osm_type": result.get("osm_type"),
                    "osm_id": result.get("osm_id"),
                    "display_name": result.get("display_name"),
                    "importance": result.get("importance")
                }
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
                metadata={"error": f"Request failed: {str(e)}"}
            )
        except (KeyError, ValueError, TypeError) as e:
            return GeocodeResult(
                latitude=None,
                longitude=None,
                status="failed",
                precision=None,
                metadata={"error": f"Failed to parse response: {str(e)}"}
            )
    
    def _enforce_rate_limit(self) -> None:
        """Enforce rate limit of 1 request per second."""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        
        if time_since_last_request < self.rate_limit_delay:
            sleep_time = self.rate_limit_delay - time_since_last_request
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def _determine_precision(self, result: dict) -> str:
        """Determine precision level from Nominatim result.
        
        Args:
            result: Nominatim API response
            
        Returns:
            Precision level string
        """
        # Nominatim provides a "type" field indicating result type
        result_type = result.get("type", "").lower()
        osm_type = result.get("osm_type", "").lower()
        
        # Map Nominatim types to precision levels
        if result_type in ["house", "building", "residential"] or osm_type == "node":
            return "rooftop"
        elif result_type in ["road", "street", "highway"]:
            return "street"
        elif result_type in ["suburb", "neighbourhood", "quarter"]:
            return "neighborhood"
        elif result_type in ["city", "town", "village"]:
            return "city"
        elif result_type in ["county", "state", "region"]:
            return "region"
        else:
            return "approximate"
    
    def batch_geocode(self, addresses: list[str]) -> list[GeocodeResult]:
        """Geocode multiple addresses (respects rate limits).
        
        Args:
            addresses: List of address strings
            
        Returns:
            List of GeocodeResults
        """
        results = []
        for address in addresses:
            result = self.geocode(address)
            results.append(result)
        
        return results
