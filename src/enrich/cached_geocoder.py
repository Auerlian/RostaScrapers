"""Cached geocoder wrapper for efficient geocoding with cache support."""

import hashlib
import json
import os
from datetime import datetime
from pathlib import Path

from src.enrich.geocoder import Geocoder, GeocodeResult
from src.models.location import Location
from src.transform.id_generator import normalize_address


class CachedGeocoder:
    """Wrapper that adds caching to any geocoder implementation.
    
    Caches geocoding results keyed by normalized address hash to avoid
    redundant API calls. Checks cache before calling underlying geocoder
    and stores successful results for future use.
    
    Cache files are stored in cache/geocoding/ directory as JSON files
    named by address hash.
    """
    
    def __init__(self, geocoder: Geocoder, cache_dir: str = "cache/geocoding"):
        """Initialize CachedGeocoder.
        
        Args:
            geocoder: The underlying Geocoder implementation to wrap
            cache_dir: Directory path for cache storage (default: cache/geocoding)
        """
        self.geocoder = geocoder
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
    
    def geocode_location(self, location: Location) -> Location:
        """Geocode location with cache lookup.
        
        Checks cache before calling underlying geocoder. If address is unchanged
        and a valid cached result exists, uses cached data. Otherwise, calls
        geocoder and caches the result.
        
        Args:
            location: Location record to geocode
            
        Returns:
            Updated Location record with geocoding data
        """
        # Compute address hash for cache key
        address_hash = self._compute_address_hash(location.formatted_address)
        
        # Check if address has changed since last geocoding
        if location.address_hash == address_hash and location.geocode_status == "success":
            # Address unchanged and already successfully geocoded - skip
            return location
        
        # Try to load from cache
        cached_result = self._load_from_cache(address_hash)
        if cached_result:
            # Apply cached result to location
            return self._apply_geocode_result(location, cached_result, address_hash)
        
        # Cache miss - call underlying geocoder
        try:
            result = self.geocoder.geocode(location.formatted_address)
            
            # Store successful results in cache
            if result.is_success():
                self._save_to_cache(address_hash, result)
            
            # Apply result to location
            return self._apply_geocode_result(location, result, address_hash)
            
        except Exception as e:
            # Handle geocoding errors gracefully
            location.geocode_status = "failed"
            location.geocode_provider = None
            location.geocode_precision = None
            location.geocoded_at = datetime.now()
            location.address_hash = address_hash
            return location
    
    def _compute_address_hash(self, address: str) -> str:
        """Compute hash of normalized address for cache key.
        
        Args:
            address: Full address string
            
        Returns:
            12-character hash of normalized address
        """
        normalized = normalize_address(address)
        return hashlib.sha256(normalized.encode()).hexdigest()[:12]
    
    def _get_cache_path(self, address_hash: str) -> Path:
        """Get cache file path for address hash.
        
        Args:
            address_hash: Hash of normalized address
            
        Returns:
            Path to cache file
        """
        return self.cache_dir / f"{address_hash}.json"
    
    def _load_from_cache(self, address_hash: str) -> GeocodeResult | None:
        """Load geocoding result from cache.
        
        Args:
            address_hash: Hash of normalized address
            
        Returns:
            GeocodeResult if found in cache, None otherwise
        """
        cache_path = self._get_cache_path(address_hash)
        
        if not cache_path.exists():
            return None
        
        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Reconstruct GeocodeResult from cached data
            return GeocodeResult(
                latitude=data.get("latitude"),
                longitude=data.get("longitude"),
                status=data.get("status"),
                precision=data.get("precision"),
                metadata=data.get("metadata", {})
            )
        except (json.JSONDecodeError, IOError, KeyError):
            # Cache file corrupted or invalid - ignore and re-geocode
            return None
    
    def _save_to_cache(self, address_hash: str, result: GeocodeResult) -> None:
        """Save geocoding result to cache.
        
        Args:
            address_hash: Hash of normalized address
            result: GeocodeResult to cache
        """
        cache_path = self._get_cache_path(address_hash)
        
        try:
            cache_data = {
                "latitude": result.latitude,
                "longitude": result.longitude,
                "status": result.status,
                "precision": result.precision,
                "metadata": result.metadata,
                "cached_at": datetime.now().isoformat()
            }
            
            with open(cache_path, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, indent=2)
        except IOError:
            # Cache write failed - log but don't fail the geocoding
            pass
    
    def _apply_geocode_result(
        self, 
        location: Location, 
        result: GeocodeResult, 
        address_hash: str
    ) -> Location:
        """Apply geocoding result to location record.
        
        Args:
            location: Location record to update
            result: GeocodeResult to apply
            address_hash: Hash of normalized address
            
        Returns:
            Updated Location record
        """
        location.latitude = result.latitude
        location.longitude = result.longitude
        location.geocode_status = result.status
        location.geocode_precision = result.precision
        location.geocode_provider = result.metadata.get("provider")
        location.geocoded_at = datetime.now()
        location.address_hash = address_hash
        
        return location
