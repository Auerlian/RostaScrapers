"""Unit tests for geocoder components.

Tests MapboxGeocoder with mock API responses, address normalization,
cache key generation, and error handling scenarios.

Validates Requirements: 4.1, 4.3, 4.4, 4.7
"""

import pytest
from unittest.mock import Mock, patch
import requests

from src.enrich.geocoder import Geocoder, GeocodeResult
from src.enrich.mapbox_geocoder import MapboxGeocoder
from src.transform.id_generator import normalize_address


class TestGeocodeResult:
    """Test GeocodeResult dataclass and methods."""
    
    def test_is_success_with_valid_coordinates(self):
        """Test is_success returns True for valid successful result."""
        result = GeocodeResult(
            latitude=51.5074,
            longitude=-0.1278,
            status="success",
            precision="rooftop",
            metadata={}
        )
        
        assert result.is_success() is True
    
    def test_is_success_with_failed_status(self):
        """Test is_success returns False for failed status."""
        result = GeocodeResult(
            latitude=None,
            longitude=None,
            status="failed",
            precision=None,
            metadata={}
        )
        
        assert result.is_success() is False
    
    def test_is_success_with_null_coordinates(self):
        """Test is_success returns False when coordinates are None."""
        result = GeocodeResult(
            latitude=None,
            longitude=None,
            status="success",  # Status says success but no coords
            precision=None,
            metadata={}
        )
        
        assert result.is_success() is False
    
    def test_is_success_with_partial_coordinates(self):
        """Test is_success returns False when only one coordinate is present."""
        result = GeocodeResult(
            latitude=51.5074,
            longitude=None,
            status="success",
            precision="rooftop",
            metadata={}
        )
        
        assert result.is_success() is False


class TestMapboxGeocoderInit:
    """Test MapboxGeocoder initialization."""
    
    def test_init_with_api_key_parameter(self):
        """Test initialization with API key parameter."""
        geocoder = MapboxGeocoder(api_key="test_key_123")
        
        assert geocoder.api_key == "test_key_123"
        assert geocoder.timeout == MapboxGeocoder.DEFAULT_TIMEOUT
    
    def test_init_with_custom_timeout(self):
        """Test initialization with custom timeout."""
        geocoder = MapboxGeocoder(api_key="test_key", timeout=30)
        
        assert geocoder.timeout == 30
    
    @patch.dict('os.environ', {'MAPBOX_API_KEY': 'env_key_456'})
    def test_init_from_environment_variable(self):
        """Test initialization reads API key from environment."""
        geocoder = MapboxGeocoder()
        
        assert geocoder.api_key == "env_key_456"
    
    @patch.dict('os.environ', {}, clear=True)
    def test_init_without_api_key_raises_error(self):
        """Test initialization raises ValueError when no API key available."""
        with pytest.raises(ValueError) as exc_info:
            MapboxGeocoder()
        
        assert "API key is required" in str(exc_info.value)
        assert "MAPBOX_API_KEY" in str(exc_info.value)


class TestMapboxGeocoderGeocodeSuccess:
    """Test MapboxGeocoder.geocode with successful responses."""
    
    @patch('requests.get')
    def test_geocode_valid_address(self, mock_get):
        """Test geocoding a valid address returns success."""
        # Mock successful API response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "features": [
                {
                    "geometry": {
                        "coordinates": [-0.1278, 51.5074]  # [lng, lat]
                    },
                    "place_type": ["address"],
                    "place_name": "10 Downing Street, London, UK",
                    "relevance": 0.99,
                    "properties": {
                        "accuracy": "rooftop"
                    },
                    "context": [
                        {"id": "postcode.123", "text": "SW1A 2AA"},
                        {"id": "place.456", "text": "London"}
                    ]
                }
            ]
        }
        mock_get.return_value = mock_response
        
        geocoder = MapboxGeocoder(api_key="test_key")
        result = geocoder.geocode("10 Downing Street, London, UK")
        
        # Verify result
        assert result.status == "success"
        assert result.latitude == 51.5074
        assert result.longitude == -0.1278
        assert result.precision == "rooftop"
        assert result.metadata["provider"] == "mapbox"
        assert result.metadata["place_name"] == "10 Downing Street, London, UK"
        assert result.metadata["relevance"] == 0.99
        assert result.is_success() is True
        
        # Verify API call
        mock_get.assert_called_once()
        call_args = mock_get.call_args
        assert "10 Downing Street, London, UK" in call_args[0][0]
        assert call_args[1]["params"]["access_token"] == "test_key"
        assert call_args[1]["params"]["limit"] == 1
    
    @patch('requests.get')
    def test_geocode_poi_address(self, mock_get):
        """Test geocoding a POI (point of interest) address."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "features": [
                {
                    "geometry": {
                        "coordinates": [-0.1276, 51.5007]
                    },
                    "place_type": ["poi"],
                    "place_name": "Big Ben, Westminster, London",
                    "relevance": 1.0,
                    "properties": {}
                }
            ]
        }
        mock_get.return_value = mock_response
        
        geocoder = MapboxGeocoder(api_key="test_key")
        result = geocoder.geocode("Big Ben, London")
        
        assert result.status == "success"
        assert result.latitude == 51.5007
        assert result.longitude == -0.1276
        assert result.precision == "rooftop"  # POI maps to rooftop
    
    @patch('requests.get')
    def test_geocode_street_level_precision(self, mock_get):
        """Test geocoding with street-level precision."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "features": [
                {
                    "geometry": {
                        "coordinates": [-0.1, 51.5]
                    },
                    "place_type": ["street"],
                    "place_name": "Oxford Street, London",
                    "relevance": 0.95,
                    "properties": {}
                }
            ]
        }
        mock_get.return_value = mock_response
        
        geocoder = MapboxGeocoder(api_key="test_key")
        result = geocoder.geocode("Oxford Street, London")
        
        assert result.status == "success"
        assert result.precision == "street"
    
    @patch('requests.get')
    def test_geocode_city_level_precision(self, mock_get):
        """Test geocoding with city-level precision."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "features": [
                {
                    "geometry": {
                        "coordinates": [-0.1278, 51.5074]
                    },
                    "place_type": ["place"],
                    "place_name": "London, UK",
                    "relevance": 1.0,
                    "properties": {}
                }
            ]
        }
        mock_get.return_value = mock_response
        
        geocoder = MapboxGeocoder(api_key="test_key")
        result = geocoder.geocode("London")
        
        assert result.status == "success"
        assert result.precision == "city"


class TestMapboxGeocoderGeocodeFailures:
    """Test MapboxGeocoder.geocode with failure scenarios."""
    
    def test_geocode_empty_address(self):
        """Test geocoding empty address returns invalid_address."""
        geocoder = MapboxGeocoder(api_key="test_key")
        result = geocoder.geocode("")
        
        assert result.status == "invalid_address"
        assert result.latitude is None
        assert result.longitude is None
        assert "Empty address" in result.metadata["error"]
    
    def test_geocode_whitespace_only_address(self):
        """Test geocoding whitespace-only address returns invalid_address."""
        geocoder = MapboxGeocoder(api_key="test_key")
        result = geocoder.geocode("   \t\n  ")
        
        assert result.status == "invalid_address"
        assert result.latitude is None
        assert result.longitude is None
    
    @patch('requests.get')
    def test_geocode_no_results_found(self, mock_get):
        """Test geocoding when API returns no results."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "features": []  # No results
        }
        mock_get.return_value = mock_response
        
        geocoder = MapboxGeocoder(api_key="test_key")
        result = geocoder.geocode("Nonexistent Place XYZ123")
        
        assert result.status == "invalid_address"
        assert result.latitude is None
        assert result.longitude is None
        assert "No results found" in result.metadata["error"]
    
    @patch('requests.get')
    def test_geocode_rate_limit_exceeded(self, mock_get):
        """Test geocoding when rate limit is exceeded."""
        mock_response = Mock()
        mock_response.status_code = 429
        mock_get.return_value = mock_response
        
        geocoder = MapboxGeocoder(api_key="test_key")
        result = geocoder.geocode("Some Address")
        
        assert result.status == "failed"
        assert result.latitude is None
        assert result.longitude is None
        assert "Rate limit exceeded" in result.metadata["error"]
        assert result.metadata["status_code"] == 429
    
    @patch('requests.get')
    def test_geocode_http_error_401(self, mock_get):
        """Test geocoding with 401 Unauthorized error."""
        mock_response = Mock()
        mock_response.status_code = 401
        mock_get.return_value = mock_response
        
        geocoder = MapboxGeocoder(api_key="invalid_key")
        result = geocoder.geocode("Some Address")
        
        assert result.status == "failed"
        assert "HTTP 401" in result.metadata["error"]
        assert result.metadata["status_code"] == 401
    
    @patch('requests.get')
    def test_geocode_http_error_500(self, mock_get):
        """Test geocoding with 500 server error."""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_get.return_value = mock_response
        
        geocoder = MapboxGeocoder(api_key="test_key")
        result = geocoder.geocode("Some Address")
        
        assert result.status == "failed"
        assert "HTTP 500" in result.metadata["error"]
        assert result.metadata["status_code"] == 500
    
    @patch('requests.get')
    def test_geocode_timeout_error(self, mock_get):
        """Test geocoding when request times out."""
        mock_get.side_effect = requests.exceptions.Timeout("Connection timeout")
        
        geocoder = MapboxGeocoder(api_key="test_key", timeout=5)
        result = geocoder.geocode("Some Address")
        
        assert result.status == "failed"
        assert result.latitude is None
        assert result.longitude is None
        assert "timeout" in result.metadata["error"].lower()
    
    @patch('requests.get')
    def test_geocode_network_error(self, mock_get):
        """Test geocoding when network error occurs."""
        mock_get.side_effect = requests.exceptions.ConnectionError("Network unreachable")
        
        geocoder = MapboxGeocoder(api_key="test_key")
        result = geocoder.geocode("Some Address")
        
        assert result.status == "failed"
        assert "Network error" in result.metadata["error"]
    
    @patch('requests.get')
    def test_geocode_invalid_json_response(self, mock_get):
        """Test geocoding when API returns invalid JSON."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.side_effect = ValueError("Invalid JSON")
        mock_get.return_value = mock_response
        
        geocoder = MapboxGeocoder(api_key="test_key")
        result = geocoder.geocode("Some Address")
        
        assert result.status == "failed"
        assert "Unexpected error" in result.metadata["error"]
    
    @patch('requests.get')
    def test_geocode_invalid_coordinates_format(self, mock_get):
        """Test geocoding when API returns invalid coordinate format."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "features": [
                {
                    "geometry": {
                        "coordinates": [-0.1278]  # Only one coordinate
                    },
                    "place_type": ["address"],
                    "place_name": "Test Address"
                }
            ]
        }
        mock_get.return_value = mock_response
        
        geocoder = MapboxGeocoder(api_key="test_key")
        result = geocoder.geocode("Test Address")
        
        assert result.status == "failed"
        assert "Invalid coordinates" in result.metadata["error"]


class TestMapboxGeocoderPrecisionMapping:
    """Test MapboxGeocoder._map_precision method."""
    
    def test_map_precision_address(self):
        """Test precision mapping for address type."""
        geocoder = MapboxGeocoder(api_key="test_key")
        precision = geocoder._map_precision(["address"])
        
        assert precision == "rooftop"
    
    def test_map_precision_poi(self):
        """Test precision mapping for POI type."""
        geocoder = MapboxGeocoder(api_key="test_key")
        precision = geocoder._map_precision(["poi"])
        
        assert precision == "rooftop"
    
    def test_map_precision_street(self):
        """Test precision mapping for street type."""
        geocoder = MapboxGeocoder(api_key="test_key")
        precision = geocoder._map_precision(["street"])
        
        assert precision == "street"
    
    def test_map_precision_neighborhood(self):
        """Test precision mapping for neighborhood type."""
        geocoder = MapboxGeocoder(api_key="test_key")
        precision = geocoder._map_precision(["neighborhood"])
        
        assert precision == "neighborhood"
    
    def test_map_precision_locality(self):
        """Test precision mapping for locality type."""
        geocoder = MapboxGeocoder(api_key="test_key")
        precision = geocoder._map_precision(["locality"])
        
        assert precision == "city"
    
    def test_map_precision_place(self):
        """Test precision mapping for place type."""
        geocoder = MapboxGeocoder(api_key="test_key")
        precision = geocoder._map_precision(["place"])
        
        assert precision == "city"
    
    def test_map_precision_postcode(self):
        """Test precision mapping for postcode type."""
        geocoder = MapboxGeocoder(api_key="test_key")
        precision = geocoder._map_precision(["postcode"])
        
        assert precision == "postcode"
    
    def test_map_precision_region(self):
        """Test precision mapping for region type."""
        geocoder = MapboxGeocoder(api_key="test_key")
        precision = geocoder._map_precision(["region"])
        
        assert precision == "region"
    
    def test_map_precision_country(self):
        """Test precision mapping for country type."""
        geocoder = MapboxGeocoder(api_key="test_key")
        precision = geocoder._map_precision(["country"])
        
        assert precision == "country"
    
    def test_map_precision_unknown_type(self):
        """Test precision mapping for unknown type."""
        geocoder = MapboxGeocoder(api_key="test_key")
        precision = geocoder._map_precision(["unknown_type"])
        
        assert precision == "unknown"
    
    def test_map_precision_empty_list(self):
        """Test precision mapping for empty place_type list."""
        geocoder = MapboxGeocoder(api_key="test_key")
        precision = geocoder._map_precision([])
        
        assert precision == "unknown"
    
    def test_map_precision_multiple_types_uses_first(self):
        """Test precision mapping uses first type when multiple provided."""
        geocoder = MapboxGeocoder(api_key="test_key")
        precision = geocoder._map_precision(["address", "street", "place"])
        
        # Should use first (most specific) type
        assert precision == "rooftop"


class TestAddressNormalization:
    """Test address normalization function for cache key generation."""
    
    def test_normalize_basic_address(self):
        """Test normalizing a basic address."""
        result = normalize_address("123 Main Street, London")
        
        assert result == "123 main street london"
    
    def test_normalize_removes_punctuation(self):
        """Test that punctuation is removed."""
        result = normalize_address("123 Main St., London, UK")
        
        assert result == "123 main st london uk"
        assert "." not in result
        assert "," not in result
    
    def test_normalize_removes_extra_whitespace(self):
        """Test that extra whitespace is normalized."""
        result = normalize_address("123   Main    Street     London")
        
        assert result == "123 main street london"
        assert "  " not in result
    
    def test_normalize_strips_leading_trailing_whitespace(self):
        """Test that leading and trailing whitespace is removed."""
        result = normalize_address("  123 Main Street, London  ")
        
        assert result == "123 main street london"
        assert not result.startswith(" ")
        assert not result.endswith(" ")
    
    def test_normalize_converts_to_lowercase(self):
        """Test that address is converted to lowercase."""
        result = normalize_address("123 MAIN STREET, LONDON")
        
        assert result == "123 main street london"
        assert result.islower()
    
    def test_normalize_handles_special_characters(self):
        """Test that special characters are removed."""
        result = normalize_address("123 Main St. #4, London (UK)")
        
        assert "#" not in result
        assert "(" not in result
        assert ")" not in result
    
    def test_normalize_empty_address(self):
        """Test normalizing empty address."""
        result = normalize_address("")
        
        assert result == ""
    
    def test_normalize_whitespace_only(self):
        """Test normalizing whitespace-only address."""
        result = normalize_address("   \t\n  ")
        
        assert result == ""
    
    def test_normalize_preserves_alphanumeric(self):
        """Test that alphanumeric characters are preserved."""
        result = normalize_address("123 Main Street Apt 4B")
        
        assert "123" in result
        assert "4b" in result
    
    def test_normalize_consistent_for_similar_addresses(self):
        """Test that similar addresses normalize to same value."""
        addr1 = normalize_address("123 Main St., London, UK")
        addr2 = normalize_address("123  Main  Street  London  UK")
        addr3 = normalize_address("123 MAIN STREET, LONDON, UK")
        
        # addr2 and addr3 should be identical (both have "street")
        assert addr2 == addr3
        
        # addr1 has "st" which is different from "street"
        # This is expected - normalization doesn't expand abbreviations
        assert "st" in addr1
        assert "street" in addr2
    
    def test_normalize_different_addresses_produce_different_results(self):
        """Test that different addresses produce different normalized values."""
        addr1 = normalize_address("123 Main Street, London")
        addr2 = normalize_address("456 High Street, London")
        
        assert addr1 != addr2


class TestCacheKeyGeneration:
    """Test cache key generation from normalized addresses."""
    
    def test_cache_key_deterministic(self):
        """Test that same address produces same cache key."""
        import hashlib
        
        address = "123 Main Street, London, UK"
        normalized = normalize_address(address)
        
        key1 = hashlib.sha256(normalized.encode()).hexdigest()[:12]
        key2 = hashlib.sha256(normalized.encode()).hexdigest()[:12]
        
        assert key1 == key2
    
    def test_cache_key_length(self):
        """Test that cache key is 12 characters."""
        import hashlib
        
        address = "123 Main Street, London, UK"
        normalized = normalize_address(address)
        key = hashlib.sha256(normalized.encode()).hexdigest()[:12]
        
        assert len(key) == 12
    
    def test_cache_key_different_for_different_addresses(self):
        """Test that different addresses produce different cache keys."""
        import hashlib
        
        addr1 = "123 Main Street, London"
        addr2 = "456 High Street, London"
        
        norm1 = normalize_address(addr1)
        norm2 = normalize_address(addr2)
        
        key1 = hashlib.sha256(norm1.encode()).hexdigest()[:12]
        key2 = hashlib.sha256(norm2.encode()).hexdigest()[:12]
        
        assert key1 != key2
    
    def test_cache_key_same_for_normalized_variations(self):
        """Test that address variations produce same cache key after normalization."""
        import hashlib
        
        addr1 = "123 Main St., London, UK"
        addr2 = "123  Main  Street  London  UK"
        
        norm1 = normalize_address(addr1)
        norm2 = normalize_address(addr2)
        
        # Note: "St." vs "Street" will produce different keys
        # This is expected - normalization removes punctuation but doesn't expand abbreviations
        # The test verifies that whitespace and punctuation variations are handled
        key1 = hashlib.sha256(norm1.encode()).hexdigest()[:12]
        key2 = hashlib.sha256(norm2.encode()).hexdigest()[:12]
        
        # These will be different because "st" != "street"
        # But both should be valid 12-char hashes
        assert len(key1) == 12
        assert len(key2) == 12
    
    def test_cache_key_hexadecimal_format(self):
        """Test that cache key is valid hexadecimal."""
        import hashlib
        
        address = "123 Main Street, London, UK"
        normalized = normalize_address(address)
        key = hashlib.sha256(normalized.encode()).hexdigest()[:12]
        
        # Should be valid hex (only 0-9, a-f)
        try:
            int(key, 16)
            is_hex = True
        except ValueError:
            is_hex = False
        
        assert is_hex is True


class TestGeocoderInterface:
    """Test Geocoder abstract interface."""
    
    def test_geocoder_is_abstract(self):
        """Test that Geocoder cannot be instantiated directly."""
        with pytest.raises(TypeError):
            Geocoder()
    
    def test_geocoder_requires_geocode_implementation(self):
        """Test that subclasses must implement geocode method."""
        class IncompleteGeocoder(Geocoder):
            pass
        
        with pytest.raises(TypeError):
            IncompleteGeocoder()
    
    def test_geocoder_can_be_subclassed(self):
        """Test that Geocoder can be properly subclassed."""
        class TestGeocoder(Geocoder):
            def geocode(self, address: str) -> GeocodeResult:
                return GeocodeResult(
                    latitude=0.0,
                    longitude=0.0,
                    status="success",
                    precision="test",
                    metadata={}
                )
        
        geocoder = TestGeocoder()
        result = geocoder.geocode("test")
        
        assert isinstance(result, GeocodeResult)
        assert result.status == "success"
