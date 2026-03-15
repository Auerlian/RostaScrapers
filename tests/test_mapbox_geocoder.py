"""Unit tests for MapboxGeocoder."""

import os
import pytest
from unittest.mock import Mock, patch
import requests

from src.enrich.mapbox_geocoder import MapboxGeocoder
from src.enrich.geocoder import GeocodeResult


class TestMapboxGeocoderInit:
    """Test MapboxGeocoder initialization."""
    
    def test_init_with_api_key_parameter(self):
        """Test initialization with explicit API key."""
        geocoder = MapboxGeocoder(api_key="test_key_123")
        assert geocoder.api_key == "test_key_123"
        assert geocoder.timeout == MapboxGeocoder.DEFAULT_TIMEOUT
    
    def test_init_with_environment_variable(self):
        """Test initialization with MAPBOX_API_KEY environment variable."""
        with patch.dict(os.environ, {"MAPBOX_API_KEY": "env_key_456"}):
            geocoder = MapboxGeocoder()
            assert geocoder.api_key == "env_key_456"
    
    def test_init_without_api_key_raises_error(self):
        """Test that initialization fails without API key."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError) as exc_info:
                MapboxGeocoder()
            
            assert "Mapbox API key is required" in str(exc_info.value)
            assert "MAPBOX_API_KEY" in str(exc_info.value)
    
    def test_init_with_custom_timeout(self):
        """Test initialization with custom timeout."""
        geocoder = MapboxGeocoder(api_key="test_key", timeout=30)
        assert geocoder.timeout == 30


class TestMapboxGeocoderGeocode:
    """Test MapboxGeocoder.geocode method."""
    
    def test_geocode_successful_address(self):
        """Test successful geocoding of a valid address."""
        geocoder = MapboxGeocoder(api_key="test_key")
        
        # Mock successful API response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "features": [
                {
                    "geometry": {
                        "coordinates": [-0.1278, 51.5074]  # [longitude, latitude]
                    },
                    "place_name": "123 Test Street, London, UK",
                    "place_type": ["address"],
                    "relevance": 0.99,
                    "properties": {
                        "accuracy": "rooftop"
                    },
                    "context": [
                        {"id": "postcode.123", "text": "SW1A 1AA"},
                        {"id": "place.456", "text": "London"}
                    ]
                }
            ]
        }
        
        with patch("requests.get", return_value=mock_response) as mock_get:
            result = geocoder.geocode("123 Test Street, London")
            
            # Verify API call
            assert mock_get.called
            call_args = mock_get.call_args
            assert "123 Test Street, London" in call_args[0][0]
            assert call_args[1]["params"]["access_token"] == "test_key"
            assert call_args[1]["params"]["limit"] == 1
            assert call_args[1]["timeout"] == geocoder.timeout
            
            # Verify result
            assert result.latitude == 51.5074
            assert result.longitude == -0.1278
            assert result.status == "success"
            assert result.precision == "rooftop"
            assert result.is_success() is True
            
            # Verify metadata
            assert result.metadata["provider"] == "mapbox"
            assert result.metadata["place_name"] == "123 Test Street, London, UK"
            assert result.metadata["place_type"] == ["address"]
            assert result.metadata["relevance"] == 0.99
            assert result.metadata["context"]["postcode"] == "SW1A 1AA"
            assert result.metadata["context"]["place"] == "London"
    
    def test_geocode_empty_address(self):
        """Test geocoding with empty address."""
        geocoder = MapboxGeocoder(api_key="test_key")
        
        result = geocoder.geocode("")
        
        assert result.latitude is None
        assert result.longitude is None
        assert result.status == "invalid_address"
        assert result.precision is None
        assert "Empty address" in result.metadata["error"]
        assert result.is_success() is False
    
    def test_geocode_whitespace_only_address(self):
        """Test geocoding with whitespace-only address."""
        geocoder = MapboxGeocoder(api_key="test_key")
        
        result = geocoder.geocode("   ")
        
        assert result.status == "invalid_address"
        assert result.is_success() is False
    
    def test_geocode_no_results_found(self):
        """Test geocoding when API returns no results."""
        geocoder = MapboxGeocoder(api_key="test_key")
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"features": []}
        
        with patch("requests.get", return_value=mock_response):
            result = geocoder.geocode("Invalid Address XYZ123")
            
            assert result.latitude is None
            assert result.longitude is None
            assert result.status == "invalid_address"
            assert result.precision is None
            assert "No results found" in result.metadata["error"]
            assert result.is_success() is False
    
    def test_geocode_rate_limit_exceeded(self):
        """Test handling of rate limit errors (HTTP 429)."""
        geocoder = MapboxGeocoder(api_key="test_key")
        
        mock_response = Mock()
        mock_response.status_code = 429
        
        with patch("requests.get", return_value=mock_response):
            result = geocoder.geocode("Any Address")
            
            assert result.latitude is None
            assert result.longitude is None
            assert result.status == "failed"
            assert result.precision is None
            assert "Rate limit exceeded" in result.metadata["error"]
            assert result.metadata["status_code"] == 429
            assert result.is_success() is False
    
    def test_geocode_http_error(self):
        """Test handling of HTTP errors."""
        geocoder = MapboxGeocoder(api_key="test_key")
        
        mock_response = Mock()
        mock_response.status_code = 500
        
        with patch("requests.get", return_value=mock_response):
            result = geocoder.geocode("Any Address")
            
            assert result.status == "failed"
            assert result.metadata["status_code"] == 500
            assert "HTTP 500" in result.metadata["error"]
            assert result.is_success() is False
    
    def test_geocode_timeout(self):
        """Test handling of request timeout."""
        geocoder = MapboxGeocoder(api_key="test_key")
        
        with patch("requests.get", side_effect=requests.exceptions.Timeout):
            result = geocoder.geocode("Any Address")
            
            assert result.latitude is None
            assert result.longitude is None
            assert result.status == "failed"
            assert result.precision is None
            assert "timeout" in result.metadata["error"].lower()
            assert result.is_success() is False
    
    def test_geocode_network_error(self):
        """Test handling of network errors."""
        geocoder = MapboxGeocoder(api_key="test_key")
        
        with patch("requests.get", side_effect=requests.exceptions.ConnectionError("Network unreachable")):
            result = geocoder.geocode("Any Address")
            
            assert result.status == "failed"
            assert "Network error" in result.metadata["error"]
            assert result.is_success() is False
    
    def test_geocode_invalid_coordinates_in_response(self):
        """Test handling of invalid coordinates in API response."""
        geocoder = MapboxGeocoder(api_key="test_key")
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "features": [
                {
                    "geometry": {
                        "coordinates": [-0.1278]  # Missing latitude
                    },
                    "place_name": "Test",
                    "place_type": ["address"]
                }
            ]
        }
        
        with patch("requests.get", return_value=mock_response):
            result = geocoder.geocode("Test Address")
            
            assert result.status == "failed"
            assert "Invalid coordinates" in result.metadata["error"]
            assert result.is_success() is False
    
    def test_geocode_unexpected_exception(self):
        """Test handling of unexpected exceptions."""
        geocoder = MapboxGeocoder(api_key="test_key")
        
        with patch("requests.get", side_effect=Exception("Unexpected error")):
            result = geocoder.geocode("Any Address")
            
            assert result.status == "failed"
            assert "Unexpected error" in result.metadata["error"]
            assert result.is_success() is False


class TestMapboxGeocoderPrecisionMapping:
    """Test precision mapping from Mapbox place types."""
    
    def test_precision_address(self):
        """Test precision for address type."""
        geocoder = MapboxGeocoder(api_key="test_key")
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "features": [{
                "geometry": {"coordinates": [-0.1278, 51.5074]},
                "place_name": "Address",
                "place_type": ["address"]
            }]
        }
        
        with patch("requests.get", return_value=mock_response):
            result = geocoder.geocode("Test")
            assert result.precision == "rooftop"
    
    def test_precision_poi(self):
        """Test precision for POI type."""
        geocoder = MapboxGeocoder(api_key="test_key")
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "features": [{
                "geometry": {"coordinates": [-0.1278, 51.5074]},
                "place_name": "POI",
                "place_type": ["poi"]
            }]
        }
        
        with patch("requests.get", return_value=mock_response):
            result = geocoder.geocode("Test")
            assert result.precision == "rooftop"
    
    def test_precision_street(self):
        """Test precision for street type."""
        geocoder = MapboxGeocoder(api_key="test_key")
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "features": [{
                "geometry": {"coordinates": [-0.1278, 51.5074]},
                "place_name": "Street",
                "place_type": ["street"]
            }]
        }
        
        with patch("requests.get", return_value=mock_response):
            result = geocoder.geocode("Test")
            assert result.precision == "street"
    
    def test_precision_city(self):
        """Test precision for city/place type."""
        geocoder = MapboxGeocoder(api_key="test_key")
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "features": [{
                "geometry": {"coordinates": [-0.1278, 51.5074]},
                "place_name": "City",
                "place_type": ["place"]
            }]
        }
        
        with patch("requests.get", return_value=mock_response):
            result = geocoder.geocode("Test")
            assert result.precision == "city"
    
    def test_precision_region(self):
        """Test precision for region type."""
        geocoder = MapboxGeocoder(api_key="test_key")
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "features": [{
                "geometry": {"coordinates": [-0.1278, 51.5074]},
                "place_name": "Region",
                "place_type": ["region"]
            }]
        }
        
        with patch("requests.get", return_value=mock_response):
            result = geocoder.geocode("Test")
            assert result.precision == "region"
    
    def test_precision_unknown_type(self):
        """Test precision for unknown place type."""
        geocoder = MapboxGeocoder(api_key="test_key")
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "features": [{
                "geometry": {"coordinates": [-0.1278, 51.5074]},
                "place_name": "Unknown",
                "place_type": ["unknown_type"]
            }]
        }
        
        with patch("requests.get", return_value=mock_response):
            result = geocoder.geocode("Test")
            assert result.precision == "unknown"
    
    def test_precision_empty_place_type(self):
        """Test precision when place_type is empty."""
        geocoder = MapboxGeocoder(api_key="test_key")
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "features": [{
                "geometry": {"coordinates": [-0.1278, 51.5074]},
                "place_name": "Test",
                "place_type": []
            }]
        }
        
        with patch("requests.get", return_value=mock_response):
            result = geocoder.geocode("Test")
            assert result.precision == "unknown"


class TestMapboxGeocoderMetadata:
    """Test metadata extraction from Mapbox responses."""
    
    def test_metadata_with_full_context(self):
        """Test metadata extraction with full context information."""
        geocoder = MapboxGeocoder(api_key="test_key")
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "features": [{
                "geometry": {"coordinates": [-0.1278, 51.5074]},
                "place_name": "123 Test St, London, UK",
                "place_type": ["address"],
                "relevance": 0.95,
                "properties": {"accuracy": "rooftop"},
                "context": [
                    {"id": "postcode.1", "text": "SW1A 1AA"},
                    {"id": "place.2", "text": "London"},
                    {"id": "region.3", "text": "England"},
                    {"id": "country.4", "text": "United Kingdom"}
                ]
            }]
        }
        
        with patch("requests.get", return_value=mock_response):
            result = geocoder.geocode("Test")
            
            assert result.metadata["provider"] == "mapbox"
            assert result.metadata["place_name"] == "123 Test St, London, UK"
            assert result.metadata["relevance"] == 0.95
            assert result.metadata["confidence"] == "rooftop"
            assert result.metadata["context"]["postcode"] == "SW1A 1AA"
            assert result.metadata["context"]["place"] == "London"
            assert result.metadata["context"]["region"] == "England"
            assert result.metadata["context"]["country"] == "United Kingdom"
    
    def test_metadata_without_context(self):
        """Test metadata extraction without context information."""
        geocoder = MapboxGeocoder(api_key="test_key")
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "features": [{
                "geometry": {"coordinates": [-0.1278, 51.5074]},
                "place_name": "Test Location",
                "place_type": ["poi"],
                "relevance": 0.8
            }]
        }
        
        with patch("requests.get", return_value=mock_response):
            result = geocoder.geocode("Test")
            
            assert result.metadata["provider"] == "mapbox"
            assert result.metadata["place_name"] == "Test Location"
            assert result.metadata["relevance"] == 0.8
            assert result.metadata["confidence"] is None
            # Context should not be in metadata if not present
            assert "context" not in result.metadata or not result.metadata.get("context")
    
    def test_metadata_with_partial_properties(self):
        """Test metadata extraction with partial properties."""
        geocoder = MapboxGeocoder(api_key="test_key")
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "features": [{
                "geometry": {"coordinates": [-0.1278, 51.5074]},
                "place_name": "Partial Data",
                "place_type": ["address"]
                # Missing relevance and properties
            }]
        }
        
        with patch("requests.get", return_value=mock_response):
            result = geocoder.geocode("Test")
            
            assert result.metadata["provider"] == "mapbox"
            assert result.metadata["place_name"] == "Partial Data"
            assert result.metadata["relevance"] is None
            assert result.metadata["confidence"] is None
