"""Integration tests for MapboxGeocoder with Geocoder interface."""

import pytest
from unittest.mock import Mock, patch

from src.enrich import Geocoder, MapboxGeocoder, GeocodeResult


class TestMapboxGeocoderInterface:
    """Test that MapboxGeocoder properly implements Geocoder interface."""
    
    def test_mapbox_geocoder_is_geocoder_instance(self):
        """Test that MapboxGeocoder is an instance of Geocoder."""
        geocoder = MapboxGeocoder(api_key="test_key")
        assert isinstance(geocoder, Geocoder)
    
    def test_mapbox_geocoder_implements_geocode_method(self):
        """Test that MapboxGeocoder implements the geocode method."""
        geocoder = MapboxGeocoder(api_key="test_key")
        assert hasattr(geocoder, "geocode")
        assert callable(geocoder.geocode)
    
    def test_mapbox_geocoder_returns_geocode_result(self):
        """Test that MapboxGeocoder.geocode returns GeocodeResult."""
        geocoder = MapboxGeocoder(api_key="test_key")
        
        # Mock API response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "features": [{
                "geometry": {"coordinates": [-0.1278, 51.5074]},
                "place_name": "London, UK",
                "place_type": ["place"]
            }]
        }
        
        with patch("requests.get", return_value=mock_response):
            result = geocoder.geocode("London")
            
            assert isinstance(result, GeocodeResult)
            assert hasattr(result, "latitude")
            assert hasattr(result, "longitude")
            assert hasattr(result, "status")
            assert hasattr(result, "precision")
            assert hasattr(result, "metadata")
            assert hasattr(result, "is_success")
    
    def test_mapbox_geocoder_can_be_used_polymorphically(self):
        """Test that MapboxGeocoder can be used through Geocoder interface."""
        # This demonstrates polymorphic usage
        def geocode_address(geocoder: Geocoder, address: str) -> GeocodeResult:
            """Function that accepts any Geocoder implementation."""
            return geocoder.geocode(address)
        
        mapbox_geocoder = MapboxGeocoder(api_key="test_key")
        
        # Mock API response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "features": [{
                "geometry": {"coordinates": [-0.1278, 51.5074]},
                "place_name": "Test",
                "place_type": ["address"]
            }]
        }
        
        with patch("requests.get", return_value=mock_response):
            result = geocode_address(mapbox_geocoder, "Test Address")
            
            assert isinstance(result, GeocodeResult)
            assert result.is_success() is True
    
    def test_mapbox_geocoder_error_handling_returns_valid_result(self):
        """Test that MapboxGeocoder always returns valid GeocodeResult even on errors."""
        geocoder = MapboxGeocoder(api_key="test_key")
        
        # Test various error conditions
        error_conditions = [
            (Mock(status_code=429), "rate limit"),
            (Mock(status_code=500), "server error"),
        ]
        
        for mock_response, error_type in error_conditions:
            with patch("requests.get", return_value=mock_response):
                result = geocoder.geocode("Test")
                
                # Should always return a valid GeocodeResult
                assert isinstance(result, GeocodeResult)
                assert result.status == "failed"
                assert result.latitude is None
                assert result.longitude is None
                assert result.is_success() is False
                assert "error" in result.metadata


class TestMapboxGeocoderRealWorldScenarios:
    """Test MapboxGeocoder with realistic scenarios."""
    
    def test_geocode_uk_address(self):
        """Test geocoding a typical UK address."""
        geocoder = MapboxGeocoder(api_key="test_key")
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "features": [{
                "geometry": {"coordinates": [-0.1278, 51.5074]},
                "place_name": "10 Downing Street, Westminster, London SW1A 2AA, United Kingdom",
                "place_type": ["address"],
                "relevance": 0.99,
                "properties": {"accuracy": "rooftop"},
                "context": [
                    {"id": "postcode.1", "text": "SW1A 2AA"},
                    {"id": "place.2", "text": "London"},
                    {"id": "region.3", "text": "England"},
                    {"id": "country.4", "text": "United Kingdom"}
                ]
            }]
        }
        
        with patch("requests.get", return_value=mock_response):
            result = geocoder.geocode("10 Downing Street, London")
            
            assert result.is_success() is True
            assert result.latitude == 51.5074
            assert result.longitude == -0.1278
            assert result.precision == "rooftop"
            assert result.metadata["provider"] == "mapbox"
            assert "London" in result.metadata["place_name"]
    
    def test_geocode_poi(self):
        """Test geocoding a point of interest."""
        geocoder = MapboxGeocoder(api_key="test_key")
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "features": [{
                "geometry": {"coordinates": [-0.1276, 51.5074]},
                "place_name": "Big Ben, Westminster, London, United Kingdom",
                "place_type": ["poi"],
                "relevance": 0.95,
                "context": [
                    {"id": "place.1", "text": "London"}
                ]
            }]
        }
        
        with patch("requests.get", return_value=mock_response):
            result = geocoder.geocode("Big Ben, London")
            
            assert result.is_success() is True
            assert result.precision == "rooftop"  # POI maps to rooftop precision
            assert "Big Ben" in result.metadata["place_name"]
    
    def test_geocode_partial_address(self):
        """Test geocoding with partial address (city only)."""
        geocoder = MapboxGeocoder(api_key="test_key")
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "features": [{
                "geometry": {"coordinates": [-0.1278, 51.5074]},
                "place_name": "London, United Kingdom",
                "place_type": ["place"],
                "relevance": 0.99
            }]
        }
        
        with patch("requests.get", return_value=mock_response):
            result = geocoder.geocode("London")
            
            assert result.is_success() is True
            assert result.precision == "city"  # Place type maps to city precision
    
    def test_geocode_ambiguous_address(self):
        """Test geocoding with ambiguous address (returns best match)."""
        geocoder = MapboxGeocoder(api_key="test_key")
        
        # Mapbox returns the best match when limit=1
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "features": [{
                "geometry": {"coordinates": [-0.1278, 51.5074]},
                "place_name": "High Street, London, United Kingdom",
                "place_type": ["street"],
                "relevance": 0.75  # Lower relevance indicates ambiguity
            }]
        }
        
        with patch("requests.get", return_value=mock_response):
            result = geocoder.geocode("High Street")
            
            assert result.is_success() is True
            assert result.precision == "street"
            assert result.metadata["relevance"] == 0.75
