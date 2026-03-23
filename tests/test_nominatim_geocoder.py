"""Tests for Nominatim geocoder."""

import pytest
from unittest.mock import Mock, patch
import time

from src.enrich.nominatim_geocoder import NominatimGeocoder


@pytest.fixture
def geocoder():
    """Create Nominatim geocoder instance."""
    return NominatimGeocoder(
        user_agent="RostaScrapers/Test",
        rate_limit_delay=0.1  # Faster for tests
    )


@pytest.fixture
def mock_nominatim_response():
    """Mock successful Nominatim API response."""
    return [
        {
            "lat": "51.5074",
            "lon": "-0.1278",
            "display_name": "London, Greater London, England, United Kingdom",
            "type": "city",
            "osm_type": "relation",
            "address": {
                "city": "London",
                "country": "United Kingdom"
            }
        }
    ]


def test_geocode_success(geocoder, mock_nominatim_response):
    """Test successful geocoding."""
    with patch('requests.get') as mock_get:
        mock_response = Mock()
        mock_response.json.return_value = mock_nominatim_response
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        result = geocoder.geocode("London, UK")
        
        assert result.status == "success"
        assert result.latitude == 51.5074
        assert result.longitude == -0.1278
        assert result.metadata["provider"] == "nominatim"
        assert "London" in result.metadata["display_name"]


def test_geocode_no_results(geocoder):
    """Test geocoding with no results."""
    with patch('requests.get') as mock_get:
        mock_response = Mock()
        mock_response.json.return_value = []
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        result = geocoder.geocode("Invalid Address XYZ123")
        
        assert result.status == "invalid_address"
        assert "No results found" in result.metadata["error"]


def test_geocode_timeout(geocoder):
    """Test geocoding timeout."""
    import requests
    with patch('requests.get') as mock_get:
        mock_get.side_effect = requests.exceptions.Timeout("Timeout")
        
        result = geocoder.geocode("London, UK")
        
        assert result.status == "failed"
        assert result.metadata.get("error") is not None


def test_rate_limit_enforcement(geocoder):
    """Test that rate limiting is enforced."""
    with patch('requests.get') as mock_get:
        mock_response = Mock()
        mock_response.json.return_value = [
            {"lat": "51.5", "lon": "-0.1", "display_name": "Test", "type": "city"}
        ]
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        start_time = time.time()
        geocoder.geocode("Address 1")
        geocoder.geocode("Address 2")
        elapsed = time.time() - start_time
        
        # Should take at least rate_limit_delay seconds
        assert elapsed >= geocoder.rate_limit_delay


def test_precision_determination(geocoder):
    """Test precision level determination."""
    # Building/house level
    assert geocoder._determine_precision({"type": "house", "osm_type": "node"}) == "rooftop"
    
    # Street level
    assert geocoder._determine_precision({"type": "road"}) == "street"
    
    # City level
    assert geocoder._determine_precision({"type": "city"}) == "city"
    
    # Region level
    assert geocoder._determine_precision({"type": "county"}) == "region"


def test_user_agent_header(geocoder):
    """Test that User-Agent header is included."""
    with patch('requests.get') as mock_get:
        mock_response = Mock()
        mock_response.json.return_value = []
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        geocoder.geocode("Test Address")
        
        # Verify User-Agent was included in headers
        call_kwargs = mock_get.call_args[1]
        assert "User-Agent" in call_kwargs["headers"]
        assert call_kwargs["headers"]["User-Agent"] == "RostaScrapers/Test"


def test_batch_geocode(geocoder):
    """Test batch geocoding."""
    with patch('requests.get') as mock_get:
        mock_response = Mock()
        mock_response.json.return_value = [
            {"lat": "51.5", "lon": "-0.1", "display_name": "Test", "type": "city"}
        ]
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        addresses = ["Address 1", "Address 2", "Address 3"]
        results = geocoder.batch_geocode(addresses)
        
        assert len(results) == 3
        assert all(r.status == "success" for r in results)
