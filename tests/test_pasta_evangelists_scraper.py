"""Tests for Pasta Evangelists scraper."""

import pytest
from unittest.mock import Mock, patch
import requests

from src.extract.pasta_evangelists import PastaEvangelistsScraper
from src.models.raw_provider_data import RawProviderData


class TestPastaEvangelistsScraper:
    """Test suite for Pasta Evangelists scraper."""
    
    def test_provider_name(self):
        """Test that provider name is correct."""
        scraper = PastaEvangelistsScraper()
        assert scraper.provider_name == "Pasta Evangelists"
    
    def test_provider_metadata(self):
        """Test that provider metadata is correct."""
        scraper = PastaEvangelistsScraper()
        metadata = scraper.provider_metadata
        
        assert metadata["website"] == "https://plan.pastaevangelists.com"
        assert metadata["contact_email"] == "events@pastaevangelists.com"
        assert metadata["source_name"] == "Pasta Evangelists API"
        assert metadata["source_base_url"] == "https://pensa.pastaevangelists.com/api/v2"
    
    @patch('src.extract.pasta_evangelists.PastaEvangelistsScraper.fetch_url')
    def test_scrape_returns_raw_provider_data(self, mock_fetch):
        """Test that scrape returns RawProviderData with locations and templates."""
        scraper = PastaEvangelistsScraper()
        
        # Mock API responses
        mock_locations_response = Mock()
        mock_locations_response.json.return_value = {
            "data": [
                {
                    "id": "loc1",
                    "attributes": {
                        "name": "Test Location",
                        "address1": "123 Test St",
                        "city": "London",
                        "zip": "SW1A 1AA"
                    }
                }
            ],
            "meta": {"next_page": None}
        }
        
        mock_templates_response = Mock()
        mock_templates_response.json.return_value = {
            "data": [
                {
                    "id": "tmpl1",
                    "attributes": {
                        "name": "Pasta Making Class",
                        "summary": "Learn to make fresh pasta",
                        "price": "75.00"
                    }
                }
            ],
            "meta": {"next_page": None}
        }
        
        # Configure mock to return different responses for different endpoints
        def fetch_side_effect(url, **kwargs):
            if "event_locations" in url:
                return mock_locations_response
            elif "event_templates" in url:
                return mock_templates_response
            raise ValueError(f"Unexpected URL: {url}")
        
        mock_fetch.side_effect = fetch_side_effect
        
        # Execute scrape
        result = scraper.scrape()
        
        # Verify result structure
        assert isinstance(result, RawProviderData)
        assert result.provider_name == "Pasta Evangelists"
        assert result.provider_website == "https://plan.pastaevangelists.com"
        assert result.provider_contact_email == "events@pastaevangelists.com"
        
        # Verify locations
        assert len(result.raw_locations) == 1
        assert result.raw_locations[0]["id"] == "loc1"
        assert result.raw_locations[0]["attributes"]["name"] == "Test Location"
        
        # Verify templates
        assert len(result.raw_templates) == 1
        assert result.raw_templates[0]["id"] == "tmpl1"
        assert result.raw_templates[0]["attributes"]["name"] == "Pasta Making Class"
        
        # Verify no raw events (this provider uses templates)
        assert len(result.raw_events) == 0
    
    @patch('src.extract.pasta_evangelists.PastaEvangelistsScraper.fetch_url')
    def test_fetch_all_pages_single_page(self, mock_fetch):
        """Test pagination with single page."""
        scraper = PastaEvangelistsScraper()
        
        mock_response = Mock()
        mock_response.json.return_value = {
            "data": [{"id": "1"}, {"id": "2"}],
            "meta": {"next_page": None}
        }
        mock_fetch.return_value = mock_response
        
        result = scraper._fetch_all_pages("test_endpoint")
        
        assert len(result) == 2
        assert result[0]["id"] == "1"
        assert result[1]["id"] == "2"
        mock_fetch.assert_called_once()
    
    @patch('src.extract.pasta_evangelists.PastaEvangelistsScraper.fetch_url')
    def test_fetch_all_pages_multiple_pages(self, mock_fetch):
        """Test pagination with multiple pages."""
        scraper = PastaEvangelistsScraper()
        
        # Mock responses for multiple pages
        page1_response = Mock()
        page1_response.json.return_value = {
            "data": [{"id": "1"}, {"id": "2"}],
            "meta": {"next_page": 2}
        }
        
        page2_response = Mock()
        page2_response.json.return_value = {
            "data": [{"id": "3"}, {"id": "4"}],
            "meta": {"next_page": 3}
        }
        
        page3_response = Mock()
        page3_response.json.return_value = {
            "data": [{"id": "5"}],
            "meta": {"next_page": None}
        }
        
        mock_fetch.side_effect = [page1_response, page2_response, page3_response]
        
        result = scraper._fetch_all_pages("test_endpoint")
        
        assert len(result) == 5
        assert result[0]["id"] == "1"
        assert result[4]["id"] == "5"
        assert mock_fetch.call_count == 3
    
    @patch('src.extract.pasta_evangelists.PastaEvangelistsScraper.fetch_url')
    def test_scrape_handles_empty_data(self, mock_fetch):
        """Test that scrape handles empty data gracefully."""
        scraper = PastaEvangelistsScraper()
        
        mock_response = Mock()
        mock_response.json.return_value = {
            "data": [],
            "meta": {"next_page": None}
        }
        mock_fetch.return_value = mock_response
        
        result = scraper.scrape()
        
        assert isinstance(result, RawProviderData)
        assert len(result.raw_locations) == 0
        assert len(result.raw_templates) == 0
        assert len(result.raw_events) == 0
    
    @patch('src.extract.pasta_evangelists.PastaEvangelistsScraper.fetch_url')
    def test_scrape_propagates_network_errors(self, mock_fetch):
        """Test that network errors are propagated."""
        scraper = PastaEvangelistsScraper()
        
        mock_fetch.side_effect = requests.RequestException("Network error")
        
        with pytest.raises(requests.RequestException):
            scraper.scrape()
