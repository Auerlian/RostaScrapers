"""Tests for Comptoir Bakery scraper."""

import pytest
from unittest.mock import Mock, patch
import requests

from src.extract.comptoir_bakery import ComptoirBakeryScraper
from src.models.raw_provider_data import RawProviderData


class TestComptoirBakeryScraper:
    """Test suite for Comptoir Bakery scraper."""
    
    def test_provider_name(self):
        """Test that provider name is correct."""
        scraper = ComptoirBakeryScraper()
        assert scraper.provider_name == "Comptoir Bakery"
    
    def test_provider_metadata(self):
        """Test that provider metadata is correct."""
        scraper = ComptoirBakeryScraper()
        metadata = scraper.provider_metadata
        
        assert metadata["website"] == "https://www.comptoirbakery.co.uk"
        assert metadata["contact_email"] == "enquiries@comptoirbakery.co.uk"
        assert metadata["source_name"] == "Comptoir Bakery Website + Bookwhen"
        assert metadata["source_base_url"] == "https://www.comptoirbakery.co.uk/pages/all-our-workshops"
    
    def test_extract_locations(self):
        """Test that location extraction returns correct data."""
        scraper = ComptoirBakeryScraper()
        locations = scraper._extract_locations()
        
        assert len(locations) == 1
        assert locations[0]["location_name"] == "Comptoir Bakery School and Workshop"
        assert locations[0]["formatted_address"] == "Comptoir Bakery School and Workshop, 96 Druid Street, London, SE1 2HQ"
        assert locations[0]["address_line_1"] == "96 Druid Street"
        assert locations[0]["city"] == "London"
        assert locations[0]["postcode"] == "SE1 2HQ"
        assert locations[0]["country"] == "UK"
    
    @patch('src.extract.comptoir_bakery.ComptoirBakeryScraper.fetch_url')
    def test_scrape_returns_raw_provider_data(self, mock_fetch):
        """Test that scrape returns RawProviderData with locations and templates."""
        scraper = ComptoirBakeryScraper()
        
        # Mock HTML response for workshops page
        mock_response = Mock()
        mock_response.text = """
        <html>
            <body>
                <div class="info-cols--image_and_text-column">
                    <a href="/pages/croissant-class">Croissant Making</a>
                    <img src="/images/croissant.jpg" />
                    <p>Learn to make perfect croissants</p>
                </div>
            </body>
        </html>
        """
        mock_fetch.return_value = mock_response
        
        # Execute scrape
        result = scraper.scrape()
        
        # Verify result structure
        assert isinstance(result, RawProviderData)
        assert result.provider_name == "Comptoir Bakery"
        assert result.provider_website == "https://www.comptoirbakery.co.uk"
        assert result.provider_contact_email == "enquiries@comptoirbakery.co.uk"
        
        # Verify locations
        assert len(result.raw_locations) == 1
        assert result.raw_locations[0]["location_name"] == "Comptoir Bakery School and Workshop"
    
    @patch('src.extract.comptoir_bakery.ComptoirBakeryScraper.fetch_url')
    def test_scrape_with_bookwhen_tickets(self, mock_fetch):
        """Test scraping with Bookwhen event that has multiple tickets."""
        scraper = ComptoirBakeryScraper()
        
        # Mock workshops page
        workshops_html = """
        <html>
            <body>
                <div class="info-cols--image_and_text-column">
                    <a href="https://bookwhen.com/comptoirbakeryschool/e/ev-123">Croissant Class</a>
                    <img src="/images/croissant.jpg" />
                    <p>Learn to make croissants</p>
                </div>
            </body>
        </html>
        """
        
        # Mock Bookwhen event page
        bookwhen_html = """
        <html>
            <head>
                <script type="application/ld+json">
                {
                    "description": "Master the art of French pastry",
                    "offers": [
                        {"price": "75", "priceCurrency": "GBP"},
                        {"price": "120", "priceCurrency": "GBP"}
                    ]
                }
                </script>
            </head>
            <body>
                <div class="ticket_information">
                    <h4 class="ticket-summary-title__title">Standard Ticket</h4>
                    <div class="summary_text">Individual class</div>
                </div>
                <div class="ticket_information">
                    <h4 class="ticket-summary-title__title">Couple Ticket</h4>
                    <div class="summary_text">For two people</div>
                </div>
            </body>
        </html>
        """
        
        def fetch_side_effect(url, **kwargs):
            mock_resp = Mock()
            if "all-our-workshops" in url:
                mock_resp.text = workshops_html
            elif "bookwhen.com" in url:
                mock_resp.text = bookwhen_html
            return mock_resp
        
        mock_fetch.side_effect = fetch_side_effect
        
        # Execute scrape
        result = scraper.scrape()
        
        # Verify templates created for each ticket
        assert len(result.raw_templates) == 2
        assert "Croissant Class — Standard Ticket" in result.raw_templates[0]["title"]
        assert "Croissant Class — Couple Ticket" in result.raw_templates[1]["title"]
        assert result.raw_templates[0]["price"] == "£75"
        assert result.raw_templates[1]["price"] == "£120"
    
    @patch('src.extract.comptoir_bakery.ComptoirBakeryScraper.fetch_url')
    def test_scrape_with_occurrences(self, mock_fetch):
        """Test scraping with Bookwhen event that has scheduled occurrences."""
        scraper = ComptoirBakeryScraper()
        
        # Mock workshops page
        workshops_html = """
        <html>
            <body>
                <div class="info-cols--image_and_text-column">
                    <a href="https://bookwhen.com/comptoirbakeryschool/e/ev-123">Bread Class</a>
                </div>
            </body>
        </html>
        """
        
        # Mock Bookwhen event page with EventSeries
        bookwhen_html = """
        <html>
            <head>
                <script type="application/ld+json">
                {
                    "@type": "EventSeries",
                    "description": "Learn bread making",
                    "subEvent": [
                        {
                            "startDate": "2025-02-15T10:00:00Z",
                            "endDate": "2025-02-15T13:00:00Z",
                            "url": "https://bookwhen.com/comptoirbakeryschool/e/ev-123-1"
                        },
                        {
                            "startDate": "2025-02-22T10:00:00Z",
                            "endDate": "2025-02-22T13:00:00Z",
                            "url": "https://bookwhen.com/comptoirbakeryschool/e/ev-123-2"
                        }
                    ]
                }
                </script>
            </head>
            <body>
                <div class="ticket_information">
                    <h4 class="ticket-summary-title__title">Standard</h4>
                </div>
            </body>
        </html>
        """
        
        def fetch_side_effect(url, **kwargs):
            mock_resp = Mock()
            if "all-our-workshops" in url:
                mock_resp.text = workshops_html
            elif "bookwhen.com" in url:
                mock_resp.text = bookwhen_html
            return mock_resp
        
        mock_fetch.side_effect = fetch_side_effect
        
        # Execute scrape
        result = scraper.scrape()
        
        # Verify occurrences extracted
        assert len(result.raw_events) == 2
        assert result.raw_events[0]["start_at"] == "2025-02-15T10:00:00Z"
        assert result.raw_events[0]["end_at"] == "2025-02-15T13:00:00Z"
        assert result.raw_events[1]["start_at"] == "2025-02-22T10:00:00Z"
    
    @patch('src.extract.comptoir_bakery.ComptoirBakeryScraper.fetch_url')
    def test_scrape_handles_empty_workshops(self, mock_fetch):
        """Test that scrape handles empty workshops page gracefully."""
        scraper = ComptoirBakeryScraper()
        
        mock_response = Mock()
        mock_response.text = "<html><body></body></html>"
        mock_fetch.return_value = mock_response
        
        result = scraper.scrape()
        
        assert isinstance(result, RawProviderData)
        assert len(result.raw_locations) == 1  # Location always present
        assert len(result.raw_templates) == 0
        assert len(result.raw_events) == 0
    
    @patch('src.extract.comptoir_bakery.ComptoirBakeryScraper.fetch_url')
    def test_scrape_propagates_network_errors(self, mock_fetch):
        """Test that network errors are propagated."""
        scraper = ComptoirBakeryScraper()
        
        mock_fetch.side_effect = requests.RequestException("Network error")
        
        with pytest.raises(requests.RequestException):
            scraper.scrape()
    
    def test_clean_text(self):
        """Test text cleaning utility."""
        scraper = ComptoirBakeryScraper()
        
        assert scraper._clean_text("  Hello   World  ") == "Hello World"
        assert scraper._clean_text("Line1\n\nLine2") == "Line1 Line2"
        assert scraper._clean_text(None) is None
        assert scraper._clean_text("") is None
    
    def test_absolute_url(self):
        """Test URL conversion utility."""
        scraper = ComptoirBakeryScraper()
        
        base = "https://example.com/page"
        assert scraper._absolute_url(base, "/path") == "https://example.com/path"
        assert scraper._absolute_url(base, "https://other.com") == "https://other.com"
        assert scraper._absolute_url(base, None) is None

