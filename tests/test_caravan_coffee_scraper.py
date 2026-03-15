"""Tests for Caravan Coffee scraper."""

import pytest
from unittest.mock import Mock, patch
import requests

from src.extract.caravan_coffee import CaravanCoffeeScraper
from src.models.raw_provider_data import RawProviderData


class TestCaravanCoffeeScraper:
    """Test suite for Caravan Coffee scraper."""
    
    def test_provider_name(self):
        """Test that provider name is correct."""
        scraper = CaravanCoffeeScraper()
        assert scraper.provider_name == "Caravan Coffee Roasters"
    
    def test_provider_metadata(self):
        """Test that provider metadata is correct."""
        scraper = CaravanCoffeeScraper()
        metadata = scraper.provider_metadata
        
        assert metadata["website"] == "https://caravanandco.com"
        assert metadata["contact_email"] is None
        assert metadata["source_name"] == "Caravan Coffee Website + Eventbrite"
        assert metadata["source_base_url"] == "https://caravanandco.com/pages/coffee-school"
    
    def test_extract_locations(self):
        """Test that location extraction returns correct data."""
        scraper = CaravanCoffeeScraper()
        locations = scraper._extract_locations()
        
        assert len(locations) == 1
        assert locations[0]["location_name"] == "Lambworks Roastery Brewbar"
        assert locations[0]["formatted_address"] == "Lambworks Roastery Brewbar, North Road, London, N7 9DP"
        assert locations[0]["address_line_1"] == "North Road"
        assert locations[0]["city"] == "London"
        assert locations[0]["postcode"] == "N7 9DP"
        assert locations[0]["country"] == "UK"
    
    @patch('src.extract.caravan_coffee.CaravanCoffeeScraper.fetch_url')
    def test_scrape_returns_raw_provider_data(self, mock_fetch):
        """Test that scrape returns RawProviderData with locations and templates."""
        scraper = CaravanCoffeeScraper()
        
        # Mock HTML response for coffee school page
        mock_response = Mock()
        mock_response.text = """
        <html>
            <body>
                <h2>LONDON ROASTERY TOUR & TASTING</h2>
                <p>Join us for a tour of our roastery</p>
                <img src="/images/roastery.jpg" />
                <a href="https://eventbrite.com/e/roastery-tour-123456789012">SIGN ME UP</a>
            </body>
        </html>
        """
        mock_fetch.return_value = mock_response
        
        # Execute scrape
        result = scraper.scrape()
        
        # Verify result structure
        assert isinstance(result, RawProviderData)
        assert result.provider_name == "Caravan Coffee Roasters"
        assert result.provider_website == "https://caravanandco.com"
        assert result.provider_contact_email is None
        
        # Verify locations
        assert len(result.raw_locations) == 1
        assert result.raw_locations[0]["location_name"] == "Lambworks Roastery Brewbar"
    
    @patch('src.extract.caravan_coffee.CaravanCoffeeScraper.fetch_url')
    def test_scrape_with_eventbrite_details(self, mock_fetch):
        """Test scraping with Eventbrite event that has price and location."""
        scraper = CaravanCoffeeScraper()
        
        # Mock coffee school page
        coffee_school_html = """
        <html>
            <body>
                <h2>HOME FILTER CLASS</h2>
                <p>Learn filter coffee brewing techniques</p>
                <img src="/images/filter.jpg" />
                <a href="https://eventbrite.com/e/filter-class-123456789012">SIGN ME UP</a>
            </body>
        </html>
        """
        
        # Mock Eventbrite event page
        eventbrite_html = """
        <html>
            <head>
                <script type="application/ld+json">
                {
                    "description": "Master filter coffee brewing",
                    "offers": {
                        "price": "45",
                        "priceCurrency": "GBP"
                    },
                    "location": {
                        "name": "Lambworks Roastery",
                        "address": {
                            "streetAddress": "North Road",
                            "addressLocality": "London",
                            "addressRegion": "Greater London",
                            "postalCode": "N7 9DP"
                        }
                    },
                    "startDate": "2025-03-15T10:00:00Z",
                    "endDate": "2025-03-15T12:00:00Z"
                }
                </script>
            </head>
            <body></body>
        </html>
        """
        
        def fetch_side_effect(url, **kwargs):
            mock_resp = Mock()
            if "coffee-school" in url:
                mock_resp.text = coffee_school_html
            elif "eventbrite.com" in url:
                mock_resp.text = eventbrite_html
            return mock_resp
        
        mock_fetch.side_effect = fetch_side_effect
        
        # Execute scrape
        result = scraper.scrape()
        
        # Verify template created
        assert len(result.raw_templates) == 1
        assert result.raw_templates[0]["title"] == "Home Filter Class"
        assert result.raw_templates[0]["price"] == "£45"
        assert "filter coffee brewing" in result.raw_templates[0]["description"].lower()
        
        # Verify occurrence extracted
        assert len(result.raw_events) == 1
        assert result.raw_events[0]["start_at"] == "2025-03-15T10:00:00Z"
        assert result.raw_events[0]["end_at"] == "2025-03-15T12:00:00Z"
        assert result.raw_events[0]["price"] == "£45"
        assert "location_data" in result.raw_events[0]
    
    @patch('src.extract.caravan_coffee.CaravanCoffeeScraper.fetch_url')
    def test_scrape_multiple_classes(self, mock_fetch):
        """Test scraping multiple coffee classes."""
        scraper = CaravanCoffeeScraper()
        
        # Mock coffee school page with multiple classes
        coffee_school_html = """
        <html>
            <body>
                <h2>LONDON ROASTERY TOUR & TASTING</h2>
                <p>Tour our roastery</p>
                <a href="https://eventbrite.com/e/tour-123456789012">SIGN ME UP</a>
                
                <h2>HOME FILTER CLASS</h2>
                <p>Learn filter brewing</p>
                <a href="https://eventbrite.com/e/filter-123456789012">SIGN ME UP</a>
                
                <h2>HOME ESPRESSO CLASS</h2>
                <p>Master espresso</p>
                <a href="https://eventbrite.com/e/espresso-123456789012">SIGN ME UP</a>
                
                <h2>MILK & LATTE ART CLASS</h2>
                <p>Create latte art</p>
                <a href="https://eventbrite.com/e/latte-123456789012">SIGN ME UP</a>
            </body>
        </html>
        """
        
        # Mock Eventbrite responses (minimal)
        eventbrite_html = """
        <html>
            <head>
                <script type="application/ld+json">
                {
                    "offers": {"price": "50", "priceCurrency": "GBP"}
                }
                </script>
            </head>
            <body></body>
        </html>
        """
        
        def fetch_side_effect(url, **kwargs):
            mock_resp = Mock()
            if "coffee-school" in url:
                mock_resp.text = coffee_school_html
            elif "eventbrite.com" in url:
                mock_resp.text = eventbrite_html
            return mock_resp
        
        mock_fetch.side_effect = fetch_side_effect
        
        # Execute scrape
        result = scraper.scrape()
        
        # Verify all 4 classes extracted
        assert len(result.raw_templates) == 4
        titles = [t["title"] for t in result.raw_templates]
        assert "London Roastery Tour & Tasting" in titles
        assert "Home Filter Class" in titles
        assert "Home Espresso Class" in titles
        assert "Milk & Latte Art Class" in titles
    
    @patch('src.extract.caravan_coffee.CaravanCoffeeScraper.fetch_url')
    def test_scrape_with_api_fallback(self, mock_fetch):
        """Test that API fallback works when JSON-LD doesn't have price."""
        scraper = CaravanCoffeeScraper()
        
        # Mock coffee school page
        coffee_school_html = """
        <html>
            <body>
                <h2>HOME FILTER CLASS</h2>
                <p>Learn filter brewing</p>
                <a href="https://eventbrite.com/e/filter-123456789012">SIGN ME UP</a>
            </body>
        </html>
        """
        
        # Mock Eventbrite page without price in JSON-LD
        eventbrite_html = """
        <html>
            <head>
                <script type="application/ld+json">
                {
                    "description": "Filter class"
                }
                </script>
            </head>
            <body></body>
        </html>
        """
        
        # Mock API response
        api_response = Mock()
        api_response.json.return_value = {
            "events": [{
                "ticket_availability": {
                    "minimum_ticket_price": {
                        "major_value": "45",
                        "currency": "GBP"
                    }
                }
            }]
        }
        
        def fetch_side_effect(url, **kwargs):
            mock_resp = Mock()
            if "coffee-school" in url:
                mock_resp.text = coffee_school_html
            elif "/api/v3/destination/events/" in url:
                return api_response
            elif "eventbrite.com" in url:
                mock_resp.text = eventbrite_html
            return mock_resp
        
        mock_fetch.side_effect = fetch_side_effect
        
        # Execute scrape
        result = scraper.scrape()
        
        # Verify price from API
        assert len(result.raw_templates) == 1
        assert result.raw_templates[0]["price"] == "£45"
    
    @patch('src.extract.caravan_coffee.CaravanCoffeeScraper.fetch_url')
    def test_scrape_handles_empty_page(self, mock_fetch):
        """Test that scrape handles empty coffee school page gracefully."""
        scraper = CaravanCoffeeScraper()
        
        mock_response = Mock()
        mock_response.text = "<html><body></body></html>"
        mock_fetch.return_value = mock_response
        
        result = scraper.scrape()
        
        assert isinstance(result, RawProviderData)
        assert len(result.raw_locations) == 1  # Location always present
        assert len(result.raw_templates) == 0
        assert len(result.raw_events) == 0
    
    @patch('src.extract.caravan_coffee.CaravanCoffeeScraper.fetch_url')
    def test_scrape_propagates_network_errors(self, mock_fetch):
        """Test that network errors are propagated."""
        scraper = CaravanCoffeeScraper()
        
        mock_fetch.side_effect = requests.RequestException("Network error")
        
        with pytest.raises(requests.RequestException):
            scraper.scrape()
    
    @patch('src.extract.caravan_coffee.CaravanCoffeeScraper.fetch_url')
    def test_scrape_handles_eventbrite_errors(self, mock_fetch):
        """Test that Eventbrite errors don't crash the scraper."""
        scraper = CaravanCoffeeScraper()
        
        # Mock coffee school page
        coffee_school_html = """
        <html>
            <body>
                <h2>HOME FILTER CLASS</h2>
                <p>Learn filter brewing</p>
                <a href="https://eventbrite.com/e/filter-123456789012">SIGN ME UP</a>
            </body>
        </html>
        """
        
        def fetch_side_effect(url, **kwargs):
            mock_resp = Mock()
            if "coffee-school" in url:
                mock_resp.text = coffee_school_html
            elif "eventbrite.com" in url:
                # Simulate Eventbrite error
                raise requests.RequestException("Eventbrite unavailable")
            return mock_resp
        
        mock_fetch.side_effect = fetch_side_effect
        
        # Execute scrape - should not crash
        result = scraper.scrape()
        
        # Verify template created without price
        assert len(result.raw_templates) == 1
        assert result.raw_templates[0]["title"] == "Home Filter Class"
        assert result.raw_templates[0]["price"] is None
    
    def test_clean_text(self):
        """Test text cleaning utility."""
        scraper = CaravanCoffeeScraper()
        
        assert scraper._clean_text("  Hello   World  ") == "Hello World"
        assert scraper._clean_text("Line1\n\nLine2") == "Line1 Line2"
        assert scraper._clean_text(None) is None
        assert scraper._clean_text("") is None
    
    def test_absolute_url(self):
        """Test URL conversion utility."""
        scraper = CaravanCoffeeScraper()
        
        base = "https://caravanandco.com/pages/coffee-school"
        assert scraper._absolute_url(base, "/images/test.jpg") == "https://caravanandco.com/images/test.jpg"
        assert scraper._absolute_url(base, "https://other.com/img.jpg") == "https://other.com/img.jpg"
        assert scraper._absolute_url(base, None) is None
    
    def test_extract_description(self):
        """Test description extraction from heading siblings."""
        scraper = CaravanCoffeeScraper()
        
        from bs4 import BeautifulSoup
        html = """
        <html>
            <body>
                <h2>Test Class</h2>
                <p>First paragraph</p>
                <p>Second paragraph</p>
                <h3>Next Section</h3>
            </body>
        </html>
        """
        soup = BeautifulSoup(html, "lxml")
        heading = soup.find("h2")
        
        description = scraper._extract_description(heading)
        assert "First paragraph" in description
        assert "Second paragraph" in description
        assert "Next Section" not in description
    
    def test_extract_image(self):
        """Test image extraction from previous sibling."""
        scraper = CaravanCoffeeScraper()
        
        from bs4 import BeautifulSoup
        html = """
        <html>
            <body>
                <img src="/images/test.jpg" />
                <h2>Test Class</h2>
            </body>
        </html>
        """
        soup = BeautifulSoup(html, "lxml")
        heading = soup.find("h2")
        
        image = scraper._extract_image(heading)
        assert image == "https://caravanandco.com/images/test.jpg"
