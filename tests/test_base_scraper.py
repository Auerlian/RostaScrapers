"""Tests for BaseScraper abstract class."""

import pytest
import time
from unittest.mock import Mock, patch, MagicMock
import requests

from src.extract.base_scraper import BaseScraper
from src.models.raw_provider_data import RawProviderData


class MockScraper(BaseScraper):
    """Concrete implementation of BaseScraper for testing."""
    
    def scrape(self) -> RawProviderData:
        """Mock scrape implementation."""
        return RawProviderData(
            provider_name="Test Provider",
            provider_website="https://test.com",
            provider_contact_email="test@test.com",
            source_name="Test Source",
            source_base_url="https://test.com/api",
            raw_locations=[{"name": "Test Location"}],
            raw_events=[{"title": "Test Event"}]
        )
    
    @property
    def provider_name(self) -> str:
        """Return provider name."""
        return "Test Provider"
    
    @property
    def provider_metadata(self) -> dict:
        """Return provider metadata."""
        return {
            "website": "https://test.com",
            "contact_email": "test@test.com",
            "source_name": "Test Source",
            "source_base_url": "https://test.com/api"
        }


class TestBaseScraper:
    """Test suite for BaseScraper abstract class."""
    
    def test_cannot_instantiate_abstract_class(self):
        """Test that BaseScraper cannot be instantiated directly."""
        with pytest.raises(TypeError):
            BaseScraper()
    
    def test_concrete_implementation_can_be_instantiated(self):
        """Test that concrete implementation can be instantiated."""
        scraper = MockScraper()
        assert scraper is not None
        assert scraper.provider_name == "Test Provider"
    
    def test_scrape_returns_raw_provider_data(self):
        """Test that scrape method returns RawProviderData."""
        scraper = MockScraper()
        result = scraper.scrape()
        
        assert isinstance(result, RawProviderData)
        assert result.provider_name == "Test Provider"
        assert result.provider_website == "https://test.com"
        assert len(result.raw_locations) == 1
        assert len(result.raw_events) == 1
    
    def test_default_configuration(self):
        """Test default configuration values."""
        scraper = MockScraper()
        
        assert scraper.timeout == BaseScraper.DEFAULT_TIMEOUT
        assert scraper.delay == BaseScraper.DEFAULT_DELAY
        assert scraper.user_agent == BaseScraper.DEFAULT_USER_AGENT
    
    def test_custom_configuration(self):
        """Test custom configuration values."""
        scraper = MockScraper(
            timeout=60,
            delay=2.0,
            user_agent="CustomAgent/1.0"
        )
        
        assert scraper.timeout == 60
        assert scraper.delay == 2.0
        assert scraper.user_agent == "CustomAgent/1.0"
    
    def test_get_session_creates_session(self):
        """Test that get_session creates a requests session."""
        scraper = MockScraper()
        session = scraper.get_session()
        
        assert isinstance(session, requests.Session)
        assert session.headers["User-Agent"] == scraper.user_agent
    
    def test_get_session_reuses_session(self):
        """Test that get_session reuses the same session."""
        scraper = MockScraper()
        session1 = scraper.get_session()
        session2 = scraper.get_session()
        
        assert session1 is session2
    
    def test_polite_delay_waits_minimum_time(self):
        """Test that polite_delay enforces minimum delay between requests."""
        scraper = MockScraper(delay=0.1)
        
        # First request - no delay
        start = time.time()
        scraper.polite_delay()
        first_delay = time.time() - start
        assert first_delay < 0.05  # Should be nearly instant
        
        # Second request immediately after - should delay
        start = time.time()
        scraper.polite_delay()
        second_delay = time.time() - start
        assert second_delay >= 0.09  # Should wait ~0.1s
    
    def test_polite_delay_no_wait_if_time_passed(self):
        """Test that polite_delay doesn't wait if enough time has passed."""
        scraper = MockScraper(delay=0.1)
        
        scraper.polite_delay()
        time.sleep(0.15)  # Wait longer than delay
        
        start = time.time()
        scraper.polite_delay()
        delay = time.time() - start
        assert delay < 0.05  # Should be nearly instant
    
    @patch('src.extract.base_scraper.requests.Session.request')
    def test_fetch_url_success(self, mock_request):
        """Test successful URL fetch."""
        scraper = MockScraper()
        
        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.raise_for_status = Mock()
        mock_request.return_value = mock_response
        
        response = scraper.fetch_url("https://test.com")
        
        assert response == mock_response
        mock_request.assert_called_once()
        assert mock_request.call_args[0][0] == "GET"
        assert mock_request.call_args[0][1] == "https://test.com"
    
    @patch('src.extract.base_scraper.requests.Session.request')
    def test_fetch_url_timeout(self, mock_request):
        """Test URL fetch with timeout."""
        scraper = MockScraper(timeout=5)
        
        # Mock timeout
        mock_request.side_effect = requests.Timeout("Request timed out")
        
        with pytest.raises(requests.Timeout):
            scraper.fetch_url("https://test.com")
    
    @patch('src.extract.base_scraper.requests.Session.request')
    def test_fetch_url_request_exception(self, mock_request):
        """Test URL fetch with request exception."""
        scraper = MockScraper()
        
        # Mock request exception
        mock_request.side_effect = requests.RequestException("Connection error")
        
        with pytest.raises(requests.RequestException):
            scraper.fetch_url("https://test.com")
    
    @patch('src.extract.base_scraper.requests.Session.request')
    def test_fetch_url_uses_custom_timeout(self, mock_request):
        """Test that fetch_url uses custom timeout."""
        scraper = MockScraper(timeout=10)
        
        mock_response = Mock()
        mock_response.raise_for_status = Mock()
        mock_request.return_value = mock_response
        
        scraper.fetch_url("https://test.com")
        
        # Check that timeout was passed
        assert mock_request.call_args[1]["timeout"] == 10
    
    @patch('src.extract.base_scraper.requests.Session.request')
    def test_fetch_url_applies_polite_delay(self, mock_request):
        """Test that fetch_url applies polite delay."""
        scraper = MockScraper(delay=0.1)
        
        mock_response = Mock()
        mock_response.raise_for_status = Mock()
        mock_request.return_value = mock_response
        
        # First request
        scraper.fetch_url("https://test.com/1")
        
        # Second request should be delayed
        start = time.time()
        scraper.fetch_url("https://test.com/2")
        delay = time.time() - start
        
        assert delay >= 0.09  # Should wait ~0.1s
    
    def test_close_closes_session(self):
        """Test that close() closes the session."""
        scraper = MockScraper()
        session = scraper.get_session()
        
        scraper.close()
        
        assert scraper._session is None
    
    def test_context_manager_closes_session(self):
        """Test that context manager closes session on exit."""
        with MockScraper() as scraper:
            session = scraper.get_session()
            assert session is not None
        
        assert scraper._session is None
    
    def test_context_manager_closes_on_exception(self):
        """Test that context manager closes session even on exception."""
        try:
            with MockScraper() as scraper:
                session = scraper.get_session()
                raise ValueError("Test exception")
        except ValueError:
            pass
        
        assert scraper._session is None
