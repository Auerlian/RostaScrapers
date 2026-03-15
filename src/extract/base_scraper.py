"""Base scraper abstract class for all provider scrapers."""

from abc import ABC, abstractmethod
import time
import logging
from typing import Any
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.models.raw_provider_data import RawProviderData


logger = logging.getLogger(__name__)


class BaseScraper(ABC):
    """
    Abstract base class for all provider scrapers.
    
    Provides common utilities for HTTP requests with polite delays,
    timeout handling, and retry logic. Subclasses must implement
    the scrape() method and provider properties.
    """
    
    # Default configuration
    DEFAULT_TIMEOUT = 30  # seconds
    DEFAULT_DELAY = 1.0  # seconds between requests
    DEFAULT_USER_AGENT = "RostaScraper/1.0 (Educational/Research)"
    
    def __init__(
        self,
        timeout: int = DEFAULT_TIMEOUT,
        delay: float = DEFAULT_DELAY,
        user_agent: str = DEFAULT_USER_AGENT
    ):
        """
        Initialize the base scraper.
        
        Args:
            timeout: Request timeout in seconds
            delay: Polite delay between requests in seconds
            user_agent: User agent string for HTTP requests
        """
        self.timeout = timeout
        self.delay = delay
        self.user_agent = user_agent
        self._session = None
        self._last_request_time = 0.0
    
    @abstractmethod
    def scrape(self) -> RawProviderData:
        """
        Execute scraping logic and return raw structured data.
        
        This method must be implemented by subclasses to fetch data
        from the provider's website or API and return it as a
        RawProviderData object.
        
        Returns:
            RawProviderData containing provider info, locations, and events
            
        Raises:
            Exception: If scraping fails (network error, parsing error, etc.)
        """
        pass
    
    @property
    @abstractmethod
    def provider_name(self) -> str:
        """
        Return the provider name.
        
        Returns:
            Provider name (e.g., "Pasta Evangelists")
        """
        pass
    
    @property
    @abstractmethod
    def provider_metadata(self) -> dict[str, Any]:
        """
        Return provider metadata.
        
        Should include fields like:
        - website: Provider website URL
        - contact_email: Contact email address
        - source_name: Name of the data source
        - source_base_url: Base URL for scraping
        
        Returns:
            Dictionary of provider metadata
        """
        pass
    
    def get_session(self) -> requests.Session:
        """
        Get or create a requests session with retry logic.
        
        Configures automatic retries for transient failures and
        sets default headers including user agent.
        
        Returns:
            Configured requests.Session object
        """
        if self._session is None:
            self._session = requests.Session()
            
            # Configure retry strategy for transient failures
            retry_strategy = Retry(
                total=3,
                backoff_factor=1,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["HEAD", "GET", "OPTIONS"]
            )
            
            adapter = HTTPAdapter(max_retries=retry_strategy)
            self._session.mount("http://", adapter)
            self._session.mount("https://", adapter)
            
            # Set default headers
            self._session.headers.update({
                "User-Agent": self.user_agent
            })
        
        return self._session
    
    def polite_delay(self) -> None:
        """
        Implement polite delay between requests.
        
        Ensures minimum time between requests to avoid overwhelming
        the provider's servers and respect rate limits.
        """
        current_time = time.time()
        time_since_last_request = current_time - self._last_request_time
        
        if time_since_last_request < self.delay:
            sleep_time = self.delay - time_since_last_request
            logger.debug(f"Polite delay: sleeping for {sleep_time:.2f}s")
            time.sleep(sleep_time)
        
        self._last_request_time = time.time()
    
    def fetch_url(
        self,
        url: str,
        method: str = "GET",
        **kwargs: Any
    ) -> requests.Response:
        """
        Fetch a URL with timeout handling and polite delays.
        
        Automatically applies polite delays between requests and
        handles timeouts gracefully with logging.
        
        Args:
            url: URL to fetch
            method: HTTP method (GET, POST, etc.)
            **kwargs: Additional arguments passed to requests
            
        Returns:
            requests.Response object
            
        Raises:
            requests.RequestException: If request fails
            requests.Timeout: If request times out
        """
        # Apply polite delay before request
        self.polite_delay()
        
        # Set timeout if not provided
        if "timeout" not in kwargs:
            kwargs["timeout"] = self.timeout
        
        session = self.get_session()
        
        try:
            logger.debug(f"Fetching {method} {url}")
            response = session.request(method, url, **kwargs)
            response.raise_for_status()
            return response
            
        except requests.Timeout as e:
            logger.error(f"Timeout fetching {url}: {e}")
            raise
            
        except requests.RequestException as e:
            logger.error(f"Error fetching {url}: {e}")
            raise
    
    def close(self) -> None:
        """
        Close the requests session and clean up resources.
        
        Should be called when scraping is complete or in a finally block.
        """
        if self._session is not None:
            self._session.close()
            self._session = None
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures session is closed."""
        self.close()
        return False
