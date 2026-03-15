"""End-to-end pipeline test with mock data.

This test validates the complete pipeline flow from extraction through export:
- Mock scrapers return known test data
- Pipeline processes data through all stages
- Canonical store contains expected records
- Merge logic works correctly
- Lifecycle rules are applied
"""

import pytest
import tempfile
import shutil
from datetime import datetime, timezone, timedelta
from pathlib import Path

from src.pipeline.orchestrator import PipelineOrchestrator
from src.storage.store import CanonicalStore
from src.sync.merge_engine import MergeEngine
from src.transform.normalizer import Normalizer
from src.extract.base_scraper import BaseScraper
from src.models.raw_provider_data import RawProviderData
from src.models.provider import Provider
from src.models.location import Location
from src.models.event_template import EventTemplate
from src.models.event_occurrence import EventOccurrence


class MockPastaScraper(BaseScraper):
    """Mock scraper for Pasta Evangelists returning known test data."""
    
    @property
    def provider_name(self) -> str:
        return "Pasta Evangelists"
    
    @property
    def provider_metadata(self) -> dict:
        return {
            "website": "https://pastaevangelists.com",
            "contact_email": "info@pastaevangelists.com",
            "source_name": "Pasta Evangelists API",
            "source_base_url": "https://plan.pastaevangelists.com"
        }
    
    def scrape(self) -> RawProviderData:
        """Return mock pasta making class data."""
        return RawProviderData(
            provider_name=self.provider_name,
            provider_website="https://pastaevangelists.com",
            source_name="Pasta Evangelists API",
            source_base_url="https://plan.pastaevangelists.com",
            raw_locations=[
                {
                    "location_id": "loc-farringdon",
                    "location_name": "The Pasta Academy Farringdon",
                    "address_line_1": "62-63 Long Lane",
                    "city": "London",
                    "postcode": "EC1A 9EJ"
                },
                {
                    "location_id": "loc-aldgate",
                    "location_name": "The Pasta Academy Aldgate",
                    "address_line_1": "45 Aldgate High Street",
                    "city": "London",
                    "postcode": "EC3N 1AL"
                }
            ],
            raw_templates=[
                {
                    "template_id": "template-beginners",
                    "title": "Beginners Pasta Making Class",
                    "description": "Learn to make fresh pasta from scratch",
                    "price": 68.0,
                    "duration_minutes": 120,
                    "category": "Cooking"
                }
            ],
            raw_events=[
                {
                    "event_id": "event-jan-20",
                    "template_id": "template-beginners",
                    "location_id": "loc-farringdon",
                    "title": "Beginners Pasta Making Class",
                    "start_at": "2026-06-20T18:00:00+00:00",
                    "end_at": "2026-06-20T20:00:00+00:00",
                    "price": 68.0,
                    "capacity": 12,
                    "remaining_spaces": 5,
                    "booking_url": "https://plan.pastaevangelists.com/book/event-jan-20"
                },
                {
                    "event_id": "event-jan-25",
                    "template_id": "template-beginners",
                    "location_id": "loc-aldgate",
                    "title": "Beginners Pasta Making Class",
                    "start_at": "2026-06-25T14:00:00+00:00",
                    "end_at": "2026-06-25T16:00:00+00:00",
                    "price": 68.0,
                    "capacity": 12,
                    "remaining_spaces": 8,
                    "booking_url": "https://plan.pastaevangelists.com/book/event-jan-25"
                }
            ]
        )


class MockBakeryScraper(BaseScraper):
    """Mock scraper for Comptoir Bakery returning known test data."""
    
    @property
    def provider_name(self) -> str:
        return "Comptoir Bakery"
    
    @property
    def provider_metadata(self) -> dict:
        return {
            "website": "https://comptoirbakery.com",
            "source_name": "Comptoir Bakery Website",
            "source_base_url": "https://comptoirbakery.com"
        }
    
    def scrape(self) -> RawProviderData:
        """Return mock croissant baking class data."""
        return RawProviderData(
            provider_name=self.provider_name,
            provider_website="https://comptoirbakery.com",
            source_name="Comptoir Bakery Website",
            source_base_url="https://comptoirbakery.com",
            raw_locations=[
                {
                    "location_id": "loc-shoreditch",
                    "location_name": "Comptoir Bakery Shoreditch",
                    "address_line_1": "123 High Street",
                    "city": "London",
                    "postcode": "E1 6JE"
                }
            ],
            raw_templates=[
                {
                    "template_id": "template-croissant",
                    "title": "Croissant Making Workshop",
                    "description": "Master the art of French croissant making",
                    "price": 75.0,
                    "duration_minutes": 180,
                    "category": "Baking"
                }
            ],
            raw_events=[
                {
                    "event_id": "event-feb-10",
                    "template_id": "template-croissant",
                    "location_id": "loc-shoreditch",
                    "title": "Croissant Making Workshop",
                    "start_at": "2026-07-10T10:00:00+00:00",
                    "end_at": "2026-07-10T13:00:00+00:00",
                    "price": 75.0,
                    "capacity": 8,
                    "remaining_spaces": 3,
                    "booking_url": "https://comptoirbakery.com/book/event-feb-10"
                }
            ]
        )


class TestPipelineEndToEnd:
    """End-to-end pipeline tests with mock data."""
    
    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for test data."""
        temp_path = tempfile.mkdtemp()
        yield temp_path
        shutil.rmtree(temp_path)
    
    @pytest.fixture
    def store(self, temp_dir):
        """Create a CanonicalStore instance with temp directory."""
        return CanonicalStore(base_path=temp_dir)
    
    @pytest.fixture
    def orchestrator(self, store):
        """Create a PipelineOrchestrator with test store."""
        return PipelineOrchestrator(
            store=store,
            merge_engine=MergeEngine(),
            normalizer=Normalizer()
        )
    
    @pytest.fixture
    def base_timestamp(self):
        """Base timestamp for testing."""
        return datetime(2025, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
    
    def test_pipeline_with_mock_scrapers_initial_run(
        self, orchestrator, store, base_timestamp
    ):
        """Test complete pipeline with mock scrapers on initial run.
        
        Validates:
        - Mock scrapers return known test data
        - Pipeline processes data through all stages
        - Canonical store contains expected records
        - All records are inserted (no existing data)
        """
        # Create mock scrapers
        pasta_scraper = MockPastaScraper()
        bakery_scraper = MockBakeryScraper()
        
        # Get raw data from scrapers
        pasta_data = pasta_scraper.scrape()
        bakery_data = bakery_scraper.scrape()
        
        # Verify mock data structure
        assert pasta_data.provider_name == "Pasta Evangelists"
        assert len(pasta_data.raw_locations) == 2
        assert len(pasta_data.raw_templates) == 1
        assert len(pasta_data.raw_events) == 2
        
        assert bakery_data.provider_name == "Comptoir Bakery"
        assert len(bakery_data.raw_locations) == 1
        assert len(bakery_data.raw_templates) == 1
        assert len(bakery_data.raw_events) == 1
        
        # Normalize data manually (since extract stage is not implemented)
        normalizer = orchestrator.normalizer
        
        # Normalize Pasta Evangelists
        pasta_provider = normalizer.normalize_provider(pasta_data)
        pasta_locations = normalizer.normalize_locations(
            pasta_data, pasta_provider.provider_id
        )
        pasta_events = normalizer.normalize_events(
            pasta_data,
            pasta_provider.provider_id,
            {loc.location_id: loc for loc in pasta_locations}
        )
        
        # Normalize Comptoir Bakery
        bakery_provider = normalizer.normalize_provider(bakery_data)
        bakery_locations = normalizer.normalize_locations(
            bakery_data, bakery_provider.provider_id
        )
        bakery_events = normalizer.normalize_events(
            bakery_data,
            bakery_provider.provider_id,
            {loc.location_id: loc for loc in bakery_locations}
        )
        
        # Combine all normalized data
        all_providers = [pasta_provider, bakery_provider]
        all_locations = pasta_locations + bakery_locations
        all_events = pasta_events + bakery_events
        
        # Run sync stage
        sync_result = orchestrator.run_stage(
            "sync",
            providers=all_providers,
            locations=all_locations,
            events=all_events
        )
        
        # Verify sync result
        assert sync_result.success is True
        assert sync_result.metrics["providers"]["inserted"] == 2
        assert sync_result.metrics["providers"]["updated"] == 0
        assert sync_result.metrics["locations"]["inserted"] == 3
        assert sync_result.metrics["locations"]["updated"] == 0
        
        # Verify canonical store contains expected records
        saved_providers = store.load_providers()
        saved_locations = store.load_locations()
        saved_events = store.load_events()
        
        assert len(saved_providers) == 2
        assert len(saved_locations) == 3
        assert len(saved_events) >= 3  # At least templates and occurrences
        
        # Verify provider records
        provider_names = {p.provider_name for p in saved_providers}
        assert "Pasta Evangelists" in provider_names
        assert "Comptoir Bakery" in provider_names
        
        # Verify location records
        location_names = {loc.location_name for loc in saved_locations}
        assert "The Pasta Academy Farringdon" in location_names
        assert "The Pasta Academy Aldgate" in location_names
        assert "Comptoir Bakery Shoreditch" in location_names
        
        # Verify all locations are active
        for location in saved_locations:
            assert location.status == "active"
        
        # Verify event records
        event_titles = {e.title for e in saved_events}
        assert "Beginners Pasta Making Class" in event_titles
        assert "Croissant Making Workshop" in event_titles
        
        # Verify all events are active
        for event in saved_events:
            assert event.status == "active"
    
    def test_pipeline_merge_logic_with_updates(
        self, orchestrator, store, base_timestamp
    ):
        """Test pipeline merge logic with updated data.
        
        Validates:
        - Unchanged records are preserved
        - Updated records are detected and merged
        - first_seen_at is preserved on updates
        - last_seen_at is updated
        """
        # Initial run: save some data
        pasta_scraper = MockPastaScraper()
        pasta_data = pasta_scraper.scrape()
        
        normalizer = orchestrator.normalizer
        pasta_provider = normalizer.normalize_provider(pasta_data)
        pasta_locations = normalizer.normalize_locations(
            pasta_data, pasta_provider.provider_id
        )
        pasta_events = normalizer.normalize_events(
            pasta_data,
            pasta_provider.provider_id,
            {loc.location_id: loc for loc in pasta_locations}
        )
        
        # Run sync
        orchestrator.run_stage(
            "sync",
            providers=[pasta_provider],
            locations=pasta_locations,
            events=pasta_events
        )
        
        # Verify initial state
        initial_events = store.load_events()
        assert len(initial_events) > 0
        
        # Get an occurrence to modify
        initial_occurrence = None
        for event in initial_events:
            if isinstance(event, EventOccurrence):
                initial_occurrence = event
                break
        
        assert initial_occurrence is not None
        initial_first_seen = initial_occurrence.first_seen_at
        
        # Second run: update the occurrence (change remaining_spaces)
        # Simulate scraper returning updated data
        updated_pasta_data = RawProviderData(
            provider_name="Pasta Evangelists",
            provider_website="https://pastaevangelists.com",
            source_name="Pasta Evangelists API",
            source_base_url="https://plan.pastaevangelists.com",
            raw_locations=pasta_data.raw_locations,
            raw_templates=pasta_data.raw_templates,
            raw_events=[
                {
                    **pasta_data.raw_events[0],
                    "remaining_spaces": 2  # Changed from 5
                },
                pasta_data.raw_events[1]
            ]
        )
        
        # Normalize updated data
        updated_provider = normalizer.normalize_provider(updated_pasta_data)
        updated_locations = normalizer.normalize_locations(
            updated_pasta_data, updated_provider.provider_id
        )
        updated_events = normalizer.normalize_events(
            updated_pasta_data,
            updated_provider.provider_id,
            {loc.location_id: loc for loc in updated_locations}
        )
        
        # Run sync again
        sync_result = orchestrator.run_stage(
            "sync",
            providers=[updated_provider],
            locations=updated_locations,
            events=updated_events
        )
        
        # Verify merge detected the update
        assert sync_result.success is True
        # At least one occurrence should be updated
        assert sync_result.metrics["occurrences"]["updated"] >= 1
        
        # Verify first_seen_at preserved
        updated_events_from_store = store.load_events()
        updated_occurrence = None
        for event in updated_events_from_store:
            if isinstance(event, EventOccurrence) and event.event_id == initial_occurrence.event_id:
                updated_occurrence = event
                break
        
        assert updated_occurrence is not None
        assert updated_occurrence.first_seen_at == initial_first_seen
        assert updated_occurrence.remaining_spaces == 2
    
    def test_pipeline_lifecycle_rules_expired_events(
        self, orchestrator, store, base_timestamp
    ):
        """Test pipeline applies lifecycle rules to mark expired events.
        
        Validates:
        - Past events are marked as expired
        - Future events remain active
        """
        # Create past event
        past_time = datetime.now(timezone.utc) - timedelta(days=1)
        
        past_event_data = RawProviderData(
            provider_name="Test Provider",
            source_name="Test",
            source_base_url="https://test.com",
            raw_locations=[
                {
                    "location_id": "loc-test",
                    "name": "Test Location",
                    "address": "123 Test St, London, EC1A 9EJ",
                    "city": "London",
                    "postcode": "EC1A 9EJ"
                }
            ],
            raw_events=[
                {
                    "event_id": "event-past",
                    "location_id": "loc-test",
                    "title": "Past Event",
                    "start_at": past_time.isoformat(),
                    "end_at": (past_time + timedelta(hours=2)).isoformat(),
                    "price": 50.0
                }
            ]
        )
        
        # Normalize and sync
        normalizer = orchestrator.normalizer
        provider = normalizer.normalize_provider(past_event_data)
        locations = normalizer.normalize_locations(
            past_event_data, provider.provider_id
        )
        events = normalizer.normalize_events(
            past_event_data,
            provider.provider_id,
            {loc.location_id: loc for loc in locations}
        )
        
        sync_result = orchestrator.run_stage(
            "sync",
            providers=[provider],
            locations=locations,
            events=events
        )
        
        assert sync_result.success is True
        
        # Verify past event is marked as expired
        saved_events = store.load_events()
        past_events = [e for e in saved_events if isinstance(e, EventOccurrence)]
        
        assert len(past_events) > 0
        for event in past_events:
            if event.start_at and event.start_at < datetime.now(timezone.utc):
                assert event.status == "expired"
    
    def test_pipeline_lifecycle_rules_removed_events(
        self, orchestrator, store, base_timestamp
    ):
        """Test pipeline marks missing future events as removed.
        
        Validates:
        - Events not in new data are marked as removed
        - deleted_at timestamp is set
        - Removed events are preserved (soft delete)
        """
        # Initial run: save a future event
        future_time = datetime.now(timezone.utc) + timedelta(days=7)
        
        initial_data = RawProviderData(
            provider_name="Test Provider",
            source_name="Test",
            source_base_url="https://test.com",
            raw_locations=[
                {
                    "location_id": "loc-test",
                    "name": "Test Location",
                    "address": "123 Test St, London, EC1A 9EJ",
                    "city": "London",
                    "postcode": "EC1A 9EJ"
                }
            ],
            raw_events=[
                {
                    "event_id": "event-future",
                    "location_id": "loc-test",
                    "title": "Future Event",
                    "start_at": future_time.isoformat(),
                    "end_at": (future_time + timedelta(hours=2)).isoformat(),
                    "price": 50.0
                }
            ]
        )
        
        # Normalize and sync
        normalizer = orchestrator.normalizer
        provider = normalizer.normalize_provider(initial_data)
        locations = normalizer.normalize_locations(
            initial_data, provider.provider_id
        )
        events = normalizer.normalize_events(
            initial_data,
            provider.provider_id,
            {loc.location_id: loc for loc in locations}
        )
        
        orchestrator.run_stage(
            "sync",
            providers=[provider],
            locations=locations,
            events=events
        )
        
        # Verify event is active
        initial_events = store.load_events()
        assert len(initial_events) > 0
        initial_event = initial_events[0]
        assert initial_event.status == "active"
        
        # Second run: event is missing from new data
        updated_data = RawProviderData(
            provider_name="Test Provider",
            source_name="Test",
            source_base_url="https://test.com",
            raw_locations=initial_data.raw_locations,
            raw_events=[]  # Event is missing
        )
        
        # Normalize and sync
        updated_provider = normalizer.normalize_provider(updated_data)
        updated_locations = normalizer.normalize_locations(
            updated_data, updated_provider.provider_id
        )
        updated_events = normalizer.normalize_events(
            updated_data,
            updated_provider.provider_id,
            {loc.location_id: loc for loc in updated_locations}
        )
        
        sync_result = orchestrator.run_stage(
            "sync",
            providers=[updated_provider],
            locations=updated_locations,
            events=updated_events
        )
        
        assert sync_result.success is True
        
        # Verify event is marked as removed
        saved_events = store.load_events()
        assert len(saved_events) > 0
        
        removed_event = saved_events[0]
        assert removed_event.status == "removed"
        assert removed_event.deleted_at is not None
    
    def test_pipeline_with_multiple_providers_mixed_operations(
        self, orchestrator, store, base_timestamp
    ):
        """Test pipeline with multiple providers and mixed operations.
        
        Validates:
        - Multiple providers can be processed together
        - Mix of insert/update/unchanged operations
        - Each provider's data is handled independently
        """
        # Initial run with both providers
        pasta_scraper = MockPastaScraper()
        bakery_scraper = MockBakeryScraper()
        
        pasta_data = pasta_scraper.scrape()
        bakery_data = bakery_scraper.scrape()
        
        normalizer = orchestrator.normalizer
        
        # Normalize both providers
        pasta_provider = normalizer.normalize_provider(pasta_data)
        pasta_locations = normalizer.normalize_locations(
            pasta_data, pasta_provider.provider_id
        )
        pasta_events = normalizer.normalize_events(
            pasta_data,
            pasta_provider.provider_id,
            {loc.location_id: loc for loc in pasta_locations}
        )
        
        bakery_provider = normalizer.normalize_provider(bakery_data)
        bakery_locations = normalizer.normalize_locations(
            bakery_data, bakery_provider.provider_id
        )
        bakery_events = normalizer.normalize_events(
            bakery_data,
            bakery_provider.provider_id,
            {loc.location_id: loc for loc in bakery_locations}
        )
        
        # Initial sync
        orchestrator.run_stage(
            "sync",
            providers=[pasta_provider, bakery_provider],
            locations=pasta_locations + bakery_locations,
            events=pasta_events + bakery_events
        )
        
        # Verify initial state
        initial_providers = store.load_providers()
        initial_locations = store.load_locations()
        initial_events = store.load_events()
        
        assert len(initial_providers) == 2
        assert len(initial_locations) == 3
        assert len(initial_events) >= 3
        
        # Second run: update pasta data, keep bakery unchanged
        updated_pasta_data = RawProviderData(
            provider_name="Pasta Evangelists",
            provider_website="https://pastaevangelists.com",
            source_name="Pasta Evangelists API",
            source_base_url="https://plan.pastaevangelists.com",
            raw_locations=pasta_data.raw_locations,
            raw_templates=[
                {
                    **pasta_data.raw_templates[0],
                    "description": "Updated description"  # Changed
                }
            ],
            raw_events=pasta_data.raw_events
        )
        
        # Normalize updated data
        updated_pasta_provider = normalizer.normalize_provider(updated_pasta_data)
        updated_pasta_locations = normalizer.normalize_locations(
            updated_pasta_data, updated_pasta_provider.provider_id
        )
        updated_pasta_events = normalizer.normalize_events(
            updated_pasta_data,
            updated_pasta_provider.provider_id,
            {loc.location_id: loc for loc in updated_pasta_locations}
        )
        
        # Sync with updated pasta and unchanged bakery
        sync_result = orchestrator.run_stage(
            "sync",
            providers=[updated_pasta_provider, bakery_provider],
            locations=updated_pasta_locations + bakery_locations,
            events=updated_pasta_events + bakery_events
        )
        
        assert sync_result.success is True
        
        # Verify mixed operations
        # Providers: 2 unchanged (or 1 updated if provider metadata changed)
        assert sync_result.metrics["providers"]["total"] == 2
        
        # Locations: 3 total (all unchanged or updated)
        assert sync_result.metrics["locations"]["total"] == 3
        
        # Events: some updated (pasta template), some unchanged (bakery)
        assert sync_result.metrics["templates"]["total"] >= 2
        
        # Verify final state
        final_providers = store.load_providers()
        final_locations = store.load_locations()
        final_events = store.load_events()
        
        assert len(final_providers) == 2
        assert len(final_locations) == 3
        assert len(final_events) >= 3
