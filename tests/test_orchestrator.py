"""Tests for pipeline orchestrator."""

import os
import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import Mock, patch

from src.pipeline.orchestrator import PipelineOrchestrator, StageResult, PipelineReport
from src.storage.store import CanonicalStore
from src.sync.merge_engine import MergeEngine
from src.transform.normalizer import Normalizer
from src.models.provider import Provider
from src.models.location import Location
from src.models.event_template import EventTemplate
from src.models.event_occurrence import EventOccurrence


class TestPipelineOrchestrator:
    """Test suite for PipelineOrchestrator."""
    
    def test_orchestrator_initialization(self):
        """Test orchestrator can be initialized with default components."""
        orchestrator = PipelineOrchestrator()
        
        assert orchestrator.store is not None
        assert orchestrator.merge_engine is not None
        assert orchestrator.normalizer is not None
        assert isinstance(orchestrator.store, CanonicalStore)
        assert isinstance(orchestrator.merge_engine, MergeEngine)
        assert isinstance(orchestrator.normalizer, Normalizer)
    
    def test_orchestrator_initialization_with_custom_components(self, tmp_path):
        """Test orchestrator can be initialized with custom components."""
        store = CanonicalStore(base_path=str(tmp_path / "data"))
        merge_engine = MergeEngine()
        normalizer = Normalizer()
        
        orchestrator = PipelineOrchestrator(
            store=store,
            merge_engine=merge_engine,
            normalizer=normalizer
        )
        
        assert orchestrator.store is store
        assert orchestrator.merge_engine is merge_engine
        assert orchestrator.normalizer is normalizer
    
    def test_run_stage_returns_stage_result(self):
        """Test run_stage returns a StageResult object."""
        orchestrator = PipelineOrchestrator()
        
        # Run a stage (extract is not implemented yet, so it will return placeholder)
        result = orchestrator.run_stage("extract", providers=["test-provider"])
        
        assert isinstance(result, StageResult)
        assert result.stage_name == "extract"
        assert isinstance(result.success, bool)
        assert isinstance(result.duration_seconds, float)
        assert isinstance(result.metrics, dict)
    
    def test_run_stage_handles_unknown_stage(self):
        """Test run_stage handles unknown stage gracefully."""
        orchestrator = PipelineOrchestrator()
        
        result = orchestrator.run_stage("unknown_stage")
        
        assert isinstance(result, StageResult)
        assert result.stage_name == "unknown_stage"
        assert result.success is False
        assert result.error is not None
        assert "Unknown stage" in result.error
    
    def test_run_sync_stage_with_empty_data(self, tmp_path):
        """Test sync stage with empty data."""
        store = CanonicalStore(base_path=str(tmp_path / "data"))
        orchestrator = PipelineOrchestrator(store=store)
        
        result = orchestrator.run_stage(
            "sync",
            providers=[],
            locations=[],
            events=[]
        )
        
        assert result.success is True
        assert "providers" in result.metrics
        assert "locations" in result.metrics
        assert "templates" in result.metrics
        assert "occurrences" in result.metrics
    
    def test_run_sync_stage_with_new_records(self, tmp_path):
        """Test sync stage with new records."""
        store = CanonicalStore(base_path=str(tmp_path / "data"))
        orchestrator = PipelineOrchestrator(store=store)
        
        # Create test records
        now = datetime.now(timezone.utc)
        
        provider = Provider(
            provider_id="provider-test",
            provider_name="Test Provider",
            provider_slug="test",
            source_name="Test Source",
            source_base_url="https://test.com",
            status="active",
            first_seen_at=now,
            last_seen_at=now,
            metadata={}
        )
        
        location = Location(
            location_id="location-test-123",
            provider_id="provider-test",
            provider_name="Test Provider",
            formatted_address="123 Test St, London, EC1A 9EJ",
            country="UK",
            geocode_status="not_geocoded",
            status="active",
            first_seen_at=now,
            last_seen_at=now,
            address_hash="abc123"
        )
        
        template = EventTemplate(
            event_template_id="event-template-test-123",
            provider_id="provider-test",
            title="Test Event",
            slug="test-event",
            currency="GBP",
            status="active",
            first_seen_at=now,
            last_seen_at=now,
            source_hash="hash123",
            record_hash="rechash123",
            tags=[],
            occasion_tags=[],
            skills_required=[],
            skills_created=[],
            image_urls=[],
            family_friendly=False,
            beginner_friendly=False
        )
        
        # Run sync stage
        result = orchestrator.run_stage(
            "sync",
            providers=[provider],
            locations=[location],
            events=[template]
        )
        
        assert result.success is True
        assert result.metrics["providers"]["inserted"] == 1
        assert result.metrics["locations"]["inserted"] == 1
        assert result.metrics["templates"]["inserted"] == 1
        assert result.metrics["occurrences"]["inserted"] == 0
        
        # Verify records were saved
        saved_providers = store.load_providers()
        saved_locations = store.load_locations()
        saved_events = store.load_events()
        
        assert len(saved_providers) == 1
        assert len(saved_locations) == 1
        assert len(saved_events) == 1
        assert saved_providers[0].provider_id == "provider-test"
        assert saved_locations[0].location_id == "location-test-123"
        assert saved_events[0].event_template_id == "event-template-test-123"
    
    def test_run_sync_stage_marks_expired_events(self, tmp_path):
        """Test sync stage marks past events as expired."""
        store = CanonicalStore(base_path=str(tmp_path / "data"))
        orchestrator = PipelineOrchestrator(store=store)
        
        now = datetime.now(timezone.utc)
        past_time = now - timedelta(days=1)
        
        provider = Provider(
            provider_id="provider-test",
            provider_name="Test Provider",
            provider_slug="test",
            source_name="Test Source",
            source_base_url="https://test.com",
            status="active",
            first_seen_at=now,
            last_seen_at=now,
            metadata={}
        )
        
        # Create past event occurrence
        past_event = EventOccurrence(
            event_id="event-test-past",
            provider_id="provider-test",
            title="Past Event",
            start_at=past_time,
            timezone="Europe/London",
            currency="GBP",
            availability_status="unknown",
            status="active",  # Will be marked as expired
            first_seen_at=now,
            last_seen_at=now,
            source_hash="hash123",
            record_hash="rechash123",
            tags=[],
            skills_required=[],
            skills_created=[]
        )
        
        # Run sync stage
        result = orchestrator.run_stage(
            "sync",
            providers=[provider],
            locations=[],
            events=[past_event]
        )
        
        assert result.success is True
        
        # Verify event was marked as expired
        saved_events = store.load_events()
        assert len(saved_events) == 1
        assert saved_events[0].status == "expired"
    
    def test_run_sync_stage_marks_removed_events(self, tmp_path):
        """Test sync stage marks missing future events as removed."""
        store = CanonicalStore(base_path=str(tmp_path / "data"))
        orchestrator = PipelineOrchestrator(store=store)
        
        now = datetime.now(timezone.utc)
        future_time = now + timedelta(days=1)
        
        provider = Provider(
            provider_id="provider-test",
            provider_name="Test Provider",
            provider_slug="test",
            source_name="Test Source",
            source_base_url="https://test.com",
            status="active",
            first_seen_at=now,
            last_seen_at=now,
            metadata={}
        )
        
        # Create future event and save it
        future_event = EventOccurrence(
            event_id="event-test-future",
            provider_id="provider-test",
            title="Future Event",
            start_at=future_time,
            timezone="Europe/London",
            currency="GBP",
            availability_status="unknown",
            status="active",
            first_seen_at=now,
            last_seen_at=now,
            source_hash="hash123",
            record_hash="rechash123",
            tags=[],
            skills_required=[],
            skills_created=[]
        )
        
        store.save_providers([provider])
        store.save_events([future_event])
        
        # Run sync with empty events (event is missing from new data)
        result = orchestrator.run_stage(
            "sync",
            providers=[provider],
            locations=[],
            events=[]  # Event not in new data
        )
        
        assert result.success is True
        
        # Verify event was marked as removed
        saved_events = store.load_events()
        assert len(saved_events) == 1
        assert saved_events[0].status == "removed"
        assert saved_events[0].deleted_at is not None
    
    def test_run_returns_pipeline_report(self):
        """Test run returns a PipelineReport object."""
        orchestrator = PipelineOrchestrator()
        
        report = orchestrator.run(providers=["test-provider"])
        
        assert isinstance(report, PipelineReport)
        assert isinstance(report.run_id, str)
        assert isinstance(report.start_time, datetime)
        assert isinstance(report.end_time, datetime)
        assert isinstance(report.duration_seconds, float)
        assert isinstance(report.success, bool)
        assert isinstance(report.providers_processed, list)
        assert isinstance(report.providers_failed, list)
        assert isinstance(report.stage_results, dict)
        assert isinstance(report.total_metrics, dict)
        assert isinstance(report.errors, list)
    
    def test_run_with_skip_geocoding(self):
        """Test run with skip_geocoding flag."""
        orchestrator = PipelineOrchestrator()
        
        report = orchestrator.run(skip_geocoding=True)
        
        assert isinstance(report, PipelineReport)
        # Enrich stage should be skipped or show geocoding_skipped=True
        if "enrich" in report.stage_results:
            enrich_result = report.stage_results["enrich"]
            assert enrich_result.metrics.get("geocoding_skipped") is True
    
    def test_run_with_skip_ai_enrichment(self):
        """Test run with skip_ai_enrichment flag."""
        orchestrator = PipelineOrchestrator()
        
        report = orchestrator.run(skip_ai_enrichment=True)
        
        assert isinstance(report, PipelineReport)
        # Enrich stage should be skipped or show ai_enrichment_skipped=True
        if "enrich" in report.stage_results:
            enrich_result = report.stage_results["enrich"]
            assert enrich_result.metrics.get("ai_enrichment_skipped") is True
    
    def test_run_with_selective_providers(self):
        """Test run with selective provider execution."""
        orchestrator = PipelineOrchestrator()
        
        report = orchestrator.run(providers=["pasta-evangelists", "comptoir-bakery"])
        
        assert isinstance(report, PipelineReport)
        # Extract stage should receive providers parameter
        if "extract" in report.stage_results:
            extract_result = report.stage_results["extract"]
            assert isinstance(extract_result, StageResult)
    
    def test_stage_result_string_representation(self):
        """Test StageResult string representation."""
        result = StageResult(
            stage_name="test",
            success=True,
            duration_seconds=1.5,
            metrics={"count": 10}
        )
        
        result_str = str(result)
        assert "test" in result_str
        assert "SUCCESS" in result_str
        assert "1.5" in result_str
    
    def test_pipeline_report_string_representation(self):
        """Test PipelineReport string representation."""
        now = datetime.now(timezone.utc)
        report = PipelineReport(
            run_id="20250120_120000",
            start_time=now,
            end_time=now + timedelta(seconds=10),
            duration_seconds=10.0,
            success=True,
            providers_processed=["provider-1"],
            providers_failed=[],
            stage_results={},
            total_metrics={},
            errors=[]
        )
        
        report_str = str(report)
        assert "20250120_120000" in report_str
        assert "SUCCESS" in report_str
        assert "10.0" in report_str

    def test_run_enrich_stage_with_geocoding(self, tmp_path, monkeypatch):
        """Test enrich stage geocodes locations successfully."""
        store = CanonicalStore(base_path=str(tmp_path / "data"))
        orchestrator = PipelineOrchestrator(store=store)
        
        # Mock MapboxGeocoder to avoid real API calls
        from src.enrich.geocoder import GeocodeResult
        
        class MockMapboxGeocoder:
            def __init__(self, api_key=None, timeout=10):
                pass
            
            def geocode(self, address):
                return GeocodeResult(
                    latitude=51.5074,
                    longitude=-0.1278,
                    status="success",
                    precision="rooftop",
                    metadata={"provider": "mapbox"}
                )
        
        # Patch MapboxGeocoder where it's imported
        monkeypatch.setattr(
            "src.enrich.mapbox_geocoder.MapboxGeocoder",
            MockMapboxGeocoder
        )
        
        # Create test location
        now = datetime.now(timezone.utc)
        location = Location(
            location_id="location-test-123",
            provider_id="provider-test",
            provider_name="Test Provider",
            formatted_address="123 Test St, London, UK",
            country="UK",
            geocode_status="not_geocoded",
            status="active",
            first_seen_at=now,
            last_seen_at=now
        )
        
        # Run enrich stage
        result = orchestrator.run_stage(
            "enrich",
            locations=[location],
            events=[],
            skip_geocoding=False,
            skip_ai_enrichment=True
        )
        
        assert result.success is True
        assert result.metrics["locations_processed"] == 1
        assert result.metrics["locations_geocoded"] == 1
        assert result.metrics["geocoding_success"] == 1
        assert result.metrics["geocoding_failed"] == 0
        assert result.metrics["geocoding_skipped"] is False
        
        # Check enriched location
        enriched_locations = result.metrics["locations"]
        assert len(enriched_locations) == 1
        enriched_location = enriched_locations[0]
        assert enriched_location.latitude == 51.5074
        assert enriched_location.longitude == -0.1278
        assert enriched_location.geocode_status == "success"
        assert enriched_location.geocode_provider == "mapbox"
        assert enriched_location.geocode_precision == "rooftop"
    
    def test_run_enrich_stage_handles_geocoding_failure(self, tmp_path, monkeypatch):
        """Test enrich stage handles geocoding failures gracefully."""
        store = CanonicalStore(base_path=str(tmp_path / "data"))
        orchestrator = PipelineOrchestrator(store=store)
        
        # Mock MapboxGeocoder to return failure
        from src.enrich.geocoder import GeocodeResult
        
        class MockMapboxGeocoder:
            def __init__(self, api_key=None, timeout=10):
                pass
            
            def geocode(self, address):
                return GeocodeResult(
                    latitude=None,
                    longitude=None,
                    status="failed",
                    precision=None,
                    metadata={"error": "API error"}
                )
        
        # Patch MapboxGeocoder where it's imported
        monkeypatch.setattr(
            "src.enrich.mapbox_geocoder.MapboxGeocoder",
            MockMapboxGeocoder
        )
        
        # Create test location
        now = datetime.now(timezone.utc)
        location = Location(
            location_id="location-test-123",
            provider_id="provider-test",
            provider_name="Test Provider",
            formatted_address="Invalid Address",
            country="UK",
            geocode_status="not_geocoded",
            status="active",
            first_seen_at=now,
            last_seen_at=now
        )
        
        # Run enrich stage
        result = orchestrator.run_stage(
            "enrich",
            locations=[location],
            events=[],
            skip_geocoding=False,
            skip_ai_enrichment=True
        )
        
        # Stage should succeed even with geocoding failure
        assert result.success is True
        assert result.metrics["locations_processed"] == 1
        assert result.metrics["locations_geocoded"] == 1
        assert result.metrics["geocoding_success"] == 0
        assert result.metrics["geocoding_failed"] == 1
        
        # Check location has failed status
        enriched_locations = result.metrics["locations"]
        assert len(enriched_locations) == 1
        enriched_location = enriched_locations[0]
        assert enriched_location.geocode_status == "failed"
        assert enriched_location.latitude is None
        assert enriched_location.longitude is None
    
    def test_run_enrich_stage_skips_already_geocoded(self, tmp_path, monkeypatch):
        """Test enrich stage skips locations already geocoded with unchanged address."""
        store = CanonicalStore(base_path=str(tmp_path / "data"))
        orchestrator = PipelineOrchestrator(store=store)
        
        # Mock MapboxGeocoder (should not be called)
        class MockMapboxGeocoder:
            def __init__(self, api_key=None, timeout=10):
                self.called = False
            
            def geocode(self, address):
                self.called = True
                raise Exception("Should not be called for already geocoded location")
        
        # Patch MapboxGeocoder where it's imported
        mock_geocoder_class = MockMapboxGeocoder
        monkeypatch.setattr(
            "src.enrich.mapbox_geocoder.MapboxGeocoder",
            mock_geocoder_class
        )
        
        # Create already geocoded location
        now = datetime.now(timezone.utc)
        location = Location(
            location_id="location-test-123",
            provider_id="provider-test",
            provider_name="Test Provider",
            formatted_address="123 Test St, London, UK",
            country="UK",
            latitude=51.5074,
            longitude=-0.1278,
            geocode_status="success",
            geocode_provider="mapbox",
            geocode_precision="rooftop",
            geocoded_at=now,
            address_hash="abc123def456",
            status="active",
            first_seen_at=now,
            last_seen_at=now
        )
        
        # Run enrich stage
        result = orchestrator.run_stage(
            "enrich",
            locations=[location],
            events=[],
            skip_geocoding=False,
            skip_ai_enrichment=True
        )
        
        assert result.success is True
        assert result.metrics["locations_processed"] == 1
        assert result.metrics["locations_geocoded"] == 0
        assert result.metrics["geocoding_cached"] == 1
        assert result.metrics["geocoding_success"] == 0
        assert result.metrics["geocoding_failed"] == 0
    
    def test_run_enrich_stage_with_skip_geocoding(self, tmp_path):
        """Test enrich stage respects skip_geocoding flag."""
        store = CanonicalStore(base_path=str(tmp_path / "data"))
        orchestrator = PipelineOrchestrator(store=store)
        
        # Create test location
        now = datetime.now(timezone.utc)
        location = Location(
            location_id="location-test-123",
            provider_id="provider-test",
            provider_name="Test Provider",
            formatted_address="123 Test St, London, UK",
            country="UK",
            geocode_status="not_geocoded",
            status="active",
            first_seen_at=now,
            last_seen_at=now
        )
        
        # Run enrich stage with skip_geocoding=True
        result = orchestrator.run_stage(
            "enrich",
            locations=[location],
            events=[],
            skip_geocoding=True,
            skip_ai_enrichment=True
        )
        
        assert result.success is True
        assert result.metrics["geocoding_skipped"] is True
        assert result.metrics["locations_processed"] == 0
        assert result.metrics["locations_geocoded"] == 0
    
    def test_run_enrich_stage_handles_missing_api_key(self, tmp_path, monkeypatch):
        """Test enrich stage handles missing API key gracefully."""
        store = CanonicalStore(base_path=str(tmp_path / "data"))
        orchestrator = PipelineOrchestrator(store=store)
        
        # Remove MAPBOX_API_KEY from environment
        monkeypatch.delenv("MAPBOX_API_KEY", raising=False)
        
        # Create test location
        now = datetime.now(timezone.utc)
        location = Location(
            location_id="location-test-123",
            provider_id="provider-test",
            provider_name="Test Provider",
            formatted_address="123 Test St, London, UK",
            country="UK",
            geocode_status="not_geocoded",
            status="active",
            first_seen_at=now,
            last_seen_at=now
        )
        
        # Run enrich stage
        result = orchestrator.run_stage(
            "enrich",
            locations=[location],
            events=[],
            skip_geocoding=False,
            skip_ai_enrichment=True
        )
        
        # Stage should succeed but skip geocoding
        assert result.success is True
        assert result.metrics["geocoding_skipped"] is True
    
    def test_run_enrich_stage_continues_on_individual_location_error(self, tmp_path, monkeypatch):
        """Test enrich stage continues processing other locations when one fails."""
        store = CanonicalStore(base_path=str(tmp_path / "data"))
        orchestrator = PipelineOrchestrator(store=store)
        
        # Mock MapboxGeocoder to fail on first location, succeed on second
        from src.enrich.geocoder import GeocodeResult
        
        class MockMapboxGeocoder:
            def __init__(self, api_key=None, timeout=10):
                self.call_count = 0
            
            def geocode(self, address):
                self.call_count += 1
                if self.call_count == 1:
                    raise Exception("Network error")
                return GeocodeResult(
                    latitude=51.5074,
                    longitude=-0.1278,
                    status="success",
                    precision="rooftop",
                    metadata={"provider": "mapbox"}
                )
        
        # Patch MapboxGeocoder where it's imported
        monkeypatch.setattr(
            "src.enrich.mapbox_geocoder.MapboxGeocoder",
            MockMapboxGeocoder
        )
        
        # Create test locations
        now = datetime.now(timezone.utc)
        location1 = Location(
            location_id="location-test-1",
            provider_id="provider-test",
            provider_name="Test Provider",
            formatted_address="Invalid Address",
            country="UK",
            geocode_status="not_geocoded",
            status="active",
            first_seen_at=now,
            last_seen_at=now
        )
        location2 = Location(
            location_id="location-test-2",
            provider_id="provider-test",
            provider_name="Test Provider",
            formatted_address="123 Test St, London, UK",
            country="UK",
            geocode_status="not_geocoded",
            status="active",
            first_seen_at=now,
            last_seen_at=now
        )
        
        # Run enrich stage
        result = orchestrator.run_stage(
            "enrich",
            locations=[location1, location2],
            events=[],
            skip_geocoding=False,
            skip_ai_enrichment=True
        )
        
        # Stage should succeed
        assert result.success is True
        assert result.metrics["locations_processed"] == 2
        assert result.metrics["locations_geocoded"] == 2
        assert result.metrics["geocoding_success"] == 1
        assert result.metrics["geocoding_failed"] == 1
    def test_run_enrich_stage_with_ai_enrichment(self):
        """Test enrich stage with AI enrichment enabled."""
        # Create events to enrich
        events = [
            EventTemplate(
                event_template_id="event-template-test-1",
                provider_id="provider-test",
                title="Cooking Class",
                slug="cooking-class",
                description_clean="Learn to cook Italian dishes",
                source_hash="hash1",
                first_seen_at=datetime.now(),
                last_seen_at=datetime.now()
            ),
            EventOccurrence(
                event_id="event-test-2",
                provider_id="provider-test", 
                title="Pasta Making",
                description_clean="Make fresh pasta from scratch",
                source_hash="hash2",
                start_at=datetime(2025, 3, 1, 18, 0),
                end_at=datetime(2025, 3, 1, 21, 0),
                first_seen_at=datetime.now(),
                last_seen_at=datetime.now()
            )
        ]
        
        orchestrator = PipelineOrchestrator()
        
        # Mock AI enricher
        with patch.dict(os.environ, {"OPENAI_API_KEY": "test-key"}):
            with patch("src.enrich.ai_enricher.AIEnricher") as mock_enricher_class:
                mock_enricher = Mock()
                mock_enricher_class.return_value = mock_enricher
                
                def mock_enrich_event(event):
                    # Simulate successful enrichment
                    event.description_ai = f"AI enhanced: {event.description_clean}"
                    event.tags = ["ai-enriched"]
                    return event
                
                mock_enricher.enrich_event.side_effect = mock_enrich_event
                
                result = orchestrator.run_stage(
                    "enrich",
                    locations=[],
                    events=events,
                    skip_geocoding=True,
                    skip_ai_enrichment=False
                )
        
        # Verify AI enrichment was successful
        assert result.success
        assert result.metrics["events_processed"] == 2
        assert result.metrics["events_enriched"] == 2
        assert result.metrics["enrichment_success"] == 2
        assert result.metrics["enrichment_failed"] == 0
        assert result.metrics["ai_enrichment_skipped"] is False
        
        # Verify events were enriched
        enriched_events = result.metrics["events"]
        assert len(enriched_events) == 2
        for event in enriched_events:
            assert event.description_ai is not None
            assert "ai-enriched" in event.tags
    
    def test_run_enrich_stage_handles_ai_enrichment_failure(self):
        """Test that enrich stage handles AI enrichment failures gracefully."""
        # Create events to enrich
        events = [
            EventTemplate(
                event_template_id="event-template-test-1",
                provider_id="provider-test",
                title="Good Event",
                slug="good-event",
                description_clean="This will work",
                source_hash="hash1",
                first_seen_at=datetime.now(),
                last_seen_at=datetime.now()
            ),
            EventTemplate(
                event_template_id="event-template-test-2",
                provider_id="provider-test",
                title="Bad Event",
                slug="bad-event", 
                description_clean="This will fail",
                source_hash="hash2",
                first_seen_at=datetime.now(),
                last_seen_at=datetime.now()
            )
        ]
        
        orchestrator = PipelineOrchestrator()
        
        # Mock AI enricher to fail on second event
        with patch.dict(os.environ, {"OPENAI_API_KEY": "test-key"}):
            with patch("src.enrich.ai_enricher.AIEnricher") as mock_enricher_class:
                mock_enricher = Mock()
                mock_enricher_class.return_value = mock_enricher
                
                def mock_enrich_event(event):
                    if "Bad Event" in event.title:
                        raise Exception("AI enrichment failed")
                    # Simulate successful enrichment for good event
                    event.description_ai = f"AI enhanced: {event.description_clean}"
                    return event
                
                mock_enricher.enrich_event.side_effect = mock_enrich_event
                
                result = orchestrator.run_stage(
                    "enrich",
                    locations=[],
                    events=events,
                    skip_geocoding=True,
                    skip_ai_enrichment=False
                )
        
        # Should succeed despite individual event failure
        assert result.success
        assert result.metrics["events_processed"] == 2
        assert result.metrics["events_enriched"] == 1  # Only one succeeded (no exception)
        assert result.metrics["enrichment_success"] == 1  # Only one succeeded
        assert result.metrics["enrichment_failed"] == 1  # One failed
    
    def test_run_enrich_stage_handles_missing_ai_api_key(self):
        """Test that enrich stage handles missing AI API key gracefully."""
        events = [
            EventTemplate(
                event_template_id="event-template-test-1",
                provider_id="provider-test",
                title="Test Event",
                slug="test-event",
                description_clean="Test description",
                source_hash="hash1",
                first_seen_at=datetime.now(),
                last_seen_at=datetime.now()
            )
        ]
        
        orchestrator = PipelineOrchestrator()
        
        # Mock AIEnricher to raise ValueError for missing API key
        with patch("src.enrich.ai_enricher.AIEnricher") as mock_enricher_class:
            mock_enricher_class.side_effect = ValueError("OpenAI API key not found")
            
            result = orchestrator.run_stage(
                "enrich",
                locations=[],
                events=events,
                skip_geocoding=True,
                skip_ai_enrichment=False
            )
        
        # Should succeed but skip AI enrichment
        assert result.success
        assert result.metrics["ai_enrichment_skipped"] is True
        assert result.metrics["events_processed"] == 0  # No events processed due to config error