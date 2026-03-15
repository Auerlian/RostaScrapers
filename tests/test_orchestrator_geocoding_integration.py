"""Integration tests for geocoding in pipeline orchestrator."""

import pytest
from datetime import datetime, timezone

from src.pipeline.orchestrator import PipelineOrchestrator
from src.storage.store import CanonicalStore
from src.models.provider import Provider
from src.models.location import Location
from src.enrich.geocoder import GeocodeResult


class TestOrchestratorGeocodingIntegration:
    """Integration tests for geocoding in pipeline orchestrator."""
    
    def test_full_pipeline_with_geocoding(self, tmp_path, monkeypatch):
        """Test full pipeline run with geocoding integration."""
        store = CanonicalStore(base_path=str(tmp_path / "data"))
        orchestrator = PipelineOrchestrator(store=store)
        
        # Mock MapboxGeocoder
        class MockMapboxGeocoder:
            def __init__(self, api_key=None, timeout=10):
                pass
            
            def geocode(self, address):
                # Return different coordinates based on address
                if "London" in address:
                    return GeocodeResult(
                        latitude=51.5074,
                        longitude=-0.1278,
                        status="success",
                        precision="rooftop",
                        metadata={"provider": "mapbox"}
                    )
                else:
                    return GeocodeResult(
                        latitude=None,
                        longitude=None,
                        status="invalid_address",
                        precision=None,
                        metadata={"error": "Address not found"}
                    )
        
        monkeypatch.setattr(
            "src.enrich.mapbox_geocoder.MapboxGeocoder",
            MockMapboxGeocoder
        )
        
        # Create test data
        now = datetime.now(timezone.utc)
        
        provider = Provider(
            provider_id="provider-test",
            provider_name="Test Provider",
            provider_slug="test",
            source_name="Test Source",
            source_base_url="https://test.com",
            status="active",
            first_seen_at=now,
            last_seen_at=now
        )
        
        location1 = Location(
            location_id="location-test-1",
            provider_id="provider-test",
            provider_name="Test Provider",
            formatted_address="123 Test St, London, UK",
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
            formatted_address="456 Invalid Address",
            country="UK",
            geocode_status="not_geocoded",
            status="active",
            first_seen_at=now,
            last_seen_at=now
        )
        
        # Run sync stage to store initial data
        sync_result = orchestrator.run_stage(
            "sync",
            providers=[provider],
            locations=[location1, location2],
            events=[]
        )
        assert sync_result.success is True
        
        # Run enrich stage with geocoding
        enrich_result = orchestrator.run_stage(
            "enrich",
            locations=[location1, location2],
            events=[],
            skip_geocoding=False,
            skip_ai_enrichment=True
        )
        
        assert enrich_result.success is True
        assert enrich_result.metrics["locations_processed"] == 2
        assert enrich_result.metrics["locations_geocoded"] == 2
        assert enrich_result.metrics["geocoding_success"] == 1
        assert enrich_result.metrics["geocoding_failed"] == 1
        
        # Verify enriched locations
        enriched_locations = enrich_result.metrics["locations"]
        assert len(enriched_locations) == 2
        
        # Location 1 should be successfully geocoded
        loc1 = next(l for l in enriched_locations if l.location_id == "location-test-1")
        assert loc1.latitude == 51.5074
        assert loc1.longitude == -0.1278
        assert loc1.geocode_status == "success"
        assert loc1.geocode_provider == "mapbox"
        
        # Location 2 should have failed geocoding
        loc2 = next(l for l in enriched_locations if l.location_id == "location-test-2")
        assert loc2.latitude is None
        assert loc2.longitude is None
        assert loc2.geocode_status == "invalid_address"
        
        # Sync enriched locations back to store
        sync_result2 = orchestrator.run_stage(
            "sync",
            providers=[provider],
            locations=enriched_locations,
            events=[]
        )
        assert sync_result2.success is True
        
        # Verify locations are persisted with geocoding data
        stored_locations = store.load_locations()
        assert len(stored_locations) == 2
        
        stored_loc1 = next(l for l in stored_locations if l.location_id == "location-test-1")
        assert stored_loc1.latitude == 51.5074
        assert stored_loc1.longitude == -0.1278
        assert stored_loc1.geocode_status == "success"
    
    def test_geocoding_cache_prevents_redundant_calls(self, tmp_path, monkeypatch):
        """Test that geocoding cache prevents redundant API calls."""
        # Set cache directory to tmp_path to isolate test
        cache_dir = str(tmp_path / "cache" / "geocoding")
        
        store = CanonicalStore(base_path=str(tmp_path / "data"))
        orchestrator = PipelineOrchestrator(store=store)
        
        # Mock MapboxGeocoder with call counter
        class MockMapboxGeocoder:
            call_count = 0
            
            def __init__(self, api_key=None, timeout=10):
                pass
            
            def geocode(self, address):
                MockMapboxGeocoder.call_count += 1
                return GeocodeResult(
                    latitude=51.5074,
                    longitude=-0.1278,
                    status="success",
                    precision="rooftop",
                    metadata={"provider": "mapbox"}
                )
        
        monkeypatch.setattr(
            "src.enrich.mapbox_geocoder.MapboxGeocoder",
            MockMapboxGeocoder
        )
        
        # Patch CachedGeocoder to use test cache directory
        from src.enrich.cached_geocoder import CachedGeocoder as OriginalCachedGeocoder
        
        class TestCachedGeocoder(OriginalCachedGeocoder):
            def __init__(self, geocoder):
                super().__init__(geocoder, cache_dir=cache_dir)
        
        monkeypatch.setattr(
            "src.enrich.cached_geocoder.CachedGeocoder",
            TestCachedGeocoder
        )
        
        # Create test location
        now = datetime.now(timezone.utc)
        location = Location(
            location_id="location-test-1",
            provider_id="provider-test",
            provider_name="Test Provider",
            formatted_address="123 Test St, London, UK",
            country="UK",
            geocode_status="not_geocoded",
            address_hash=None,  # No hash yet - needs geocoding
            status="active",
            first_seen_at=now,
            last_seen_at=now
        )
        
        # First geocoding - should call API
        MockMapboxGeocoder.call_count = 0
        enrich_result1 = orchestrator.run_stage(
            "enrich",
            locations=[location],
            events=[],
            skip_geocoding=False,
            skip_ai_enrichment=True
        )
        
        assert enrich_result1.success is True
        assert MockMapboxGeocoder.call_count == 1
        assert enrich_result1.metrics["geocoding_success"] == 1
        
        # Get enriched location
        enriched_location = enrich_result1.metrics["locations"][0]
        
        # Second geocoding with same address - should use cache
        MockMapboxGeocoder.call_count = 0
        enrich_result2 = orchestrator.run_stage(
            "enrich",
            locations=[enriched_location],
            events=[],
            skip_geocoding=False,
            skip_ai_enrichment=True
        )
        
        assert enrich_result2.success is True
        # Should not call API again (already geocoded with unchanged address)
        assert MockMapboxGeocoder.call_count == 0
        assert enrich_result2.metrics["geocoding_cached"] == 1
    
    def test_geocoding_statistics_in_pipeline_report(self, tmp_path, monkeypatch):
        """Test that geocoding statistics are included in pipeline report."""
        store = CanonicalStore(base_path=str(tmp_path / "data"))
        orchestrator = PipelineOrchestrator(store=store)
        
        # Mock MapboxGeocoder
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
        
        monkeypatch.setattr(
            "src.enrich.mapbox_geocoder.MapboxGeocoder",
            MockMapboxGeocoder
        )
        
        # Create test data
        now = datetime.now(timezone.utc)
        
        provider = Provider(
            provider_id="provider-test",
            provider_name="Test Provider",
            provider_slug="test",
            source_name="Test Source",
            source_base_url="https://test.com",
            status="active",
            first_seen_at=now,
            last_seen_at=now
        )
        
        location = Location(
            location_id="location-test-1",
            provider_id="provider-test",
            provider_name="Test Provider",
            formatted_address="123 Test St, London, UK",
            country="UK",
            geocode_status="not_geocoded",
            status="active",
            first_seen_at=now,
            last_seen_at=now
        )
        
        # Mock normalize stage to return test data
        def mock_normalize_stage(**kwargs):
            return {
                "providers": [provider],
                "locations": [location],
                "events": [],
                "provider_ids": ["provider-test"],
                "providers_normalized": 1,
                "locations_normalized": 1,
                "events_normalized": 0
            }
        
        orchestrator._run_normalize_stage = mock_normalize_stage
        
        # Run full pipeline
        report = orchestrator.run(
            providers=["test"],
            skip_geocoding=False,
            skip_ai_enrichment=True
        )
        
        # Verify report contains geocoding statistics
        assert report.success is True
        assert "enrich" in report.stage_results
        
        enrich_metrics = report.stage_results["enrich"].metrics
        assert "locations_processed" in enrich_metrics
        assert "locations_geocoded" in enrich_metrics
        assert "geocoding_success" in enrich_metrics
        assert "geocoding_failed" in enrich_metrics
        assert "geocoding_cached" in enrich_metrics
        assert "geocoding_skipped" in enrich_metrics
        
        # Verify actual values
        assert enrich_metrics["locations_processed"] == 1
        assert enrich_metrics["locations_geocoded"] == 1
        assert enrich_metrics["geocoding_success"] == 1
        assert enrich_metrics["geocoding_failed"] == 0
