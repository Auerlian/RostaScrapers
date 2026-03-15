"""Integration tests for export stage in pipeline orchestrator."""

import pytest
import tempfile
import shutil
from datetime import datetime, timezone, timedelta
from pathlib import Path
import csv

from src.pipeline.orchestrator import PipelineOrchestrator
from src.storage.store import CanonicalStore
from src.sync.merge_engine import MergeEngine
from src.transform.normalizer import Normalizer
from src.models.provider import Provider
from src.models.location import Location
from src.models.event_template import EventTemplate
from src.models.event_occurrence import EventOccurrence


class TestExportIntegration:
    """Test export stage integration in pipeline orchestrator."""
    
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
    
    def test_export_stage_creates_csv_files(self, orchestrator, store, temp_dir):
        """Test export stage creates events.csv and locations.csv files."""
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
        
        # Save to store
        store.save_providers([provider])
        store.save_locations([location])
        store.save_events([template])
        
        # Run export stage
        result = orchestrator.run_stage("export")
        
        # Verify stage succeeded
        assert result.success is True
        assert result.metrics["events_exported"] == 1
        assert result.metrics["locations_exported"] == 1
        assert len(result.metrics["export_files"]) == 2
        
        # Verify files were created
        exports_dir = Path("exports")
        assert (exports_dir / "events.csv").exists()
        assert (exports_dir / "locations.csv").exists()
        
        # Verify events.csv content
        with open(exports_dir / "events.csv", 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) == 1
            assert rows[0]["record_type"] == "template"
            assert rows[0]["record_id"] == "event-template-test-123"
            assert rows[0]["title"] == "Test Event"
        
        # Verify locations.csv content
        with open(exports_dir / "locations.csv", 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) == 1
            assert rows[0]["location_id"] == "location-test-123"
            assert rows[0]["formatted_address"] == "123 Test St, London, EC1A 9EJ"
    
    def test_export_stage_filters_active_records_only(self, orchestrator, store):
        """Test export stage only exports active records by default."""
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
        
        # Create active and removed events
        active_event = EventTemplate(
            event_template_id="event-active",
            provider_id="provider-test",
            title="Active Event",
            slug="active-event",
            currency="GBP",
            status="active",
            first_seen_at=now,
            last_seen_at=now,
            source_hash="hash1",
            record_hash="rechash1",
            tags=[],
            occasion_tags=[],
            skills_required=[],
            skills_created=[],
            image_urls=[],
            family_friendly=False,
            beginner_friendly=False
        )
        
        removed_event = EventTemplate(
            event_template_id="event-removed",
            provider_id="provider-test",
            title="Removed Event",
            slug="removed-event",
            currency="GBP",
            status="removed",
            first_seen_at=now,
            last_seen_at=now,
            deleted_at=now,
            source_hash="hash2",
            record_hash="rechash2",
            tags=[],
            occasion_tags=[],
            skills_required=[],
            skills_created=[],
            image_urls=[],
            family_friendly=False,
            beginner_friendly=False
        )
        
        # Save to store
        store.save_providers([provider])
        store.save_events([active_event, removed_event])
        
        # Run export stage
        result = orchestrator.run_stage("export")
        
        # Verify only active event was exported
        assert result.success is True
        assert result.metrics["events_exported"] == 1
        
        # Verify CSV contains only active event
        exports_dir = Path("exports")
        with open(exports_dir / "events.csv", 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) == 1
            assert rows[0]["record_id"] == "event-active"
    
    def test_export_stage_includes_both_templates_and_occurrences(
        self, orchestrator, store
    ):
        """Test export stage includes both templates and occurrences."""
        now = datetime.now(timezone.utc)
        future_time = now + timedelta(days=7)
        
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
            event_template_id="event-template-test",
            provider_id="provider-test",
            title="Test Template",
            slug="test-template",
            currency="GBP",
            status="active",
            first_seen_at=now,
            last_seen_at=now,
            source_hash="hash1",
            record_hash="rechash1",
            tags=[],
            occasion_tags=[],
            skills_required=[],
            skills_created=[],
            image_urls=[],
            family_friendly=False,
            beginner_friendly=False
        )
        
        occurrence = EventOccurrence(
            event_id="event-occurrence-test",
            event_template_id="event-template-test",
            provider_id="provider-test",
            location_id="location-test-123",
            title="Test Occurrence",
            start_at=future_time,
            timezone="Europe/London",
            currency="GBP",
            availability_status="available",
            status="active",
            first_seen_at=now,
            last_seen_at=now,
            source_hash="hash2",
            record_hash="rechash2",
            tags=[],
            skills_required=[],
            skills_created=[]
        )
        
        # Save to store
        store.save_providers([provider])
        store.save_locations([location])
        store.save_events([template, occurrence])
        
        # Run export stage
        result = orchestrator.run_stage("export")
        
        # Verify both were exported
        assert result.success is True
        assert result.metrics["events_exported"] == 2
        assert result.metrics["events_templates"] == 1
        assert result.metrics["events_occurrences"] == 1
        
        # Verify CSV contains both records
        exports_dir = Path("exports")
        with open(exports_dir / "events.csv", 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) == 2
            
            # Find template and occurrence rows
            template_row = next(r for r in rows if r["record_type"] == "template")
            occurrence_row = next(r for r in rows if r["record_type"] == "occurrence")
            
            assert template_row["record_id"] == "event-template-test"
            assert template_row["event_template_id"] == ""  # Empty for templates
            
            assert occurrence_row["record_id"] == "event-occurrence-test"
            assert occurrence_row["event_template_id"] == "event-template-test"
    
    def test_export_stage_in_full_pipeline(self, orchestrator, store):
        """Test export stage runs successfully in full pipeline."""
        # Run full pipeline (with empty data since extract is not implemented)
        report = orchestrator.run()
        
        # Verify export stage was executed
        assert "export" in report.stage_results
        export_result = report.stage_results["export"]
        
        assert export_result.success is True
        assert "events_exported" in export_result.metrics
        assert "locations_exported" in export_result.metrics
        assert "export_files" in export_result.metrics
        
        # Verify export files were created
        exports_dir = Path("exports")
        assert (exports_dir / "events.csv").exists()
        assert (exports_dir / "locations.csv").exists()
    
    def test_export_stage_includes_statistics_in_report(self, orchestrator, store):
        """Test export stage includes detailed statistics in metrics."""
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
            latitude=51.5074,
            longitude=-0.1278,
            geocode_status="success",
            status="active",
            first_seen_at=now,
            last_seen_at=now,
            address_hash="abc123"
        )
        
        template = EventTemplate(
            event_template_id="event-template-test",
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
        
        # Save to store
        store.save_providers([provider])
        store.save_locations([location])
        store.save_events([template])
        
        # Run export stage
        result = orchestrator.run_stage("export")
        
        # Verify detailed statistics
        assert result.metrics["events_exported"] == 1
        assert result.metrics["events_templates"] == 1
        assert result.metrics["events_occurrences"] == 0
        assert result.metrics["locations_exported"] == 1
        assert result.metrics["locations_with_coordinates"] == 1
        assert len(result.metrics["export_files"]) == 2
        
        # Verify file paths are included
        assert any("events.csv" in f for f in result.metrics["export_files"])
        assert any("locations.csv" in f for f in result.metrics["export_files"])
