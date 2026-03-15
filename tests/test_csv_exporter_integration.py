"""Integration tests for CSV exporter with canonical store."""

import csv
import tempfile
from pathlib import Path
from datetime import datetime

import pytest

from src.models.event_template import EventTemplate
from src.models.event_occurrence import EventOccurrence
from src.models.location import Location
from src.models.provider import Provider
from src.storage.store import CanonicalStore
from src.export.csv_exporter import CSVExporter


@pytest.fixture
def populated_store():
    """Create a store populated with test data."""
    with tempfile.TemporaryDirectory() as tmpdir:
        store = CanonicalStore(base_path=tmpdir)
        
        # Create provider
        provider = Provider(
            provider_id="provider-a",
            provider_name="Provider A",
            provider_slug="provider-a",
            source_name="Provider A API",
            source_base_url="https://example.com",
            status="active",
            first_seen_at=datetime(2025, 1, 15, 10, 0, 0),
            last_seen_at=datetime(2025, 1, 20, 14, 0, 0)
        )
        
        # Create locations
        locations = [
            Location(
                location_id="location-provider-a-loc1",
                provider_id="provider-a",
                provider_name="Provider A",
                formatted_address="100 Main St, London, EC1A 1AA",
                location_name="Main Venue",
                city="London",
                postcode="EC1A 1AA",
                country="UK",
                latitude=51.5074,
                longitude=-0.1278,
                geocode_status="success",
                status="active",
                first_seen_at=datetime(2025, 1, 15, 10, 0, 0),
                last_seen_at=datetime(2025, 1, 20, 14, 0, 0)
            ),
            Location(
                location_id="location-provider-a-loc2",
                provider_id="provider-a",
                provider_name="Provider A",
                formatted_address="200 Side St, London, EC2B 2BB",
                location_name="Side Venue",
                city="London",
                postcode="EC2B 2BB",
                country="UK",
                status="active",
                first_seen_at=datetime(2025, 1, 15, 10, 0, 0),
                last_seen_at=datetime(2025, 1, 20, 14, 0, 0)
            )
        ]
        
        # Create templates
        templates = [
            EventTemplate(
                event_template_id="event-template-provider-a-class1",
                provider_id="provider-a",
                title="Cooking Class",
                slug="cooking-class",
                category="Cooking",
                description_clean="Learn to cook",
                tags=["hands-on", "cooking"],
                skills_created=["cooking"],
                price_from=50.0,
                currency="GBP",
                status="active",
                first_seen_at=datetime(2025, 1, 15, 10, 0, 0),
                last_seen_at=datetime(2025, 1, 20, 14, 0, 0)
            ),
            EventTemplate(
                event_template_id="event-template-provider-a-class2",
                provider_id="provider-a",
                title="Baking Class",
                slug="baking-class",
                category="Baking",
                description_clean="Learn to bake",
                tags=["hands-on", "baking"],
                skills_created=["baking"],
                price_from=60.0,
                currency="GBP",
                status="inactive",  # Inactive template
                first_seen_at=datetime(2025, 1, 15, 10, 0, 0),
                last_seen_at=datetime(2025, 1, 20, 14, 0, 0)
            )
        ]
        
        # Create occurrences
        occurrences = [
            EventOccurrence(
                event_id="event-provider-a-session1",
                event_template_id="event-template-provider-a-class1",
                provider_id="provider-a",
                location_id="location-provider-a-loc1",
                title="Cooking Class",
                start_at=datetime(2025, 2, 15, 18, 0, 0),
                end_at=datetime(2025, 2, 15, 20, 0, 0),
                timezone="Europe/London",
                booking_url="https://example.com/book/1",
                price=50.0,
                currency="GBP",
                capacity=10,
                remaining_spaces=3,
                availability_status="available",
                status="active",
                first_seen_at=datetime(2025, 1, 18, 9, 0, 0),
                last_seen_at=datetime(2025, 1, 20, 14, 0, 0)
            ),
            EventOccurrence(
                event_id="event-provider-a-session2",
                event_template_id="event-template-provider-a-class1",
                provider_id="provider-a",
                location_id="location-provider-a-loc2",
                title="Cooking Class",
                start_at=datetime(2025, 2, 16, 18, 0, 0),
                end_at=datetime(2025, 2, 16, 20, 0, 0),
                timezone="Europe/London",
                booking_url="https://example.com/book/2",
                price=50.0,
                currency="GBP",
                capacity=10,
                remaining_spaces=0,
                availability_status="sold_out",
                status="active",
                first_seen_at=datetime(2025, 1, 18, 9, 0, 0),
                last_seen_at=datetime(2025, 1, 20, 14, 0, 0)
            ),
            EventOccurrence(
                event_id="event-provider-a-session3",
                provider_id="provider-a",
                location_id="location-provider-a-loc1",
                title="Past Event",
                start_at=datetime(2024, 12, 15, 18, 0, 0),
                end_at=datetime(2024, 12, 15, 20, 0, 0),
                timezone="Europe/London",
                price=40.0,
                currency="GBP",
                status="expired",  # Expired occurrence
                first_seen_at=datetime(2024, 12, 1, 9, 0, 0),
                last_seen_at=datetime(2024, 12, 20, 14, 0, 0)
            )
        ]
        
        # Save to store
        store.save_providers([provider])
        store.save_locations(locations)
        store.save_events(templates + occurrences)
        
        yield store


def test_export_events_integration_full_pipeline(populated_store):
    """Test full export pipeline with realistic data."""
    exporter = CSVExporter(populated_store)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "exports" / "events.csv"
        result = exporter.export_events(str(output_path))
        
        # Verify statistics (only active records by default)
        assert result["total_records"] == 3  # 1 active template + 2 active occurrences
        assert result["template_count"] == 1
        assert result["occurrence_count"] == 2
        
        # Read and verify CSV
        with open(output_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        assert len(rows) == 3
        
        # Verify template row
        template_row = rows[0]
        assert template_row["record_type"] == "template"
        assert template_row["record_id"] == "event-template-provider-a-class1"
        assert template_row["provider_name"] == "Provider A"
        assert template_row["title"] == "Cooking Class"
        assert template_row["category"] == "Cooking"
        assert template_row["price"] == "50.0"
        
        # Verify first occurrence row
        occ1_row = rows[1]
        assert occ1_row["record_type"] == "occurrence"
        assert occ1_row["record_id"] == "event-provider-a-session1"
        assert occ1_row["event_template_id"] == "event-template-provider-a-class1"
        assert occ1_row["location_id"] == "location-provider-a-loc1"
        assert occ1_row["location_name"] == "Main Venue"
        assert occ1_row["formatted_address"] == "100 Main St, London, EC1A 1AA"
        assert occ1_row["availability_status"] == "available"
        assert occ1_row["remaining_spaces"] == "3"
        
        # Verify second occurrence row
        occ2_row = rows[2]
        assert occ2_row["record_type"] == "occurrence"
        assert occ2_row["record_id"] == "event-provider-a-session2"
        assert occ2_row["location_id"] == "location-provider-a-loc2"
        assert occ2_row["location_name"] == "Side Venue"
        assert occ2_row["availability_status"] == "sold_out"
        assert occ2_row["remaining_spaces"] == "0"


def test_export_events_integration_include_all_statuses(populated_store):
    """Test exporting all records regardless of status."""
    exporter = CSVExporter(populated_store)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "events.csv"
        result = exporter.export_events(str(output_path), filters={})
        
        # Should include all records (2 templates + 3 occurrences)
        assert result["total_records"] == 5
        assert result["template_count"] == 2
        assert result["occurrence_count"] == 3
        
        # Read CSV
        with open(output_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        assert len(rows) == 5
        
        # Verify we have both active and inactive templates
        template_rows = [r for r in rows if r["record_type"] == "template"]
        assert len(template_rows) == 2
        statuses = {r["status"] for r in template_rows}
        assert "active" in statuses
        assert "inactive" in statuses
        
        # Verify we have both active and expired occurrences
        occurrence_rows = [r for r in rows if r["record_type"] == "occurrence"]
        assert len(occurrence_rows) == 3
        statuses = {r["status"] for r in occurrence_rows}
        assert "active" in statuses
        assert "expired" in statuses


def test_export_events_integration_csv_format_validation(populated_store):
    """Test that exported CSV has correct format and encoding."""
    exporter = CSVExporter(populated_store)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "events.csv"
        exporter.export_events(str(output_path))
        
        # Verify file is valid CSV
        with open(output_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            # Verify header row exists
            assert reader.fieldnames is not None
            assert len(reader.fieldnames) > 0
            
            # Verify all expected columns present
            expected_columns = [
                "record_type", "record_id", "event_template_id",
                "provider_id", "title", "status", "first_seen_at", "last_seen_at"
            ]
            for col in expected_columns:
                assert col in reader.fieldnames
            
            # Verify all rows can be read
            rows = list(reader)
            assert len(rows) > 0
            
            # Verify no rows have missing required fields
            for row in rows:
                assert row["record_type"] in ["template", "occurrence"]
                assert row["record_id"] != ""
                assert row["provider_id"] != ""
                assert row["title"] != ""
                assert row["status"] != ""


def test_export_events_integration_location_denormalization(populated_store):
    """Test that location data is correctly denormalized into event rows."""
    exporter = CSVExporter(populated_store)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "events.csv"
        exporter.export_events(str(output_path))
        
        # Read CSV
        with open(output_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        # Find occurrence rows
        occurrence_rows = [r for r in rows if r["record_type"] == "occurrence"]
        
        # Verify each occurrence has location data
        for row in occurrence_rows:
            if row["location_id"]:
                # If location_id is set, location_name and formatted_address should be set
                assert row["location_name"] != ""
                assert row["formatted_address"] != ""
                
                # Verify location data matches expected values
                if row["location_id"] == "location-provider-a-loc1":
                    assert row["location_name"] == "Main Venue"
                    assert "100 Main St" in row["formatted_address"]
                elif row["location_id"] == "location-provider-a-loc2":
                    assert row["location_name"] == "Side Venue"
                    assert "200 Side St" in row["formatted_address"]


def test_export_events_integration_empty_store():
    """Test exporting from an empty store."""
    with tempfile.TemporaryDirectory() as tmpdir:
        store = CanonicalStore(base_path=tmpdir)
        exporter = CSVExporter(store)
        
        output_path = Path(tmpdir) / "events.csv"
        result = exporter.export_events(str(output_path))
        
        # Should create file with header but no data rows
        assert result["total_records"] == 0
        assert result["template_count"] == 0
        assert result["occurrence_count"] == 0
        
        # Verify file exists with header
        with open(output_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            assert reader.fieldnames is not None
            rows = list(reader)
            assert len(rows) == 0



def test_export_locations_integration_full_pipeline(populated_store):
    """Test full locations export pipeline with realistic data."""
    exporter = CSVExporter(populated_store)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "exports" / "locations.csv"
        result = exporter.export_locations(str(output_path))
        
        # Verify statistics (only active locations by default)
        assert result["total_records"] == 2
        assert result["with_coordinates"] == 1  # Only loc1 has coordinates
        
        # Read and verify CSV
        with open(output_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        assert len(rows) == 2
        
        # Verify first location row
        loc1_row = rows[0]
        assert loc1_row["location_id"] == "location-provider-a-loc1"
        assert loc1_row["provider_name"] == "Provider A"
        assert loc1_row["location_name"] == "Main Venue"
        assert loc1_row["formatted_address"] == "100 Main St, London, EC1A 1AA"
        assert loc1_row["latitude"] == "51.5074"
        assert loc1_row["longitude"] == "-0.1278"
        assert loc1_row["geocode_status"] == "success"
        
        # Verify event counts for loc1 (has 1 active occurrence, session3 is expired)
        assert loc1_row["event_count"] == "1"
        assert loc1_row["active_event_count"] == "1"
        
        # Verify event names and IDs
        event_names = loc1_row["event_names"].split(";")
        assert len(event_names) == 1
        assert "Cooking Class" in event_names
        assert "Past Event" not in event_names  # Expired event not included
        
        event_ids = loc1_row["active_event_ids"].split(";")
        assert len(event_ids) == 1
        assert "event-provider-a-session1" in event_ids
        
        # Verify second location row
        loc2_row = rows[1]
        assert loc2_row["location_id"] == "location-provider-a-loc2"
        assert loc2_row["location_name"] == "Side Venue"
        assert loc2_row["latitude"] == ""  # No coordinates
        assert loc2_row["longitude"] == ""
        
        # Verify event counts for loc2 (has 1 active occurrence)
        assert loc2_row["event_count"] == "1"
        assert loc2_row["active_event_count"] == "1"


def test_export_locations_integration_csv_format_validation(populated_store):
    """Test that exported locations CSV has correct format and encoding."""
    exporter = CSVExporter(populated_store)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "locations.csv"
        exporter.export_locations(str(output_path))
        
        # Verify file is valid CSV
        with open(output_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            # Verify header row exists
            assert reader.fieldnames is not None
            assert len(reader.fieldnames) > 0
            
            # Verify all expected columns present
            expected_columns = [
                "location_id", "provider_id", "provider_name",
                "formatted_address", "latitude", "longitude",
                "geocode_status", "event_count", "active_event_count",
                "event_names", "active_event_ids", "status",
                "first_seen_at", "last_seen_at"
            ]
            for col in expected_columns:
                assert col in reader.fieldnames
            
            # Verify all rows can be read
            rows = list(reader)
            assert len(rows) > 0
            
            # Verify no rows have missing required fields
            for row in rows:
                assert row["location_id"] != ""
                assert row["provider_id"] != ""
                assert row["formatted_address"] != ""
                assert row["geocode_status"] != ""
                assert row["status"] != ""


def test_export_locations_integration_event_summary_accuracy(populated_store):
    """Test that event summaries accurately reflect linked events."""
    exporter = CSVExporter(populated_store)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "locations.csv"
        exporter.export_locations(str(output_path))
        
        # Read CSV
        with open(output_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        # Find location with multiple events
        loc1_row = next(r for r in rows if r["location_id"] == "location-provider-a-loc1")
        
        # Verify event count matches actual linked events
        # Location 1 has: session1 (active), session3 (expired)
        # Only active events are loaded and counted
        assert loc1_row["event_count"] == "1"  # Only session1
        assert loc1_row["active_event_count"] == "1"
        
        # Verify event IDs list contains correct events
        event_ids = loc1_row["active_event_ids"].split(";")
        assert "event-provider-a-session1" in event_ids
        # session3 is expired, so it's not loaded (we filter to active events only)


def test_export_locations_integration_empty_store():
    """Test exporting locations from an empty store."""
    with tempfile.TemporaryDirectory() as tmpdir:
        store = CanonicalStore(base_path=tmpdir)
        exporter = CSVExporter(store)
        
        output_path = Path(tmpdir) / "locations.csv"
        result = exporter.export_locations(str(output_path))
        
        # Should create file with header but no data rows
        assert result["total_records"] == 0
        assert result["with_coordinates"] == 0
        
        # Verify file exists with header
        with open(output_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            assert reader.fieldnames is not None
            rows = list(reader)
            assert len(rows) == 0


def test_export_locations_integration_provider_denormalization(populated_store):
    """Test that provider data is correctly denormalized into location rows."""
    exporter = CSVExporter(populated_store)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "locations.csv"
        exporter.export_locations(str(output_path))
        
        # Read CSV
        with open(output_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        # Verify all locations have provider data
        for row in rows:
            assert row["provider_id"] != ""
            assert row["provider_name"] != ""
            
            # Verify provider name matches expected value
            if row["provider_id"] == "provider-a":
                assert row["provider_name"] == "Provider A"



def test_validate_export_integration(populated_store):
    """Test validate_export with a full export workflow."""
    exporter = CSVExporter(populated_store)

    with tempfile.TemporaryDirectory() as tmpdir:
        events_path = Path(tmpdir) / "events.csv"
        locations_path = Path(tmpdir) / "locations.csv"

        # Export data
        exporter.export_events(str(events_path))
        exporter.export_locations(str(locations_path))

        # Validate export
        result = exporter.validate_export(str(events_path), str(locations_path))

        # Verify validation passed
        assert result["valid"] is True
        assert len(result["errors"]) == 0

        # Verify events validation details
        events_val = result["events_validation"]
        assert events_val["parseable"] is True
        assert events_val["active_records_count"] > 0
        assert events_val["csv_records_count"] == events_val["active_records_count"]
        assert len(events_val["missing_records"]) == 0
        assert len(events_val["duplicate_records"]) == 0

        # Verify locations validation details
        locations_val = result["locations_validation"]
        assert locations_val["parseable"] is True
        assert locations_val["active_records_count"] > 0
        assert locations_val["csv_records_count"] == locations_val["active_records_count"]
        assert len(locations_val["missing_records"]) == 0
        assert len(locations_val["duplicate_records"]) == 0

