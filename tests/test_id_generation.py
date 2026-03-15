"""
Unit tests for ID generation and validation.

Tests deterministic ID generation for all canonical models, ensuring
the same source data always produces the same IDs across runs.

Also tests validation rules for all models including required fields,
constraints, formats, and foreign key validation.
"""

import pytest
from datetime import datetime
from src.transform.id_generator import (
    slugify,
    normalize_address,
    generate_provider_id,
    generate_location_id,
    generate_event_template_id,
    generate_event_occurrence_id,
)
from src.models import Provider, Location, EventTemplate, EventOccurrence


class TestSlugify:
    """Tests for slugify helper function."""
    
    def test_basic_slugification(self):
        """Test basic text to slug conversion."""
        assert slugify("Pasta Making Workshop") == "pasta-making-workshop"
        assert slugify("Coffee Class") == "coffee-class"
    
    def test_special_characters(self):
        """Test removal of special characters."""
        assert slugify("Coffee & Latte Art!") == "coffee-latte-art"
        assert slugify("Baking 101: Basics") == "baking-101-basics"
        assert slugify("Wine & Cheese Tasting") == "wine-cheese-tasting"
    
    def test_whitespace_normalization(self):
        """Test whitespace handling."""
        assert slugify("  Multiple   Spaces  ") == "multiple-spaces"
        assert slugify("Tab\tSeparated") == "tab-separated"
    
    def test_unicode_characters(self):
        """Test unicode character handling."""
        assert slugify("Café au Lait") == "caf-au-lait"
        assert slugify("Crème Brûlée") == "cr-me-br-l-e"
    
    def test_length_limit(self):
        """Test slug is limited to 50 characters."""
        long_title = "This is a very long event title that exceeds fifty characters"
        result = slugify(long_title)
        assert len(result) <= 50
        assert result == "this-is-a-very-long-event-title-that-exceeds-fifty"
    
    def test_empty_string(self):
        """Test empty string handling."""
        assert slugify("") == ""
        assert slugify("   ") == ""
    
    def test_only_special_characters(self):
        """Test string with only special characters."""
        assert slugify("!!!") == ""
        assert slugify("@#$%") == ""
    
    def test_mixed_unicode_and_ascii(self):
        """Test mixed unicode and ASCII characters."""
        assert slugify("Café Racer 2024") == "caf-racer-2024"
        assert slugify("Naïve Approach") == "na-ve-approach"
    
    def test_leading_trailing_hyphens(self):
        """Test that leading/trailing hyphens are stripped."""
        assert slugify("-Leading Hyphen") == "leading-hyphen"
        assert slugify("Trailing Hyphen-") == "trailing-hyphen"
        assert slugify("---Multiple---") == "multiple"
    
    def test_consecutive_special_chars(self):
        """Test consecutive special characters become single hyphen."""
        assert slugify("Coffee!!!Latte") == "coffee-latte"
        assert slugify("Wine & & Cheese") == "wine-cheese"
    
    def test_numbers_preserved(self):
        """Test that numbers are preserved in slugs."""
        assert slugify("Baking 101") == "baking-101"
        assert slugify("2024 Workshop") == "2024-workshop"


class TestNormalizeAddress:
    """Tests for normalize_address helper function."""
    
    def test_basic_normalization(self):
        """Test basic address normalization."""
        assert normalize_address("123 Main St, London") == "123 main st london"
        assert normalize_address("456 High Street") == "456 high street"
    
    def test_punctuation_removal(self):
        """Test punctuation is removed."""
        assert normalize_address("123 Main St., London") == "123 main st london"
        assert normalize_address("Flat 4, 56 Park Rd.") == "flat 4 56 park rd"
    
    def test_whitespace_normalization(self):
        """Test multiple spaces are normalized."""
        assert normalize_address("  456   High  Street  ") == "456 high street"
        assert normalize_address("123\t\tMain\nStreet") == "123 main street"
    
    def test_case_normalization(self):
        """Test case is normalized to lowercase."""
        assert normalize_address("LONDON") == "london"
        assert normalize_address("MiXeD CaSe") == "mixed case"
    
    def test_empty_string(self):
        """Test empty string handling."""
        assert normalize_address("") == ""
        assert normalize_address("   ") == ""
    
    def test_unicode_in_address(self):
        """Test unicode characters in addresses."""
        # Unicode characters are preserved, only punctuation is removed
        assert normalize_address("Café Street, London") == "café street london"
        assert normalize_address("Rue de la Paix") == "rue de la paix"
    
    def test_special_address_characters(self):
        """Test various special characters in addresses."""
        assert normalize_address("123 Main St. #4") == "123 main st 4"
        assert normalize_address("Flat 2/3, Park Road") == "flat 23 park road"
        assert normalize_address("Unit A-B, 56 High St") == "unit ab 56 high st"
    
    def test_newlines_and_tabs(self):
        """Test newlines and tabs are normalized to spaces."""
        assert normalize_address("123 Main\nLondon\nUK") == "123 main london uk"
        assert normalize_address("456\tHigh\tStreet") == "456 high street"
    
    def test_multiple_consecutive_spaces(self):
        """Test multiple consecutive spaces become single space."""
        assert normalize_address("123    Main     St") == "123 main st"


class TestGenerateProviderId:
    """Tests for generate_provider_id function."""
    
    def test_basic_provider_names(self):
        """Test ID generation for common provider names."""
        assert generate_provider_id("Pasta Evangelists") == "provider-pasta-evangelists"
        assert generate_provider_id("Comptoir Bakery") == "provider-comptoir-bakery"
        assert generate_provider_id("Caravan Coffee") == "provider-caravan-coffee"
    
    def test_special_characters(self):
        """Test provider names with special characters."""
        assert generate_provider_id("Coffee & Co.") == "provider-coffee-co"
        assert generate_provider_id("The Wine Bar!") == "provider-the-wine-bar"
    
    def test_determinism(self):
        """Test same input produces same output."""
        name = "Test Provider"
        id1 = generate_provider_id(name)
        id2 = generate_provider_id(name)
        assert id1 == id2
    
    def test_whitespace_handling(self):
        """Test leading/trailing whitespace is handled."""
        assert generate_provider_id("  Pasta Evangelists  ") == "provider-pasta-evangelists"
        assert generate_provider_id("Pasta  Evangelists") == "provider-pasta-evangelists"
    
    def test_unicode_in_provider_name(self):
        """Test unicode characters in provider names."""
        assert generate_provider_id("Café Delight") == "provider-caf-delight"
        assert generate_provider_id("Crème Bakery") == "provider-cr-me-bakery"
    
    def test_empty_provider_name(self):
        """Test empty provider name handling."""
        assert generate_provider_id("") == "provider-"
        assert generate_provider_id("   ") == "provider-"
    
    def test_numbers_in_provider_name(self):
        """Test numbers are preserved in provider IDs."""
        assert generate_provider_id("Bakery 101") == "provider-bakery-101"
        assert generate_provider_id("24/7 Coffee") == "provider-24-7-coffee"


class TestGenerateLocationId:
    """Tests for generate_location_id function."""
    
    def test_basic_location_id(self):
        """Test basic location ID generation."""
        result = generate_location_id("pasta-evangelists", "123 Main St, London")
        assert result.startswith("location-pasta-evangelists-")
        assert len(result.split("-")[-1]) == 12  # 12-char hash
    
    def test_determinism(self):
        """Test same inputs produce same ID."""
        id1 = generate_location_id("pasta-evangelists", "123 Main St, London")
        id2 = generate_location_id("pasta-evangelists", "123 Main St, London")
        assert id1 == id2
    
    def test_address_normalization_effect(self):
        """Test minor address differences produce same ID."""
        id1 = generate_location_id("pasta-evangelists", "123 Main St, London")
        id2 = generate_location_id("pasta-evangelists", "123 Main St., London")
        id3 = generate_location_id("pasta-evangelists", "123  Main  St  London")
        assert id1 == id2 == id3
    
    def test_different_addresses_different_ids(self):
        """Test different addresses produce different IDs."""
        id1 = generate_location_id("pasta-evangelists", "123 Main St, London")
        id2 = generate_location_id("pasta-evangelists", "456 High St, London")
        assert id1 != id2
    
    def test_different_providers_different_ids(self):
        """Test same address for different providers produces different IDs."""
        id1 = generate_location_id("pasta-evangelists", "123 Main St, London")
        id2 = generate_location_id("comptoir-bakery", "123 Main St, London")
        assert id1 != id2
    
    def test_unicode_in_address(self):
        """Test unicode characters in location addresses."""
        id1 = generate_location_id("pasta-evangelists", "Café Street, London")
        id2 = generate_location_id("pasta-evangelists", "Caf Street, London")
        # Unicode normalization should make these similar but not identical
        assert id1.startswith("location-pasta-evangelists-")
    
    def test_empty_address(self):
        """Test empty address handling."""
        result = generate_location_id("pasta-evangelists", "")
        assert result.startswith("location-pasta-evangelists-")
        assert len(result.split("-")[-1]) == 12
    
    def test_very_long_address(self):
        """Test very long addresses are hashed consistently."""
        long_addr = "Unit 123, Building A, Floor 4, 456 Very Long Street Name, District, City, Region, Postcode, Country"
        id1 = generate_location_id("pasta-evangelists", long_addr)
        id2 = generate_location_id("pasta-evangelists", long_addr)
        assert id1 == id2
        assert len(id1.split("-")[-1]) == 12  # Hash is still 12 chars


class TestGenerateEventTemplateId:
    """Tests for generate_event_template_id function."""
    
    def test_with_source_template_id(self):
        """Test ID generation when source template ID is available."""
        result = generate_event_template_id("pasta-evangelists", "tmpl-123", "Pasta Making")
        assert result == "event-template-pasta-evangelists-tmpl-123"
    
    def test_without_source_template_id(self):
        """Test ID generation using title slug fallback."""
        result = generate_event_template_id("pasta-evangelists", None, "Pasta Making Workshop")
        assert result == "event-template-pasta-evangelists-pasta-making-workshop"
    
    def test_determinism_with_source_id(self):
        """Test determinism when source ID is provided."""
        id1 = generate_event_template_id("pasta-evangelists", "tmpl-123", "Pasta Making")
        id2 = generate_event_template_id("pasta-evangelists", "tmpl-123", "Pasta Making")
        assert id1 == id2
    
    def test_determinism_without_source_id(self):
        """Test determinism when using title slug."""
        id1 = generate_event_template_id("pasta-evangelists", None, "Pasta Making")
        id2 = generate_event_template_id("pasta-evangelists", None, "Pasta Making")
        assert id1 == id2
    
    def test_title_slug_normalization(self):
        """Test title is properly slugified."""
        result = generate_event_template_id("pasta-evangelists", None, "Coffee & Latte Art!")
        assert result == "event-template-pasta-evangelists-coffee-latte-art"
    
    def test_different_titles_different_ids(self):
        """Test different titles produce different IDs."""
        id1 = generate_event_template_id("pasta-evangelists", None, "Pasta Making")
        id2 = generate_event_template_id("pasta-evangelists", None, "Pizza Making")
        assert id1 != id2
    
    def test_unicode_in_title(self):
        """Test unicode characters in event titles."""
        result = generate_event_template_id("pasta-evangelists", None, "Café Latte Art")
        assert result == "event-template-pasta-evangelists-caf-latte-art"
    
    def test_empty_title(self):
        """Test empty title handling."""
        result = generate_event_template_id("pasta-evangelists", None, "")
        assert result == "event-template-pasta-evangelists-"
    
    def test_very_long_title(self):
        """Test very long titles are truncated in slug."""
        long_title = "This is an extremely long event title that definitely exceeds the fifty character limit for slugs"
        result = generate_event_template_id("pasta-evangelists", None, long_title)
        # Slug should be limited to 50 chars
        slug_part = result.replace("event-template-pasta-evangelists-", "")
        assert len(slug_part) <= 50
    
    def test_source_id_takes_precedence(self):
        """Test source template ID is used when available, regardless of title."""
        id1 = generate_event_template_id("pasta-evangelists", "tmpl-123", "Title A")
        id2 = generate_event_template_id("pasta-evangelists", "tmpl-123", "Title B")
        assert id1 == id2  # Same source ID produces same result


class TestGenerateEventOccurrenceId:
    """Tests for generate_event_occurrence_id function."""
    
    def test_with_source_event_id(self):
        """Test ID generation when source event ID is available."""
        result = generate_event_occurrence_id(
            "pasta-evangelists", "evt-456", "Pasta Class", None, None
        )
        assert result == "event-pasta-evangelists-evt-456"
    
    def test_without_source_event_id(self):
        """Test ID generation using composite hash fallback."""
        dt = datetime(2024, 3, 15, 18, 0)
        result = generate_event_occurrence_id(
            "pasta-evangelists", None, "Pasta Class", "location-123", dt
        )
        assert result.startswith("event-pasta-evangelists-")
        assert len(result.split("-")[-1]) == 8  # 8-char hash
    
    def test_determinism_with_source_id(self):
        """Test determinism when source ID is provided."""
        id1 = generate_event_occurrence_id(
            "pasta-evangelists", "evt-456", "Pasta Class", None, None
        )
        id2 = generate_event_occurrence_id(
            "pasta-evangelists", "evt-456", "Pasta Class", None, None
        )
        assert id1 == id2
    
    def test_determinism_without_source_id(self):
        """Test determinism when using composite hash."""
        dt = datetime(2024, 3, 15, 18, 0)
        id1 = generate_event_occurrence_id(
            "pasta-evangelists", None, "Pasta Class", "location-123", dt
        )
        id2 = generate_event_occurrence_id(
            "pasta-evangelists", None, "Pasta Class", "location-123", dt
        )
        assert id1 == id2
    
    def test_different_times_different_ids(self):
        """Test different start times produce different IDs."""
        dt1 = datetime(2024, 3, 15, 18, 0)
        dt2 = datetime(2024, 3, 16, 18, 0)
        id1 = generate_event_occurrence_id(
            "pasta-evangelists", None, "Pasta Class", "location-123", dt1
        )
        id2 = generate_event_occurrence_id(
            "pasta-evangelists", None, "Pasta Class", "location-123", dt2
        )
        assert id1 != id2
    
    def test_different_locations_different_ids(self):
        """Test different locations produce different IDs."""
        dt = datetime(2024, 3, 15, 18, 0)
        id1 = generate_event_occurrence_id(
            "pasta-evangelists", None, "Pasta Class", "location-123", dt
        )
        id2 = generate_event_occurrence_id(
            "pasta-evangelists", None, "Pasta Class", "location-456", dt
        )
        assert id1 != id2
    
    def test_no_location_handling(self):
        """Test handling when location is None."""
        dt = datetime(2024, 3, 15, 18, 0)
        result = generate_event_occurrence_id(
            "pasta-evangelists", None, "Pasta Class", None, dt
        )
        assert result.startswith("event-pasta-evangelists-")
        assert "no-location" in result or len(result.split("-")[-1]) == 8
    
    def test_no_date_handling(self):
        """Test handling when start_at is None."""
        result = generate_event_occurrence_id(
            "pasta-evangelists", None, "Pasta Class", "location-123", None
        )
        assert result.startswith("event-pasta-evangelists-")
        assert "no-date" in result or len(result.split("-")[-1]) == 8


class TestValidationRules:
    """Tests for model validation rules."""
    
    def test_provider_required_fields(self):
        """Test Provider validates required fields."""
        provider = Provider(
            provider_id="",
            provider_name="",
            provider_slug="",
            source_name="",
            source_base_url=""
        )
        errors = provider.validate()
        assert len(errors) >= 3
        assert any("provider_id" in err for err in errors)
        assert any("provider_name" in err for err in errors)
        assert any("provider_slug" in err for err in errors)
    
    def test_provider_slug_format(self):
        """Test Provider validates slug format."""
        # Invalid formats
        invalid_slugs = [
            "Invalid_Slug",  # Underscore
            "Invalid Slug",  # Space
            "Invalid!Slug",  # Special char
            "UPPERCASE",     # Uppercase
            "-leading",      # Leading hyphen
            "trailing-",     # Trailing hyphen
        ]
        
        for slug in invalid_slugs:
            provider = Provider(
                provider_id="provider-test",
                provider_name="Test",
                provider_slug=slug,
                source_name="Test",
                source_base_url="https://example.com"
            )
            errors = provider.validate()
            assert any("kebab-case" in err for err in errors), f"Failed for slug: {slug}"
    
    def test_provider_valid_slug_formats(self):
        """Test Provider accepts valid slug formats."""
        valid_slugs = [
            "valid-slug",
            "valid-slug-123",
            "a",
            "123",
            "multi-word-slug-here",
        ]
        
        for slug in valid_slugs:
            provider = Provider(
                provider_id="provider-test",
                provider_name="Test",
                provider_slug=slug,
                source_name="Test",
                source_base_url="https://example.com"
            )
            errors = provider.validate()
            assert not any("kebab-case" in err for err in errors), f"Failed for slug: {slug}"
    
    def test_location_coordinate_constraints(self):
        """Test Location validates coordinate boundaries."""
        # Test latitude boundaries
        invalid_coords = [
            (91.0, 0.0),    # Latitude too high
            (-91.0, 0.0),   # Latitude too low
            (0.0, 181.0),   # Longitude too high
            (0.0, -181.0),  # Longitude too low
        ]
        
        for lat, lon in invalid_coords:
            location = Location(
                location_id="location-test",
                provider_id="provider-test",
                provider_name="Test",
                formatted_address="123 Test St",
                latitude=lat,
                longitude=lon
            )
            errors = location.validate()
            assert len(errors) > 0, f"Failed for coords: ({lat}, {lon})"
    
    def test_location_valid_coordinate_boundaries(self):
        """Test Location accepts valid coordinate boundaries."""
        valid_coords = [
            (90.0, 180.0),    # Max boundaries
            (-90.0, -180.0),  # Min boundaries
            (0.0, 0.0),       # Zero
            (51.5074, -0.1278),  # London
        ]
        
        for lat, lon in valid_coords:
            location = Location(
                location_id="location-test",
                provider_id="provider-test",
                provider_name="Test",
                formatted_address="123 Test St",
                latitude=lat,
                longitude=lon
            )
            errors = location.validate()
            assert not any("latitude" in err or "longitude" in err for err in errors), \
                f"Failed for coords: ({lat}, {lon})"
    
    def test_location_geocode_status_values(self):
        """Test Location validates geocode_status values."""
        valid_statuses = ["not_geocoded", "success", "failed", "invalid_address"]
        
        for status in valid_statuses:
            location = Location(
                location_id="location-test",
                provider_id="provider-test",
                provider_name="Test",
                formatted_address="123 Test St",
                geocode_status=status
            )
            errors = location.validate()
            assert not any("geocode_status" in err for err in errors)
        
        # Test invalid status
        location = Location(
            location_id="location-test",
            provider_id="provider-test",
            provider_name="Test",
            formatted_address="123 Test St",
            geocode_status="invalid"
        )
        errors = location.validate()
        assert any("geocode_status" in err for err in errors)
    
    def test_event_template_price_constraint(self):
        """Test EventTemplate validates price >= 0."""
        template = EventTemplate(
            event_template_id="event-template-test",
            provider_id="provider-test",
            title="Test Event",
            slug="test-event",
            price_from=-10.0
        )
        errors = template.validate()
        assert any("price_from" in err for err in errors)
        
        # Test valid prices
        for price in [0.0, 0.01, 50.0, 1000.0]:
            template = EventTemplate(
                event_template_id="event-template-test",
                provider_id="provider-test",
                title="Test Event",
                slug="test-event",
                price_from=price
            )
            errors = template.validate()
            assert not any("price_from" in err for err in errors)
    
    def test_event_template_age_constraints(self):
        """Test EventTemplate validates age constraints."""
        # Test age_min > 0
        template = EventTemplate(
            event_template_id="event-template-test",
            provider_id="provider-test",
            title="Test Event",
            slug="test-event",
            age_min=0
        )
        errors = template.validate()
        assert any("age_min" in err for err in errors)
        
        # Test age_max > 0
        template = EventTemplate(
            event_template_id="event-template-test",
            provider_id="provider-test",
            title="Test Event",
            slug="test-event",
            age_max=-5
        )
        errors = template.validate()
        assert any("age_max" in err for err in errors)
        
        # Test age_min <= age_max
        template = EventTemplate(
            event_template_id="event-template-test",
            provider_id="provider-test",
            title="Test Event",
            slug="test-event",
            age_min=65,
            age_max=18
        )
        errors = template.validate()
        assert any("age_min" in err and "age_max" in err for err in errors)
        
        # Test valid age ranges
        valid_ranges = [(18, 65), (1, 100), (5, 5)]
        for age_min, age_max in valid_ranges:
            template = EventTemplate(
                event_template_id="event-template-test",
                provider_id="provider-test",
                title="Test Event",
                slug="test-event",
                age_min=age_min,
                age_max=age_max
            )
            errors = template.validate()
            assert not any("age" in err for err in errors), \
                f"Failed for age range: ({age_min}, {age_max})"
    
    def test_event_occurrence_datetime_constraint(self):
        """Test EventOccurrence validates start_at < end_at."""
        # Test invalid: start >= end
        occurrence = EventOccurrence(
            event_id="event-test",
            provider_id="provider-test",
            title="Test Event",
            start_at=datetime(2025, 6, 1, 12, 0),
            end_at=datetime(2025, 6, 1, 10, 0)
        )
        errors = occurrence.validate()
        assert any("start_at" in err and "end_at" in err for err in errors)
        
        # Test invalid: start == end
        occurrence = EventOccurrence(
            event_id="event-test",
            provider_id="provider-test",
            title="Test Event",
            start_at=datetime(2025, 6, 1, 10, 0),
            end_at=datetime(2025, 6, 1, 10, 0)
        )
        errors = occurrence.validate()
        assert any("start_at" in err and "end_at" in err for err in errors)
        
        # Test valid: start < end
        occurrence = EventOccurrence(
            event_id="event-test",
            provider_id="provider-test",
            title="Test Event",
            start_at=datetime(2025, 6, 1, 10, 0),
            end_at=datetime(2025, 6, 1, 12, 0)
        )
        errors = occurrence.validate()
        assert not any("start_at" in err or "end_at" in err for err in errors)
    
    def test_event_occurrence_availability_status_values(self):
        """Test EventOccurrence validates availability_status values."""
        valid_statuses = ["available", "sold_out", "limited", "unknown"]
        
        for status in valid_statuses:
            occurrence = EventOccurrence(
                event_id="event-test",
                provider_id="provider-test",
                title="Test Event",
                availability_status=status
            )
            errors = occurrence.validate()
            assert not any("availability_status" in err for err in errors)
        
        # Test invalid status
        occurrence = EventOccurrence(
            event_id="event-test",
            provider_id="provider-test",
            title="Test Event",
            availability_status="invalid"
        )
        errors = occurrence.validate()
        assert any("availability_status" in err for err in errors)
    
    def test_timestamp_constraints(self):
        """Test all models validate first_seen_at <= last_seen_at."""
        # Test Provider
        provider = Provider(
            provider_id="provider-test",
            provider_name="Test",
            provider_slug="test",
            source_name="Test",
            source_base_url="https://example.com",
            first_seen_at=datetime(2025, 1, 20),
            last_seen_at=datetime(2025, 1, 15)
        )
        errors = provider.validate()
        assert any("first_seen_at" in err and "last_seen_at" in err for err in errors)
        
        # Test Location
        location = Location(
            location_id="location-test",
            provider_id="provider-test",
            provider_name="Test",
            formatted_address="123 Test St",
            first_seen_at=datetime(2025, 1, 20),
            last_seen_at=datetime(2025, 1, 15)
        )
        errors = location.validate()
        assert any("first_seen_at" in err and "last_seen_at" in err for err in errors)
        
        # Test EventTemplate
        template = EventTemplate(
            event_template_id="event-template-test",
            provider_id="provider-test",
            title="Test",
            slug="test",
            first_seen_at=datetime(2025, 1, 20),
            last_seen_at=datetime(2025, 1, 15)
        )
        errors = template.validate()
        assert any("first_seen_at" in err and "last_seen_at" in err for err in errors)
        
        # Test EventOccurrence
        occurrence = EventOccurrence(
            event_id="event-test",
            provider_id="provider-test",
            title="Test",
            first_seen_at=datetime(2025, 1, 20),
            last_seen_at=datetime(2025, 1, 15)
        )
        errors = occurrence.validate()
        assert any("first_seen_at" in err and "last_seen_at" in err for err in errors)


class TestForeignKeyValidation:
    """Tests for foreign key validation logic."""
    
    def test_location_references_provider(self):
        """Test Location has provider_id foreign key."""
        location = Location(
            location_id="location-test",
            provider_id="provider-nonexistent",
            provider_name="Test",
            formatted_address="123 Test St"
        )
        # Model itself doesn't validate FK existence, just that field is non-empty
        errors = location.validate()
        assert not any("provider_id" in err for err in errors)
        
        # Empty provider_id should fail
        location = Location(
            location_id="location-test",
            provider_id="",
            provider_name="Test",
            formatted_address="123 Test St"
        )
        errors = location.validate()
        assert any("provider_id" in err for err in errors)
    
    def test_event_template_references_provider(self):
        """Test EventTemplate has provider_id foreign key."""
        template = EventTemplate(
            event_template_id="event-template-test",
            provider_id="provider-nonexistent",
            title="Test",
            slug="test"
        )
        # Model itself doesn't validate FK existence, just that field is non-empty
        errors = template.validate()
        assert not any("provider_id" in err for err in errors)
        
        # Empty provider_id should fail
        template = EventTemplate(
            event_template_id="event-template-test",
            provider_id="",
            title="Test",
            slug="test"
        )
        errors = template.validate()
        assert any("provider_id" in err for err in errors)
    
    def test_event_occurrence_references_provider(self):
        """Test EventOccurrence has provider_id foreign key."""
        occurrence = EventOccurrence(
            event_id="event-test",
            provider_id="provider-nonexistent",
            title="Test"
        )
        # Model itself doesn't validate FK existence, just that field is non-empty
        errors = occurrence.validate()
        assert not any("provider_id" in err for err in errors)
        
        # Empty provider_id should fail
        occurrence = EventOccurrence(
            event_id="event-test",
            provider_id="",
            title="Test"
        )
        errors = occurrence.validate()
        assert any("provider_id" in err for err in errors)
    
    def test_event_occurrence_optional_location_reference(self):
        """Test EventOccurrence location_id is optional."""
        # location_id can be None
        occurrence = EventOccurrence(
            event_id="event-test",
            provider_id="provider-test",
            title="Test",
            location_id=None
        )
        errors = occurrence.validate()
        assert occurrence.is_valid()
        
        # location_id can be set
        occurrence = EventOccurrence(
            event_id="event-test",
            provider_id="provider-test",
            title="Test",
            location_id="location-test"
        )
        errors = occurrence.validate()
        assert occurrence.is_valid()
    
    def test_event_occurrence_optional_template_reference(self):
        """Test EventOccurrence event_template_id is optional."""
        # event_template_id can be None
        occurrence = EventOccurrence(
            event_id="event-test",
            provider_id="provider-test",
            title="Test",
            event_template_id=None
        )
        errors = occurrence.validate()
        assert occurrence.is_valid()
        
        # event_template_id can be set
        occurrence = EventOccurrence(
            event_id="event-test",
            provider_id="provider-test",
            title="Test",
            event_template_id="event-template-test"
        )
        errors = occurrence.validate()
        assert occurrence.is_valid()

