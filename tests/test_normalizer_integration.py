"""Integration tests for Normalizer with real scraper data patterns."""

import pytest
from datetime import datetime

from src.transform.normalizer import Normalizer
from src.models.raw_provider_data import RawProviderData


class TestCaravanCoffeePattern:
    """Test normalization with Caravan Coffee scraper data pattern."""
    
    def test_caravan_coffee_normalization(self):
        """Test complete normalization flow with Caravan Coffee data pattern."""
        # Simulate data from Caravan Coffee scraper
        raw_data = RawProviderData(
            provider_name="Caravan Coffee Roasters",
            provider_website="https://caravanandco.com",
            source_name="Caravan Coffee Website + Eventbrite",
            source_base_url="https://caravanandco.com/pages/coffee-school",
            raw_locations=[{
                "location_name": "Lambworks Roastery Brewbar",
                "formatted_address": "North Road, London, N7 9DP",
                "address_line_1": "North Road",
                "city": "London",
                "postcode": "N7 9DP",
                "country": "UK"
            }],
            raw_templates=[
                {
                    "title": "London Roastery Tour & Tasting",
                    "description": "Join us for a tour of our roastery",
                    "price": "£25",
                    "source_url": "https://www.eventbrite.com/e/roastery-tour",
                    "image_url": "https://example.com/roastery.jpg"
                },
                {
                    "title": "Home Filter Class",
                    "description": "Learn to brew perfect filter coffee at home",
                    "price": "£35",
                    "source_url": "https://www.eventbrite.com/e/filter-class",
                    "image_url": "https://example.com/filter.jpg"
                }
            ],
            raw_events=[
                {
                    "title": "London Roastery Tour & Tasting",
                    "description": "Join us for a tour of our roastery",
                    "start_at": "2024-06-15T10:00:00",
                    "end_at": "2024-06-15T11:30:00",
                    "price": "£25",
                    "booking_url": "https://www.eventbrite.com/e/roastery-tour-123",
                    "source_url": "https://www.eventbrite.com/e/roastery-tour",
                    "image_url": "https://example.com/roastery.jpg",
                    "location_data": {
                        "formatted_address": "North Road, London, N7 9DP"
                    }
                }
            ]
        )
        
        normalizer = Normalizer()
        
        # Normalize provider
        provider = normalizer.normalize_provider(raw_data)
        assert provider.provider_id == "provider-caravan-coffee-roasters"
        assert provider.provider_name == "Caravan Coffee Roasters"
        assert provider.is_valid()
        
        # Normalize locations
        locations = normalizer.normalize_locations(raw_data, provider.provider_id)
        assert len(locations) == 1
        assert locations[0].location_name == "Lambworks Roastery Brewbar"
        assert locations[0].city == "London"
        assert locations[0].postcode == "N7 9DP"
        assert locations[0].is_valid()
        
        # Build location map
        location_map = {
            loc.formatted_address: loc.location_id for loc in locations
        }
        
        # Normalize events
        events = normalizer.normalize_events(raw_data, provider.provider_id, location_map)
        assert len(events) == 3  # 2 templates + 1 occurrence
        
        # Check templates (have event_template_id as primary ID)
        from src.models.event_template import EventTemplate
        from src.models.event_occurrence import EventOccurrence
        
        templates = [e for e in events if isinstance(e, EventTemplate)]
        assert len(templates) == 2
        assert all(t.is_valid() for t in templates)
        assert templates[0].price_from == 25.0
        assert templates[1].price_from == 35.0
        
        # Check occurrence (has event_id as primary ID)
        occurrences = [e for e in events if isinstance(e, EventOccurrence)]
        assert len(occurrences) == 1
        occurrence = occurrences[0]
        assert occurrence.is_valid()
        assert occurrence.title == "London Roastery Tour & Tasting"
        assert occurrence.price == 25.0
        assert occurrence.start_at == datetime(2024, 6, 15, 10, 0)
        assert occurrence.location_id == locations[0].location_id


class TestPastaEvangelistsPattern:
    """Test normalization with Pasta Evangelists scraper data pattern."""
    
    def test_pasta_evangelists_normalization(self):
        """Test complete normalization flow with Pasta Evangelists data pattern."""
        # Simulate data from Pasta Evangelists scraper
        raw_data = RawProviderData(
            provider_name="Pasta Evangelists",
            provider_website="https://pastaevangelists.com",
            source_name="Pasta Evangelists API",
            source_base_url="https://api.pastaevangelists.com",
            raw_locations=[
                {
                    "location_name": "The Pasta Academy Farringdon",
                    "formatted_address": "62-63 Long Lane, London, EC1A 9EJ",
                    "address_line_1": "62-63 Long Lane",
                    "city": "London",
                    "postcode": "EC1A 9EJ",
                    "country": "UK"
                },
                {
                    "location_name": "The Pasta Academy Aldgate",
                    "formatted_address": "45 Aldgate High Street, London, EC3N 1AL",
                    "address_line_1": "45 Aldgate High Street",
                    "city": "London",
                    "postcode": "EC3N 1AL",
                    "country": "UK"
                }
            ],
            raw_templates=[
                {
                    "source_template_id": "1156436138",
                    "title": "Beginners Class",
                    "description": "<p>Join us to master the techniques of <b>pasta fatta a mano</b></p>",
                    "price": 68.0,
                    "category": "Cooking",
                    "sub_category": "Pasta Making",
                    "duration": "2 hours",
                    "tags": ["hands-on", "italian", "beginner-friendly"],
                    "source_url": "https://plan.pastaevangelists.com/events/themes",
                    "image_url": "https://cdn.example.com/beginners.jpg"
                }
            ]
        )
        
        normalizer = Normalizer()
        
        # Normalize provider
        provider = normalizer.normalize_provider(raw_data)
        assert provider.provider_id == "provider-pasta-evangelists"
        assert provider.is_valid()
        
        # Normalize locations
        locations = normalizer.normalize_locations(raw_data, provider.provider_id)
        assert len(locations) == 2
        assert all(loc.is_valid() for loc in locations)
        assert locations[0].location_name == "The Pasta Academy Farringdon"
        assert locations[1].location_name == "The Pasta Academy Aldgate"
        
        # Normalize events
        events = normalizer.normalize_events(raw_data, provider.provider_id, {})
        assert len(events) == 1
        
        template = events[0]
        assert template.event_template_id == "event-template-pasta-evangelists-1156436138"
        assert template.title == "Beginners Class"
        assert template.slug == "beginners-class"
        assert template.description_clean == "Join us to master the techniques of pasta fatta a mano"
        assert template.category == "Cooking"
        assert template.sub_category == "Pasta Making"
        assert template.price_from == 68.0
        assert template.duration_minutes == 120
        assert template.tags == ["hands-on", "italian", "beginner-friendly"]
        assert template.is_valid()


class TestEdgeCases:
    """Test edge cases and error handling."""
    
    def test_empty_raw_data(self):
        """Test normalization with empty raw data."""
        raw_data = RawProviderData(
            provider_name="Empty Provider"
        )
        
        normalizer = Normalizer()
        
        provider = normalizer.normalize_provider(raw_data)
        assert provider.is_valid()
        
        locations = normalizer.normalize_locations(raw_data, provider.provider_id)
        assert len(locations) == 0
        
        events = normalizer.normalize_events(raw_data, provider.provider_id, {})
        assert len(events) == 0
    
    def test_malformed_data_skipped(self):
        """Test that malformed data is gracefully skipped."""
        raw_data = RawProviderData(
            provider_name="Test Provider",
            raw_locations=[
                {"location_name": "Valid", "formatted_address": "123 Main St"},
                {"location_name": "Invalid - No Address"},  # Should be skipped
                {},  # Should be skipped
            ],
            raw_templates=[
                {"title": "Valid Template", "price": 50.0},
                {"description": "No Title"},  # Should be skipped
                {},  # Should be skipped
            ]
        )
        
        normalizer = Normalizer()
        
        locations = normalizer.normalize_locations(raw_data, "provider-test")
        assert len(locations) == 1
        
        events = normalizer.normalize_events(raw_data, "provider-test", {})
        assert len(events) == 1
    
    def test_html_stripping_in_descriptions(self):
        """Test that HTML is properly stripped from descriptions."""
        raw_data = RawProviderData(
            provider_name="Test Provider",
            raw_templates=[{
                "title": "Test Event",
                "description": "<div><p>This is <strong>bold</strong> and <em>italic</em> text.</p><ul><li>Item 1</li><li>Item 2</li></ul></div>"
            }]
        )
        
        normalizer = Normalizer()
        events = normalizer.normalize_events(raw_data, "provider-test", {})
        
        assert len(events) == 1
        template = events[0]
        assert "<" not in template.description_clean
        assert ">" not in template.description_clean
        assert "bold" in template.description_clean
        assert "italic" in template.description_clean
    
    def test_price_parsing_variations(self):
        """Test various price format parsing."""
        raw_data = RawProviderData(
            provider_name="Test Provider",
            raw_templates=[
                {"title": "Event 1", "price": 50},
                {"title": "Event 2", "price": 50.0},
                {"title": "Event 3", "price": "50"},
                {"title": "Event 4", "price": "£50"},
                {"title": "Event 5", "price": "$50.00"},
                {"title": "Event 6", "price": "GBP 50"},
            ]
        )
        
        normalizer = Normalizer()
        events = normalizer.normalize_events(raw_data, "provider-test", {})
        
        assert len(events) == 6
        for event in events:
            assert event.price_from == 50.0
    
    def test_datetime_parsing_variations(self):
        """Test various datetime format parsing."""
        raw_data = RawProviderData(
            provider_name="Test Provider",
            raw_events=[
                {"title": "Event 1", "start_at": "2024-06-15T18:00:00"},
                {"title": "Event 2", "start_at": "2024-06-15T18:00:00Z"},
                {"title": "Event 3", "start_at": "2024-06-15 18:00:00"},
                {"title": "Event 4", "start_at": "15/06/2024 18:00"},
            ]
        )
        
        normalizer = Normalizer()
        events = normalizer.normalize_events(raw_data, "provider-test", {})
        
        assert len(events) == 4
        for event in events:
            assert event.start_at is not None
            assert event.start_at.year == 2024
            assert event.start_at.month == 6
            assert event.start_at.day == 15
            assert event.start_at.hour == 18
