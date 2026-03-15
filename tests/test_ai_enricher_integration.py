"""Integration tests for AIEnricher with event models."""

import os
from datetime import datetime
from pathlib import Path

import pytest

from src.enrich.ai_enricher import AIEnricher
from src.models.event_template import EventTemplate
from src.models.event_occurrence import EventOccurrence


@pytest.fixture
def temp_cache_dir(tmp_path):
    """Create temporary cache directory."""
    cache_dir = tmp_path / "cache" / "ai"
    cache_dir.mkdir(parents=True)
    return str(cache_dir)


class TestAIEnricherIntegration:
    """Integration tests for AIEnricher with event models."""
    
    def test_enricher_with_event_template_full_workflow(self, temp_cache_dir):
        """Test full enrichment workflow with EventTemplate."""
        # Create event template
        event = EventTemplate(
            event_template_id="event-template-test-cooking",
            provider_id="provider-test",
            title="Italian Cooking Class",
            slug="italian-cooking-class",
            description_clean="Learn to cook authentic Italian dishes. Hands-on class with professional chef.",
            source_hash="test_hash_123",
            category="Cooking",
            price_from=85.0,
            currency="GBP",
            first_seen_at=datetime.now(),
            last_seen_at=datetime.now()
        )
        
        # Validate event before enrichment
        assert event.is_valid()
        assert event.description_ai is None
        assert len(event.tags) == 0
        
        # Create enricher (without API key, will skip actual LLM call)
        enricher = AIEnricher(
            api_key="test-key",
            cache_dir=temp_cache_dir
        )
        
        # Pre-populate cache to avoid actual API call
        from src.enrich.ai_enricher import EnrichmentData
        cache_key = enricher._compute_cache_key(event.source_hash)
        cached_enrichment = EnrichmentData(
            description_ai="Master authentic Italian cooking techniques in this hands-on class.",
            summary_short="Italian cooking masterclass",
            summary_medium="Learn traditional Italian recipes with a professional chef in this immersive cooking experience.",
            tags=["hands-on", "italian", "cooking"],
            occasion_tags=["date-night", "team-building"],
            skills_created=["italian-cooking", "pasta-making"],
            age_min=18,
            audience="adults",
            beginner_friendly=True,
            duration_minutes=180
        )
        enricher._save_to_cache(cache_key, cached_enrichment)
        
        # Enrich event
        enriched_event = enricher.enrich_event(event)
        
        # Verify enrichment was applied
        assert enriched_event.description_ai == "Master authentic Italian cooking techniques in this hands-on class."
        assert enriched_event.summary_short == "Italian cooking masterclass"
        assert "hands-on" in enriched_event.tags
        assert "italian" in enriched_event.tags
        assert enriched_event.age_min == 18
        assert enriched_event.beginner_friendly is True
        
        # Verify event is still valid after enrichment
        assert enriched_event.is_valid()
    
    def test_enricher_with_event_occurrence_full_workflow(self, temp_cache_dir):
        """Test full enrichment workflow with EventOccurrence."""
        # Create event occurrence
        event = EventOccurrence(
            event_id="event-test-session-1",
            provider_id="provider-test",
            title="Pasta Making Session",
            description_clean="Join us for an evening of pasta making. Learn traditional Italian techniques.",
            source_hash="test_hash_456",
            price=75.0,
            currency="GBP",
            start_at=datetime(2025, 2, 15, 18, 0),
            end_at=datetime(2025, 2, 15, 21, 0),
            first_seen_at=datetime.now(),
            last_seen_at=datetime.now()
        )
        
        # Validate event before enrichment
        assert event.is_valid()
        assert event.description_ai is None
        
        # Create enricher
        enricher = AIEnricher(
            api_key="test-key",
            cache_dir=temp_cache_dir
        )
        
        # Pre-populate cache
        from src.enrich.ai_enricher import EnrichmentData
        cache_key = enricher._compute_cache_key(event.source_hash)
        cached_enrichment = EnrichmentData(
            description_ai="Create fresh pasta from scratch in this evening workshop.",
            tags=["pasta", "italian", "hands-on"],
            beginner_friendly=True
        )
        enricher._save_to_cache(cache_key, cached_enrichment)
        
        # Enrich event
        enriched_event = enricher.enrich_event(event)
        
        # Verify enrichment was applied
        assert enriched_event.description_ai == "Create fresh pasta from scratch in this evening workshop."
        assert "pasta" in enriched_event.tags
        # Note: EventOccurrence doesn't have beginner_friendly attribute
        
        # Verify event is still valid after enrichment
        assert enriched_event.is_valid()
    
    def test_enricher_preserves_existing_event_data(self, temp_cache_dir):
        """Test that enricher doesn't overwrite existing event data."""
        # Create event with existing data
        event = EventTemplate(
            event_template_id="event-template-test-existing",
            provider_id="provider-test",
            title="Existing Event",
            slug="existing-event",
            description_clean="Original description",
            source_hash="test_hash_789",
            tags=["existing-tag"],
            age_min=16,
            audience="families",
            beginner_friendly=False,
            first_seen_at=datetime.now(),
            last_seen_at=datetime.now()
        )
        
        # Create enricher
        enricher = AIEnricher(
            api_key="test-key",
            cache_dir=temp_cache_dir
        )
        
        # Pre-populate cache with different values
        from src.enrich.ai_enricher import EnrichmentData
        cache_key = enricher._compute_cache_key(event.source_hash)
        cached_enrichment = EnrichmentData(
            description_ai="New AI description",
            tags=["new-tag"],
            age_min=18,  # Different from existing
            audience="adults",  # Different from existing
            beginner_friendly=True  # Different from existing
        )
        enricher._save_to_cache(cache_key, cached_enrichment)
        
        # Enrich event
        enriched_event = enricher.enrich_event(event)
        
        # Verify AI description was added
        assert enriched_event.description_ai == "New AI description"
        
        # Verify tags were merged (not replaced)
        assert "existing-tag" in enriched_event.tags
        assert "new-tag" in enriched_event.tags
        
        # Verify existing values were preserved
        assert enriched_event.age_min == 16  # Original preserved
        assert enriched_event.audience == "families"  # Original preserved
        
        # Verify beginner_friendly was updated (False -> True is allowed)
        assert enriched_event.beginner_friendly is True
    
    def test_enricher_cache_invalidation_on_source_change(self, temp_cache_dir):
        """Test that cache is invalidated when source_hash changes."""
        # Create enricher
        enricher = AIEnricher(
            api_key="test-key",
            cache_dir=temp_cache_dir
        )
        
        # Create event with initial source_hash
        event = EventTemplate(
            event_template_id="event-template-test-cache",
            provider_id="provider-test",
            title="Cache Test Event",
            slug="cache-test-event",
            description_clean="Original description",
            source_hash="original_hash",
            first_seen_at=datetime.now(),
            last_seen_at=datetime.now()
        )
        
        # Pre-populate cache for original hash
        from src.enrich.ai_enricher import EnrichmentData
        original_cache_key = enricher._compute_cache_key("original_hash")
        original_enrichment = EnrichmentData(
            description_ai="Original AI description",
            tags=["original-tag"]
        )
        enricher._save_to_cache(original_cache_key, original_enrichment)
        
        # Enrich with original hash
        enriched_event = enricher.enrich_event(event)
        assert enriched_event.description_ai == "Original AI description"
        assert "original-tag" in enriched_event.tags
        
        # Change source_hash (simulating source data change)
        event.source_hash = "updated_hash"
        event.description_clean = "Updated description"
        
        # Pre-populate cache for new hash
        updated_cache_key = enricher._compute_cache_key("updated_hash")
        updated_enrichment = EnrichmentData(
            description_ai="Updated AI description",
            tags=["updated-tag"]
        )
        enricher._save_to_cache(updated_cache_key, updated_enrichment)
        
        # Enrich with new hash (should use new cache)
        enriched_event = enricher.enrich_event(event)
        assert enriched_event.description_ai == "Updated AI description"
        assert "updated-tag" in enriched_event.tags
    
    def test_enricher_handles_missing_description_gracefully(self, temp_cache_dir):
        """Test that enricher handles events without description_clean."""
        # Create event without description
        event = EventTemplate(
            event_template_id="event-template-test-no-desc",
            provider_id="provider-test",
            title="No Description Event",
            slug="no-description-event",
            description_clean=None,  # No description
            source_hash="test_hash_no_desc",
            first_seen_at=datetime.now(),
            last_seen_at=datetime.now()
        )
        
        # Create enricher
        enricher = AIEnricher(
            api_key="test-key",
            cache_dir=temp_cache_dir
        )
        
        # Enrich event (should skip enrichment)
        enriched_event = enricher.enrich_event(event)
        
        # Verify event unchanged
        assert enriched_event.description_ai is None
        assert len(enriched_event.tags) == 0
        assert enriched_event == event
    
    def test_enricher_validates_enriched_events(self, temp_cache_dir):
        """Test that enriched events remain valid after enrichment."""
        # Create various events
        events = [
            EventTemplate(
                event_template_id="event-template-test-1",
                provider_id="provider-test",
                title="Test Event 1",
                slug="test-event-1",
                description_clean="Test description 1",
                source_hash="hash_1",
                first_seen_at=datetime.now(),
                last_seen_at=datetime.now()
            ),
            EventOccurrence(
                event_id="event-test-2",
                provider_id="provider-test",
                title="Test Event 2",
                description_clean="Test description 2",
                source_hash="hash_2",
                start_at=datetime(2025, 3, 1, 10, 0),
                end_at=datetime(2025, 3, 1, 12, 0),
                first_seen_at=datetime.now(),
                last_seen_at=datetime.now()
            )
        ]
        
        # Create enricher
        enricher = AIEnricher(
            api_key="test-key",
            cache_dir=temp_cache_dir
        )
        
        # Pre-populate cache for all events
        from src.enrich.ai_enricher import EnrichmentData
        for i, event in enumerate(events):
            cache_key = enricher._compute_cache_key(event.source_hash)
            enrichment = EnrichmentData(
                description_ai=f"AI description {i+1}",
                tags=[f"tag-{i+1}"],
                age_min=18,
                beginner_friendly=True
            )
            enricher._save_to_cache(cache_key, enrichment)
        
        # Enrich all events and validate
        for event in events:
            enriched_event = enricher.enrich_event(event)
            
            # Verify enrichment was applied
            assert enriched_event.description_ai is not None
            assert len(enriched_event.tags) > 0
            
            # Verify event is still valid
            assert enriched_event.is_valid(), f"Event {enriched_event} failed validation"
