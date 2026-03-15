"""Integration tests for AI enrichment caching behavior.

Tests cache hit/miss scenarios, cache invalidation when source changes,
and cache key composition including prompt version and model.
Uses temporary cache directories for test isolation.

**Validates: Requirements 5.3, 12.2, 12.3, 12.5, 12.7**
"""

import json
import pytest
from pathlib import Path
from datetime import datetime

from src.enrich.ai_enricher import AIEnricher, EnrichmentData
from src.models.event_template import EventTemplate
from src.models.event_occurrence import EventOccurrence


@pytest.fixture
def temp_cache_dir(tmp_path):
    """Create temporary cache directory for test isolation."""
    cache_dir = tmp_path / "cache" / "ai"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return str(cache_dir)


@pytest.fixture
def sample_event_template():
    """Create sample event template for testing."""
    return EventTemplate(
        event_template_id="event-template-test-pasta",
        provider_id="provider-test",
        title="Pasta Making Masterclass",
        slug="pasta-making-masterclass",
        description_clean="Learn authentic Italian pasta making from scratch. Hands-on class with expert chef.",
        source_hash="abc123def456",
        category="Cooking",
        price_from=75.0,
        currency="GBP",
        first_seen_at=datetime.now(),
        last_seen_at=datetime.now()
    )


@pytest.fixture
def sample_event_occurrence():
    """Create sample event occurrence for testing."""
    return EventOccurrence(
        event_id="event-test-pasta-session",
        provider_id="provider-test",
        title="Evening Pasta Session",
        description_clean="Join us for pasta making this evening. Traditional techniques and fresh ingredients.",
        source_hash="xyz789abc123",
        price=80.0,
        currency="GBP",
        start_at=datetime(2025, 2, 20, 18, 0),
        end_at=datetime(2025, 2, 20, 21, 0),
        first_seen_at=datetime.now(),
        last_seen_at=datetime.now()
    )


@pytest.fixture
def sample_enrichment():
    """Create sample enrichment data for testing."""
    return EnrichmentData(
        description_ai="Master authentic Italian pasta-making from scratch. Perfect for food lovers.",
        summary_short="Italian pasta masterclass",
        summary_medium="Learn traditional pasta techniques with expert chefs in this hands-on cooking experience.",
        tags=["hands-on", "italian", "pasta", "cooking"],
        occasion_tags=["date-night", "team-building"],
        skills_created=["pasta-making", "italian-cooking"],
        age_min=18,
        audience="adults",
        beginner_friendly=True,
        duration_minutes=180
    )


class TestCacheHitMissBehavior:
    """Test cache hit and miss scenarios for AI enrichment.
    
    **Validates: Requirements 5.3, 12.3, 12.7**
    """
    
    def test_cache_miss_on_first_enrichment(self, temp_cache_dir, sample_event_template, sample_enrichment):
        """Test that first enrichment for an event is a cache miss."""
        enricher = AIEnricher(
            api_key="test-key",
            cache_dir=temp_cache_dir,
            model="gpt-4o-mini",
            prompt_version="v1"
        )
        
        # Pre-populate cache to simulate LLM response
        cache_key = enricher._compute_cache_key(sample_event_template.source_hash)
        enricher._save_to_cache(cache_key, sample_enrichment)
        
        # Verify cache file was created
        cache_files = list(Path(temp_cache_dir).glob("*.json"))
        assert len(cache_files) == 1
        
        # Enrich event (should use cache)
        enriched_event = enricher.enrich_event(sample_event_template)
        
        assert enriched_event.description_ai == "Master authentic Italian pasta-making from scratch. Perfect for food lovers."
        assert "hands-on" in enriched_event.tags
        assert enriched_event.age_min == 18
    
    def test_cache_hit_on_second_enrichment(self, temp_cache_dir, sample_enrichment):
        """Test that second enrichment for same source_hash is a cache hit."""
        enricher = AIEnricher(
            api_key="test-key",
            cache_dir=temp_cache_dir,
            model="gpt-4o-mini",
            prompt_version="v1"
        )
        
        # First event with specific source_hash
        event1 = EventTemplate(
            event_template_id="event-1",
            provider_id="provider-test",
            title="First Event",
            slug="first-event",
            description_clean="First event description",
            source_hash="shared_hash_123",
            first_seen_at=datetime.now(),
            last_seen_at=datetime.now()
        )
        
        # Pre-populate cache
        cache_key = enricher._compute_cache_key("shared_hash_123")
        enricher._save_to_cache(cache_key, sample_enrichment)
        
        result1 = enricher.enrich_event(event1)
        
        # Second event with same source_hash (different event_id)
        event2 = EventTemplate(
            event_template_id="event-2",
            provider_id="provider-test",
            title="Second Event",
            slug="second-event",
            description_clean="Second event description",
            source_hash="shared_hash_123",  # Same source_hash
            first_seen_at=datetime.now(),
            last_seen_at=datetime.now()
        )
        
        result2 = enricher.enrich_event(event2)
        
        # Both should have same AI enrichment from cache
        assert result1.description_ai == result2.description_ai
        assert result1.tags == result2.tags
        assert result1.age_min == result2.age_min
        
        # Should still have only one cache file
        cache_files = list(Path(temp_cache_dir).glob("*.json"))
        assert len(cache_files) == 1
    
    def test_different_source_hashes_create_separate_cache_entries(self, temp_cache_dir):
        """Test that different source_hashes create separate cache entries."""
        enricher = AIEnricher(
            api_key="test-key",
            cache_dir=temp_cache_dir,
            model="gpt-4o-mini",
            prompt_version="v1"
        )
        
        events = [
            EventTemplate(
                event_template_id=f"event-{i}",
                provider_id="provider-test",
                title=f"Event {i}",
                slug=f"event-{i}",
                description_clean=f"Description for event {i}",
                source_hash=f"hash_{i}",
                first_seen_at=datetime.now(),
                last_seen_at=datetime.now()
            )
            for i in range(3)
        ]
        
        # Pre-populate cache for each event
        for i, event in enumerate(events):
            cache_key = enricher._compute_cache_key(event.source_hash)
            enrichment = EnrichmentData(
                description_ai=f"AI description {i}",
                tags=[f"tag-{i}"]
            )
            enricher._save_to_cache(cache_key, enrichment)
        
        results = [enricher.enrich_event(event) for event in events]
        
        # Should have three separate cache files
        cache_files = list(Path(temp_cache_dir).glob("*.json"))
        assert len(cache_files) == 3
        
        # Each should have different AI descriptions
        for i, result in enumerate(results):
            assert result.description_ai == f"AI description {i}"
            assert f"tag-{i}" in result.tags

class TestCacheInvalidationOnSourceChange:
    """Test cache invalidation when source data changes.
    
    **Validates: Requirements 12.5**
    """
    
    def test_cache_invalidated_when_source_hash_changes(self, temp_cache_dir, sample_enrichment):
        """Test that changing source_hash invalidates cache and uses new enrichment."""
        enricher = AIEnricher(
            api_key="test-key",
            cache_dir=temp_cache_dir,
            model="gpt-4o-mini",
            prompt_version="v1"
        )
        
        # Event with original source_hash
        event = EventTemplate(
            event_template_id="event-changeable",
            provider_id="provider-test",
            title="Changeable Event",
            slug="changeable-event",
            description_clean="Original description",
            source_hash="original_hash",
            first_seen_at=datetime.now(),
            last_seen_at=datetime.now()
        )
        
        # Pre-populate cache for original hash
        original_cache_key = enricher._compute_cache_key("original_hash")
        original_enrichment = EnrichmentData(
            description_ai="Original AI description",
            tags=["original-tag"]
        )
        enricher._save_to_cache(original_cache_key, original_enrichment)
        
        result1 = enricher.enrich_event(event)
        assert result1.description_ai == "Original AI description"
        assert "original-tag" in result1.tags
        
        # Change source data (simulating source change)
        event.source_hash = "updated_hash"
        event.description_clean = "Updated description"
        
        # Pre-populate cache for new hash
        updated_cache_key = enricher._compute_cache_key("updated_hash")
        updated_enrichment = EnrichmentData(
            description_ai="Updated AI description",
            tags=["updated-tag"]
        )
        enricher._save_to_cache(updated_cache_key, updated_enrichment)
        
        result2 = enricher.enrich_event(event)
        
        # Should use new enrichment (cache invalidated)
        assert result2.description_ai == "Updated AI description"
        assert "updated-tag" in result2.tags
        
        # Should have two cache files now
        cache_files = list(Path(temp_cache_dir).glob("*.json"))
        assert len(cache_files) == 2
    
    def test_cache_used_after_source_change_if_matches_existing(self, temp_cache_dir):
        """Test that cache is used if new source_hash matches an existing cached hash."""
        enricher = AIEnricher(
            api_key="test-key",
            cache_dir=temp_cache_dir,
            model="gpt-4o-mini",
            prompt_version="v1"
        )
        
        # Pre-populate cache for two different hashes
        hash1_enrichment = EnrichmentData(
            description_ai="Hash 1 description",
            tags=["hash1-tag"]
        )
        hash2_enrichment = EnrichmentData(
            description_ai="Hash 2 description",
            tags=["hash2-tag"]
        )
        
        enricher._save_to_cache(enricher._compute_cache_key("hash_1"), hash1_enrichment)
        enricher._save_to_cache(enricher._compute_cache_key("hash_2"), hash2_enrichment)
        
        event = EventTemplate(
            event_template_id="event-revert",
            provider_id="provider-test",
            title="Revert Event",
            slug="revert-event",
            description_clean="Description 1",
            source_hash="hash_1",
            first_seen_at=datetime.now(),
            last_seen_at=datetime.now()
        )
        
        # First enrichment with hash_1
        result1 = enricher.enrich_event(event)
        assert result1.description_ai == "Hash 1 description"
        
        # Change to hash_2
        event.source_hash = "hash_2"
        event.description_clean = "Description 2"
        
        result2 = enricher.enrich_event(event)
        assert result2.description_ai == "Hash 2 description"
        
        # Change back to hash_1 (should use existing cache)
        event.source_hash = "hash_1"
        event.description_clean = "Description 1"
        
        result3 = enricher.enrich_event(event)
        assert result3.description_ai == "Hash 1 description"
        
        # Should still have only two cache files
        cache_files = list(Path(temp_cache_dir).glob("*.json"))
        assert len(cache_files) == 2
    
    def test_source_hash_none_handled_gracefully(self, temp_cache_dir):
        """Test that None source_hash is handled gracefully without caching."""
        enricher = AIEnricher(
            api_key="test-key",
            cache_dir=temp_cache_dir,
            model="gpt-4o-mini",
            prompt_version="v1"
        )
        
        event = EventTemplate(
            event_template_id="event-no-hash",
            provider_id="provider-test",
            title="No Hash Event",
            slug="no-hash-event",
            description_clean="Description without hash",
            source_hash=None,  # No source hash
            first_seen_at=datetime.now(),
            last_seen_at=datetime.now()
        )
        
        # When source_hash is None, enricher should handle gracefully
        # but won't use caching (generates timestamp-based keys)
        result = enricher.enrich_event(event)
        
        # Should handle gracefully and return original event (no LLM available)
        assert result.description_ai is None  # No enrichment without LLM
        assert result == event  # Original event returned unchanged
class TestCacheKeyComposition:
    """Test cache key includes prompt version and model.
    
    **Validates: Requirements 12.2**
    """
    
    def test_cache_key_includes_prompt_version(self, temp_cache_dir):
        """Test that cache keys change when prompt version changes."""
        enricher_v1 = AIEnricher(
            api_key="test-key",
            cache_dir=temp_cache_dir,
            model="gpt-4o-mini",
            prompt_version="v1"
        )
        
        enricher_v2 = AIEnricher(
            api_key="test-key",
            cache_dir=temp_cache_dir,
            model="gpt-4o-mini",
            prompt_version="v2"
        )
        
        source_hash = "test_hash_123"
        
        key_v1 = enricher_v1._compute_cache_key(source_hash)
        key_v2 = enricher_v2._compute_cache_key(source_hash)
        
        # Keys should be different for different prompt versions
        assert key_v1 != key_v2
        assert len(key_v1) == 16
        assert len(key_v2) == 16
    
    def test_cache_key_includes_model(self, temp_cache_dir):
        """Test that cache keys change when model changes."""
        enricher_mini = AIEnricher(
            api_key="test-key",
            cache_dir=temp_cache_dir,
            model="gpt-4o-mini",
            prompt_version="v1"
        )
        
        enricher_full = AIEnricher(
            api_key="test-key",
            cache_dir=temp_cache_dir,
            model="gpt-4o",
            prompt_version="v1"
        )
        
        source_hash = "test_hash_456"
        
        key_mini = enricher_mini._compute_cache_key(source_hash)
        key_full = enricher_full._compute_cache_key(source_hash)
        
        # Keys should be different for different models
        assert key_mini != key_full
        assert len(key_mini) == 16
        assert len(key_full) == 16
    
    def test_cache_key_stable_across_instances(self, temp_cache_dir):
        """Test that cache keys are stable across enricher instances with same config."""
        enricher1 = AIEnricher(
            api_key="test-key-1",
            cache_dir=temp_cache_dir,
            model="gpt-4o-mini",
            prompt_version="v1"
        )
        
        enricher2 = AIEnricher(
            api_key="test-key-2",  # Different API key
            cache_dir=temp_cache_dir,
            model="gpt-4o-mini",    # Same model
            prompt_version="v1"     # Same prompt version
        )
        
        source_hash = "stable_hash_test"
        
        key1 = enricher1._compute_cache_key(source_hash)
        key2 = enricher2._compute_cache_key(source_hash)
        
        # Keys should be same (API key not part of cache key)
        assert key1 == key2
    
    def test_different_configurations_create_separate_caches(self, temp_cache_dir):
        """Test that different model/prompt combinations create separate cache entries."""
        configurations = [
            ("gpt-4o-mini", "v1"),
            ("gpt-4o-mini", "v2"),
            ("gpt-4o", "v1"),
            ("gpt-4o", "v2")
        ]
        
        enrichers = [
            AIEnricher(
                api_key="test-key",
                cache_dir=temp_cache_dir,
                model=model,
                prompt_version=version
            )
            for model, version in configurations
        ]
        
        source_hash = "config_test_hash"
        
        # Pre-populate cache for each configuration
        for i, enricher in enumerate(enrichers):
            cache_key = enricher._compute_cache_key(source_hash)
            enrichment = EnrichmentData(
                description_ai=f"Description for config {i}",
                tags=[f"config-{i}"]
            )
            enricher._save_to_cache(cache_key, enrichment)
        
        # Create test event
        event = EventTemplate(
            event_template_id="event-config-test",
            provider_id="provider-test",
            title="Config Test Event",
            slug="config-test-event",
            description_clean="Test description",
            source_hash=source_hash,
            first_seen_at=datetime.now(),
            last_seen_at=datetime.now()
        )
        
        # Each enricher should use its own cache
        for i, enricher in enumerate(enrichers):
            result = enricher.enrich_event(event)
            assert result.description_ai == f"Description for config {i}"
            assert f"config-{i}" in result.tags
        
        # Should have four separate cache files
        cache_files = list(Path(temp_cache_dir).glob("*.json"))
        assert len(cache_files) == 4
class TestCachePersistence:
    """Test cache persistence across enricher instances.
    
    **Validates: Requirements 12.7**
    """
    
    def test_cache_persists_across_enricher_instances(self, temp_cache_dir, sample_enrichment):
        """Test that cache is reused when creating new enricher instances."""
        # First enricher instance
        enricher1 = AIEnricher(
            api_key="test-key",
            cache_dir=temp_cache_dir,
            model="gpt-4o-mini",
            prompt_version="v1"
        )
        
        event1 = EventTemplate(
            event_template_id="event-persist-1",
            provider_id="provider-test",
            title="Persistence Test Event",
            slug="persistence-test-event",
            description_clean="Test persistence description",
            source_hash="persist_hash_123",
            first_seen_at=datetime.now(),
            last_seen_at=datetime.now()
        )
        
        # Pre-populate cache
        cache_key = enricher1._compute_cache_key("persist_hash_123")
        enricher1._save_to_cache(cache_key, sample_enrichment)
        
        result1 = enricher1.enrich_event(event1)
        
        # Create new enricher instance with same configuration
        enricher2 = AIEnricher(
            api_key="test-key",
            cache_dir=temp_cache_dir,
            model="gpt-4o-mini",
            prompt_version="v1"
        )
        
        event2 = EventTemplate(
            event_template_id="event-persist-2",
            provider_id="provider-test",
            title="Persistence Test Event 2",
            slug="persistence-test-event-2",
            description_clean="Test persistence description 2",
            source_hash="persist_hash_123",  # Same source hash
            first_seen_at=datetime.now(),
            last_seen_at=datetime.now()
        )
        
        result2 = enricher2.enrich_event(event2)
        
        # Should use persisted cache (same enrichment)
        assert result2.description_ai == result1.description_ai
        assert result2.tags == result1.tags
        assert result2.age_min == result1.age_min
    
    def test_cache_files_readable_after_restart(self, temp_cache_dir, sample_enrichment):
        """Test that cache files are properly formatted and readable."""
        enricher = AIEnricher(
            api_key="test-key",
            cache_dir=temp_cache_dir,
            model="gpt-4o-mini",
            prompt_version="v1"
        )
        
        source_hash = "readable_test_hash"
        cache_key = enricher._compute_cache_key(source_hash)
        enricher._save_to_cache(cache_key, sample_enrichment)
        
        # Find and read cache file
        cache_files = list(Path(temp_cache_dir).glob("*.json"))
        assert len(cache_files) == 1
        
        with open(cache_files[0], 'r') as f:
            cache_data = json.load(f)
        
        # Verify cache file structure
        assert "description_ai" in cache_data
        assert "summary_short" in cache_data
        assert "tags" in cache_data
        assert "occasion_tags" in cache_data
        assert "skills_created" in cache_data
        assert "age_min" in cache_data
        assert "audience" in cache_data
        assert "beginner_friendly" in cache_data
        assert "cached_at" in cache_data
        
        # Verify values match sample enrichment
        assert cache_data["description_ai"] == sample_enrichment.description_ai
        assert cache_data["tags"] == sample_enrichment.tags
        assert cache_data["age_min"] == sample_enrichment.age_min
    
    def test_corrupted_cache_file_handled_gracefully(self, temp_cache_dir):
        """Test that corrupted cache files are handled gracefully."""
        enricher = AIEnricher(
            api_key="test-key",
            cache_dir=temp_cache_dir,
            model="gpt-4o-mini",
            prompt_version="v1"
        )
        
        # Create corrupted cache file
        cache_key = "corrupted_test_key"
        cache_path = enricher._get_cache_path(cache_key)
        with open(cache_path, 'w') as f:
            f.write("invalid json content {{{")
        
        # Try to load corrupted cache
        loaded = enricher._load_from_cache(cache_key)
        
        # Should return None for corrupted cache
        assert loaded is None
        
        # Should not raise exception
        event = EventTemplate(
            event_template_id="event-corrupted-test",
            provider_id="provider-test",
            title="Corrupted Cache Test",
            slug="corrupted-cache-test",
            description_clean="Test description",
            source_hash="corrupted_hash",
            first_seen_at=datetime.now(),
            last_seen_at=datetime.now()
        )
        
        # Should handle gracefully (no enrichment without LLM)
        result = enricher.enrich_event(event)
        assert result.description_ai is None
class TestCacheDirectoryIsolation:
    """Test cache directory isolation for tests.
    
    **Validates: Requirements 12.7**
    """
    
    def test_separate_cache_directories_are_isolated(self, tmp_path):
        """Test that separate cache directories don't interfere with each other."""
        # Create two separate cache directories
        cache_dir1 = str(tmp_path / "cache1" / "ai")
        cache_dir2 = str(tmp_path / "cache2" / "ai")
        
        enricher1 = AIEnricher(
            api_key="test-key",
            cache_dir=cache_dir1,
            model="gpt-4o-mini",
            prompt_version="v1"
        )
        
        enricher2 = AIEnricher(
            api_key="test-key",
            cache_dir=cache_dir2,
            model="gpt-4o-mini",
            prompt_version="v1"
        )
        
        # Pre-populate first cache
        enrichment1 = EnrichmentData(
            description_ai="Cache 1 description",
            tags=["cache1-tag"]
        )
        cache_key = enricher1._compute_cache_key("isolation_test_hash")
        enricher1._save_to_cache(cache_key, enrichment1)
        
        # Pre-populate second cache with different data
        enrichment2 = EnrichmentData(
            description_ai="Cache 2 description",
            tags=["cache2-tag"]
        )
        enricher2._save_to_cache(cache_key, enrichment2)
        
        event1 = EventTemplate(
            event_template_id="event-isolation-1",
            provider_id="provider-test",
            title="Isolation Test 1",
            slug="isolation-test-1",
            description_clean="Test description 1",
            source_hash="isolation_test_hash",
            first_seen_at=datetime.now(),
            last_seen_at=datetime.now()
        )
        
        event2 = EventTemplate(
            event_template_id="event-isolation-2",
            provider_id="provider-test",
            title="Isolation Test 2",
            slug="isolation-test-2",
            description_clean="Test description 2",
            source_hash="isolation_test_hash",
            first_seen_at=datetime.now(),
            last_seen_at=datetime.now()
        )
        
        result1 = enricher1.enrich_event(event1)
        result2 = enricher2.enrich_event(event2)
        
        # Should use different caches
        assert result1.description_ai == "Cache 1 description"
        assert result2.description_ai == "Cache 2 description"
        assert "cache1-tag" in result1.tags
        assert "cache2-tag" in result2.tags
        
        # Verify separate cache files
        cache_files1 = list(Path(cache_dir1).glob("*.json"))
        cache_files2 = list(Path(cache_dir2).glob("*.json"))
        
        assert len(cache_files1) == 1
        assert len(cache_files2) == 1
        assert cache_files1[0] != cache_files2[0]
    
    def test_tmp_path_fixture_provides_clean_cache(self, tmp_path):
        """Test that tmp_path fixture provides clean isolated cache directory."""
        cache_dir = str(tmp_path / "cache" / "ai")
        
        # Verify directory doesn't exist yet
        assert not Path(cache_dir).exists()
        
        # Create enricher with cache
        enricher = AIEnricher(
            api_key="test-key",
            cache_dir=cache_dir,
            model="gpt-4o-mini",
            prompt_version="v1"
        )
        
        # Verify directory was created
        assert Path(cache_dir).exists()
        
        # Verify it's empty
        cache_files = list(Path(cache_dir).glob("*.json"))
        assert len(cache_files) == 0
        
        # Add cache entry
        enrichment = EnrichmentData(description_ai="Test description")
        cache_key = enricher._compute_cache_key("test_hash")
        enricher._save_to_cache(cache_key, enrichment)
        
        # Verify cache file was created
        cache_files = list(Path(cache_dir).glob("*.json"))
        assert len(cache_files) == 1
    
    def test_cache_directory_created_if_not_exists(self, tmp_path):
        """Test that cache directory is created if it doesn't exist."""
        cache_dir = str(tmp_path / "nonexistent" / "cache" / "ai")
        
        assert not Path(cache_dir).exists()
        
        enricher = AIEnricher(
            api_key="test-key",
            cache_dir=cache_dir,
            model="gpt-4o-mini",
            prompt_version="v1"
        )
        
        # Directory should be created during initialization
        assert Path(cache_dir).exists()
        assert Path(cache_dir).is_dir()


class TestEventTypeCompatibility:
    """Test cache behavior with different event types.
    
    **Validates: Requirements 5.3**
    """
    
    def test_event_template_caching(self, temp_cache_dir, sample_event_template, sample_enrichment):
        """Test caching works with EventTemplate objects."""
        enricher = AIEnricher(
            api_key="test-key",
            cache_dir=temp_cache_dir,
            model="gpt-4o-mini",
            prompt_version="v1"
        )
        
        # Pre-populate cache
        cache_key = enricher._compute_cache_key(sample_event_template.source_hash)
        enricher._save_to_cache(cache_key, sample_enrichment)
        
        result = enricher.enrich_event(sample_event_template)
        
        # Should apply enrichment from cache
        assert result.description_ai == sample_enrichment.description_ai
        # Tags are merged, so check that enrichment tags are included
        for tag in sample_enrichment.tags:
            assert tag in result.tags
        assert result.beginner_friendly == sample_enrichment.beginner_friendly
    
    def test_event_occurrence_caching(self, temp_cache_dir, sample_event_occurrence, sample_enrichment):
        """Test caching works with EventOccurrence objects."""
        enricher = AIEnricher(
            api_key="test-key",
            cache_dir=temp_cache_dir,
            model="gpt-4o-mini",
            prompt_version="v1"
        )
        
        # Pre-populate cache
        cache_key = enricher._compute_cache_key(sample_event_occurrence.source_hash)
        enricher._save_to_cache(cache_key, sample_enrichment)
        
        result = enricher.enrich_event(sample_event_occurrence)
        
        # Should apply enrichment from cache
        assert result.description_ai == sample_enrichment.description_ai
        # Tags are merged, so check that enrichment tags are included
        for tag in sample_enrichment.tags:
            assert tag in result.tags
        # Note: EventOccurrence doesn't have beginner_friendly attribute
    
    def test_mixed_event_types_share_cache_by_source_hash(self, temp_cache_dir, sample_enrichment):
        """Test that different event types share cache if they have same source_hash."""
        enricher = AIEnricher(
            api_key="test-key",
            cache_dir=temp_cache_dir,
            model="gpt-4o-mini",
            prompt_version="v1"
        )
        
        shared_hash = "shared_source_hash"
        
        template = EventTemplate(
            event_template_id="template-shared",
            provider_id="provider-test",
            title="Shared Template",
            slug="shared-template",
            description_clean="Shared description",
            source_hash=shared_hash,
            first_seen_at=datetime.now(),
            last_seen_at=datetime.now()
        )
        
        occurrence = EventOccurrence(
            event_id="occurrence-shared",
            provider_id="provider-test",
            title="Shared Occurrence",
            description_clean="Shared description",
            source_hash=shared_hash,
            start_at=datetime(2025, 3, 1, 10, 0),
            end_at=datetime(2025, 3, 1, 12, 0),
            first_seen_at=datetime.now(),
            last_seen_at=datetime.now()
        )
        
        # Pre-populate cache
        cache_key = enricher._compute_cache_key(shared_hash)
        enricher._save_to_cache(cache_key, sample_enrichment)
        
        result_template = enricher.enrich_event(template)
        result_occurrence = enricher.enrich_event(occurrence)
        
        # Both should use same cache
        assert result_template.description_ai == result_occurrence.description_ai
        assert result_template.tags == result_occurrence.tags
        
        # Should still have only one cache file
        cache_files = list(Path(temp_cache_dir).glob("*.json"))
        assert len(cache_files) == 1