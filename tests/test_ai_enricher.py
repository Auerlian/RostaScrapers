"""Unit tests for AIEnricher."""

import json
import os
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

import pytest

from src.enrich.ai_enricher import AIEnricher, EnrichmentData
from src.models.event_template import EventTemplate
from src.models.event_occurrence import EventOccurrence


@pytest.fixture
def temp_cache_dir(tmp_path):
    """Create temporary cache directory."""
    cache_dir = tmp_path / "cache" / "ai"
    cache_dir.mkdir(parents=True)
    return str(cache_dir)


@pytest.fixture
def sample_event_template():
    """Create sample event template for testing."""
    return EventTemplate(
        event_template_id="event-template-test-pasta-class",
        provider_id="provider-test",
        title="Pasta Making Class",
        slug="pasta-making-class",
        description_clean="Learn to make fresh pasta from scratch. Hands-on class with expert chef.",
        source_hash="abc123def456",
        category="Cooking",
        price_from=68.0,
        currency="GBP"
    )


@pytest.fixture
def sample_event_occurrence():
    """Create sample event occurrence for testing."""
    return EventOccurrence(
        event_id="event-test-pasta-session-1",
        provider_id="provider-test",
        title="Pasta Making Session",
        description_clean="Join us for an evening of pasta making. Learn traditional techniques.",
        source_hash="xyz789abc123",
        price=75.0,
        currency="GBP"
    )


@pytest.fixture
def mock_openai_response():
    """Create mock OpenAI API response."""
    return {
        "description_ai": "Master authentic Italian pasta-making from scratch. Perfect for beginners.",
        "summary_short": "Hands-on pasta making for beginners",
        "summary_medium": "Learn traditional Italian pasta techniques in this beginner-friendly class with expert chefs.",
        "tags": ["hands-on", "italian", "beginner-friendly"],
        "occasion_tags": ["date-night", "team-building"],
        "skills_required": ["none"],
        "skills_created": ["pasta-making", "hand-rolling"],
        "age_min": 18,
        "age_max": None,
        "audience": "adults",
        "family_friendly": False,
        "beginner_friendly": True,
        "duration_minutes": 120
    }


class TestAIEnricher:
    """Test suite for AIEnricher class."""
    
    def test_init_with_api_key(self, temp_cache_dir):
        """Test initialization with explicit API key."""
        enricher = AIEnricher(
            api_key="test-key",
            cache_dir=temp_cache_dir
        )
        
        assert enricher.api_key == "test-key"
        assert enricher.model == "gpt-4o-mini"
        assert enricher.prompt_version == "v1"
        assert enricher.timeout == 30
        assert Path(temp_cache_dir).exists()
    
    def test_init_from_env_var(self, temp_cache_dir, monkeypatch):
        """Test initialization reading API key from environment."""
        monkeypatch.setenv("OPENAI_API_KEY", "env-test-key")
        
        enricher = AIEnricher(cache_dir=temp_cache_dir)
        
        assert enricher.api_key == "env-test-key"
    
    def test_init_creates_cache_dir(self, tmp_path):
        """Test that initialization creates cache directory."""
        cache_dir = tmp_path / "new_cache" / "ai"
        
        enricher = AIEnricher(
            api_key="test-key",
            cache_dir=str(cache_dir)
        )
        
        assert cache_dir.exists()
    
    @pytest.mark.skipif(True, reason="OpenAI library not required for core tests")
    def test_client_lazy_initialization(self, temp_cache_dir):
        """Test that OpenAI client is lazily initialized."""
        enricher = AIEnricher(
            api_key="test-key",
            cache_dir=temp_cache_dir
        )
        
        # Client should not be initialized yet
        assert enricher._client is None
        
        # Access client property to trigger initialization
        with patch('openai.OpenAI') as mock_openai:
            _ = enricher.client
            mock_openai.assert_called_once_with(api_key="test-key", timeout=30)
    
    def test_client_raises_without_api_key(self, temp_cache_dir):
        """Test that accessing client without API key raises error."""
        enricher = AIEnricher(
            api_key=None,
            cache_dir=temp_cache_dir
        )
        
        with pytest.raises(ValueError, match="OpenAI API key not found"):
            _ = enricher.client
    
    def test_compute_cache_key(self, temp_cache_dir):
        """Test cache key computation."""
        enricher = AIEnricher(
            api_key="test-key",
            cache_dir=temp_cache_dir,
            model="gpt-4o-mini",
            prompt_version="v1"
        )
        
        cache_key = enricher._compute_cache_key("abc123")
        
        # Should be 16-character hash
        assert len(cache_key) == 16
        assert cache_key.isalnum()
        
        # Same input should produce same key
        cache_key2 = enricher._compute_cache_key("abc123")
        assert cache_key == cache_key2
        
        # Different input should produce different key
        cache_key3 = enricher._compute_cache_key("xyz789")
        assert cache_key != cache_key3
    
    def test_compute_cache_key_with_none(self, temp_cache_dir):
        """Test cache key computation with None source_hash."""
        enricher = AIEnricher(
            api_key="test-key",
            cache_dir=temp_cache_dir
        )
        
        # Should not raise error
        cache_key = enricher._compute_cache_key(None)
        assert len(cache_key) == 16
    
    def test_save_and_load_cache(self, temp_cache_dir):
        """Test saving and loading enrichment data from cache."""
        enricher = AIEnricher(
            api_key="test-key",
            cache_dir=temp_cache_dir
        )
        
        # Create enrichment data
        enrichment = EnrichmentData(
            description_ai="Test description",
            summary_short="Short summary",
            tags=["test", "example"],
            beginner_friendly=True
        )
        
        # Save to cache
        cache_key = "test_cache_key"
        enricher._save_to_cache(cache_key, enrichment)
        
        # Load from cache
        loaded = enricher._load_from_cache(cache_key)
        
        assert loaded is not None
        assert loaded.description_ai == "Test description"
        assert loaded.summary_short == "Short summary"
        assert loaded.tags == ["test", "example"]
        assert loaded.beginner_friendly is True
    
    def test_load_cache_missing_file(self, temp_cache_dir):
        """Test loading from cache when file doesn't exist."""
        enricher = AIEnricher(
            api_key="test-key",
            cache_dir=temp_cache_dir
        )
        
        loaded = enricher._load_from_cache("nonexistent_key")
        
        assert loaded is None
    
    def test_load_cache_corrupted_file(self, temp_cache_dir):
        """Test loading from cache with corrupted JSON."""
        enricher = AIEnricher(
            api_key="test-key",
            cache_dir=temp_cache_dir
        )
        
        # Create corrupted cache file
        cache_path = enricher._get_cache_path("corrupted_key")
        with open(cache_path, 'w') as f:
            f.write("invalid json {{{")
        
        loaded = enricher._load_from_cache("corrupted_key")
        
        # Should return None for corrupted cache
        assert loaded is None
    
    def test_build_prompt(self, temp_cache_dir, sample_event_template):
        """Test prompt building with ROSTA tone guidelines."""
        from src.enrich.prompts import build_enrichment_prompt
        
        prompt = build_enrichment_prompt(sample_event_template)
        
        # Check prompt contains key elements
        assert "Pasta Making Class" in prompt
        assert "ROSTA" in prompt
        assert "Modern, confident, curated" in prompt
        assert "JSON" in prompt
        assert "description_ai" in prompt
        assert "tags" in prompt
    
    def test_parse_response(self, temp_cache_dir, mock_openai_response):
        """Test parsing LLM JSON response."""
        enricher = AIEnricher(
            api_key="test-key",
            cache_dir=temp_cache_dir
        )
        
        response_text = json.dumps(mock_openai_response)
        enrichment = enricher._parse_response(response_text)
        
        assert enrichment.description_ai == "Master authentic Italian pasta-making from scratch. Perfect for beginners."
        assert enrichment.summary_short == "Hands-on pasta making for beginners"
        assert enrichment.tags == ["hands-on", "italian", "beginner-friendly"]
        assert enrichment.age_min == 18
        assert enrichment.beginner_friendly is True
    
    def test_parse_response_invalid_json(self, temp_cache_dir):
        """Test parsing invalid JSON response."""
        enricher = AIEnricher(
            api_key="test-key",
            cache_dir=temp_cache_dir
        )
        
        with pytest.raises(ValueError, match="Failed to parse LLM response"):
            enricher._parse_response("invalid json {{{")
    
    def test_apply_enrichment_to_template(self, temp_cache_dir, sample_event_template):
        """Test applying enrichment data to event template."""
        enricher = AIEnricher(
            api_key="test-key",
            cache_dir=temp_cache_dir
        )
        
        enrichment = EnrichmentData(
            description_ai="Enhanced description",
            summary_short="Short summary",
            summary_medium="Medium summary",
            tags=["new-tag"],
            skills_created=["new-skill"],
            age_min=18,
            beginner_friendly=True
        )
        
        enriched_event = enricher._apply_enrichment(sample_event_template, enrichment)
        
        assert enriched_event.description_ai == "Enhanced description"
        assert enriched_event.summary_short == "Short summary"
        assert enriched_event.summary_medium == "Medium summary"
        assert "new-tag" in enriched_event.tags
        assert "new-skill" in enriched_event.skills_created
        assert enriched_event.age_min == 18
        assert enriched_event.beginner_friendly is True
    
    def test_apply_enrichment_merges_tags(self, temp_cache_dir, sample_event_template):
        """Test that enrichment merges tags without duplicates."""
        enricher = AIEnricher(
            api_key="test-key",
            cache_dir=temp_cache_dir
        )
        
        # Set existing tags
        sample_event_template.tags = ["existing-tag", "shared-tag"]
        
        enrichment = EnrichmentData(
            tags=["shared-tag", "new-tag"]
        )
        
        enriched_event = enricher._apply_enrichment(sample_event_template, enrichment)
        
        # Should have all unique tags
        assert set(enriched_event.tags) == {"existing-tag", "shared-tag", "new-tag"}
    
    def test_apply_enrichment_preserves_existing_values(self, temp_cache_dir, sample_event_template):
        """Test that enrichment doesn't override existing values."""
        enricher = AIEnricher(
            api_key="test-key",
            cache_dir=temp_cache_dir
        )
        
        # Set existing values
        sample_event_template.age_min = 16
        sample_event_template.audience = "families"
        
        enrichment = EnrichmentData(
            age_min=18,  # Should not override
            audience="adults"  # Should not override
        )
        
        enriched_event = enricher._apply_enrichment(sample_event_template, enrichment)
        
        # Existing values should be preserved
        assert enriched_event.age_min == 16
        assert enriched_event.audience == "families"
    
    def test_enrich_event_skips_without_description(self, temp_cache_dir, sample_event_template):
        """Test that enrichment is skipped if no description_clean."""
        enricher = AIEnricher(
            api_key="test-key",
            cache_dir=temp_cache_dir
        )
        
        sample_event_template.description_clean = None
        
        result = enricher.enrich_event(sample_event_template)
        
        # Should return unchanged event
        assert result == sample_event_template
        assert result.description_ai is None
    
    def test_enrich_event_uses_cache(self, temp_cache_dir, sample_event_template):
        """Test that enrichment uses cached data when available."""
        enricher = AIEnricher(
            api_key="test-key",
            cache_dir=temp_cache_dir
        )
        
        # Pre-populate cache
        cache_key = enricher._compute_cache_key(sample_event_template.source_hash)
        cached_enrichment = EnrichmentData(
            description_ai="Cached description",
            tags=["cached-tag"]
        )
        enricher._save_to_cache(cache_key, cached_enrichment)
        
        # Enrich event (should use cache, not call LLM)
        enriched_event = enricher.enrich_event(sample_event_template)
        
        assert enriched_event.description_ai == "Cached description"
        assert "cached-tag" in enriched_event.tags
    
    @pytest.mark.skipif(True, reason="OpenAI library not required for core tests")
    @patch('openai.OpenAI')
    def test_enrich_event_calls_llm(self, mock_openai_class, temp_cache_dir, sample_event_template, mock_openai_response):
        """Test that enrichment calls LLM when cache miss."""
        # Setup mock OpenAI client
        mock_client = MagicMock()
        mock_openai_class.return_value = mock_client
        
        # Setup mock response
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = json.dumps(mock_openai_response)
        mock_client.chat.completions.create.return_value = mock_response
        
        enricher = AIEnricher(
            api_key="test-key",
            cache_dir=temp_cache_dir
        )
        
        # Enrich event (cache miss, should call LLM)
        enriched_event = enricher.enrich_event(sample_event_template)
        
        # Verify LLM was called
        mock_client.chat.completions.create.assert_called_once()
        
        # Verify enrichment was applied
        assert enriched_event.description_ai == "Master authentic Italian pasta-making from scratch. Perfect for beginners."
        assert "hands-on" in enriched_event.tags
    
    @pytest.mark.skipif(True, reason="OpenAI library not required for core tests")
    @patch('openai.OpenAI')
    def test_enrich_event_handles_llm_error(self, mock_openai_class, temp_cache_dir, sample_event_template):
        """Test that enrichment handles LLM errors gracefully."""
        # Setup mock OpenAI client to raise error
        mock_client = MagicMock()
        mock_openai_class.return_value = mock_client
        mock_client.chat.completions.create.side_effect = Exception("API error")
        
        enricher = AIEnricher(
            api_key="test-key",
            cache_dir=temp_cache_dir
        )
        
        # Enrich event (should handle error gracefully)
        enriched_event = enricher.enrich_event(sample_event_template)
        
        # Should return original event unchanged
        assert enriched_event.description_ai is None
        assert enriched_event == sample_event_template
    
    def test_enrich_event_occurrence(self, temp_cache_dir, sample_event_occurrence):
        """Test enriching event occurrence (not just templates)."""
        enricher = AIEnricher(
            api_key="test-key",
            cache_dir=temp_cache_dir
        )
        
        # Pre-populate cache
        cache_key = enricher._compute_cache_key(sample_event_occurrence.source_hash)
        cached_enrichment = EnrichmentData(
            description_ai="Occurrence description",
            tags=["occurrence-tag"]
        )
        enricher._save_to_cache(cache_key, cached_enrichment)
        
        # Enrich occurrence
        enriched_event = enricher.enrich_event(sample_event_occurrence)
        
        assert enriched_event.description_ai == "Occurrence description"
        assert "occurrence-tag" in enriched_event.tags
    
    def test_validate_list_field_valid_list(self, temp_cache_dir):
        """Test validation of valid list field."""
        enricher = AIEnricher(
            api_key="test-key",
            cache_dir=temp_cache_dir
        )
        
        valid_list = ["tag1", "tag2", "tag3"]
        result = enricher._validate_list_field(valid_list, "tags")
        
        assert result == ["tag1", "tag2", "tag3"]
    
    def test_validate_list_field_mixed_types(self, temp_cache_dir, capsys):
        """Test validation of list with mixed types."""
        enricher = AIEnricher(
            api_key="test-key",
            cache_dir=temp_cache_dir
        )
        
        mixed_list = ["tag1", 123, "tag2", None, "tag3"]
        result = enricher._validate_list_field(mixed_list, "tags")
        
        # Should filter out non-strings
        assert result == ["tag1", "tag2", "tag3"]
        
        # Should print warnings
        captured = capsys.readouterr()
        assert "Non-string item in tags: 123" in captured.out
        assert "Non-string item in tags: None" in captured.out
    
    def test_validate_list_field_not_list(self, temp_cache_dir, capsys):
        """Test validation when field is not a list."""
        enricher = AIEnricher(
            api_key="test-key",
            cache_dir=temp_cache_dir
        )
        
        not_list = "not a list"
        result = enricher._validate_list_field(not_list, "tags")
        
        # Should return empty list
        assert result == []
        
        # Should print warning
        captured = capsys.readouterr()
        assert "tags is not a list, using empty list" in captured.out
    
    def test_validate_bool_field_valid_bool(self, temp_cache_dir):
        """Test validation of valid boolean field."""
        enricher = AIEnricher(
            api_key="test-key",
            cache_dir=temp_cache_dir
        )
        
        assert enricher._validate_bool_field(True, "beginner_friendly") is True
        assert enricher._validate_bool_field(False, "family_friendly") is False
    
    def test_validate_bool_field_not_bool(self, temp_cache_dir, capsys):
        """Test validation when field is not a boolean."""
        enricher = AIEnricher(
            api_key="test-key",
            cache_dir=temp_cache_dir
        )
        
        # Test various non-boolean values
        assert enricher._validate_bool_field("true", "beginner_friendly") is False
        assert enricher._validate_bool_field(1, "family_friendly") is False
        assert enricher._validate_bool_field(None, "beginner_friendly") is False
        
        # Should print warnings
        captured = capsys.readouterr()
        assert "beginner_friendly is not a boolean, using False" in captured.out
        assert "family_friendly is not a boolean, using False" in captured.out
    
    def test_parse_response_with_validation_errors(self, temp_cache_dir, capsys):
        """Test parsing response with validation errors in metadata."""
        enricher = AIEnricher(
            api_key="test-key",
            cache_dir=temp_cache_dir
        )
        
        # Response with invalid metadata types
        invalid_response = {
            "description_ai": "Valid description",
            "summary_short": "Valid summary",
            "tags": "not a list",  # Should be list
            "occasion_tags": ["valid", 123, "tags"],  # Mixed types
            "beginner_friendly": "yes",  # Should be boolean
            "family_friendly": 1,  # Should be boolean
            "duration_minutes": "not a number"  # Should be int/float
        }
        
        response_text = json.dumps(invalid_response)
        enrichment = enricher._parse_response(response_text)
        
        # Should handle validation gracefully
        assert enrichment.description_ai == "Valid description"
        assert enrichment.summary_short == "Valid summary"
        assert enrichment.tags == []  # Invalid list becomes empty
        assert enrichment.occasion_tags == ["valid", "tags"]  # Filtered
        assert enrichment.beginner_friendly is False  # Invalid bool becomes False
        assert enrichment.family_friendly is False  # Invalid bool becomes False
        assert enrichment.duration_minutes is None  # Invalid duration becomes None
        
        # Should print validation warnings
        captured = capsys.readouterr()
        assert "tags is not a list" in captured.out
        assert "Non-string item in occasion_tags: 123" in captured.out
        assert "beginner_friendly is not a boolean" in captured.out
        assert "Invalid duration_minutes" in captured.out
    
    def test_parse_response_missing_fields(self, temp_cache_dir):
        """Test parsing response with missing optional fields."""
        enricher = AIEnricher(
            api_key="test-key",
            cache_dir=temp_cache_dir
        )
        
        # Minimal response with only required fields
        minimal_response = {
            "description_ai": "Enhanced description"
        }
        
        response_text = json.dumps(minimal_response)
        enrichment = enricher._parse_response(response_text)
        
        # Should use defaults for missing fields
        assert enrichment.description_ai == "Enhanced description"
        assert enrichment.summary_short is None
        assert enrichment.summary_medium is None
        assert enrichment.tags == []
        assert enrichment.occasion_tags == []
        assert enrichment.beginner_friendly is False
        assert enrichment.family_friendly is False
    
    def test_parse_response_empty_json(self, temp_cache_dir):
        """Test parsing empty JSON response."""
        enricher = AIEnricher(
            api_key="test-key",
            cache_dir=temp_cache_dir
        )
        
        response_text = "{}"
        enrichment = enricher._parse_response(response_text)
        
        # Should create enrichment with defaults
        assert enrichment.description_ai is None
        assert enrichment.tags == []
        assert enrichment.beginner_friendly is False
    
    def test_parse_response_age_validation(self, temp_cache_dir, capsys):
        """Test age field validation in response parsing."""
        enricher = AIEnricher(
            api_key="test-key",
            cache_dir=temp_cache_dir
        )
        
        # Test negative ages
        response_with_negative_ages = {
            "description_ai": "Test description",
            "age_min": -5,  # Negative age
            "age_max": -10  # Negative age
        }
        
        response_text = json.dumps(response_with_negative_ages)
        enrichment = enricher._parse_response(response_text)
        
        # Negative ages should be set to None
        assert enrichment.age_min is None
        assert enrichment.age_max is None
        
        # Should print warnings
        captured = capsys.readouterr()
        assert "age_min (-5) is negative" in captured.out
        assert "age_max (-10) is negative" in captured.out
    
    def test_parse_response_age_consistency(self, temp_cache_dir, capsys):
        """Test age consistency validation (min <= max)."""
        enricher = AIEnricher(
            api_key="test-key",
            cache_dir=temp_cache_dir
        )
        
        # Test age_min > age_max
        response_with_inconsistent_ages = {
            "description_ai": "Test description",
            "age_min": 25,
            "age_max": 18  # Max less than min
        }
        
        response_text = json.dumps(response_with_inconsistent_ages)
        enrichment = enricher._parse_response(response_text)
        
        # age_max should be set to None when inconsistent
        assert enrichment.age_min == 25
        assert enrichment.age_max is None
        
        # Should print warning
        captured = capsys.readouterr()
        assert "age_min (25) > age_max (18)" in captured.out
    
    def test_build_prompt_comprehensive(self, temp_cache_dir):
        """Test comprehensive prompt building with all ROSTA elements."""
        from src.enrich.prompts import build_enrichment_prompt
        
        # Create event with full details
        event = EventTemplate(
            event_template_id="test-event",
            provider_id="test-provider",
            title="Advanced Wine Tasting Masterclass",
            slug="wine-tasting-masterclass",
            description_clean="Join our sommelier for an in-depth exploration of premium wines. Learn tasting techniques, food pairing, and wine regions. Includes 8 wine tastings and artisanal cheese board.",
            source_hash="test-hash",
            category="Wine & Spirits",
            price_from=95.0,
            currency="GBP"
        )
        
        prompt = build_enrichment_prompt(event)
        
        # Check all required elements are present
        assert "Advanced Wine Tasting Masterclass" in prompt
        assert "Wine & Spirits" in prompt
        assert "£95.0 GBP" in prompt  # Note: price shows as 95.0, not 95
        assert "sommelier" in prompt
        assert "premium wines" in prompt
        
        # Check ROSTA tone guidelines
        assert "ROSTA BRAND TONE:" in prompt
        assert "Modern, confident, curated" in prompt
        assert "Premium but friendly" in prompt
        assert "Never invent facts" in prompt
        
        # Check metadata guidelines
        assert "METADATA GUIDELINES:" in prompt
        assert "tags: descriptive keywords" in prompt
        assert "occasion_tags: use cases" in prompt
        assert "beginner_friendly: no experience needed" in prompt
        
        # Check JSON format
        assert "OUTPUT FORMAT (JSON):" in prompt
        assert '"description_ai":' in prompt
        assert '"summary_short":' in prompt
        assert '"tags":' in prompt
        assert '"beginner_friendly":' in prompt
    
    def test_build_prompt_with_missing_fields(self, temp_cache_dir):
        """Test prompt building with missing optional fields."""
        from src.enrich.prompts import build_enrichment_prompt
        
        # Create minimal event
        event = EventTemplate(
            event_template_id="test-event",
            provider_id="test-provider",
            title="Basic Cooking Class",
            slug="basic-cooking",
            description_clean=None,  # No description
            source_hash="test-hash"
            # No category, price, etc.
        )
        
        prompt = build_enrichment_prompt(event)
        
        # Should handle missing fields gracefully
        assert "Basic Cooking Class" in prompt
        assert "Category: Unknown" in prompt
        assert "Price not specified" in prompt
        assert "Description: " in prompt  # Empty description
    
    def test_html_cleaning_integration(self, temp_cache_dir):
        """Test that HTML is properly cleaned before enrichment."""
        # This tests the integration with the normalizer's HTML cleaning
        from src.transform.normalizer import Normalizer
        
        normalizer = Normalizer()
        
        # Test HTML cleaning functionality
        html_description = "<p>Learn to make <b>authentic</b> pasta from <i>scratch</i>.</p><br><ul><li>Hand-rolling techniques</li><li>Sauce preparation</li></ul>"
        
        cleaned = normalizer._strip_html(html_description)
        
        # Should preserve meaningful content while removing HTML
        assert "Learn to make authentic pasta from scratch" in cleaned
        assert "Hand-rolling techniques" in cleaned
        assert "Sauce preparation" in cleaned
        assert "<p>" not in cleaned
        assert "<b>" not in cleaned
        assert "<ul>" not in cleaned
    
    def test_error_handling_network_timeout(self, temp_cache_dir):
        """Test handling of network timeout errors."""
        enricher = AIEnricher(
            api_key="test-key",
            cache_dir=temp_cache_dir,
            timeout=1  # Very short timeout
        )
        
        # Mock a timeout error
        with patch.object(enricher, '_call_llm') as mock_call_llm:
            mock_call_llm.side_effect = Exception("Request timeout")
            
            event = EventTemplate(
                event_template_id="test-event",
                provider_id="test-provider",
                title="Test Event",
                slug="test-event",
                description_clean="Test description",
                source_hash="test-hash"
            )
            
            # Should handle timeout gracefully
            result = enricher.enrich_event(event)
            
            # Should return original event unchanged
            assert result == event
            assert result.description_ai is None
    
    def test_error_handling_api_key_missing(self, temp_cache_dir):
        """Test handling when API key is missing during enrichment."""
        enricher = AIEnricher(
            api_key=None,  # No API key
            cache_dir=temp_cache_dir
        )
        
        event = EventTemplate(
            event_template_id="test-event",
            provider_id="test-provider",
            title="Test Event",
            slug="test-event",
            description_clean="Test description",
            source_hash="test-hash"
        )
        
        # Should handle missing API key gracefully
        result = enricher.enrich_event(event)
        
        # Should return original event unchanged
        assert result == event
        assert result.description_ai is None
    
    def test_cache_key_stability(self, temp_cache_dir):
        """Test that cache keys are stable across different enricher instances."""
        enricher1 = AIEnricher(
            api_key="test-key",
            cache_dir=temp_cache_dir,
            model="gpt-4o-mini",
            prompt_version="v1"
        )
        
        enricher2 = AIEnricher(
            api_key="different-key",  # Different API key
            cache_dir=temp_cache_dir,
            model="gpt-4o-mini",      # Same model
            prompt_version="v1"       # Same prompt version
        )
        
        # Same source hash should produce same cache key
        source_hash = "test-hash-123"
        key1 = enricher1._compute_cache_key(source_hash)
        key2 = enricher2._compute_cache_key(source_hash)
        
        assert key1 == key2  # Cache key should not depend on API key
    
    def test_cache_key_changes_with_model(self, temp_cache_dir):
        """Test that cache keys change when model or prompt version changes."""
        enricher1 = AIEnricher(
            api_key="test-key",
            cache_dir=temp_cache_dir,
            model="gpt-4o-mini",
            prompt_version="v1"
        )
        
        enricher2 = AIEnricher(
            api_key="test-key",
            cache_dir=temp_cache_dir,
            model="gpt-4o",  # Different model
            prompt_version="v1"
        )
        
        enricher3 = AIEnricher(
            api_key="test-key",
            cache_dir=temp_cache_dir,
            model="gpt-4o-mini",
            prompt_version="v2"  # Different prompt version
        )
        
        source_hash = "test-hash-123"
        key1 = enricher1._compute_cache_key(source_hash)
        key2 = enricher2._compute_cache_key(source_hash)
        key3 = enricher3._compute_cache_key(source_hash)
        
        # All keys should be different
        assert key1 != key2
        assert key1 != key3
        assert key2 != key3


class TestEnrichmentData:
    """Test suite for EnrichmentData class."""
    
    def test_init_with_defaults(self):
        """Test EnrichmentData initialization with defaults."""
        enrichment = EnrichmentData()
        
        assert enrichment.description_ai is None
        assert enrichment.tags == []
        assert enrichment.occasion_tags == []
        assert enrichment.skills_required == []
        assert enrichment.skills_created == []
        assert enrichment.family_friendly is False
        assert enrichment.beginner_friendly is False
    
    def test_init_with_values(self):
        """Test EnrichmentData initialization with values."""
        enrichment = EnrichmentData(
            description_ai="Test description",
            tags=["tag1", "tag2"],
            age_min=18,
            beginner_friendly=True
        )
        
        assert enrichment.description_ai == "Test description"
        assert enrichment.tags == ["tag1", "tag2"]
        assert enrichment.age_min == 18
        assert enrichment.beginner_friendly is True
