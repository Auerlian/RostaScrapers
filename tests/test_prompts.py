"""Tests for prompt templates module."""

import pytest
from src.enrich.prompts import (
    build_enrichment_prompt,
    build_system_message,
    get_tone_guidelines,
    get_metadata_guidelines,
    get_json_output_format,
    ROSTA_TONE_GUIDELINES,
    METADATA_GUIDELINES,
    JSON_OUTPUT_FORMAT
)
from src.models.event_template import EventTemplate
from src.models.event_occurrence import EventOccurrence


class TestPromptTemplates:
    """Test prompt template functions."""
    
    def test_build_enrichment_prompt_with_template(self):
        """Test building enrichment prompt for EventTemplate."""
        event = EventTemplate(
            event_template_id="event-template-test-pasta",
            provider_id="provider-test",
            title="Pasta Making Workshop",
            slug="pasta-making-workshop",
            description_clean="Learn to make fresh pasta from scratch.",
            category="Cooking",
            price_from=75.0,
            currency="GBP",
            tags=[],
            occasion_tags=[],
            skills_required=[],
            skills_created=[],
            family_friendly=False,
            beginner_friendly=True,
            image_urls=[],
            status="active",
            source_hash="test123",
            record_hash=None
        )
        
        prompt = build_enrichment_prompt(event)
        
        # Check prompt contains event details
        assert "Pasta Making Workshop" in prompt
        assert "Cooking" in prompt
        assert "£75.0 GBP" in prompt
        assert "Learn to make fresh pasta from scratch." in prompt
        
        # Check prompt contains ROSTA tone guidelines
        assert "ROSTA" in prompt
        assert "Modern, confident, curated" in prompt
        assert "Premium but friendly" in prompt
        assert "Never invent facts not in source data" in prompt
        
        # Check prompt contains task instructions
        assert "Rewrite the description in ROSTA tone" in prompt
        assert "Create a short summary" in prompt
        assert "Create a medium summary" in prompt
        assert "Extract structured metadata" in prompt
        
        # Check prompt contains JSON output format
        assert "OUTPUT FORMAT (JSON)" in prompt
        assert "description_ai" in prompt
        assert "summary_short" in prompt
        assert "summary_medium" in prompt
        assert "tags" in prompt
        assert "occasion_tags" in prompt
        
        # Check prompt contains metadata guidelines
        assert "METADATA GUIDELINES" in prompt
        assert "skills_required" in prompt
        assert "skills_created" in prompt
        assert "family_friendly" in prompt
        assert "beginner_friendly" in prompt
    
    def test_build_enrichment_prompt_with_occurrence(self):
        """Test building enrichment prompt for EventOccurrence."""
        event = EventOccurrence(
            event_id="event-test-pasta-001",
            provider_id="provider-test",
            title="Pasta Making Class",
            description_clean="Join us for a hands-on pasta making experience.",
            price=85.0,
            currency="GBP",
            tags=[],
            skills_required=[],
            skills_created=[],
            status="active",
            source_hash="test456",
            record_hash=None
        )
        
        prompt = build_enrichment_prompt(event)
        
        # Check prompt contains event details
        assert "Pasta Making Class" in prompt
        assert "£85.0 GBP" in prompt
        assert "Join us for a hands-on pasta making experience." in prompt
        
        # Check prompt contains ROSTA guidelines
        assert "ROSTA" in prompt
        assert "Modern, confident, curated" in prompt
    
    def test_build_enrichment_prompt_without_price(self):
        """Test building prompt when price is not specified."""
        event = EventTemplate(
            event_template_id="event-template-test-free",
            provider_id="provider-test",
            title="Free Tasting Event",
            slug="free-tasting-event",
            description_clean="Sample our products.",
            price_from=None,
            currency="GBP",
            tags=[],
            occasion_tags=[],
            skills_required=[],
            skills_created=[],
            family_friendly=True,
            beginner_friendly=True,
            image_urls=[],
            status="active",
            source_hash="test789",
            record_hash=None
        )
        
        prompt = build_enrichment_prompt(event)
        
        # Check prompt handles missing price
        assert "Price not specified" in prompt
        assert "Free Tasting Event" in prompt
    
    def test_build_enrichment_prompt_without_category(self):
        """Test building prompt when category is not specified."""
        event = EventTemplate(
            event_template_id="event-template-test-nocategory",
            provider_id="provider-test",
            title="Mystery Event",
            slug="mystery-event",
            description_clean="A surprise experience.",
            category=None,
            price_from=50.0,
            currency="GBP",
            tags=[],
            occasion_tags=[],
            skills_required=[],
            skills_created=[],
            family_friendly=False,
            beginner_friendly=False,
            image_urls=[],
            status="active",
            source_hash="test999",
            record_hash=None
        )
        
        prompt = build_enrichment_prompt(event)
        
        # Check prompt handles missing category
        assert "Category: Unknown" in prompt
        assert "Mystery Event" in prompt
    
    def test_build_system_message(self):
        """Test building system message for LLM."""
        message = build_system_message()
        
        # Check system message contains key elements
        assert "content editor" in message
        assert "ROSTA" in message
        assert "premium lifestyle events platform" in message
        assert "enhance event descriptions" in message
        assert "extract structured metadata" in message
    
    def test_get_tone_guidelines(self):
        """Test getting ROSTA tone guidelines."""
        guidelines = get_tone_guidelines()
        
        # Check guidelines contain key elements
        assert "Modern, confident, curated" in guidelines
        assert "Premium but friendly" in guidelines
        assert "Short, punchy, clear" in guidelines
        assert "Never invent facts" in guidelines
    
    def test_get_metadata_guidelines(self):
        """Test getting metadata extraction guidelines."""
        guidelines = get_metadata_guidelines()
        
        # Check guidelines contain key elements
        assert "tags:" in guidelines
        assert "occasion_tags:" in guidelines
        assert "skills_required:" in guidelines
        assert "skills_created:" in guidelines
        assert "family_friendly:" in guidelines
        assert "beginner_friendly:" in guidelines
        assert "Only extract metadata that is clearly stated" in guidelines
    
    def test_get_json_output_format(self):
        """Test getting JSON output format template."""
        format_template = get_json_output_format()
        
        # Check format contains all required fields
        assert "description_ai" in format_template
        assert "summary_short" in format_template
        assert "summary_medium" in format_template
        assert "tags" in format_template
        assert "occasion_tags" in format_template
        assert "skills_required" in format_template
        assert "skills_created" in format_template
        assert "age_min" in format_template
        assert "age_max" in format_template
        assert "audience" in format_template
        assert "family_friendly" in format_template
        assert "beginner_friendly" in format_template
        assert "duration_minutes" in format_template
    
    def test_constants_are_strings(self):
        """Test that all constants are properly defined as strings."""
        assert isinstance(ROSTA_TONE_GUIDELINES, str)
        assert isinstance(METADATA_GUIDELINES, str)
        assert isinstance(JSON_OUTPUT_FORMAT, str)
        
        # Check constants are not empty
        assert len(ROSTA_TONE_GUIDELINES) > 0
        assert len(METADATA_GUIDELINES) > 0
        assert len(JSON_OUTPUT_FORMAT) > 0
    
    def test_prompt_includes_all_sections(self):
        """Test that prompt includes all required sections."""
        event = EventTemplate(
            event_template_id="event-template-test-complete",
            provider_id="provider-test",
            title="Complete Event",
            slug="complete-event",
            description_clean="A complete event description.",
            category="Test",
            price_from=100.0,
            currency="GBP",
            tags=[],
            occasion_tags=[],
            skills_required=[],
            skills_created=[],
            family_friendly=True,
            beginner_friendly=True,
            image_urls=[],
            status="active",
            source_hash="complete123",
            record_hash=None
        )
        
        prompt = build_enrichment_prompt(event)
        
        # Check all major sections are present
        assert "EVENT DETAILS:" in prompt
        assert "ROSTA BRAND TONE:" in prompt
        assert "TASK:" in prompt
        assert "OUTPUT FORMAT (JSON):" in prompt
        assert "METADATA GUIDELINES:" in prompt
