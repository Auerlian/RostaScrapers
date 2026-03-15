"""Tests for AIEnricher metadata validation in _parse_response."""

import json
import pytest

from src.enrich.ai_enricher import AIEnricher


@pytest.fixture
def enricher(tmp_path):
    """Create AIEnricher instance for testing."""
    cache_dir = tmp_path / "cache" / "ai"
    return AIEnricher(
        api_key="test-key",
        cache_dir=str(cache_dir)
    )


class TestMetadataValidation:
    """Test suite for metadata validation in _parse_response."""
    
    def test_validate_age_min_less_than_age_max(self, enricher, capsys):
        """Test that age_min > age_max is handled gracefully."""
        response = {
            "description_ai": "Test description",
            "age_min": 25,
            "age_max": 18  # Invalid: max < min
        }
        
        enrichment = enricher._parse_response(json.dumps(response))
        
        # age_max should be set to None
        assert enrichment.age_min == 25
        assert enrichment.age_max is None
        
        # Should log warning
        captured = capsys.readouterr()
        assert "age_min (25) > age_max (18)" in captured.out
    
    def test_validate_negative_age_min(self, enricher, capsys):
        """Test that negative age_min is handled gracefully."""
        response = {
            "description_ai": "Test description",
            "age_min": -5,
            "age_max": 18
        }
        
        enrichment = enricher._parse_response(json.dumps(response))
        
        # age_min should be set to None
        assert enrichment.age_min is None
        assert enrichment.age_max == 18
        
        # Should log warning
        captured = capsys.readouterr()
        assert "age_min (-5) is negative" in captured.out
    
    def test_validate_negative_age_max(self, enricher, capsys):
        """Test that negative age_max is handled gracefully."""
        response = {
            "description_ai": "Test description",
            "age_min": 18,
            "age_max": -10
        }
        
        enrichment = enricher._parse_response(json.dumps(response))
        
        # age_max should be set to None
        assert enrichment.age_min == 18
        assert enrichment.age_max is None
        
        # Should log warning
        captured = capsys.readouterr()
        assert "age_max (-10) is negative" in captured.out
    
    def test_validate_valid_age_range(self, enricher):
        """Test that valid age range is preserved."""
        response = {
            "description_ai": "Test description",
            "age_min": 18,
            "age_max": 65
        }
        
        enrichment = enricher._parse_response(json.dumps(response))
        
        assert enrichment.age_min == 18
        assert enrichment.age_max == 65
    
    def test_validate_summary_short_length(self, enricher, capsys):
        """Test that overly long summary_short triggers warning."""
        long_summary = "A" * 100  # Much longer than ~50 chars
        response = {
            "description_ai": "Test description",
            "summary_short": long_summary
        }
        
        enrichment = enricher._parse_response(json.dumps(response))
        
        # Should still accept the value
        assert enrichment.summary_short == long_summary
        
        # But should log warning
        captured = capsys.readouterr()
        assert "summary_short is 100 chars" in captured.out
    
    def test_validate_summary_medium_length(self, enricher, capsys):
        """Test that overly long summary_medium triggers warning."""
        long_summary = "B" * 250  # Much longer than ~150 chars
        response = {
            "description_ai": "Test description",
            "summary_medium": long_summary
        }
        
        enrichment = enricher._parse_response(json.dumps(response))
        
        # Should still accept the value
        assert enrichment.summary_medium == long_summary
        
        # But should log warning
        captured = capsys.readouterr()
        assert "summary_medium is 250 chars" in captured.out
    
    def test_validate_tags_not_list(self, enricher, capsys):
        """Test that non-list tags field is handled gracefully."""
        response = {
            "description_ai": "Test description",
            "tags": "not-a-list"  # Invalid: should be list
        }
        
        enrichment = enricher._parse_response(json.dumps(response))
        
        # Should default to empty list
        assert enrichment.tags == []
        
        # Should log warning
        captured = capsys.readouterr()
        assert "tags is not a list" in captured.out
    
    def test_validate_tags_with_non_string_items(self, enricher, capsys):
        """Test that non-string items in tags are filtered out."""
        response = {
            "description_ai": "Test description",
            "tags": ["valid-tag", 123, "another-tag", None, True]
        }
        
        enrichment = enricher._parse_response(json.dumps(response))
        
        # Should only keep string items
        assert enrichment.tags == ["valid-tag", "another-tag"]
        
        # Should log warnings for non-string items
        captured = capsys.readouterr()
        assert "Non-string item in tags: 123" in captured.out
    
    def test_validate_occasion_tags_list(self, enricher, capsys):
        """Test that occasion_tags validation works."""
        response = {
            "description_ai": "Test description",
            "occasion_tags": {"not": "a list"}  # Invalid
        }
        
        enrichment = enricher._parse_response(json.dumps(response))
        
        assert enrichment.occasion_tags == []
        
        captured = capsys.readouterr()
        assert "occasion_tags is not a list" in captured.out
    
    def test_validate_skills_required_list(self, enricher, capsys):
        """Test that skills_required validation works."""
        response = {
            "description_ai": "Test description",
            "skills_required": ["valid-skill", 456]
        }
        
        enrichment = enricher._parse_response(json.dumps(response))
        
        assert enrichment.skills_required == ["valid-skill"]
        
        captured = capsys.readouterr()
        assert "Non-string item in skills_required: 456" in captured.out
    
    def test_validate_skills_created_list(self, enricher, capsys):
        """Test that skills_created validation works."""
        response = {
            "description_ai": "Test description",
            "skills_created": [789]
        }
        
        enrichment = enricher._parse_response(json.dumps(response))
        
        assert enrichment.skills_created == []
        
        captured = capsys.readouterr()
        assert "Non-string item in skills_created: 789" in captured.out
    
    def test_validate_family_friendly_not_bool(self, enricher, capsys):
        """Test that non-boolean family_friendly is handled gracefully."""
        response = {
            "description_ai": "Test description",
            "family_friendly": "yes"  # Invalid: should be bool
        }
        
        enrichment = enricher._parse_response(json.dumps(response))
        
        # Should default to False
        assert enrichment.family_friendly is False
        
        # Should log warning
        captured = capsys.readouterr()
        assert "family_friendly is not a boolean" in captured.out
    
    def test_validate_beginner_friendly_not_bool(self, enricher, capsys):
        """Test that non-boolean beginner_friendly is handled gracefully."""
        response = {
            "description_ai": "Test description",
            "beginner_friendly": 1  # Invalid: should be bool
        }
        
        enrichment = enricher._parse_response(json.dumps(response))
        
        # Should default to False
        assert enrichment.beginner_friendly is False
        
        # Should log warning
        captured = capsys.readouterr()
        assert "beginner_friendly is not a boolean" in captured.out
    
    def test_validate_duration_negative(self, enricher, capsys):
        """Test that negative duration is handled gracefully."""
        response = {
            "description_ai": "Test description",
            "duration_minutes": -30
        }
        
        enrichment = enricher._parse_response(json.dumps(response))
        
        # Should be set to None
        assert enrichment.duration_minutes is None
        
        # Should log warning
        captured = capsys.readouterr()
        assert "Invalid duration_minutes (-30)" in captured.out
    
    def test_validate_duration_non_numeric(self, enricher, capsys):
        """Test that non-numeric duration is handled gracefully."""
        response = {
            "description_ai": "Test description",
            "duration_minutes": "two hours"
        }
        
        enrichment = enricher._parse_response(json.dumps(response))
        
        # Should be set to None
        assert enrichment.duration_minutes is None
        
        # Should log warning
        captured = capsys.readouterr()
        assert "Invalid duration_minutes (two hours)" in captured.out
    
    def test_validate_duration_float_converted_to_int(self, enricher):
        """Test that float duration is converted to int."""
        response = {
            "description_ai": "Test description",
            "duration_minutes": 120.5
        }
        
        enrichment = enricher._parse_response(json.dumps(response))
        
        # Should be converted to int
        assert enrichment.duration_minutes == 120
        assert isinstance(enrichment.duration_minutes, int)
    
    def test_validate_all_fields_valid(self, enricher):
        """Test that all valid fields are preserved correctly."""
        response = {
            "description_ai": "Enhanced description",
            "summary_short": "Short summary",
            "summary_medium": "Medium length summary with more details",
            "tags": ["tag1", "tag2"],
            "occasion_tags": ["date-night"],
            "skills_required": ["none"],
            "skills_created": ["pasta-making"],
            "age_min": 18,
            "age_max": 65,
            "audience": "adults",
            "family_friendly": False,
            "beginner_friendly": True,
            "duration_minutes": 120
        }
        
        enrichment = enricher._parse_response(json.dumps(response))
        
        assert enrichment.description_ai == "Enhanced description"
        assert enrichment.summary_short == "Short summary"
        assert enrichment.summary_medium == "Medium length summary with more details"
        assert enrichment.tags == ["tag1", "tag2"]
        assert enrichment.occasion_tags == ["date-night"]
        assert enrichment.skills_required == ["none"]
        assert enrichment.skills_created == ["pasta-making"]
        assert enrichment.age_min == 18
        assert enrichment.age_max == 65
        assert enrichment.audience == "adults"
        assert enrichment.family_friendly is False
        assert enrichment.beginner_friendly is True
        assert enrichment.duration_minutes == 120
    
    def test_validate_missing_optional_fields(self, enricher):
        """Test that missing optional fields are handled gracefully."""
        response = {
            "description_ai": "Minimal description"
            # All other fields missing
        }
        
        enrichment = enricher._parse_response(json.dumps(response))
        
        assert enrichment.description_ai == "Minimal description"
        assert enrichment.summary_short is None
        assert enrichment.summary_medium is None
        assert enrichment.tags == []
        assert enrichment.occasion_tags == []
        assert enrichment.skills_required == []
        assert enrichment.skills_created == []
        assert enrichment.age_min is None
        assert enrichment.age_max is None
        assert enrichment.audience is None
        assert enrichment.family_friendly is False
        assert enrichment.beginner_friendly is False
        assert enrichment.duration_minutes is None
