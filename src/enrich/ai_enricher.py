"""AI enrichment for event descriptions using LLM."""

import hashlib
import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any

from src.models.event_template import EventTemplate
from src.models.event_occurrence import EventOccurrence
from src.enrich.prompts import build_enrichment_prompt, build_system_message


class EnrichmentData:
    """Structured data extracted from AI enrichment."""
    
    def __init__(
        self,
        description_ai: str | None = None,
        summary_short: str | None = None,
        summary_medium: str | None = None,
        tags: list[str] | None = None,
        occasion_tags: list[str] | None = None,
        skills_required: list[str] | None = None,
        skills_created: list[str] | None = None,
        age_min: int | None = None,
        age_max: int | None = None,
        audience: str | None = None,
        family_friendly: bool = False,
        beginner_friendly: bool = False,
        duration_minutes: int | None = None,
        metadata: dict[str, Any] | None = None
    ):
        self.description_ai = description_ai
        self.summary_short = summary_short
        self.summary_medium = summary_medium
        self.tags = tags or []
        self.occasion_tags = occasion_tags or []
        self.skills_required = skills_required or []
        self.skills_created = skills_created or []
        self.age_min = age_min
        self.age_max = age_max
        self.audience = audience
        self.family_friendly = family_friendly
        self.beginner_friendly = beginner_friendly
        self.duration_minutes = duration_minutes
        self.metadata = metadata or {}


class AIEnricher:
    """Enriches event data using LLM for enhanced descriptions and metadata extraction.
    
    Integrates with OpenAI API to:
    - Generate AI-enhanced descriptions in ROSTA brand tone
    - Extract structured metadata (tags, skills, age ranges, audience info)
    - Cache enrichments to avoid redundant API calls
    - Handle errors and timeouts gracefully
    """
    
    def __init__(
        self,
        api_key: str | None = None,
        cache_dir: str = "cache/ai",
        model: str = "gpt-4o-mini",
        prompt_version: str = "v1",
        timeout: int = 30
    ):
        """Initialize AIEnricher.
        
        Args:
            api_key: OpenAI API key (reads from OPENAI_API_KEY env var if not provided)
            cache_dir: Directory path for cache storage (default: cache/ai)
            model: OpenAI model to use (default: gpt-4o-mini)
            prompt_version: Version identifier for prompt template (default: v1)
            timeout: Request timeout in seconds (default: 30)
        """
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.model = model
        self.prompt_version = prompt_version
        self.timeout = timeout
        
        # Lazy import and client initialization
        self._client = None
    
    @property
    def client(self):
        """Lazy initialization of OpenAI client."""
        if self._client is None:
            if not self.api_key:
                raise ValueError(
                    "OpenAI API key not found. Set OPENAI_API_KEY environment variable "
                    "or pass api_key parameter to AIEnricher."
                )
            
            try:
                from openai import OpenAI
                self._client = OpenAI(api_key=self.api_key, timeout=self.timeout)
            except ImportError:
                raise ImportError(
                    "OpenAI library not installed. Install with: pip install openai"
                )
        
        return self._client
    
    def enrich_event(
        self, 
        event: EventTemplate | EventOccurrence
    ) -> EventTemplate | EventOccurrence:
        """Enrich event with AI-generated content and metadata.
        
        Checks cache before calling LLM. If source_hash unchanged and cached
        enrichment exists, uses cached data. Otherwise, calls LLM and caches result.
        
        Args:
            event: Event record to enrich (EventTemplate or EventOccurrence)
            
        Returns:
            Updated event record with AI-enriched fields
        """
        # Skip if no description to enrich
        if not event.description_clean:
            return event
        
        # Compute cache key from source_hash + prompt_version + model
        cache_key = self._compute_cache_key(event.source_hash)
        
        # Try to load from cache
        cached_enrichment = self._load_from_cache(cache_key)
        if cached_enrichment:
            return self._apply_enrichment(event, cached_enrichment)
        
        # Cache miss - call LLM
        try:
            enrichment = self._call_llm(event)
            
            # Store in cache
            self._save_to_cache(cache_key, enrichment)
            
            # Apply enrichment to event
            return self._apply_enrichment(event, enrichment)
            
        except Exception as e:
            # Log error but don't fail - preserve original descriptions
            print(f"AI enrichment failed for event {event.title}: {e}")
            return event
    
    def _compute_cache_key(self, source_hash: str | None) -> str:
        """Compute cache key from source_hash, prompt_version, and model.
        
        Args:
            source_hash: Hash of source event data
            
        Returns:
            Cache key string
        """
        if not source_hash:
            # Fallback to timestamp-based key if no source_hash
            source_hash = datetime.now().isoformat()
        
        composite = f"{source_hash}:{self.prompt_version}:{self.model}"
        return hashlib.sha256(composite.encode()).hexdigest()[:16]
    
    def _get_cache_path(self, cache_key: str) -> Path:
        """Get cache file path for cache key.
        
        Args:
            cache_key: Cache key string
            
        Returns:
            Path to cache file
        """
        return self.cache_dir / f"{cache_key}.json"
    
    def _load_from_cache(self, cache_key: str) -> EnrichmentData | None:
        """Load enrichment data from cache.
        
        Args:
            cache_key: Cache key string
            
        Returns:
            EnrichmentData if found in cache, None otherwise
        """
        cache_path = self._get_cache_path(cache_key)
        
        if not cache_path.exists():
            return None
        
        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Reconstruct EnrichmentData from cached data
            return EnrichmentData(
                description_ai=data.get("description_ai"),
                summary_short=data.get("summary_short"),
                summary_medium=data.get("summary_medium"),
                tags=data.get("tags", []),
                occasion_tags=data.get("occasion_tags", []),
                skills_required=data.get("skills_required", []),
                skills_created=data.get("skills_created", []),
                age_min=data.get("age_min"),
                age_max=data.get("age_max"),
                audience=data.get("audience"),
                family_friendly=data.get("family_friendly", False),
                beginner_friendly=data.get("beginner_friendly", False),
                duration_minutes=data.get("duration_minutes"),
                metadata=data.get("metadata", {})
            )
        except (json.JSONDecodeError, IOError, KeyError):
            # Cache file corrupted or invalid - ignore and re-enrich
            return None
    
    def _save_to_cache(self, cache_key: str, enrichment: EnrichmentData) -> None:
        """Save enrichment data to cache.
        
        Args:
            cache_key: Cache key string
            enrichment: EnrichmentData to cache
        """
        cache_path = self._get_cache_path(cache_key)
        
        try:
            cache_data = {
                "description_ai": enrichment.description_ai,
                "summary_short": enrichment.summary_short,
                "summary_medium": enrichment.summary_medium,
                "tags": enrichment.tags,
                "occasion_tags": enrichment.occasion_tags,
                "skills_required": enrichment.skills_required,
                "skills_created": enrichment.skills_created,
                "age_min": enrichment.age_min,
                "age_max": enrichment.age_max,
                "audience": enrichment.audience,
                "family_friendly": enrichment.family_friendly,
                "beginner_friendly": enrichment.beginner_friendly,
                "duration_minutes": enrichment.duration_minutes,
                "metadata": enrichment.metadata,
                "cached_at": datetime.now().isoformat(),
                "prompt_version": self.prompt_version,
                "model": self.model
            }
            
            with open(cache_path, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, indent=2)
        except IOError as e:
            # Cache write failed - log but don't fail the enrichment
            print(f"Failed to write cache: {e}")
    
    def _call_llm(self, event: EventTemplate | EventOccurrence) -> EnrichmentData:
        """Call LLM to enrich event data.
        
        Args:
            event: Event record to enrich
            
        Returns:
            EnrichmentData with AI-generated content
        """
        # Build prompt using prompts module
        prompt = build_enrichment_prompt(event)
        system_message = build_system_message()
        
        # Call OpenAI API
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {
                    "role": "system",
                    "content": system_message
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            response_format={"type": "json_object"},
            temperature=0.7,
            max_tokens=1000
        )
        
        # Parse response
        response_text = response.choices[0].message.content
        return self._parse_response(response_text)
    
    def _parse_response(self, response_text: str) -> EnrichmentData:
        """Parse structured JSON response from LLM.
        
        Extracts all metadata fields and validates for consistency:
        - age_min <= age_max
        - summary_short ~50 chars
        - summary_medium ~150 chars
        - All list fields are valid lists
        - Boolean fields are valid booleans
        
        Args:
            response_text: JSON response from LLM
            
        Returns:
            EnrichmentData with parsed and validated content
            
        Raises:
            ValueError: If JSON parsing fails or validation errors occur
        """
        try:
            data = json.loads(response_text)
        except json.JSONDecodeError as e:
            raise ValueError(f"Failed to parse LLM response as JSON: {e}")
        
        # Extract and validate age fields
        age_min = data.get("age_min")
        age_max = data.get("age_max")
        
        # Validate age values are positive first
        if age_min is not None and age_min < 0:
            print(f"Warning: age_min ({age_min}) is negative, setting to None")
            age_min = None
        
        if age_max is not None and age_max < 0:
            print(f"Warning: age_max ({age_max}) is negative, setting to None")
            age_max = None
        
        # Then validate age_min and age_max consistency
        if age_min is not None and age_max is not None:
            if age_min > age_max:
                print(f"Warning: age_min ({age_min}) > age_max ({age_max}), setting age_max to None")
                age_max = None
        
        # Extract and validate list fields
        tags = self._validate_list_field(data.get("tags", []), "tags")
        occasion_tags = self._validate_list_field(data.get("occasion_tags", []), "occasion_tags")
        skills_required = self._validate_list_field(data.get("skills_required", []), "skills_required")
        skills_created = self._validate_list_field(data.get("skills_created", []), "skills_created")
        
        # Extract and validate summary fields
        summary_short = data.get("summary_short")
        summary_medium = data.get("summary_medium")
        
        # Validate summary lengths (warn if significantly off target)
        if summary_short and len(summary_short) > 80:
            print(f"Warning: summary_short is {len(summary_short)} chars (target ~50)")
        
        if summary_medium and len(summary_medium) > 200:
            print(f"Warning: summary_medium is {len(summary_medium)} chars (target ~150)")
        
        # Extract and validate boolean fields
        family_friendly = self._validate_bool_field(data.get("family_friendly", False), "family_friendly")
        beginner_friendly = self._validate_bool_field(data.get("beginner_friendly", False), "beginner_friendly")
        
        # Extract and validate duration
        duration_minutes = data.get("duration_minutes")
        if duration_minutes is not None:
            if not isinstance(duration_minutes, (int, float)) or duration_minutes < 0:
                print(f"Warning: Invalid duration_minutes ({duration_minutes}), setting to None")
                duration_minutes = None
            elif isinstance(duration_minutes, float):
                duration_minutes = int(duration_minutes)
        
        return EnrichmentData(
            description_ai=data.get("description_ai"),
            summary_short=summary_short,
            summary_medium=summary_medium,
            tags=tags,
            occasion_tags=occasion_tags,
            skills_required=skills_required,
            skills_created=skills_created,
            age_min=age_min,
            age_max=age_max,
            audience=data.get("audience"),
            family_friendly=family_friendly,
            beginner_friendly=beginner_friendly,
            duration_minutes=duration_minutes
        )
    
    def _validate_list_field(self, value: any, field_name: str) -> list[str]:
        """Validate that a field is a list of strings.
        
        Args:
            value: Value to validate
            field_name: Name of field for error messages
            
        Returns:
            Validated list of strings (empty list if invalid)
        """
        if not isinstance(value, list):
            print(f"Warning: {field_name} is not a list, using empty list")
            return []
        
        # Filter out non-string values
        validated = []
        for item in value:
            if isinstance(item, str):
                validated.append(item)
            else:
                print(f"Warning: Non-string item in {field_name}: {item}")
        
        return validated
    
    def _validate_bool_field(self, value: any, field_name: str) -> bool:
        """Validate that a field is a boolean.
        
        Args:
            value: Value to validate
            field_name: Name of field for error messages
            
        Returns:
            Validated boolean (False if invalid)
        """
        if not isinstance(value, bool):
            print(f"Warning: {field_name} is not a boolean, using False")
            return False
        
        return value
    
    def _apply_enrichment(
        self,
        event: EventTemplate | EventOccurrence,
        enrichment: EnrichmentData
    ) -> EventTemplate | EventOccurrence:
        """Apply enrichment data to event record.
        
        Args:
            event: Event record to update
            enrichment: EnrichmentData to apply
            
        Returns:
            Updated event record
        """
        # Apply AI-generated descriptions
        if enrichment.description_ai:
            event.description_ai = enrichment.description_ai
        
        if enrichment.summary_short:
            event.summary_short = enrichment.summary_short
        
        if enrichment.summary_medium:
            event.summary_medium = enrichment.summary_medium
        
        # Apply tags (merge with existing, avoid duplicates)
        if enrichment.tags:
            existing_tags = set(event.tags)
            event.tags = list(existing_tags | set(enrichment.tags))
        
        if enrichment.occasion_tags:
            existing_occasion_tags = set(getattr(event, 'occasion_tags', []))
            event.occasion_tags = list(existing_occasion_tags | set(enrichment.occasion_tags))
        
        if enrichment.skills_required:
            existing_skills_required = set(event.skills_required)
            event.skills_required = list(existing_skills_required | set(enrichment.skills_required))
        
        if enrichment.skills_created:
            existing_skills_created = set(event.skills_created)
            event.skills_created = list(existing_skills_created | set(enrichment.skills_created))
        
        # Apply audience metadata (only if not already set)
        if enrichment.age_min is not None and event.age_min is None:
            event.age_min = enrichment.age_min
        
        if enrichment.age_max is not None and event.age_max is None:
            event.age_max = enrichment.age_max
        
        if enrichment.audience and not getattr(event, 'audience', None):
            event.audience = enrichment.audience
        
        # Apply boolean flags (only for EventTemplate, not EventOccurrence)
        if hasattr(event, 'family_friendly'):
            if enrichment.family_friendly and not event.family_friendly:
                event.family_friendly = enrichment.family_friendly
        
        if hasattr(event, 'beginner_friendly'):
            if enrichment.beginner_friendly and not event.beginner_friendly:
                event.beginner_friendly = enrichment.beginner_friendly
        
        # Apply duration (only if not already set and attribute exists)
        if enrichment.duration_minutes is not None and hasattr(event, 'duration_minutes'):
            if not getattr(event, 'duration_minutes', None):
                event.duration_minutes = enrichment.duration_minutes
        
        return event
