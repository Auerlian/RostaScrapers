# AI Enricher Documentation

## Overview

The `AIEnricher` class provides AI-powered enrichment for event descriptions using OpenAI's GPT models. It enhances event descriptions with ROSTA brand tone and extracts structured metadata.

## Features

- **AI-Enhanced Descriptions**: Rewrites event descriptions in ROSTA brand tone (modern, confident, curated, minimal)
- **Structured Metadata Extraction**: Extracts tags, skills, age ranges, audience info, and more
- **Intelligent Caching**: Caches enrichments keyed by source_hash + prompt_version + model to avoid redundant API calls
- **Error Handling**: Handles LLM errors and timeouts gracefully without failing the pipeline
- **Lazy Loading**: OpenAI client is only initialized when needed

## Installation

The AIEnricher requires the OpenAI Python library:

```bash
pip install openai
```

## Configuration

Set your OpenAI API key as an environment variable:

```bash
export OPENAI_API_KEY="your-api-key-here"
```

Or create a `.env` file:

```
OPENAI_API_KEY=your-api-key-here
```

## Basic Usage

```python
from src.enrich.ai_enricher import AIEnricher
from src.models.event_template import EventTemplate

# Initialize enricher
enricher = AIEnricher(
    api_key="your-api-key",  # Optional if OPENAI_API_KEY env var is set
    cache_dir="cache/ai",     # Default cache directory
    model="gpt-4o-mini",      # OpenAI model to use
    prompt_version="v1",      # Prompt version for cache invalidation
    timeout=30                # Request timeout in seconds
)

# Create an event
event = EventTemplate(
    event_template_id="event-template-pasta-class",
    provider_id="provider-test",
    title="Pasta Making Class",
    slug="pasta-making-class",
    description_clean="Learn to make fresh pasta from scratch. Hands-on class with expert chef.",
    source_hash="abc123",
    category="Cooking",
    price_from=68.0
)

# Enrich the event
enriched_event = enricher.enrich_event(event)

# Access enriched fields
print(enriched_event.description_ai)  # AI-enhanced description
print(enriched_event.summary_short)   # Short summary (~50 chars)
print(enriched_event.summary_medium)  # Medium summary (~150 chars)
print(enriched_event.tags)            # Extracted tags
print(enriched_event.skills_created)  # Skills learned
```

## Enrichment Output

The enricher adds the following fields to events:

### Description Fields
- `description_ai`: AI-enhanced description in ROSTA tone (2-3 sentences, ~150 chars)
- `summary_short`: Short punchy summary (~50 chars for cards)
- `summary_medium`: Medium-length summary (~150 chars for listings)

### Metadata Fields
- `tags`: Descriptive keywords (e.g., "hands-on", "italian", "beginner-friendly")
- `occasion_tags`: Use cases (e.g., "date-night", "team-building", "family-outing")
- `skills_required`: Prerequisites (e.g., "none", "basic-cooking")
- `skills_created`: What you'll learn (e.g., "pasta-making", "knife-skills")

### Audience Fields
- `age_min`: Minimum age restriction
- `age_max`: Maximum age restriction
- `audience`: Target audience (e.g., "adults", "families", "children")
- `family_friendly`: Boolean indicating if suitable for children
- `beginner_friendly`: Boolean indicating if no experience needed
- `duration_minutes`: Estimated duration

## ROSTA Brand Tone

The enricher follows ROSTA brand tone guidelines:

- **Modern, confident, curated, clean, minimal**
- **Premium but friendly, lifestyle-oriented**
- **Short, punchy, clear, plain-English**
- **Avoid dense corporate phrasing, jargon, overexplaining**
- **Emotionally immediate, low-friction**
- **Never invent facts not in source data**

## Caching

The enricher implements intelligent caching to reduce API costs and improve performance:

### Cache Key
Cache keys are computed from:
- `source_hash`: Hash of source event data
- `prompt_version`: Version identifier for prompt template
- `model`: OpenAI model identifier

### Cache Behavior
- **Cache Hit**: If source_hash unchanged and cached enrichment exists, uses cached data
- **Cache Miss**: Calls LLM and caches the result
- **Cache Invalidation**: Automatically invalidated when source_hash changes

### Cache Location
Enrichments are cached in `cache/ai/` directory as JSON files named by cache key.

## Error Handling

The enricher handles errors gracefully:

- **Missing API Key**: Raises `ValueError` with clear message
- **LLM API Errors**: Logs error and returns original event unchanged
- **Timeout**: Respects timeout parameter and handles gracefully
- **Invalid JSON Response**: Raises `ValueError` with parse error details
- **Missing Description**: Skips enrichment if `description_clean` is None

## Advanced Usage

### Custom Model Configuration

```python
enricher = AIEnricher(
    model="gpt-4",           # Use GPT-4 for higher quality
    prompt_version="v2",     # Use custom prompt version
    timeout=60               # Longer timeout for complex events
)
```

### Batch Enrichment

```python
events = [event1, event2, event3]
enriched_events = [enricher.enrich_event(event) for event in events]
```

### Conditional Enrichment

```python
# Only enrich if description exists and is long enough
if event.description_clean and len(event.description_clean) > 50:
    enriched_event = enricher.enrich_event(event)
```

### Preserving Existing Data

The enricher preserves existing event data:
- Tags are merged (not replaced)
- Existing age restrictions are preserved
- Existing audience values are not overwritten

## Integration with Pipeline

The AIEnricher is designed to integrate with the pipeline orchestrator:

```python
from src.enrich.ai_enricher import AIEnricher
from src.storage.store import CanonicalStore

# Initialize components
enricher = AIEnricher()
store = CanonicalStore()

# Load events from store
events = store.load_events()

# Enrich all events
enriched_events = []
for event in events:
    try:
        enriched_event = enricher.enrich_event(event)
        enriched_events.append(enriched_event)
    except Exception as e:
        print(f"Failed to enrich event {event.title}: {e}")
        enriched_events.append(event)  # Keep original on error

# Save enriched events
store.save_events(enriched_events)
```

## Performance Considerations

- **First Run**: Calls LLM for all events (slow, ~5 events/minute)
- **Subsequent Runs**: Uses cache for unchanged events (fast, ~1000 events/second)
- **API Costs**: ~$0.001 per event with gpt-4o-mini
- **Cache Size**: ~2KB per cached enrichment

## Troubleshooting

### "OpenAI API key not found"
Set the `OPENAI_API_KEY` environment variable or pass `api_key` parameter.

### "No module named 'openai'"
Install the OpenAI library: `pip install openai`

### "Failed to parse LLM response as JSON"
The LLM returned invalid JSON. This is rare but can happen. The enricher will log the error and return the original event unchanged.

### Enrichment not applied
Check that:
- Event has `description_clean` field populated
- Event has valid `source_hash`
- API key is valid and has credits

## Testing

Run the test suite:

```bash
# Unit tests
pytest tests/test_ai_enricher.py -v

# Integration tests
pytest tests/test_ai_enricher_integration.py -v
```

## API Reference

### AIEnricher

```python
class AIEnricher:
    def __init__(
        self,
        api_key: str | None = None,
        cache_dir: str = "cache/ai",
        model: str = "gpt-4o-mini",
        prompt_version: str = "v1",
        timeout: int = 30
    )
```

**Parameters:**
- `api_key`: OpenAI API key (reads from OPENAI_API_KEY env var if not provided)
- `cache_dir`: Directory path for cache storage
- `model`: OpenAI model to use
- `prompt_version`: Version identifier for prompt template
- `timeout`: Request timeout in seconds

**Methods:**

#### enrich_event()
```python
def enrich_event(
    self,
    event: EventTemplate | EventOccurrence
) -> EventTemplate | EventOccurrence
```

Enrich event with AI-generated content and metadata.

**Parameters:**
- `event`: Event record to enrich (EventTemplate or EventOccurrence)

**Returns:**
- Updated event record with AI-enriched fields

### EnrichmentData

```python
class EnrichmentData:
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
    )
```

Container for structured enrichment data extracted from LLM response.
