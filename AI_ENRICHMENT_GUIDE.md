# AI Enrichment Guide

## Overview

The pipeline includes an AI enrichment stage that uses OpenAI's GPT models to enhance event descriptions and extract structured metadata. This makes events more discoverable and appealing to users.

## How It Works

### 1. Input Processing
The AI enricher takes raw event data with basic descriptions and transforms it into rich, structured content:

**Input:**
```json
{
  "title": "Pasta Making Class",
  "description_clean": "Learn to make fresh pasta from scratch. Hands-on class with expert chef. Includes prosecco and antipasti."
}
```

**Output:**
```json
{
  "title": "Pasta Making Class",
  "description_clean": "Learn to make fresh pasta from scratch...",
  "description_ai": "Master authentic Italian pasta-making from scratch. Perfect for beginners—no experience needed.",
  "summary_short": "Hands-on pasta making for beginners",
  "summary_medium": "Master traditional Italian pasta techniques in this beginner-friendly class with unlimited prosecco",
  "tags": ["hands-on", "italian", "beginner-friendly", "prosecco-included"],
  "occasion_tags": ["date-night", "team-building", "gift-experience"],
  "skills_created": ["pasta-making", "hand-rolling", "italian-cooking"],
  "beginner_friendly": true,
  "family_friendly": false,
  "age_min": 18
}
```

### 2. ROSTA Brand Tone

The AI follows strict brand guidelines:
- **Modern, confident, curated** - Premium but approachable
- **Short, punchy, clear** - No corporate jargon or fluff
- **Emotionally immediate** - Benefits-focused, not feature-focused
- **Never invents facts** - Only enhances what's in the source data

### 3. Intelligent Caching

The enricher uses smart caching to minimize API costs:

**Cache Key Formula:**
```
cache_key = hash(source_hash + prompt_version + model)
```

**Cache Behavior:**
- ✅ **Cache Hit**: Source data unchanged → Use cached enrichment (instant, free)
- ❌ **Cache Miss**: Source data changed → Call LLM and cache result (~$0.001 per event)
- 🔄 **Cache Invalidation**: Automatic when source data or prompt changes

**Performance:**
- First run: ~5 events/minute (LLM calls)
- Subsequent runs: ~1000 events/second (cache hits)
- Cache size: ~2KB per event

### 4. Error Handling

The enricher is designed to never break the pipeline:

```python
try:
    enriched_event = enricher.enrich_event(event)
except Exception as e:
    # Log error but continue with original event
    print(f"AI enrichment failed: {e}")
    enriched_event = event  # Use original
```

**Graceful degradation:**
- Missing API key → Skip AI enrichment
- LLM timeout → Use original descriptions
- Invalid JSON response → Log and continue
- Network error → Retry once, then skip

## Setup

### 1. Get OpenAI API Key

1. Go to https://platform.openai.com/api-keys
2. Create a new API key
3. Copy the key (starts with `sk-`)

### 2. Set Environment Variable

**Local development:**
```bash
export OPENAI_API_KEY="sk-your-key-here"
```

Or create a `.env` file:
```
OPENAI_API_KEY=sk-your-key-here
```

**GitHub Actions:**
1. Go to repository Settings → Secrets and variables → Actions
2. Click "New repository secret"
3. Name: `OPENAI_API_KEY`
4. Value: Your API key
5. Click "Add secret"

### 3. Run Pipeline with AI

```bash
# With AI enrichment (requires API key)
python run_pipeline.py run

# Without AI enrichment
python run_pipeline.py run --skip-ai
```

## Cost Estimation

Using `gpt-4o-mini` (recommended):

| Events | First Run | Subsequent Runs | Monthly Cost* |
|--------|-----------|-----------------|---------------|
| 50     | $0.05     | $0.00           | $1.50         |
| 100    | $0.10     | $0.00           | $3.00         |
| 500    | $0.50     | $0.00           | $15.00        |
| 1000   | $1.00     | $0.00           | $30.00        |

*Assuming daily runs with 10% new/changed events per day

**Cost optimization:**
- Cache hits are free (most runs after first)
- Only new or changed events trigger LLM calls
- Batch processing reduces overhead

## What Gets Enriched

### Description Fields
- **description_ai**: 2-3 sentences in ROSTA tone (~150 chars)
- **summary_short**: Card preview (~50 chars)
- **summary_medium**: List preview (~150 chars)

### Metadata Fields
- **tags**: Descriptive keywords (e.g., "hands-on", "italian", "beginner-friendly")
- **occasion_tags**: Use cases (e.g., "date-night", "team-building", "family-outing")
- **skills_required**: Prerequisites (e.g., "none", "basic-cooking")
- **skills_created**: What you'll learn (e.g., "pasta-making", "knife-skills")

### Audience Fields
- **age_min/age_max**: Age restrictions
- **audience**: Target audience ("adults", "families", "children")
- **family_friendly**: Boolean for child suitability
- **beginner_friendly**: Boolean for experience level
- **duration_minutes**: Estimated duration

## Monitoring

### Check Cache Status
```bash
# View cached enrichments
ls -lh cache/ai/

# Count cached events
ls cache/ai/ | wc -l

# View a cached enrichment
cat cache/ai/abc123def456.json | jq
```

### Check Enrichment Quality
```bash
# Run validation tests
pytest tests/test_ai_enricher_validation.py -v

# Check for events without AI enrichment
python -c "
from src.storage.store import CanonicalStore
store = CanonicalStore()
events = store.load_events()
missing = [e for e in events if not e.description_ai]
print(f'{len(missing)} events without AI enrichment')
"
```

### Monitor API Usage
Check your OpenAI dashboard:
- https://platform.openai.com/usage
- View daily API costs
- Set spending limits
- Monitor rate limits

## Troubleshooting

### "OpenAI API key not found"
**Solution:** Set the `OPENAI_API_KEY` environment variable or pass it to the enricher.

### "No module named 'openai'"
**Solution:** Install the OpenAI library:
```bash
pip install openai
```

### Enrichments not being applied
**Check:**
1. Events have `description_clean` populated
2. API key is valid and has credits
3. Check logs for LLM errors
4. Verify cache directory is writable

### High API costs
**Solutions:**
1. Ensure caching is working (check cache directory)
2. Use `gpt-4o-mini` instead of `gpt-4`
3. Run less frequently (weekly instead of daily)
4. Skip AI for unchanged events (automatic with caching)

### Poor quality enrichments
**Solutions:**
1. Update prompt version to invalidate cache
2. Switch to `gpt-4` for higher quality
3. Adjust temperature (lower = more consistent)
4. Review and update prompts in `src/enrich/prompts.py`

## Advanced Configuration

### Custom Model
```python
from src.enrich.ai_enricher import AIEnricher

enricher = AIEnricher(
    model="gpt-4",           # Higher quality, higher cost
    prompt_version="v2",     # Custom prompt version
    timeout=60               # Longer timeout
)
```

### Batch Processing
```python
from src.enrich.ai_enricher import AIEnricher
from src.storage.store import CanonicalStore

enricher = AIEnricher()
store = CanonicalStore()

events = store.load_events()
enriched = []

for event in events:
    try:
        enriched.append(enricher.enrich_event(event))
    except Exception as e:
        print(f"Failed to enrich {event.title}: {e}")
        enriched.append(event)

store.save_events(enriched)
```

### Conditional Enrichment
```python
# Only enrich events with long descriptions
if event.description_clean and len(event.description_clean) > 100:
    enriched_event = enricher.enrich_event(event)
```

## Testing

Run the test suite to verify AI enrichment:

```bash
# Unit tests (mocked LLM)
pytest tests/test_ai_enricher.py -v

# Integration tests (real LLM calls, requires API key)
pytest tests/test_ai_enricher_integration.py -v

# Validation tests (check enrichment quality)
pytest tests/test_ai_enricher_validation.py -v
```

## Best Practices

1. **Always use caching** - Saves money and improves performance
2. **Start with gpt-4o-mini** - Good quality, low cost
3. **Monitor API usage** - Set spending limits in OpenAI dashboard
4. **Version your prompts** - Increment `prompt_version` when changing prompts
5. **Test before deploying** - Run integration tests with real API
6. **Handle errors gracefully** - Never let AI enrichment break the pipeline
7. **Review enrichments** - Spot-check quality regularly
8. **Cache in git** - Commit cache directory to avoid re-enriching

## Future Enhancements

Potential improvements:
- Multi-language support
- Image analysis for better descriptions
- Sentiment analysis
- Personalized recommendations
- A/B testing different prompts
- Batch API calls for better performance
- Streaming responses for real-time feedback
