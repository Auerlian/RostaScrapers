# Prompt Templates Module

## Overview

The `src/enrich/prompts.py` module provides prompt templates for AI enrichment with ROSTA brand tone guidelines. It separates prompt construction logic from the AI enricher implementation, making it easier to maintain and update prompts.

## Features

- **ROSTA Brand Tone Guidelines**: Defines the brand voice (modern, confident, curated, minimal, friendly)
- **Structured Prompt Building**: Generates consistent prompts for event enrichment
- **Metadata Extraction Guidelines**: Specifies what metadata to extract and how
- **JSON Output Format**: Defines the expected structure of LLM responses
- **Modular Design**: Easy to update prompts without modifying enricher logic

## Usage

### Basic Usage

```python
from src.enrich.prompts import build_enrichment_prompt, build_system_message
from src.models.event_template import EventTemplate

# Create an event
event = EventTemplate(
    event_template_id="event-template-pasta",
    provider_id="provider-test",
    title="Pasta Making Workshop",
    description_clean="Learn to make fresh pasta from scratch.",
    category="Cooking",
    price_from=75.0,
    currency="GBP",
    # ... other fields
)

# Build prompt for LLM
prompt = build_enrichment_prompt(event)
system_message = build_system_message()

# Use with OpenAI API
response = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[
        {"role": "system", "content": system_message},
        {"role": "user", "content": prompt}
    ],
    response_format={"type": "json_object"}
)
```

### Accessing Guidelines

```python
from src.enrich.prompts import (
    get_tone_guidelines,
    get_metadata_guidelines,
    get_json_output_format
)

# Get ROSTA tone guidelines
tone = get_tone_guidelines()
print(tone)
# Output: "ROSTA BRAND TONE:\n- Modern, confident, curated..."

# Get metadata extraction guidelines
metadata = get_metadata_guidelines()
print(metadata)
# Output: "METADATA GUIDELINES:\n- tags: descriptive keywords..."

# Get JSON output format
format_template = get_json_output_format()
print(format_template)
# Output: "OUTPUT FORMAT (JSON):\n{...}"
```

## ROSTA Brand Tone Guidelines

The prompts enforce the following ROSTA brand tone:

- **Modern, confident, curated, clean, minimal**: Contemporary and polished
- **Premium but friendly**: High-quality without being stuffy
- **Lifestyle-oriented**: Focus on experiences and lifestyle
- **Short, punchy, clear, plain-English**: Easy to understand
- **Avoid dense corporate phrasing, jargon, overexplaining**: Keep it simple
- **Emotionally immediate, low-friction**: Connect quickly with users
- **Never invent facts not in source data**: Stay truthful and accurate

## Prompt Structure

The enrichment prompt includes the following sections:

1. **Event Details**: Title, category, price, description
2. **ROSTA Brand Tone**: Guidelines for writing style
3. **Task Instructions**: What the LLM should do
4. **JSON Output Format**: Expected response structure
5. **Metadata Guidelines**: How to extract structured data

## Expected JSON Output

The LLM is expected to return JSON with the following fields:

```json
{
  "description_ai": "Enhanced description in ROSTA tone",
  "summary_short": "Short punchy summary (~50 chars)",
  "summary_medium": "Medium-length summary (~150 chars)",
  "tags": ["hands-on", "italian", "beginner-friendly"],
  "occasion_tags": ["date-night", "team-building", "gift-experience"],
  "skills_required": ["none"],
  "skills_created": ["pasta-making", "hand-rolling"],
  "age_min": 18,
  "age_max": null,
  "audience": "adults",
  "family_friendly": false,
  "beginner_friendly": true,
  "duration_minutes": 120
}
```

## Metadata Fields

### tags
Descriptive keywords about the event:
- Examples: `hands-on`, `italian`, `beginner-friendly`, `prosecco-included`

### occasion_tags
Use cases for the event:
- Examples: `date-night`, `team-building`, `family-outing`, `gift-experience`

### skills_required
Prerequisites for the event:
- Examples: `none`, `basic-cooking`, `intermediate`

### skills_created
What participants will learn:
- Examples: `pasta-making`, `knife-skills`, `baking`

### age_min / age_max
Age restrictions (null if not specified):
- Examples: `18`, `null`

### audience
Target audience:
- Values: `adults`, `families`, `children`, `all-ages`

### family_friendly
Whether suitable for children:
- Values: `true`, `false`

### beginner_friendly
Whether no experience is needed:
- Values: `true`, `false`

### duration_minutes
Estimated duration (null if not specified):
- Examples: `120`, `90`, `null`

## Integration with AIEnricher

The `AIEnricher` class uses this module to build prompts:

```python
from src.enrich.ai_enricher import AIEnricher
from src.enrich.prompts import build_enrichment_prompt, build_system_message

class AIEnricher:
    def _call_llm(self, event):
        # Build prompt using prompts module
        prompt = build_enrichment_prompt(event)
        system_message = build_system_message()
        
        # Call OpenAI API
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": system_message},
                {"role": "user", "content": prompt}
            ],
            response_format={"type": "json_object"}
        )
        
        return self._parse_response(response.choices[0].message.content)
```

## Updating Prompts

To update the prompts, modify the constants in `src/enrich/prompts.py`:

1. **ROSTA_TONE_GUIDELINES**: Update brand tone guidelines
2. **METADATA_GUIDELINES**: Update metadata extraction rules
3. **JSON_OUTPUT_FORMAT**: Update expected output structure

After updating prompts, consider incrementing the `prompt_version` parameter in `AIEnricher` to invalidate the cache and re-enrich events with the new prompt.

## Testing

The module includes comprehensive tests in `tests/test_prompts.py`:

```bash
# Run prompt tests
python -m pytest tests/test_prompts.py -v

# Run all AI enrichment tests
python -m pytest tests/test_ai_enricher.py tests/test_prompts.py -v
```

## API Reference

### build_enrichment_prompt(event)

Build enrichment prompt with ROSTA tone guidelines.

**Parameters:**
- `event` (EventTemplate | EventOccurrence): Event record to enrich

**Returns:**
- `str`: Formatted prompt string for LLM

### build_system_message()

Build system message for LLM chat context.

**Returns:**
- `str`: System message string defining the AI's role

### get_tone_guidelines()

Get ROSTA brand tone guidelines.

**Returns:**
- `str`: ROSTA tone guidelines string

### get_metadata_guidelines()

Get metadata extraction guidelines.

**Returns:**
- `str`: Metadata guidelines string

### get_json_output_format()

Get JSON output format template.

**Returns:**
- `str`: JSON output format string

## Constants

### ROSTA_TONE_GUIDELINES

String constant containing ROSTA brand tone guidelines.

### METADATA_GUIDELINES

String constant containing metadata extraction guidelines.

### JSON_OUTPUT_FORMAT

String constant containing JSON output format template.

## Requirements

This module validates **Requirement 5.1** from the spec:
- AI enrichment generates descriptions in ROSTA brand tone
- Structured JSON output with description and metadata fields
- Never invents facts not in source data

## Related Documentation

- [AI Enricher Documentation](./ai_enricher.md)
- [Design Document - Component 4: AI Enricher](../.kiro/specs/scraper-pipeline-refactor/design.md)
- [Requirements Document - Requirement 5](../.kiro/specs/scraper-pipeline-refactor/requirements.md)
