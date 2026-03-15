"""Prompt templates for AI enrichment with ROSTA brand tone guidelines."""

from src.models.event_template import EventTemplate
from src.models.event_occurrence import EventOccurrence


# ROSTA Brand Tone Guidelines
ROSTA_TONE_GUIDELINES = """
ROSTA BRAND TONE:
- Modern, confident, curated, clean, minimal
- Premium but friendly, lifestyle-oriented
- Short, punchy, clear, plain-English
- Avoid dense corporate phrasing, jargon, overexplaining
- Emotionally immediate, low-friction
- Never invent facts not in source data
"""

# Metadata extraction guidelines
METADATA_GUIDELINES = """
METADATA GUIDELINES:
- tags: descriptive keywords (hands-on, italian, beginner-friendly, prosecco-included)
- occasion_tags: use cases (date-night, team-building, family-outing, gift-experience)
- skills_required: prerequisites (none, basic-cooking, intermediate)
- skills_created: what you'll learn (pasta-making, knife-skills, baking)
- age_min/age_max: age restrictions (null if not specified)
- audience: adults, families, children, all-ages
- family_friendly: suitable for children
- beginner_friendly: no experience needed
- duration_minutes: estimated duration (null if not specified)

Only extract metadata that is clearly stated or strongly implied in the source description. Do not invent details.
"""

# JSON output format template
JSON_OUTPUT_FORMAT = """
OUTPUT FORMAT (JSON):
{
  "description_ai": "Enhanced description in ROSTA tone",
  "summary_short": "Short punchy summary",
  "summary_medium": "Medium-length summary with more detail",
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
"""


def build_enrichment_prompt(event: EventTemplate | EventOccurrence) -> str:
    """Build enrichment prompt with ROSTA tone guidelines.
    
    Generates a structured prompt for LLM to enhance event descriptions
    and extract metadata following ROSTA brand guidelines.
    
    Args:
        event: Event record to enrich (EventTemplate or EventOccurrence)
        
    Returns:
        Formatted prompt string for LLM
    """
    # Extract event details
    title = event.title
    description = event.description_clean or ""
    price = getattr(event, 'price_from', None) or getattr(event, 'price', None)
    category = getattr(event, 'category', None)
    
    # Format price display
    price_display = f"£{price} GBP" if price else "Price not specified"
    
    # Build prompt sections
    prompt = f"""Enhance this event description for ROSTA, a premium lifestyle events platform.

EVENT DETAILS:
Title: {title}
Category: {category or "Unknown"}
Price: {price_display}
Description: {description}

{ROSTA_TONE_GUIDELINES}

TASK:
1. Rewrite the description in ROSTA tone (2-3 sentences max, ~150 chars)
2. Create a short summary (~50 chars for cards)
3. Create a medium summary (~150 chars for listings)
4. Extract structured metadata

{JSON_OUTPUT_FORMAT}

{METADATA_GUIDELINES}"""
    
    return prompt


def build_system_message() -> str:
    """Build system message for LLM chat context.
    
    Returns:
        System message string defining the AI's role
    """
    return (
        "You are a content editor for ROSTA, a premium lifestyle events platform. "
        "Your task is to enhance event descriptions and extract structured metadata."
    )


def get_tone_guidelines() -> str:
    """Get ROSTA brand tone guidelines.
    
    Returns:
        ROSTA tone guidelines string
    """
    return ROSTA_TONE_GUIDELINES.strip()


def get_metadata_guidelines() -> str:
    """Get metadata extraction guidelines.
    
    Returns:
        Metadata guidelines string
    """
    return METADATA_GUIDELINES.strip()


def get_json_output_format() -> str:
    """Get JSON output format template.
    
    Returns:
        JSON output format string
    """
    return JSON_OUTPUT_FORMAT.strip()
