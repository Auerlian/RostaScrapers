# CSV Export Schemas

This document defines the CSV export schemas for events and locations data from the ROSTA scraper pipeline.

## Overview

The pipeline generates two primary CSV exports:
- **events.csv**: Contains both event templates and event occurrences
- **locations.csv**: Contains venue/location data optimized for map display

Both exports use consistent formatting conventions for lists, nulls, and data types.

## General Formatting Conventions

### List Encoding
All list fields (tags, skills, image URLs, etc.) are encoded as **semicolon-separated values**.

**Examples:**
- Tags: `hands-on;italian;beginner-friendly;prosecco-included`
- Skills: `pasta-making;hand-rolling;italian-cooking`
- Image URLs: `https://example.com/img1.jpg;https://example.com/img2.jpg`

**Rationale:** Semicolons are rarely used in natural text and URLs, making them safer than commas for CSV list encoding.

### Null Handling
Null or missing values are represented as **empty strings** in CSV output.

**Examples:**
- Null string field: `` (empty)
- Null numeric field: `` (empty)
- Null boolean field: `` (empty)
- Empty list: `` (empty, not `[]`)

**Rationale:** CSV format has no native null representation. Empty strings are the standard convention and work well with spreadsheet software.

### Date/Time Format
All datetime fields use **ISO 8601 format with UTC timezone**.

**Format:** `YYYY-MM-DDTHH:MM:SSZ`

**Examples:**
- `2025-01-15T10:30:00Z`
- `2025-12-31T23:59:59Z`

### Boolean Format
Boolean values are represented as lowercase strings: `true` or `false`.

**Examples:**
- `family_friendly`: `true`
- `beginner_friendly`: `false`

### Numeric Format
- **Integers**: Plain numbers without decimals (e.g., `120`, `18`)
- **Floats**: Numbers with decimal point (e.g., `68.0`, `75.50`)
- **Currency**: Float values without currency symbols (currency code in separate column)

### Text Escaping
- CSV standard escaping applies: fields containing commas, quotes, or newlines are quoted
- Internal quotes are doubled (`""`)
- Newlines within fields are preserved
- HTML tags are stripped from description fields

---

## Events CSV Schema

**File:** `exports/events.csv`

**Purpose:** Contains all event data including both templates (recurring/undated events) and occurrences (specific dated sessions).

### Record Type Handling

The events CSV contains two types of records distinguished by the `record_type` column:

1. **Template Records** (`record_type=template`):
   - Represent recurring or undated event types
   - `record_id` contains the event template ID
   - `event_template_id` is empty (not self-referential)
   - Date fields (`start_at`, `end_at`) are empty
   - Booking fields may be empty or contain general booking URL

2. **Occurrence Records** (`record_type=occurrence`):
   - Represent specific scheduled sessions
   - `record_id` contains the event occurrence ID
   - `event_template_id` contains parent template ID (if linked)
   - Date fields contain specific session datetime
   - Booking fields contain session-specific data

### Column Definitions

| Column | Type | Required | Description | Example |
|--------|------|----------|-------------|---------|
| `record_type` | string | Yes | Type of record: `template` or `occurrence` | `template` |
| `record_id` | string | Yes | Canonical ID for this record (template ID or occurrence ID) | `event-template-pasta-evangelists-beginners-class` |
| `event_template_id` | string | No | Parent template ID (only for occurrence records) | `event-template-comptoir-bakery-croissant-class` |
| `provider_id` | string | Yes | Foreign key to provider | `provider-pasta-evangelists` |
| `provider_name` | string | Yes | Human-readable provider name | `Pasta Evangelists` |
| `title` | string | Yes | Event title | `Beginners Class` |
| `slug` | string | Yes | URL-friendly slug | `beginners-class` |
| `category` | string | No | Primary category | `Cooking` |
| `sub_category` | string | No | Secondary category | `Pasta Making` |
| `description_raw` | text | No | Original source description (may contain HTML) | `Join us to master the techniques...` |
| `description_clean` | text | No | HTML-stripped, whitespace-normalized description | `Join us to master the techniques of pasta fatta a mano...` |
| `description_ai` | text | No | AI-enhanced description in ROSTA tone | `Learn authentic Italian pasta-making from scratch. Perfect for beginners—no experience needed.` |
| `summary_short` | string | No | ~50 character summary for cards | `Hands-on pasta making for beginners` |
| `summary_medium` | string | No | ~150 character summary for listings | `Master traditional Italian pasta techniques in this beginner-friendly class...` |
| `tags` | list | No | Semicolon-separated descriptive tags | `hands-on;italian;beginner-friendly;prosecco-included` |
| `occasion_tags` | list | No | Semicolon-separated occasion tags | `date-night;team-building;gift-experience` |
| `skills_required` | list | No | Semicolon-separated prerequisite skills | `basic-cooking` or empty for none |
| `skills_created` | list | No | Semicolon-separated skills learned | `pasta-making;hand-rolling;italian-cooking` |
| `age_min` | integer | No | Minimum age requirement | `18` |
| `age_max` | integer | No | Maximum age limit | `65` or empty for no limit |
| `audience` | string | No | Target audience | `adults`, `families`, `children` |
| `family_friendly` | boolean | No | Whether suitable for families | `true` or `false` |
| `beginner_friendly` | boolean | No | Whether suitable for beginners | `true` or `false` |
| `duration_minutes` | integer | No | Event duration in minutes | `120` |
| `price` | float | No | Price (for occurrences) or starting price (for templates) | `68.0` |
| `currency` | string | No | ISO currency code | `GBP` |
| `location_id` | string | No | Foreign key to location | `location-pasta-evangelists-a3f5e8d9c2b1` |
| `location_name` | string | No | Denormalized location name for convenience | `The Pasta Academy Farringdon` |
| `formatted_address` | string | No | Denormalized full address for convenience | `The Pasta Academy, 62-63 Long Lane, London, EC1A 9EJ` |
| `start_at` | datetime | No | Event start datetime (occurrences only) | `2025-01-18T09:00:00Z` |
| `end_at` | datetime | No | Event end datetime (occurrences only) | `2025-01-18T12:00:00Z` |
| `timezone` | string | No | IANA timezone (occurrences only) | `Europe/London` |
| `booking_url` | string | No | Direct booking link | `https://bookwhen.com/comptoir/e/ev-abc123` |
| `source_url` | string | No | Link to provider's event page | `https://comptoirbakery.com/classes` |
| `image_urls` | list | No | Semicolon-separated image URLs | `https://cdn.example.com/img1.jpg;https://cdn.example.com/img2.jpg` |
| `capacity` | integer | No | Total capacity (occurrences only) | `12` |
| `remaining_spaces` | integer | No | Available spaces (occurrences only) | `3` |
| `availability_status` | string | No | Booking availability status | `available`, `sold_out`, `limited`, `unknown` |
| `status` | string | Yes | Lifecycle status | `active`, `expired`, `removed`, `cancelled` |
| `first_seen_at` | datetime | Yes | First time record appeared in pipeline | `2025-01-15T10:30:00Z` |
| `last_seen_at` | datetime | Yes | Most recent time record appeared in pipeline | `2025-01-20T14:22:00Z` |

### Field Notes

**record_type and record_id:**
- `record_type` distinguishes templates from occurrences
- `record_id` always contains the canonical ID for the current row
- For template rows: `record_id` = template ID, `event_template_id` is empty
- For occurrence rows: `record_id` = occurrence ID, `event_template_id` = parent template ID (if linked)

**Description Fields:**
- `description_raw`: Original from source, may contain HTML
- `description_clean`: HTML stripped, whitespace normalized, suitable for display
- `description_ai`: Optional AI-enhanced version in ROSTA brand tone

**Tags and Skills:**
- All list fields use semicolon separators
- Empty lists are represented as empty strings, not `[]`
- Tags are lowercase with hyphens (e.g., `beginner-friendly`)

**Location Fields:**
- `location_id`: Foreign key reference to locations.csv
- `location_name` and `formatted_address`: Denormalized for convenience
- May be empty if event has no specific location

**Date/Time Fields:**
- Only populated for occurrence records
- Templates have empty date fields
- All times in UTC with `Z` suffix

**Availability vs Status:**
- `availability_status`: Booking state (available, sold_out, limited, unknown)
- `status`: Lifecycle state (active, expired, removed, cancelled)
- These are independent: an active event can be sold_out

**Filtering:**
- Default export includes only `status=active` records
- Expired, removed, and cancelled events excluded by default
- Can be configured to include all statuses

### Example Rows

**Template Row:**
```csv
template,event-template-pasta-evangelists-beginners-class,,provider-pasta-evangelists,Pasta Evangelists,Beginners Class,beginners-class,Cooking,Pasta Making,"Join us to master the techniques of pasta fatta a mano...","Learn authentic Italian pasta-making from scratch. Perfect for beginners—no experience needed.",Hands-on pasta making for beginners,hands-on;italian;beginner-friendly;prosecco-included,date-night;team-building;gift-experience,,pasta-making;hand-rolling;italian-cooking,18,,adults,false,true,120,68.0,GBP,,,,,,,https://plan.pastaevangelists.com/events/themes,https://plan.pastaevangelists.com/events/themes,https://cdn.shopify.com/.../beginners-class.png,,,available,active,2025-01-15T10:30:00Z,2025-01-20T14:22:00Z
```

**Occurrence Row:**
```csv
occurrence,event-comptoir-bakery-abc123,event-template-comptoir-bakery-croissant-class,provider-comptoir-bakery,Comptoir Bakery,Croissant Making Class,croissant-making-class,Baking,Pastry,"Learn the art of French croissant making...","Master flaky, buttery croissants in this hands-on French baking class.",French croissant baking class,hands-on;french;baking;pastry,date-night;family-activity,,croissant-making;lamination;french-baking,16,,adults,false,true,180,75.0,GBP,location-comptoir-bakery-xyz789,Comptoir Bakery Shoreditch,"123 High Street, London, E1 6JE",2025-01-18T09:00:00Z,2025-01-18T12:00:00Z,Europe/London,https://bookwhen.com/comptoir/e/ev-abc123,https://comptoirbakery.com/classes,https://comptoir.com/.../croissant.jpg,12,3,available,active,2025-01-18T09:00:00Z,2025-01-20T14:22:00Z
```

---

## Locations CSV Schema

**File:** `exports/locations.csv`

**Purpose:** Contains venue/location data optimized for map display with geographic coordinates and event summaries.

### Column Definitions

| Column | Type | Required | Description | Example |
|--------|------|----------|-------------|---------|
| `location_id` | string | Yes | Canonical location ID | `location-pasta-evangelists-a3f5e8d9c2b1` |
| `provider_id` | string | Yes | Foreign key to provider | `provider-pasta-evangelists` |
| `provider_name` | string | Yes | Human-readable provider name | `Pasta Evangelists` |
| `location_name` | string | No | Venue name | `The Pasta Academy Farringdon` |
| `formatted_address` | string | Yes | Full address for display | `The Pasta Academy, 62-63 Long Lane, London, EC1A 9EJ` |
| `address_line_1` | string | No | First line of address | `62-63 Long Lane` |
| `address_line_2` | string | No | Second line of address | `Suite 100` |
| `city` | string | No | City | `London` |
| `region` | string | No | Region/state/county | `Greater London` |
| `postcode` | string | No | Postal code | `EC1A 9EJ` |
| `country` | string | Yes | Country code | `UK` |
| `latitude` | float | No | Geographic latitude | `51.5201` |
| `longitude` | float | No | Geographic longitude | `-0.0982` |
| `geocode_status` | string | Yes | Geocoding result status | `success`, `failed`, `not_geocoded`, `invalid_address` |
| `geocode_precision` | string | No | Geocoding precision level | `rooftop`, `street`, `city`, `region` |
| `geocode_provider` | string | No | Geocoding service used | `mapbox`, `nominatim` |
| `geocoded_at` | datetime | No | When geocoding was performed | `2025-01-15T11:00:00Z` |
| `venue_phone` | string | No | Venue phone number | `+44 20 1234 5678` |
| `venue_email` | string | No | Venue email address | `venue@example.com` |
| `venue_website` | string | No | Venue website URL | `https://example.com` |
| `event_count` | integer | Yes | Total events at this location | `15` |
| `active_event_count` | integer | Yes | Active events at this location | `12` |
| `event_names` | list | No | Semicolon-separated event titles (truncated if too long) | `Beginners Class;Taste of Rome;Taste of Amalfi;Four Cheese Ravioli` |
| `active_event_ids` | list | No | Semicolon-separated active event IDs for linking | `event-template-pasta-evangelists-beginners-class;event-template-pasta-evangelists-taste-of-rome` |
| `status` | string | Yes | Lifecycle status | `active`, `inactive`, `removed` |
| `first_seen_at` | datetime | Yes | First time location appeared in pipeline | `2025-01-15T10:30:00Z` |
| `last_seen_at` | datetime | Yes | Most recent time location appeared in pipeline | `2025-01-20T14:22:00Z` |

### Field Notes

**Geographic Coordinates:**
- `latitude`: Range -90 to 90 (negative = south, positive = north)
- `longitude`: Range -180 to 180 (negative = west, positive = east)
- May be empty if geocoding failed or not yet performed
- Precision typically 4-6 decimal places

**Geocoding Status:**
- `success`: Coordinates successfully obtained
- `failed`: Geocoding attempted but failed
- `not_geocoded`: Geocoding not yet attempted
- `invalid_address`: Address format invalid for geocoding

**Geocoding Precision:**
- `rooftop`: Exact building location (most precise)
- `street`: Street-level accuracy
- `city`: City-level accuracy
- `region`: Region-level accuracy
- Empty if geocoding not successful

**Event Summaries:**
- `event_count`: Total events (all statuses) linked to this location
- `active_event_count`: Only events with `status=active`
- `event_names`: Truncated list of event titles for display (may not include all events)
- `active_event_ids`: Complete list of active event IDs for programmatic linking

**Status Values:**
- `active`: Location has active events and is currently in use
- `inactive`: Location has no active events but may return
- `removed`: Location no longer appears in source data (soft delete)

**Map Display Optimization:**
- Includes provider context for map markers
- Event summaries allow showing "what's here" without joining to events table
- Coordinates enable direct plotting on maps
- Formatted address suitable for display in map popups

### Example Row

```csv
location-pasta-evangelists-a3f5e8d9c2b1,provider-pasta-evangelists,Pasta Evangelists,The Pasta Academy Farringdon,"The Pasta Academy, 62-63 Long Lane, London, EC1A 9EJ",62-63 Long Lane,,London,Greater London,EC1A 9EJ,UK,51.5201,-0.0982,success,rooftop,mapbox,2025-01-15T11:00:00Z,,,https://pastaevangelists.com,15,12,"Beginners Class;Taste of Rome;Taste of Amalfi;Four Cheese Ravioli;Advanced Techniques","event-template-pasta-evangelists-beginners-class;event-template-pasta-evangelists-taste-of-rome;event-template-pasta-evangelists-taste-of-amalfi;event-template-pasta-evangelists-four-cheese-ravioli",active,2025-01-15T10:30:00Z,2025-01-20T14:22:00Z
```

---

## Implementation Notes

### CSV Generation

**Python Implementation:**
```python
import csv
from typing import Any

def format_list_field(items: list[str] | None) -> str:
    """Format list as semicolon-separated string."""
    if not items:
        return ""
    return ";".join(items)

def format_null(value: Any) -> str:
    """Format null/None values as empty string."""
    if value is None:
        return ""
    return str(value)

def format_boolean(value: bool | None) -> str:
    """Format boolean as lowercase string."""
    if value is None:
        return ""
    return "true" if value else "false"

def format_datetime(dt: datetime | None) -> str:
    """Format datetime as ISO 8601 UTC string."""
    if dt is None:
        return ""
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
```

**CSV Writer Configuration:**
```python
with open("events.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=EVENT_COLUMNS)
    writer.writeheader()
    for event in events:
        writer.writerow(format_event_row(event))
```

### Parsing CSV Exports

**Reading Lists:**
```python
def parse_list_field(value: str) -> list[str]:
    """Parse semicolon-separated list."""
    if not value or value.strip() == "":
        return []
    return [item.strip() for item in value.split(";")]
```

**Reading Nulls:**
```python
def parse_null(value: str) -> Any | None:
    """Parse empty string as None."""
    if value == "":
        return None
    return value
```

**Reading Booleans:**
```python
def parse_boolean(value: str) -> bool | None:
    """Parse boolean string."""
    if value == "":
        return None
    return value.lower() == "true"
```

### Validation

**Export Completeness Check:**
```python
def validate_export_completeness(store, csv_path):
    """Ensure all active records appear in export."""
    active_records = store.load_events(filters={"status": "active"})
    csv_records = load_csv(csv_path)
    
    active_ids = {r.event_id for r in active_records}
    csv_ids = {r["record_id"] for r in csv_records}
    
    missing = active_ids - csv_ids
    if missing:
        raise ValueError(f"Missing {len(missing)} records in export")
```

**Field Format Validation:**
```python
def validate_csv_format(csv_path):
    """Validate CSV format and encoding."""
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Validate required fields present
            # Validate list fields use semicolons
            # Validate dates are ISO 8601
            # Validate booleans are true/false
            pass
```

---

## Migration from Old Format

### Old Format (Flat JSON)
```json
{
  "experience_name": "Beginners Class",
  "experience_description": "<p>Join us...</p>",
  "detailed_location": ["Address 1", "Address 2", "..."],
  "price": "£68",
  "images": ["url1", "url2"]
}
```

### New Format (CSV)
```csv
record_type,record_id,title,description_clean,formatted_address,price,currency,image_urls
template,event-template-pasta-evangelists-beginners-class,Beginners Class,"Join us...",The Pasta Academy...,68.0,GBP,url1;url2
```

### Key Changes
1. **Location data separated**: Now in locations.csv with foreign key reference
2. **Lists use semicolons**: Not JSON arrays
3. **Nulls are empty strings**: Not `null` keyword
4. **Prices are numeric**: Currency symbols removed, currency code in separate column
5. **Record types explicit**: `record_type` column distinguishes templates from occurrences
6. **IDs are stable**: Deterministic IDs enable incremental updates

---

## Future Enhancements

### Potential Additional Exports (v1.5+)

**providers.csv:**
- Provider directory with contact information
- Useful for provider management and reporting

**event_location_links.csv:**
- Many-to-many relationships between templates and locations
- Only needed if templates are available at multiple specific locations

**changes.csv:**
- Change log showing what was added/updated/removed in each run
- Useful for auditing and monitoring

### Potential Schema Extensions

**Events:**
- `instructor_name`: Instructor/chef name
- `difficulty_level`: Numeric difficulty rating
- `dietary_options`: Dietary accommodations available
- `language`: Event language (if not English)
- `cancellation_policy`: Cancellation terms

**Locations:**
- `accessibility_notes`: Accessibility information
- `parking_info`: Parking availability
- `public_transport`: Nearest transit stations
- `neighborhood`: Neighborhood/district name

---

## Questions and Clarifications

### When to use Templates vs Occurrences?
- **Template**: Recurring event type without specific dates (e.g., "Beginners Class" offered weekly)
- **Occurrence**: Specific scheduled session with date/time (e.g., "Beginners Class on Jan 18, 2025 at 9am")
- If source provides specific dates → create occurrences
- If source only provides event types → create templates
- Never invent occurrence rows from templates without source data

### How are multi-location templates handled?
- If template is available at multiple locations, use `location_scope="provider-wide"` field in canonical model
- In CSV export, location fields remain empty for such templates
- Specific location is determined during booking process
- Do not create duplicate rows per location

### What if geocoding fails?
- Location still exported with empty lat/lng fields
- `geocode_status` set to `failed` or `invalid_address`
- Map display can skip locations without coordinates
- Next pipeline run will retry geocoding

### How are removed events handled?
- Marked with `status=removed` in canonical store
- Excluded from CSV exports by default
- Can be included with `--include-removed` flag
- Preserved for historical analysis

---

## Appendix: Complete Column Lists

### Events CSV Columns (in order)
1. record_type
2. record_id
3. event_template_id
4. provider_id
5. provider_name
6. title
7. slug
8. category
9. sub_category
10. description_raw
11. description_clean
12. description_ai
13. summary_short
14. summary_medium
15. tags
16. occasion_tags
17. skills_required
18. skills_created
19. age_min
20. age_max
21. audience
22. family_friendly
23. beginner_friendly
24. duration_minutes
25. price
26. currency
27. location_id
28. location_name
29. formatted_address
30. start_at
31. end_at
32. timezone
33. booking_url
34. source_url
35. image_urls
36. capacity
37. remaining_spaces
38. availability_status
39. status
40. first_seen_at
41. last_seen_at

### Locations CSV Columns (in order)
1. location_id
2. provider_id
3. provider_name
4. location_name
5. formatted_address
6. address_line_1
7. address_line_2
8. city
9. region
10. postcode
11. country
12. latitude
13. longitude
14. geocode_status
15. geocode_precision
16. geocode_provider
17. geocoded_at
18. venue_phone
19. venue_email
20. venue_website
21. event_count
22. active_event_count
23. event_names
24. active_event_ids
25. status
26. first_seen_at
27. last_seen_at
