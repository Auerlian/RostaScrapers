# Implementation Plan: Scraper Pipeline Refactor

## Overview

This implementation plan refactors the RostaScrapers repository from independent scraper scripts into a maintainable data pipeline with normalized data models, incremental sync, and CSV exports. The plan focuses on v1 deliverables (core functionality) and defers v1.5 enhancements (geocoding, AI enrichment) to later phases.

The implementation follows 7 phases: Foundation, Storage & Sync, Exports, Geocoding (v1.5), AI Enrichment (v1.5), Validation & Reporting, and Documentation. Each phase builds incrementally on previous work with checkpoints to validate progress.

## Tasks

- [x] 1. Phase 1: Foundation and Data Models
  - [x] 1.1 Create project directory structure
    - Create src/ with subdirectories: extract/, transform/, enrich/, sync/, export/, models/, storage/, pipeline/, config/
    - Create data/ with subdirectories: raw/, current/, archive/
    - Create cache/ with subdirectories: geocoding/, ai/
    - Create exports/, logs/, tests/ directories
    - _Requirements: 8.1, 8.5_

  - [x] 1.2 Implement canonical data models with validation
    - Create src/models/provider.py with Provider dataclass
    - Create src/models/location.py with Location dataclass
    - Create src/models/event_template.py with EventTemplate dataclass
    - Create src/models/event_occurrence.py with EventOccurrence dataclass
    - Add validation methods for required fields, constraints, and field formats
    - _Requirements: 2.1, 6.1, 6.2, 6.3, 6.4, 16.1, 16.2_

  - [x] 1.3 Implement ID generation functions
    - Create src/transform/id_generator.py with deterministic ID generation functions
    - Implement generate_provider_id() using provider name slugification
    - Implement generate_location_id() using provider slug and normalized address hash
    - Implement generate_event_template_id() using provider slug and source template ID or title slug
    - Implement generate_event_occurrence_id() using provider slug, source event ID, or composite hash
    - Implement slugify() and normalize_address() helper functions
    - _Requirements: 2.2, 2.6_


  - [ ]* 1.4 Write property test for ID determinism
    - **Property 1: ID Determinism**
    - **Validates: Requirements 2.2, 2.6**
    - Generate random provider names and addresses
    - Assert ID generation produces identical output for identical input
    - Assert IDs match expected format patterns

  - [x] 1.5 Implement hash computation for change detection
    - Create src/transform/hash_computer.py with hash computation functions
    - Implement compute_source_hash() for source fields only
    - Implement compute_record_hash() for all canonical fields
    - Implement compute_address_hash() for location address fields
    - Use SHA256 with 12-character truncation for hash values
    - _Requirements: 2.7, 2.8, 3.1_

  - [ ]* 1.6 Write property test for hash stability
    - **Property 2: Hash Stability**
    - **Validates: Requirements 2.7, 3.1**
    - Generate random records
    - Compute hash, modify non-source fields, recompute hash
    - Assert source_hash remains identical when source fields unchanged

  - [x] 1.7 Create BaseScraper abstract class
    - Create src/extract/base_scraper.py with BaseScraper ABC
    - Define abstract scrape() method returning RawProviderData
    - Define abstract provider_name and provider_metadata properties
    - Add common utilities for HTTP requests with polite delays and timeout handling
    - _Requirements: 1.1, 1.6_

  - [x] 1.8 Refactor Pasta Evangelists scraper
    - Create src/extract/pasta_evangelists.py inheriting from BaseScraper
    - Migrate existing scraping logic to new structure
    - Return structured RawProviderData instead of writing JSON directly
    - Extract locations as separate entities
    - Investigate API to determine correct event-location relationships
    - Do NOT assign all locations to all events (current bug)
    - If location mapping unavailable, set location_scope="provider-wide" on templates
    - _Requirements: 1.1, 1.2, 1.4, 2.4, 2.5, 13.1, 13.3, 13.5_

  - [x] 1.9 Refactor Comptoir Bakery scraper
    - Create src/extract/comptoir_bakery.py inheriting from BaseScraper
    - Migrate existing scraping logic to new structure
    - Return structured RawProviderData
    - Extract locations from Bookwhen event data
    - Create EventOccurrence records for dated sessions
    - _Requirements: 1.1, 1.2, 1.4, 2.4, 2.5_

  - [x] 1.10 Refactor Caravan Coffee scraper
    - Create src/extract/caravan_coffee.py inheriting from BaseScraper
    - Migrate existing scraping logic to new structure
    - Return structured RawProviderData
    - Extract locations from Eventbrite data
    - Handle Eventbrite page structure carefully (known fragility)
    - _Requirements: 1.1, 1.2, 1.4, 2.4, 2.5_

  - [x] 1.11 Implement Normalizer for data transformation
    - Create src/transform/normalizer.py with Normalizer class
    - Implement normalize_provider() to create Provider records
    - Implement normalize_locations() to create Location records with deterministic IDs
    - Implement normalize_events() to create EventTemplate or EventOccurrence records
    - Strip HTML tags, normalize whitespace, handle null values consistently
    - Parse dates, prices, currencies into canonical formats
    - Link events to locations using location IDs
    - Compute source_hash and record_hash for each record
    - _Requirements: 2.1, 2.3, 2.4, 2.5, 2.7, 2.8_

  - [x] 1.12 Write unit tests for ID generation and validation
    - Create tests/test_id_generation.py
    - Test ID generation with various inputs (special characters, unicode, empty strings)
    - Test slugify() and normalize_address() edge cases
    - Test validation rules for all models (required fields, constraints, formats)
    - Test foreign key validation
    - _Requirements: 2.2, 2.6, 6.1, 6.2, 6.3, 6.4, 6.5_

- [x] 2. Checkpoint - Validate foundation components
  - Ensure all tests pass, ask the user if questions arise.


- [x] 3. Phase 2: Storage and Sync
  - [x] 3.1 Implement CanonicalStore with dict-keyed JSON backend
    - Create src/storage/store.py with CanonicalStore class
    - Implement save_providers(), save_locations(), save_events() methods
    - Implement load_providers(), load_locations(), load_events() methods with filtering
    - Structure each file as dict keyed by record ID (not arrays)
    - Store in data/current/ as providers.json, locations.json, event_templates.json, event_occurrences.json
    - Handle missing or corrupted files gracefully
    - Implement atomic write operations for data integrity
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 8.6_

  - [x] 3.2 Implement archive snapshot functionality
    - Add archive_snapshot() method to CanonicalStore
    - Create timestamped backup in data/archive/{timestamp}/
    - Copy all current canonical JSON files to archive directory
    - _Requirements: 8.7_

  - [x] 3.3 Implement MergeEngine with hash-based change detection
    - Create src/sync/merge_engine.py with MergeEngine class
    - Implement merge_records() to merge new records with existing
    - Implement _detect_change() using source_hash comparison
    - Insert new records with first_seen_at timestamp
    - Update changed records, preserving first_seen_at
    - Preserve unchanged records exactly, update last_seen_at only
    - Generate MergeResult with statistics (inserted, updated, unchanged)
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.8_

  - [ ]* 3.4 Write property test for merge idempotence
    - **Property 3: Merge Idempotence**
    - **Validates: Requirements 3.2**
    - Generate random record sets
    - Assert merging same new records multiple times produces identical results
    - Assert merge operation is commutative for disjoint record sets

  - [x] 3.5 Implement lifecycle management
    - Create src/sync/lifecycle.py with lifecycle management functions
    - Implement mark_expired() to set status=expired for past events
    - Implement mark_removed() to set status=removed and deleted_at for missing future events
    - Ensure lifecycle rules only apply per provider (failed scrapes don't affect other providers)
    - Preserve first_seen_at timestamps when updating lifecycle status
    - _Requirements: 3.5, 3.6, 3.7, 14.1, 14.2, 14.3, 14.4, 14.5, 14.7_

  - [ ]* 3.6 Write property test for lifecycle consistency
    - **Property 4: Lifecycle Consistency**
    - **Validates: Requirements 3.6, 14.2**
    - Generate events with random dates
    - Apply lifecycle rules
    - Assert past events have status=expired
    - Assert future removed events have status=removed

  - [x] 3.7 Create pipeline orchestrator skeleton
    - Create src/pipeline/orchestrator.py with PipelineOrchestrator class
    - Implement run() method to execute pipeline stages in order
    - Support selective provider execution via providers parameter
    - Support skipping optional stages (skip_geocoding, skip_ai_enrichment)
    - Implement run_stage() for individual stage execution
    - Collect metrics from each stage
    - Handle stage failures gracefully with error logging
    - _Requirements: 9.1, 9.2, 9.4, 9.5, 9.6_

  - [x] 3.8 Implement basic CLI with click
    - Create run_pipeline.py as CLI entry point
    - Implement "run" command to execute full pipeline
    - Implement "run --provider <name>" to run specific provider only
    - Implement "export-only" command to regenerate exports without scraping
    - Implement "validate" command to validate canonical store
    - Display progress information during execution
    - Display summary of results on completion
    - _Requirements: 10.1, 10.2, 10.3, 10.4, 10.5, 10.6_

  - [x] 3.9 Write unit tests for merge logic and lifecycle rules
    - Create tests/test_merge_logic.py
    - Test new record insertion
    - Test existing record update detection
    - Test unchanged record preservation
    - Test soft delete for missing records
    - Test lifecycle state transitions
    - Test timestamp updates
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.8, 14.2, 14.3, 14.4, 14.7_

  - [x] 3.10 Write integration tests for store operations
    - Create tests/test_store_integration.py
    - Test save and load operations with dict-keyed JSON files
    - Test filtering by status, provider, date ranges
    - Test handling of missing or corrupted files
    - Test atomic write operations
    - Use temporary directories for test isolation
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.6_

  - [x] 3.11 Write end-to-end pipeline test with mock data
    - Create tests/test_pipeline_e2e.py
    - Create mock scrapers returning known test data
    - Run complete pipeline
    - Assert canonical store contains expected records
    - Assert merge logic works correctly
    - Assert lifecycle rules applied
    - _Requirements: 9.1, 9.2_

- [x] 4. Checkpoint - Validate storage and sync
  - Ensure all tests pass, ask the user if questions arise.


- [x] 5. Phase 3: Exports
  - [x] 5.1 Design CSV schemas for events and locations
    - Define events.csv schema with record_type and record_id columns
    - Define locations.csv schema optimized for map display
    - Document field meanings and formats
    - Plan semicolon-separated list encoding for tags, skills, image URLs
    - Plan null handling (empty strings in CSV)
    - _Requirements: 7.1, 7.2, 7.5_

  - [x] 5.2 Implement CSVExporter for events
    - Create src/export/csv_exporter.py with CSVExporter class
    - Implement export_events() to generate events.csv
    - Include both EventTemplate and EventOccurrence records
    - Add record_type field ("template" or "occurrence")
    - Add record_id field (always contains the real canonical ID)
    - For templates: record_id = event_template_id, event_template_id column is empty
    - For occurrences: record_id = event_id, event_template_id column contains parent reference
    - Filter to active records by default (support filters parameter)
    - Flatten nested data structures for CSV format
    - _Requirements: 7.1, 7.3, 7.5, 16.4_

  - [x] 5.3 Implement field formatting for CSV export
    - Create src/export/formatters.py with formatting functions
    - Implement format_list() to join lists with semicolons
    - Implement format_null() to convert None to empty string
    - Implement format_boolean() to convert bool to "true"/"false"
    - Implement format_datetime() to convert to ISO 8601 format
    - Handle CSV escaping properly (quotes, commas, newlines)
    - _Requirements: 7.4, 7.7_

  - [x] 5.4 Implement CSVExporter for locations
    - Add export_locations() method to CSVExporter
    - Generate locations.csv with geocoded coordinates
    - Include event_count and active_event_count for each location
    - Include event_names as semicolon-separated list (truncated if too long)
    - Include active_event_ids as semicolon-separated list for linking
    - Include provider context for map markers
    - Filter to active locations by default
    - _Requirements: 7.2, 7.6_

  - [x] 5.5 Implement export validation
    - Add validate_export() method to check export completeness
    - Assert all active records appear in export files
    - Assert no duplicate records in exports
    - Assert CSV files are valid and parseable
    - Report validation results
    - _Requirements: 7.8_

  - [ ]* 5.6 Write property test for export completeness
    - **Property 6: Export Completeness**
    - **Validates: Requirements 7.8**
    - Generate random active records
    - Export to CSV
    - Parse CSV back
    - Assert each record appears exactly once

  - [x] 5.7 Write unit tests for CSV formatting
    - Create tests/test_csv_formatting.py
    - Test semicolon-separated list encoding
    - Test null handling
    - Test boolean formatting
    - Test datetime formatting
    - Test CSV escaping (quotes, commas, newlines, special characters)
    - _Requirements: 7.4, 7.7_

  - [x] 5.8 Write integration tests for export completeness
    - Create tests/test_export_integration.py
    - Create test records in canonical store
    - Export to CSV
    - Parse CSV files
    - Assert all active records present
    - Assert record_type and record_id fields correct
    - Assert field formatting correct
    - Test CSV imports in Python csv module
    - _Requirements: 7.1, 7.2, 7.3, 7.5, 7.8_

  - [x] 5.9 Integrate exports into pipeline orchestrator
    - Add export stage to PipelineOrchestrator.run()
    - Call CSVExporter after sync stage
    - Write exports to exports/ directory
    - Include export statistics in pipeline report
    - _Requirements: 9.1_

- [x] 6. Checkpoint - Validate exports
  - Ensure all tests pass, ask the user if questions arise.


- [x] 7. Phase 4: Geocoding (v1.5 - Nice-to-Have)
  - [x] 7.1 Implement Geocoder abstract interface
    - Create src/enrich/geocoder.py with Geocoder ABC
    - Define abstract geocode() method returning GeocodeResult
    - Define GeocodeResult dataclass with lat, lng, status, precision, metadata
    - _Requirements: 4.6_

  - [x] 7.2 Implement MapboxGeocoder
    - Create MapboxGeocoder class implementing Geocoder interface
    - Integrate with Mapbox Geocoding API
    - Read MAPBOX_API_KEY from environment variables
    - Parse API response to extract coordinates and metadata
    - Handle API errors, rate limits, and timeouts gracefully
    - _Requirements: 4.1, 15.1, 15.7_

  - [x] 7.3 Implement address normalization for cache keys
    - Add normalize_address() function for consistent hashing
    - Lowercase, strip whitespace, remove punctuation
    - Compute address_hash for cache key
    - _Requirements: 4.7_

  - [x] 7.4 Implement CachedGeocoder wrapper
    - Create CachedGeocoder class wrapping any Geocoder
    - Implement geocode_location() with cache lookup
    - Check cache before calling underlying geocoder
    - Store results in cache/geocoding/ keyed by address_hash
    - Skip geocoding if address unchanged and valid cached result exists
    - _Requirements: 4.2, 12.1, 12.3, 12.6, 12.7_

  - [ ]* 7.5 Write property test for geocoding cache effectiveness
    - **Property 5: Geocoding Cache Effectiveness**
    - **Validates: Requirements 4.2, 12.3, 12.7**
    - Generate locations with unchanged addresses
    - Mock geocoder to track API calls
    - Assert no API call made when cache hit

  - [x] 7.6 Store geocoding metadata in Location records
    - Update Location model to include geocode_provider, geocode_status, geocode_precision, geocoded_at
    - Set geocode_status to "success", "failed", "invalid_address", or "not_geocoded"
    - Store timestamp of geocoding operation
    - _Requirements: 4.3_

  - [x] 7.7 Integrate geocoding into pipeline orchestrator
    - Add geocoding stage to PipelineOrchestrator.run()
    - Call CachedGeocoder for each location after normalization
    - Support skip_geocoding flag to skip stage
    - Handle geocoding failures gracefully (continue processing)
    - Include geocoding statistics in pipeline report
    - _Requirements: 4.4, 4.5, 9.5_

  - [x] 7.8 Write unit tests for geocoder
    - Create tests/test_geocoder.py
    - Test MapboxGeocoder with mock API responses
    - Test address normalization
    - Test cache key generation
    - Test error handling (API failures, invalid addresses)
    - _Requirements: 4.1, 4.3, 4.4, 4.7_

  - [x] 7.9 Write integration tests for geocoding cache
    - Create tests/test_geocoding_cache.py
    - Test cache hit/miss behavior
    - Test cache invalidation when address changes
    - Test that unchanged addresses use cached results
    - Use temporary cache directory for test isolation
    - _Requirements: 4.2, 12.1, 12.3, 12.4, 12.7_

  - [x] 7.10 Update locations.csv export with coordinates
    - Add latitude and longitude columns to locations.csv
    - Add geocode_status column
    - Handle null coordinates for failed geocoding
    - _Requirements: 7.2_

- [x] 8. Checkpoint - Validate geocoding
  - Ensure all tests pass, ask the user if questions arise.


- [~] 9. Phase 5: AI Enrichment (v1.5 - Nice-to-Have)
  - [x] 9.1 Implement AIEnricher with LLM client integration
    - Create src/enrich/ai_enricher.py with AIEnricher class
    - Integrate with OpenAI API (or Anthropic as alternative)
    - Read OPENAI_API_KEY from environment variables
    - Implement enrich_event() to enhance event descriptions and extract metadata
    - Handle LLM errors and timeouts gracefully
    - _Requirements: 5.1, 5.2, 15.1, 15.7_

  - [x] 9.2 Create prompt templates with ROSTA tone guidelines
    - Create src/enrich/prompts.py with prompt templates
    - Implement _build_prompt() to generate enrichment prompts
    - Include ROSTA brand tone guidelines (modern, confident, curated, minimal, friendly)
    - Request structured JSON output with description and metadata fields
    - Instruct LLM to never invent facts not in source data
    - _Requirements: 5.1_

  - [x] 9.3 Implement HTML cleaning before enrichment
    - Add clean_html() function to strip HTML tags from descriptions
    - Normalize whitespace and handle null values
    - Preserve meaningful structure (paragraphs, lists)
    - Store cleaned description in description_clean field
    - _Requirements: 5.6_

  - [x] 9.4 Implement structured metadata extraction
    - Implement _parse_response() to parse LLM JSON output
    - Extract tags, occasion_tags, skills_required, skills_created
    - Extract age_min, age_max, audience, family_friendly, beginner_friendly
    - Extract summary_short (~50 chars) and summary_medium (~150 chars)
    - Validate extracted metadata for consistency
    - _Requirements: 5.2, 5.9_

  - [x] 9.5 Implement enrichment caching
    - Cache enrichments in cache/ai/ keyed by source_hash + prompt_version + model
    - Check cache before calling LLM API
    - Only re-enrich when source content or prompt changes
    - Preserve original raw descriptions alongside AI-enhanced versions
    - _Requirements: 5.3, 5.7, 5.8, 12.2, 12.3, 12.5, 12.7_

  - [x] 9.6 Integrate AI enrichment into pipeline orchestrator
    - Add AI enrichment stage to PipelineOrchestrator.run()
    - Call AIEnricher for each event after normalization
    - Support skip_ai_enrichment flag to skip stage
    - Handle enrichment failures gracefully (preserve clean description)
    - Include enrichment statistics in pipeline report
    - _Requirements: 5.4, 5.5, 9.5_

  - [x] 9.7 Write unit tests for AI enricher
    - Create tests/test_ai_enricher.py
    - Test prompt building with ROSTA tone guidelines
    - Test response parsing with mock LLM outputs
    - Test HTML cleaning
    - Test metadata validation
    - Test error handling (API failures, invalid JSON)
    - _Requirements: 5.1, 5.2, 5.4, 5.6, 5.9_

  - [x] 9.8 Write integration tests for enrichment caching
    - Create tests/test_ai_cache.py
    - Test cache hit/miss behavior
    - Test cache invalidation when source changes
    - Test cache key includes prompt version and model
    - Use temporary cache directory for test isolation
    - _Requirements: 5.3, 12.2, 12.3, 12.5, 12.7_

  - [x] 9.9 Update events.csv export with AI-enriched fields
    - Add description_ai, summary_short, summary_medium columns
    - Add tags, occasion_tags, skills_required, skills_created columns (semicolon-separated)
    - Add audience, family_friendly, beginner_friendly columns
    - Handle null values for events without enrichment
    - _Requirements: 7.1_

- [x] 10. Checkpoint - Validate AI enrichment
  - Ensure all tests pass, ask the user if questions arise.


- [~] 11. Phase 6: Validation and Reporting
  - [~] 11.1 Implement comprehensive validation rules
    - Create src/pipeline/validator.py with validation functions
    - Implement validate_provider() for Provider records
    - Implement validate_location() for Location records
    - Implement validate_event_template() for EventTemplate records
    - Implement validate_event_occurrence() for EventOccurrence records
    - Check required fields, field formats, constraints, and referential integrity
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5_

  - [ ]* 11.2 Write property test for referential integrity
    - **Property 7: Referential Integrity**
    - **Validates: Requirements 6.5, 13.6**
    - Load all events and locations from store
    - For each event with location_id, assert location exists
    - For each event with provider_id, assert provider exists

  - [~] 11.3 Implement run report generation
    - Add generate_report() method to PipelineOrchestrator
    - Collect statistics from each stage (extraction, normalization, sync, export)
    - Include counts: records inserted, updated, unchanged, removed, expired
    - Include validation summary with error counts
    - Include timing information for each stage
    - Format report as structured text or JSON
    - _Requirements: 9.3, 9.7_

  - [~] 11.4 Implement logging throughout pipeline
    - Add logging to all pipeline stages
    - Log progress indicators for long-running operations
    - Log errors with detailed context (provider, record ID, error message)
    - Write logs to logs/ directory with timestamps
    - Use appropriate log levels (DEBUG, INFO, WARNING, ERROR)
    - _Requirements: 9.6, 11.1_

  - [~] 11.5 Add validation summary to run report
    - Include validation errors in pipeline report
    - Group errors by record type and validation rule
    - Include sample invalid records for debugging
    - Report validation error counts per provider
    - _Requirements: 6.6, 6.7, 9.3_

  - [~] 11.6 Implement error handling for all failure scenarios
    - Handle scraper failures (log error, continue with other providers)
    - Handle geocoding API failures (mark failed, continue processing)
    - Handle AI enrichment failures (preserve clean description, continue)
    - Handle validation failures (log error, skip invalid record)
    - Handle store corruption (attempt archive recovery)
    - Handle export failures (halt execution, report failure)
    - _Requirements: 11.1, 11.2, 11.3, 11.4, 11.5, 11.6, 11.7_

  - [~] 11.7 Write unit tests for validation rules
    - Create tests/test_validation.py
    - Test required field validation
    - Test field format validation (email, URL, ISO codes)
    - Test constraint validation (age_min <= age_max, start_at < end_at)
    - Test referential integrity checks
    - Test validation error reporting
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6_

  - [~] 11.8 Write integration tests for error handling
    - Create tests/test_error_handling.py
    - Test scraper failure handling
    - Test geocoding failure handling
    - Test AI enrichment failure handling
    - Test validation failure handling
    - Test store corruption recovery
    - Assert pipeline continues gracefully after errors
    - _Requirements: 11.1, 11.2, 11.3, 11.4, 11.5_

- [~] 12. Checkpoint - Validate error handling and reporting
  - Ensure all tests pass, ask the user if questions arise.


- [~] 13. Phase 7: Documentation and Polish
  - [~] 13.1 Write comprehensive README
    - Document project overview and architecture
    - Document installation and setup instructions
    - Document how to run the pipeline (CLI commands)
    - Document directory structure and file organization
    - Document how to add new provider scrapers
    - Include troubleshooting guide for common issues
    - _Requirements: 10.1, 10.2, 10.3, 10.4_

  - [~] 13.2 Document environment variables and configuration
    - Create .env.example with all required environment variables
    - Document MAPBOX_API_KEY for geocoding (optional for v1)
    - Document OPENAI_API_KEY or ANTHROPIC_API_KEY for AI enrichment (optional for v1)
    - Document configuration options for pipeline behavior
    - Explain how to enable/disable optional stages
    - _Requirements: 15.1, 15.2, 15.3, 15.4, 15.6, 15.7_

  - [~] 13.3 Document CSV schemas and field meanings
    - Document events.csv schema with all columns
    - Document locations.csv schema with all columns
    - Explain record_type and record_id fields
    - Explain semicolon-separated list encoding
    - Explain null handling and date formats
    - Provide example CSV rows
    - _Requirements: 7.1, 7.2, 7.4, 7.5_

  - [~] 13.4 Document sync behavior and status values
    - Explain incremental sync and change detection
    - Document lifecycle status values (active, expired, removed, cancelled)
    - Document availability status values (available, sold_out, limited, unknown)
    - Explain soft delete behavior
    - Explain how merge logic works
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 14.1, 14.2, 14.3, 14.4, 14.5, 14.6_

  - [~] 13.5 Document event-location relationship management
    - Explain how events are linked to locations
    - Document location_scope field for templates
    - Explain when to use EventTemplate vs EventOccurrence
    - Document the rule: never link events to all locations without evidence
    - Explain how to handle providers without location data
    - _Requirements: 13.1, 13.2, 13.3, 13.4, 13.5, 13.6, 13.7, 16.1, 16.2, 16.3, 16.4, 16.5, 16.6, 16.7_

  - [~] 13.6 Add inline code documentation
    - Add docstrings to all classes and public methods
    - Add type hints to all function signatures
    - Add comments for complex logic
    - Document assumptions and limitations
    - Document error handling behavior
    - _Requirements: All_

  - [~] 13.7 Create migration guide from old format
    - Document differences between old flat JSON and new canonical store
    - Explain how to validate migration (compare record counts)
    - Document breaking changes (output format, field names)
    - Provide data mapping table (old field → new field)
    - Explain cutover process
    - _Requirements: All_

  - [~] 13.8 Add example output files to documentation
    - Include example events.csv with sample rows
    - Include example locations.csv with sample rows
    - Include example canonical JSON files
    - Include example pipeline run report
    - _Requirements: 7.1, 7.2, 9.3_

  - [~] 13.9 Create requirements.txt files
    - Create requirements.txt with core dependencies (requests, beautifulsoup4, lxml, pydantic, python-dotenv, click)
    - Create requirements-dev.txt with development dependencies (pytest, pytest-cov, black, ruff, mypy)
    - Create requirements-optional.txt with optional dependencies (openai, anthropic, hypothesis, httpx)
    - Pin dependency versions for reproducibility
    - _Requirements: 15.1_

  - [~] 13.10 Final code review and cleanup
    - Review all code for consistency and best practices
    - Remove debug print statements and commented code
    - Ensure consistent naming conventions
    - Ensure consistent error handling patterns
    - Run linter (ruff) and formatter (black)
    - Run type checker (mypy)
    - Ensure all tests pass
    - _Requirements: All_

- [~] 14. Final checkpoint - Complete validation
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional property-based tests (v1.5 nice-to-have) and can be skipped for faster v1 MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation at phase boundaries
- Property tests validate universal correctness properties using Hypothesis
- Unit tests validate specific examples and edge cases
- Integration tests validate end-to-end behavior
- Phase 4 (Geocoding) and Phase 5 (AI Enrichment) are v1.5 enhancements and can be deferred
- Focus on v1 deliverables: canonical models, deterministic IDs, normalized locations, persistent store, merge logic, CSV exports, validation, and CLI
