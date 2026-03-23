"""Pipeline orchestrator for coordinating all pipeline stages.

This module orchestrates the complete pipeline execution:
- Extract: Run provider scrapers
- Normalize: Transform to canonical models
- Enrich: Geocode locations and AI enrich events (optional)
- Sync: Merge with existing store
- Export: Generate CSV exports

The orchestrator handles stage failures gracefully, collects metrics,
and generates comprehensive run reports.
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from src.storage.store import CanonicalStore
from src.sync.merge_engine import MergeEngine, MergeResult
from src.sync.lifecycle import mark_expired, mark_removed
from src.transform.normalizer import Normalizer
from src.models.provider import Provider
from src.models.location import Location
from src.models.event_template import EventTemplate
from src.models.event_occurrence import EventOccurrence


logger = logging.getLogger(__name__)


@dataclass
class StageResult:
    """Result of a single pipeline stage execution."""
    
    stage_name: str
    success: bool
    duration_seconds: float
    metrics: dict[str, Any]
    error: str | None = None
    
    def __str__(self) -> str:
        """String representation of stage result."""
        status = "SUCCESS" if self.success else "FAILED"
        return (
            f"StageResult({self.stage_name}: {status}, "
            f"duration={self.duration_seconds:.2f}s, metrics={self.metrics})"
        )


@dataclass
class PipelineReport:
    """Comprehensive report of pipeline execution."""
    
    run_id: str
    start_time: datetime
    end_time: datetime
    duration_seconds: float
    success: bool
    providers_processed: list[str]
    providers_failed: list[str]
    stage_results: dict[str, StageResult]
    total_metrics: dict[str, Any]
    errors: list[str]
    
    def __str__(self) -> str:
        """String representation of pipeline report."""
        status = "SUCCESS" if self.success else "FAILED"
        return (
            f"PipelineReport(run_id={self.run_id}, status={status}, "
            f"duration={self.duration_seconds:.2f}s, "
            f"providers={len(self.providers_processed)}, "
            f"failed={len(self.providers_failed)})"
        )


class PipelineOrchestrator:
    """Orchestrates the complete pipeline execution."""
    
    def __init__(
        self,
        store: CanonicalStore | None = None,
        merge_engine: MergeEngine | None = None,
        normalizer: Normalizer | None = None
    ):
        """Initialize pipeline orchestrator.
        
        Args:
            store: Canonical store instance (defaults to new instance)
            merge_engine: Merge engine instance (defaults to new instance)
            normalizer: Normalizer instance (defaults to new instance)
        """
        self.store = store or CanonicalStore()
        self.merge_engine = merge_engine or MergeEngine()
        self.normalizer = normalizer or Normalizer()
    
    def run(
        self,
        providers: list[str] | None = None,
        skip_geocoding: bool = False,
        skip_ai_enrichment: bool = False
    ) -> PipelineReport:
        """Execute full pipeline with optional stage skipping.
        
        Runs all pipeline stages in order:
        1. Extract: Run provider scrapers
        2. Normalize: Transform to canonical models
        3. Enrich: Geocode locations and AI enrich events (optional)
        4. Sync: Merge with existing store
        5. Export: Generate CSV exports
        
        Args:
            providers: List of provider slugs to process (None = all providers)
            skip_geocoding: Skip geocoding stage if True
            skip_ai_enrichment: Skip AI enrichment stage if True
        
        Returns:
            PipelineReport with execution summary and metrics
        
        Example:
            >>> orchestrator = PipelineOrchestrator()
            >>> report = orchestrator.run(providers=["pasta-evangelists"])
            >>> print(report)
            PipelineReport(run_id=..., status=SUCCESS, ...)
        """
        # Generate run ID
        run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        start_time = datetime.now(timezone.utc)
        
        logger.info(f"Starting pipeline run {run_id}")
        logger.info(f"Providers: {providers or 'all'}")
        logger.info(f"Skip geocoding: {skip_geocoding}")
        logger.info(f"Skip AI enrichment: {skip_ai_enrichment}")
        
        # Track results
        stage_results: dict[str, StageResult] = {}
        providers_processed: list[str] = []
        providers_failed: list[str] = []
        errors: list[str] = []
        
        try:
            # Stage 1: Extract
            logger.info("Stage 1: Extract - Running provider scrapers")
            extract_result = self.run_stage(
                "extract",
                providers=providers
            )
            stage_results["extract"] = extract_result
            
            if not extract_result.success:
                errors.append(f"Extract stage failed: {extract_result.error}")
                # Continue with other stages if partial data available
            
            # Stage 2: Normalize
            logger.info("Stage 2: Normalize - Transforming to canonical models")
            normalize_result = self.run_stage(
                "normalize",
                raw_data=extract_result.metrics.get("raw_data", [])
            )
            stage_results["normalize"] = normalize_result
            
            if not normalize_result.success:
                errors.append(f"Normalize stage failed: {normalize_result.error}")
                # Cannot continue without normalized data
                raise Exception("Normalize stage failed, cannot continue")
            
            # Stage 3: Enrich (optional)
            enriched_locations = normalize_result.metrics.get("locations", [])
            enriched_events = normalize_result.metrics.get("events", [])
            
            if not skip_geocoding or not skip_ai_enrichment:
                logger.info("Stage 3: Enrich - Geocoding and AI enrichment")
                enrich_result = self.run_stage(
                    "enrich",
                    locations=enriched_locations,
                    events=enriched_events,
                    skip_geocoding=skip_geocoding,
                    skip_ai_enrichment=skip_ai_enrichment
                )
                stage_results["enrich"] = enrich_result
                
                if not enrich_result.success:
                    errors.append(f"Enrich stage failed: {enrich_result.error}")
                    # Continue with sync even if enrichment fails
                else:
                    # Use enriched data for sync stage
                    enriched_locations = enrich_result.metrics.get("locations", enriched_locations)
                    enriched_events = enrich_result.metrics.get("events", enriched_events)
            else:
                logger.info("Stage 3: Enrich - Skipped (all enrichment disabled)")
            
            # Stage 4: Sync
            logger.info("Stage 4: Sync - Merging with existing store")
            sync_result = self.run_stage(
                "sync",
                providers=normalize_result.metrics.get("providers", []),
                locations=enriched_locations,
                events=enriched_events
            )
            stage_results["sync"] = sync_result
            
            if not sync_result.success:
                errors.append(f"Sync stage failed: {sync_result.error}")
                raise Exception("Sync stage failed, cannot continue")
            
            # Stage 5: Export
            logger.info("Stage 5: Export - Generating CSV exports")
            export_result = self.run_stage("export")
            stage_results["export"] = export_result
            
            if not export_result.success:
                errors.append(f"Export stage failed: {export_result.error}")
                raise Exception("Export stage failed")
            
            # Track processed providers
            providers_processed = normalize_result.metrics.get("provider_ids", [])
            
        except Exception as e:
            logger.error(f"Pipeline run {run_id} failed: {e}")
            errors.append(str(e))
        
        # Generate report
        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()
        
        # Aggregate metrics
        total_metrics = self._aggregate_metrics(stage_results)
        
        # Determine overall success
        success = len(errors) == 0 and all(
            result.success for result in stage_results.values()
        )
        
        report = PipelineReport(
            run_id=run_id,
            start_time=start_time,
            end_time=end_time,
            duration_seconds=duration,
            success=success,
            providers_processed=providers_processed,
            providers_failed=providers_failed,
            stage_results=stage_results,
            total_metrics=total_metrics,
            errors=errors
        )
        
        logger.info(f"Pipeline run {run_id} completed: {report}")
        
        return report
    
    def run_stage(self, stage_name: str, **kwargs: Any) -> StageResult:
        """Execute a single pipeline stage.
        
        Handles stage-specific logic and error handling. Collects metrics
        and timing information for reporting.
        
        Args:
            stage_name: Name of stage to execute (extract, normalize, enrich, sync, export)
            **kwargs: Stage-specific parameters
        
        Returns:
            StageResult with execution summary and metrics
        
        Example:
            >>> orchestrator = PipelineOrchestrator()
            >>> result = orchestrator.run_stage("extract", providers=["pasta-evangelists"])
            >>> print(result)
            StageResult(extract: SUCCESS, ...)
        """
        start_time = datetime.now(timezone.utc)
        
        try:
            logger.info(f"Running stage: {stage_name}")
            
            # Route to stage-specific handler
            if stage_name == "extract":
                metrics = self._run_extract_stage(**kwargs)
            elif stage_name == "normalize":
                metrics = self._run_normalize_stage(**kwargs)
            elif stage_name == "enrich":
                metrics = self._run_enrich_stage(**kwargs)
            elif stage_name == "sync":
                metrics = self._run_sync_stage(**kwargs)
            elif stage_name == "export":
                metrics = self._run_export_stage(**kwargs)
            else:
                raise ValueError(f"Unknown stage: {stage_name}")
            
            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()
            
            logger.info(f"Stage {stage_name} completed successfully in {duration:.2f}s")
            
            return StageResult(
                stage_name=stage_name,
                success=True,
                duration_seconds=duration,
                metrics=metrics
            )
            
        except Exception as e:
            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()
            
            error_msg = f"Stage {stage_name} failed: {e}"
            logger.error(error_msg, exc_info=True)
            
            return StageResult(
                stage_name=stage_name,
                success=False,
                duration_seconds=duration,
                metrics={},
                error=str(e)
            )
    
    def _run_extract_stage(self, **kwargs: Any) -> dict[str, Any]:
        """Run extraction stage - scrape provider data.
        
        Args:
            providers: Optional list of provider slugs to scrape
        
        Returns:
            Metrics dict with raw_data list
        """
        from src.extract.pasta_evangelists import PastaEvangelistsScraper
        from src.extract.caravan_coffee import CaravanCoffeeScraper
        from src.extract.comptoir_bakery import ComptoirBakeryScraper
        
        # Map of provider slugs to scraper classes
        SCRAPER_REGISTRY = {
            "pasta-evangelists": PastaEvangelistsScraper,
            "caravan-coffee": CaravanCoffeeScraper,
            "comptoir-bakery": ComptoirBakeryScraper,
        }
        
        # Determine which providers to scrape
        provider_filter = kwargs.get("providers")
        if provider_filter:
            # Filter to requested providers
            scrapers_to_run = {
                slug: scraper_cls
                for slug, scraper_cls in SCRAPER_REGISTRY.items()
                if slug in provider_filter
            }
            if not scrapers_to_run:
                logger.warning(f"No scrapers found for providers: {provider_filter}")
        else:
            # Run all scrapers
            scrapers_to_run = SCRAPER_REGISTRY
        
        # Execute scrapers
        raw_data_list = []
        providers_scraped = 0
        providers_failed = 0
        failed_providers = []
        
        for slug, scraper_cls in scrapers_to_run.items():
            try:
                logger.info(f"Running scraper: {slug}")
                scraper = scraper_cls()
                raw_data = scraper.scrape()
                raw_data_list.append(raw_data)
                providers_scraped += 1
                logger.info(
                    f"Successfully scraped {slug}: "
                    f"{len(raw_data.raw_locations)} locations, "
                    f"{len(raw_data.raw_events)} events, "
                    f"{len(raw_data.raw_templates)} templates"
                )
            except Exception as e:
                providers_failed += 1
                failed_providers.append(slug)
                logger.error(f"Failed to scrape {slug}: {e}", exc_info=True)
                # Continue with other providers
        
        logger.info(
            f"Extract stage complete: {providers_scraped} succeeded, "
            f"{providers_failed} failed"
        )
        
        return {
            "raw_data": raw_data_list,
            "providers_scraped": providers_scraped,
            "providers_failed": providers_failed,
            "failed_providers": failed_providers,
            "raw_records_count": sum(
                len(rd.raw_locations) + len(rd.raw_events) + len(rd.raw_templates)
                for rd in raw_data_list
            )
        }
    
    def _run_normalize_stage(self, **kwargs: Any) -> dict[str, Any]:
        """Run normalization stage - transform to canonical models.
        
        Args:
            raw_data: List of RawProviderData from scrapers
        
        Returns:
            Metrics dict with providers, locations, events lists
        """
        raw_data_list = kwargs.get("raw_data", [])
        
        if not raw_data_list:
            logger.warning("No raw data to normalize")
            return {
                "providers": [],
                "locations": [],
                "events": [],
                "provider_ids": [],
                "providers_normalized": 0,
                "locations_normalized": 0,
                "templates_normalized": 0,
                "occurrences_normalized": 0
            }
        
        # Normalize each provider's data
        all_providers = []
        all_locations = []
        all_events = []
        provider_ids = []
        
        for raw_data in raw_data_list:
            try:
                logger.info(f"Normalizing data for {raw_data.provider_name}")
                
                # Normalize provider
                provider = self.normalizer.normalize_provider(raw_data)
                all_providers.append(provider)
                provider_ids.append(provider.provider_id)
                
                # Normalize locations
                locations = self.normalizer.normalize_locations(raw_data, provider.provider_id)
                all_locations.extend(locations)
                
                # Build location map for event linking
                # Map formatted_address -> location_id
                location_map = {
                    loc.formatted_address: loc.location_id
                    for loc in locations
                    if loc.formatted_address
                }
                
                # Also map by location_name if available
                for loc in locations:
                    if loc.location_name:
                        location_map[loc.location_name] = loc.location_id
                
                # Normalize events (templates and occurrences)
                events = self.normalizer.normalize_events(
                    raw_data, provider.provider_id, location_map
                )
                all_events.extend(events)
                
                logger.info(
                    f"Normalized {raw_data.provider_name}: "
                    f"{len(locations)} locations, {len(events)} events"
                )
                
            except Exception as e:
                logger.error(
                    f"Failed to normalize {raw_data.provider_name}: {e}",
                    exc_info=True
                )
                # Continue with other providers
        
        # Count templates vs occurrences
        templates = [e for e in all_events if isinstance(e, EventTemplate)]
        occurrences = [e for e in all_events if isinstance(e, EventOccurrence)]
        
        logger.info(
            f"Normalize stage complete: {len(all_providers)} providers, "
            f"{len(all_locations)} locations, {len(templates)} templates, "
            f"{len(occurrences)} occurrences"
        )
        
        return {
            "providers": all_providers,
            "locations": all_locations,
            "events": all_events,
            "provider_ids": provider_ids,
            "providers_normalized": len(all_providers),
            "locations_normalized": len(all_locations),
            "templates_normalized": len(templates),
            "occurrences_normalized": len(occurrences)
        }
    
    def _run_enrich_stage(self, **kwargs: Any) -> dict[str, Any]:
        """Run enrichment stage - geocode and AI enrich.

        Args:
            locations: List of Location records
            events: List of Event records
            skip_geocoding: Skip geocoding if True
            skip_ai_enrichment: Skip AI enrichment if True

        Returns:
            Metrics dict with enrichment statistics
        """
        locations = kwargs.get("locations", [])
        events = kwargs.get("events", [])
        skip_geocoding = kwargs.get("skip_geocoding", False)
        skip_ai_enrichment = kwargs.get("skip_ai_enrichment", False)

        geocoding_stats = {
            "locations_processed": 0,
            "locations_geocoded": 0,
            "geocoding_success": 0,
            "geocoding_failed": 0,
            "geocoding_cached": 0,
            "geocoding_skipped": skip_geocoding
        }

        ai_enrichment_stats = {
            "events_processed": 0,
            "events_enriched": 0,
            "enrichment_success": 0,
            "enrichment_failed": 0,
            "enrichment_cached": 0,
            "ai_enrichment_skipped": skip_ai_enrichment
        }

        # Geocoding stage
        if not skip_geocoding and locations:
            logger.info(f"Geocoding {len(locations)} locations")

            try:
                # Initialize geocoder - try Mapbox first, fallback to Nominatim
                from src.enrich.cached_geocoder import CachedGeocoder
                
                geocoder = None
                geocoder_name = None
                
                # Try Mapbox first (commercial, more accurate)
                try:
                    from src.enrich.mapbox_geocoder import MapboxGeocoder
                    geocoder = MapboxGeocoder()
                    geocoder_name = "Mapbox"
                    logger.info("Using Mapbox geocoder (commercial)")
                except ValueError:
                    # Mapbox API key not available, try Nominatim
                    logger.info("Mapbox API key not found, trying Nominatim")
                
                # Fallback to Nominatim (free, open source)
                if geocoder is None:
                    from src.enrich.nominatim_geocoder import NominatimGeocoder
                    geocoder = NominatimGeocoder()
                    geocoder_name = "Nominatim"
                    logger.info("Using Nominatim geocoder (free, open source)")

                # Wrap with caching
                cached_geocoder = CachedGeocoder(geocoder)
                logger.info(f"Geocoder initialized: {geocoder_name}")

                # Geocode each location
                for location in locations:
                    geocoding_stats["locations_processed"] += 1

                    try:
                        # Check if already geocoded with unchanged address
                        if (location.geocode_status == "success" and 
                            location.address_hash and 
                            location.latitude is not None):
                            # Already geocoded and address unchanged - skip
                            geocoding_stats["geocoding_cached"] += 1
                            logger.debug(f"Skipping already geocoded location: {location.location_id}")
                            continue

                        # Geocode location (uses cache internally)
                        logger.debug(f"Geocoding location: {location.location_id}")
                        updated_location = cached_geocoder.geocode_location(location)

                        # Update location in list
                        location_index = locations.index(location)
                        locations[location_index] = updated_location

                        # Track statistics
                        geocoding_stats["locations_geocoded"] += 1

                        if updated_location.geocode_status == "success":
                            geocoding_stats["geocoding_success"] += 1
                            logger.debug(
                                f"Successfully geocoded {location.location_id}: "
                                f"({updated_location.latitude}, {updated_location.longitude})"
                            )
                        else:
                            geocoding_stats["geocoding_failed"] += 1
                            logger.warning(
                                f"Failed to geocode {location.location_id}: "
                                f"status={updated_location.geocode_status}"
                            )

                    except Exception as e:
                        # Handle individual location geocoding errors gracefully
                        geocoding_stats["geocoding_failed"] += 1
                        logger.error(
                            f"Error geocoding location {location.location_id}: {e}",
                            exc_info=True
                        )
                        # Continue processing other locations
                        continue

                logger.info(
                    f"Geocoding complete: {geocoding_stats['geocoding_success']} succeeded, "
                    f"{geocoding_stats['geocoding_failed']} failed, "
                    f"{geocoding_stats['geocoding_cached']} cached"
                )

            except ValueError as e:
                # API key missing or invalid configuration
                logger.error(f"Geocoding configuration error: {e}")
                logger.warning("Skipping geocoding stage due to configuration error")
                geocoding_stats["geocoding_skipped"] = True

            except Exception as e:
                # Geocoding API unavailable or other critical error
                logger.error(f"Geocoding stage error: {e}", exc_info=True)
                logger.warning("Continuing pipeline execution despite geocoding error")
                # Don't fail the entire stage - continue with other enrichment

        else:
            if skip_geocoding:
                logger.info("Geocoding skipped (skip_geocoding=True)")
            else:
                logger.info("No locations to geocode")

        # AI enrichment stage
        if not skip_ai_enrichment and events:
            logger.info(f"AI enrichment for {len(events)} events")

            try:
                # Initialize AI enricher
                from src.enrich.ai_enricher import AIEnricher

                # Create AI enricher (will raise ValueError if API key missing)
                ai_enricher = AIEnricher()

                # Enrich each event
                for i, event in enumerate(events):
                    ai_enrichment_stats["events_processed"] += 1

                    try:
                        # Check if event already has AI enrichment
                        original_description_ai = getattr(event, 'description_ai', None)
                        
                        # Enrich event (uses cache internally)
                        logger.debug(f"Enriching event: {event.title}")
                        enriched_event = ai_enricher.enrich_event(event)

                        # Update event in list
                        events[i] = enriched_event

                        # Track statistics
                        ai_enrichment_stats["events_enriched"] += 1
                        
                        # Check if enrichment was successful (has AI description)
                        if hasattr(enriched_event, 'description_ai') and enriched_event.description_ai:
                            ai_enrichment_stats["enrichment_success"] += 1
                            logger.debug(f"Successfully enriched event: {event.title}")
                        else:
                            # Event was processed but no AI description added (e.g., no clean description)
                            logger.debug(f"Event processed but not enriched: {event.title}")

                    except Exception as e:
                        # Handle individual event enrichment errors gracefully
                        ai_enrichment_stats["enrichment_failed"] += 1
                        logger.error(
                            f"Error enriching event {event.title}: {e}",
                            exc_info=True
                        )
                        # Continue processing other events - preserve original description
                        continue

                logger.info(
                    f"AI enrichment complete: {ai_enrichment_stats['enrichment_success']} succeeded, "
                    f"{ai_enrichment_stats['enrichment_failed']} failed, "
                    f"{ai_enrichment_stats['events_enriched']} total processed"
                )

            except ValueError as e:
                # API key missing or invalid configuration
                logger.error(f"AI enrichment configuration error: {e}")
                logger.warning("Skipping AI enrichment stage due to configuration error")
                ai_enrichment_stats["ai_enrichment_skipped"] = True

            except Exception as e:
                # AI API unavailable or other critical error
                logger.error(f"AI enrichment stage error: {e}", exc_info=True)
                logger.warning("Continuing pipeline execution despite AI enrichment error")
                # Don't fail the entire stage - continue with export

        else:
            if skip_ai_enrichment:
                logger.info("AI enrichment skipped (skip_ai_enrichment=True)")
            else:
                logger.info("No events to enrich")

        # Combine statistics
        return {
            **geocoding_stats,
            **ai_enrichment_stats,
            "locations": locations,  # Return enriched locations
            "events": events  # Return enriched events
        }
    
    def _run_sync_stage(self, **kwargs: Any) -> dict[str, Any]:
        """Run sync stage - merge with existing store.
        
        Args:
            providers: List of Provider records
            locations: List of Location records
            events: List of Event records
        
        Returns:
            Metrics dict with merge statistics
        """
        new_providers = kwargs.get("providers", [])
        new_locations = kwargs.get("locations", [])
        new_events = kwargs.get("events", [])
        
        if not new_providers:
            logger.warning("No providers to sync")
            return {
                "providers": {"inserted": 0, "updated": 0, "unchanged": 0, "total": 0},
                "locations": {"inserted": 0, "updated": 0, "unchanged": 0, "total": 0},
                "templates": {"inserted": 0, "updated": 0, "unchanged": 0, "total": 0},
                "occurrences": {"inserted": 0, "updated": 0, "unchanged": 0, "total": 0}
            }
        
        # Load existing records
        existing_providers = self.store.load_providers()
        existing_locations = self.store.load_locations()
        existing_events = self.store.load_events()
        
        # Merge providers
        merged_providers, provider_result = self.merge_engine.merge_records(
            new_providers, existing_providers, "provider"
        )
        
        # Merge locations
        merged_locations, location_result = self.merge_engine.merge_records(
            new_locations, existing_locations, "location"
        )
        
        # Separate templates and occurrences
        new_templates = [e for e in new_events if isinstance(e, EventTemplate)]
        new_occurrences = [e for e in new_events if isinstance(e, EventOccurrence)]
        
        existing_templates = [e for e in existing_events if isinstance(e, EventTemplate)]
        existing_occurrences = [e for e in existing_events if isinstance(e, EventOccurrence)]
        
        # Merge templates
        merged_templates, template_result = self.merge_engine.merge_records(
            new_templates, existing_templates, "event_template"
        )
        
        # Merge occurrences
        merged_occurrences, occurrence_result = self.merge_engine.merge_records(
            new_occurrences, existing_occurrences, "event_occurrence"
        )
        
        # Apply lifecycle rules to occurrences
        now = datetime.now(timezone.utc)
        
        # Mark expired events (all providers)
        merged_occurrences = mark_expired(merged_occurrences, now)
        
        # Mark removed events per provider (only for successfully scraped providers)
        # This ensures failed scrapes don't mark existing records as removed
        for provider in new_providers:
            provider_id = provider.provider_id
            
            # Get new record IDs for this provider
            new_template_ids = {
                t.event_template_id for t in new_templates
                if t.provider_id == provider_id
            }
            new_occurrence_ids = {
                o.event_id for o in new_occurrences
                if o.provider_id == provider_id
            }
            new_location_ids = {
                loc.location_id for loc in new_locations
                if loc.provider_id == provider_id
            }
            
            # Mark removed templates for this provider
            merged_templates = mark_removed(
                merged_templates, new_template_ids, provider_id, now
            )
            
            # Mark removed occurrences for this provider
            merged_occurrences = mark_removed(
                merged_occurrences, new_occurrence_ids, provider_id, now
            )
            
            # Mark removed locations for this provider
            merged_locations = mark_removed(
                merged_locations, new_location_ids, provider_id, now
            )
        
        # Combine events
        merged_events = merged_templates + merged_occurrences
        
        # Save to store
        self.store.save_providers(merged_providers)
        self.store.save_locations(merged_locations)
        self.store.save_events(merged_events)
        
        logger.info(f"Sync complete: {provider_result}, {location_result}, "
                   f"{template_result}, {occurrence_result}")
        
        return {
            "providers": {
                "inserted": provider_result.inserted,
                "updated": provider_result.updated,
                "unchanged": provider_result.unchanged,
                "total": provider_result.total
            },
            "locations": {
                "inserted": location_result.inserted,
                "updated": location_result.updated,
                "unchanged": location_result.unchanged,
                "total": location_result.total
            },
            "templates": {
                "inserted": template_result.inserted,
                "updated": template_result.updated,
                "unchanged": template_result.unchanged,
                "total": template_result.total
            },
            "occurrences": {
                "inserted": occurrence_result.inserted,
                "updated": occurrence_result.updated,
                "unchanged": occurrence_result.unchanged,
                "total": occurrence_result.total
            }
        }
    
    def _run_export_stage(self, **kwargs: Any) -> dict[str, Any]:
        """Run export stage - generate CSV exports.
        
        Returns:
            Metrics dict with export statistics
        """
        from pathlib import Path
        from src.export.csv_exporter import CSVExporter
        
        # Create exports directory if it doesn't exist
        exports_dir = Path("exports")
        exports_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize CSV exporter
        exporter = CSVExporter(self.store)
        
        # Export events to CSV
        events_result = exporter.export_events(
            output_path=str(exports_dir / "events.csv"),
            filters={"status": "active"}
        )
        
        logger.info(
            f"Exported {events_result['total_records']} events "
            f"({events_result['template_count']} templates, "
            f"{events_result['occurrence_count']} occurrences) "
            f"to {events_result['output_file']}"
        )
        
        # Export locations to CSV
        locations_result = exporter.export_locations(
            output_path=str(exports_dir / "locations.csv"),
            filters={"status": "active"}
        )
        
        logger.info(
            f"Exported {locations_result['total_records']} locations "
            f"({locations_result['with_coordinates']} with coordinates) "
            f"to {locations_result['output_file']}"
        )
        
        return {
            "events_exported": events_result["total_records"],
            "events_templates": events_result["template_count"],
            "events_occurrences": events_result["occurrence_count"],
            "locations_exported": locations_result["total_records"],
            "locations_with_coordinates": locations_result["with_coordinates"],
            "export_files": [
                events_result["output_file"],
                locations_result["output_file"]
            ]
        }
    
    def _aggregate_metrics(
        self,
        stage_results: dict[str, StageResult]
    ) -> dict[str, Any]:
        """Aggregate metrics from all stages.
        
        Args:
            stage_results: Dictionary of stage results
        
        Returns:
            Aggregated metrics dictionary
        """
        total_duration = sum(
            result.duration_seconds for result in stage_results.values()
        )
        
        # Extract key metrics from sync stage if available
        sync_metrics = {}
        if "sync" in stage_results and stage_results["sync"].success:
            sync_metrics = stage_results["sync"].metrics
        
        return {
            "total_duration_seconds": total_duration,
            "stages_completed": len([r for r in stage_results.values() if r.success]),
            "stages_failed": len([r for r in stage_results.values() if not r.success]),
            "sync_summary": sync_metrics
        }
