#!/usr/bin/env python3
"""
CLI entry point for the scraper pipeline.

Provides commands to run the full pipeline, export data, and validate the store.
"""

import sys
from datetime import datetime, timezone
from pathlib import Path

import click

# Load environment variables from .env file if it exists
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv not installed, skip

from src.pipeline.orchestrator import PipelineOrchestrator
from src.storage.store import CanonicalStore


@click.group()
def cli():
    """ROSTA Scraper Pipeline - Data extraction and normalization for event providers."""
    pass


@cli.command()
@click.option(
    "--provider",
    type=str,
    help="Run pipeline for specific provider only (e.g., pasta-evangelists)",
)
@click.option(
    "--skip-geocoding",
    is_flag=True,
    help="Skip geocoding stage",
)
@click.option(
    "--skip-ai",
    is_flag=True,
    help="Skip AI enrichment stage",
)
def run(provider, skip_geocoding, skip_ai):
    """Execute full pipeline for all providers or a specific provider.
    
    Runs all pipeline stages:
    1. Extract: Scrape data from provider sources
    2. Normalize: Transform to canonical models
    3. Enrich: Geocode locations and AI enrich events (optional)
    4. Sync: Merge with existing store
    5. Export: Generate CSV exports
    
    Examples:
        run_pipeline.py run
        run_pipeline.py run --provider pasta-evangelists
        run_pipeline.py run --skip-ai --skip-geocoding
    """
    click.echo("=" * 60)
    click.echo("ROSTA Scraper Pipeline")
    click.echo("=" * 60)
    click.echo()
    
    # Prepare provider list
    providers = [provider] if provider else None
    
    # Display configuration
    click.echo("Configuration:")
    click.echo(f"  Providers: {provider or 'all'}")
    click.echo(f"  Skip geocoding: {skip_geocoding}")
    click.echo(f"  Skip AI enrichment: {skip_ai}")
    click.echo()
    
    # Initialize orchestrator
    click.echo("Initializing pipeline orchestrator...")
    orchestrator = PipelineOrchestrator()
    
    # Run pipeline
    click.echo("Starting pipeline execution...")
    click.echo()
    
    try:
        report = orchestrator.run(
            providers=providers,
            skip_geocoding=skip_geocoding,
            skip_ai_enrichment=skip_ai
        )
        
        # Display results
        click.echo()
        click.echo("=" * 60)
        click.echo("Pipeline Execution Summary")
        click.echo("=" * 60)
        click.echo()
        
        # Status
        status_color = "green" if report.success else "red"
        click.echo(f"Status: ", nl=False)
        click.secho(f"{'SUCCESS' if report.success else 'FAILED'}", fg=status_color, bold=True)
        click.echo(f"Run ID: {report.run_id}")
        click.echo(f"Duration: {report.duration_seconds:.2f}s")
        click.echo()
        
        # Providers
        if report.providers_processed:
            click.echo(f"Providers processed: {', '.join(report.providers_processed)}")
        if report.providers_failed:
            click.secho(f"Providers failed: {', '.join(report.providers_failed)}", fg="red")
        click.echo()
        
        # Stage results
        click.echo("Stage Results:")
        for stage_name, stage_result in report.stage_results.items():
            status_icon = "✓" if stage_result.success else "✗"
            status_color = "green" if stage_result.success else "red"
            click.echo(f"  {status_icon} ", nl=False)
            click.secho(f"{stage_name.capitalize()}", fg=status_color, nl=False)
            click.echo(f" ({stage_result.duration_seconds:.2f}s)")
            
            if stage_result.error:
                click.secho(f"    Error: {stage_result.error}", fg="red")
        click.echo()
        
        # Metrics
        if report.total_metrics:
            click.echo("Metrics:")
            for key, value in report.total_metrics.items():
                if isinstance(value, (int, float)):
                    click.echo(f"  {key}: {value}")
        click.echo()
        
        # Errors
        if report.errors:
            click.secho("Errors:", fg="red", bold=True)
            for error in report.errors:
                click.secho(f"  - {error}", fg="red")
            click.echo()
        
        # Exit with appropriate code
        sys.exit(0 if report.success else 1)
        
    except Exception as e:
        click.secho(f"\nFatal error: {e}", fg="red", bold=True)
        sys.exit(1)


@cli.command()
def export_only():
    """Regenerate CSV exports from existing canonical store without scraping.
    
    Useful when you want to update export format or filters without
    re-running the entire pipeline.
    
    Example:
        run_pipeline.py export-only
    """
    click.echo("=" * 60)
    click.echo("Export Only Mode")
    click.echo("=" * 60)
    click.echo()
    
    click.echo("Regenerating exports from canonical store...")
    
    try:
        # Initialize orchestrator
        orchestrator = PipelineOrchestrator()
        
        # Run only export stage
        result = orchestrator.run_stage("export")
        
        click.echo()
        if result.success:
            click.secho("✓ Export completed successfully", fg="green", bold=True)
            click.echo(f"Duration: {result.duration_seconds:.2f}s")
            
            # Display metrics
            if result.metrics:
                click.echo()
                click.echo("Export Metrics:")
                for key, value in result.metrics.items():
                    if isinstance(value, (int, float, str)):
                        click.echo(f"  {key}: {value}")
            
            sys.exit(0)
        else:
            click.secho(f"✗ Export failed: {result.error}", fg="red", bold=True)
            sys.exit(1)
            
    except Exception as e:
        click.secho(f"\nFatal error: {e}", fg="red", bold=True)
        sys.exit(1)


@cli.command()
def validate():
    """Validate canonical store integrity and display statistics.
    
    Checks:
    - File existence and readability
    - Record counts by type and status
    - Referential integrity (location_id, provider_id references)
    - Data quality issues
    
    Example:
        run_pipeline.py validate
    """
    click.echo("=" * 60)
    click.echo("Canonical Store Validation")
    click.echo("=" * 60)
    click.echo()
    
    try:
        store = CanonicalStore()
        
        # Load all records
        click.echo("Loading records from store...")
        providers = store.load_providers()
        locations = store.load_locations()
        events = store.load_events()
        
        click.echo()
        click.secho("✓ Store loaded successfully", fg="green")
        click.echo()
        
        # Display counts
        click.echo("Record Counts:")
        click.echo(f"  Providers: {len(providers)}")
        click.echo(f"  Locations: {len(locations)}")
        click.echo(f"  Events: {len(events)}")
        click.echo()
        
        # Status breakdown for locations
        location_status_counts = {}
        for loc in locations:
            status = loc.status
            location_status_counts[status] = location_status_counts.get(status, 0) + 1
        
        click.echo("Location Status:")
        for status, count in sorted(location_status_counts.items()):
            click.echo(f"  {status}: {count}")
        click.echo()
        
        # Status breakdown for events
        event_status_counts = {}
        event_type_counts = {"templates": 0, "occurrences": 0}
        for event in events:
            status = event.status
            event_status_counts[status] = event_status_counts.get(status, 0) + 1
            
            # Count types
            if hasattr(event, "event_template_id") and event.event_template_id:
                event_type_counts["occurrences"] += 1
            else:
                event_type_counts["templates"] += 1
        
        click.echo("Event Status:")
        for status, count in sorted(event_status_counts.items()):
            click.echo(f"  {status}: {count}")
        click.echo()
        
        click.echo("Event Types:")
        click.echo(f"  Templates: {event_type_counts['templates']}")
        click.echo(f"  Occurrences: {event_type_counts['occurrences']}")
        click.echo()
        
        # Referential integrity checks
        click.echo("Referential Integrity Checks:")
        
        # Build ID sets
        provider_ids = {p.provider_id for p in providers}
        location_ids = {loc.location_id for loc in locations}
        
        # Check location provider references
        invalid_location_providers = []
        for loc in locations:
            if loc.provider_id not in provider_ids:
                invalid_location_providers.append(loc.location_id)
        
        if invalid_location_providers:
            click.secho(f"  ✗ {len(invalid_location_providers)} locations with invalid provider_id", fg="red")
        else:
            click.secho(f"  ✓ All locations have valid provider_id", fg="green")
        
        # Check event provider references
        invalid_event_providers = []
        for event in events:
            if event.provider_id not in provider_ids:
                invalid_event_providers.append(getattr(event, "event_id", getattr(event, "event_template_id", "unknown")))
        
        if invalid_event_providers:
            click.secho(f"  ✗ {len(invalid_event_providers)} events with invalid provider_id", fg="red")
        else:
            click.secho(f"  ✓ All events have valid provider_id", fg="green")
        
        # Check event location references (only for occurrences)
        invalid_event_locations = []
        for event in events:
            # Only EventOccurrence has location_id
            location_id = getattr(event, "location_id", None)
            if location_id and location_id not in location_ids:
                invalid_event_locations.append(getattr(event, "event_id", getattr(event, "event_template_id", "unknown")))
        
        if invalid_event_locations:
            click.secho(f"  ✗ {len(invalid_event_locations)} events with invalid location_id", fg="red")
        else:
            click.secho(f"  ✓ All events have valid location_id (or null)", fg="green")
        
        click.echo()
        
        # Data quality checks
        click.echo("Data Quality Checks:")
        
        # Geocoding status
        geocoded_count = sum(1 for loc in locations if loc.geocode_status == "success")
        not_geocoded_count = sum(1 for loc in locations if loc.geocode_status == "not_geocoded")
        failed_geocode_count = sum(1 for loc in locations if loc.geocode_status == "failed")
        
        click.echo(f"  Geocoded locations: {geocoded_count}/{len(locations)}")
        if not_geocoded_count > 0:
            click.secho(f"  Not geocoded: {not_geocoded_count}", fg="yellow")
        if failed_geocode_count > 0:
            click.secho(f"  Failed geocoding: {failed_geocode_count}", fg="red")
        
        # Events with locations (only occurrences have location_id)
        events_with_location = sum(1 for e in events if getattr(e, "location_id", None))
        events_without_location = len(events) - events_with_location
        click.echo(f"  Events with location: {events_with_location}/{len(events)}")
        if events_without_location > 0:
            click.secho(f"  Events without location: {events_without_location}", fg="yellow")
        
        click.echo()
        
        # Overall validation result
        has_errors = (
            invalid_location_providers or
            invalid_event_providers or
            invalid_event_locations
        )
        
        if has_errors:
            click.secho("✗ Validation completed with errors", fg="red", bold=True)
            sys.exit(1)
        else:
            click.secho("✓ Validation completed successfully", fg="green", bold=True)
            sys.exit(0)
            
    except Exception as e:
        click.secho(f"\nFatal error: {e}", fg="red", bold=True)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    cli()
