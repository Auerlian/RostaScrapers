#!/usr/bin/env python3
"""
Live test script for Comptoir Bakery scraper.

This script runs the scraper against the real website to verify it works.
Use sparingly to avoid overwhelming the provider's servers.

Usage:
    python examples/test_comptoir_bakery_live.py
"""

import sys
import json
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.extract import ComptoirBakeryScraper


def main():
    """Run the Comptoir Bakery scraper and display results."""
    print("=" * 80)
    print("Comptoir Bakery Scraper - Live Test")
    print("=" * 80)
    print()
    
    # Create scraper instance
    scraper = ComptoirBakeryScraper()
    
    print(f"Provider: {scraper.provider_name}")
    print(f"Metadata: {json.dumps(scraper.provider_metadata, indent=2)}")
    print()
    
    print("Starting scrape...")
    print("(This may take a minute due to polite delays)")
    print()
    
    try:
        # Execute scrape
        result = scraper.scrape()
        
        # Display results
        print("=" * 80)
        print("SCRAPE RESULTS")
        print("=" * 80)
        print()
        
        print(f"Provider: {result.provider_name}")
        print(f"Website: {result.provider_website}")
        print(f"Contact: {result.provider_contact_email}")
        print()
        
        print(f"Locations found: {len(result.raw_locations)}")
        for i, loc in enumerate(result.raw_locations, 1):
            print(f"  {i}. {loc.get('location_name', 'Unknown')}")
            print(f"     Address: {loc.get('formatted_address', 'N/A')}")
        print()
        
        print(f"Templates found: {len(result.raw_templates)}")
        for i, tmpl in enumerate(result.raw_templates[:5], 1):  # Show first 5
            print(f"  {i}. {tmpl.get('title', 'Unknown')}")
            print(f"     Price: {tmpl.get('price', 'N/A')}")
            print(f"     URL: {tmpl.get('source_url', 'N/A')}")
        if len(result.raw_templates) > 5:
            print(f"  ... and {len(result.raw_templates) - 5} more")
        print()
        
        print(f"Event occurrences found: {len(result.raw_events)}")
        for i, event in enumerate(result.raw_events[:5], 1):  # Show first 5
            print(f"  {i}. {event.get('title', 'Unknown')}")
            print(f"     Start: {event.get('start_at', 'N/A')}")
            print(f"     URL: {event.get('booking_url', 'N/A')}")
        if len(result.raw_events) > 5:
            print(f"  ... and {len(result.raw_events) - 5} more")
        print()
        
        print("=" * 80)
        print("SUCCESS")
        print("=" * 80)
        
    except Exception as e:
        print("=" * 80)
        print("ERROR")
        print("=" * 80)
        print(f"Scraping failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        # Clean up
        scraper.close()


if __name__ == "__main__":
    main()

