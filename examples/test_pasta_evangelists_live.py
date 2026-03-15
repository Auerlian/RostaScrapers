#!/usr/bin/env python3
"""
Live test for Pasta Evangelists scraper.

This script demonstrates the scraper working with the real API.
Run manually to verify the scraper works correctly.

Usage:
    python examples/test_pasta_evangelists_live.py
"""

import sys
import json
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.extract import PastaEvangelistsScraper


def main():
    """Run the scraper and display results."""
    print("=" * 60)
    print("Pasta Evangelists Scraper - Live Test")
    print("=" * 60)
    print()
    
    # Create scraper instance
    scraper = PastaEvangelistsScraper()
    
    print(f"Provider: {scraper.provider_name}")
    print(f"Metadata: {json.dumps(scraper.provider_metadata, indent=2)}")
    print()
    
    # Execute scraping
    print("Fetching data from API...")
    try:
        raw_data = scraper.scrape()
        print("✓ Scraping successful!")
        print()
        
        # Display summary
        print("Results:")
        print(f"  Provider: {raw_data.provider_name}")
        print(f"  Website: {raw_data.provider_website}")
        print(f"  Contact: {raw_data.provider_contact_email}")
        print(f"  Locations: {len(raw_data.raw_locations)}")
        print(f"  Templates: {len(raw_data.raw_templates)}")
        print(f"  Events: {len(raw_data.raw_events)}")
        print()
        
        # Display sample location
        if raw_data.raw_locations:
            print("Sample Location:")
            loc = raw_data.raw_locations[0]
            print(f"  ID: {loc.get('id')}")
            attrs = loc.get('attributes', {})
            print(f"  Name: {attrs.get('name')}")
            print(f"  Address: {attrs.get('address1')}, {attrs.get('city')}")
            print()
        
        # Display sample template
        if raw_data.raw_templates:
            print("Sample Template:")
            tmpl = raw_data.raw_templates[0]
            print(f"  ID: {tmpl.get('id')}")
            attrs = tmpl.get('attributes', {})
            print(f"  Name: {attrs.get('name')}")
            print(f"  Price: £{attrs.get('price')}")
            summary = attrs.get('summary', '')[:100]
            print(f"  Summary: {summary}...")
            print()
        
        print("=" * 60)
        print("✓ Test completed successfully!")
        print("=" * 60)
        
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
