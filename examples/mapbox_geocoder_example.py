"""Example usage of MapboxGeocoder.

This example demonstrates how to use the MapboxGeocoder class to geocode addresses.

Requirements:
- Set MAPBOX_API_KEY environment variable before running
- Or pass api_key parameter directly to MapboxGeocoder constructor

Usage:
    export MAPBOX_API_KEY="your_api_key_here"
    python examples/mapbox_geocoder_example.py
"""

import os
from src.enrich import MapboxGeocoder


def main():
    """Demonstrate MapboxGeocoder usage."""
    
    # Check if API key is available
    api_key = os.getenv("MAPBOX_API_KEY")
    if not api_key:
        print("Error: MAPBOX_API_KEY environment variable not set")
        print("Set it with: export MAPBOX_API_KEY='your_api_key_here'")
        return
    
    # Initialize geocoder
    print("Initializing MapboxGeocoder...")
    geocoder = MapboxGeocoder()  # Reads from MAPBOX_API_KEY env var
    
    # Example addresses to geocode
    addresses = [
        "10 Downing Street, London, UK",
        "Big Ben, London",
        "London",
        "Invalid Address XYZ123",
    ]
    
    print("\nGeocoding addresses:\n")
    
    for address in addresses:
        print(f"Address: {address}")
        result = geocoder.geocode(address)
        
        if result.is_success():
            print(f"  ✓ Success!")
            print(f"    Latitude: {result.latitude}")
            print(f"    Longitude: {result.longitude}")
            print(f"    Precision: {result.precision}")
            print(f"    Place Name: {result.metadata.get('place_name')}")
            print(f"    Relevance: {result.metadata.get('relevance')}")
        else:
            print(f"  ✗ Failed: {result.status}")
            print(f"    Error: {result.metadata.get('error', 'Unknown error')}")
        
        print()


if __name__ == "__main__":
    main()
