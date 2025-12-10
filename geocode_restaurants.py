#!/usr/bin/env python3
"""
Google Maps Geocoding Script for Restaurant Locations

This script uses Google Maps Geocoding API to fetch accurate latitude, longitude,
and generate Google Maps links for restaurants based on their name, city, state, and pincode.

Requirements:
    pip install pandas openpyxl googlemaps requests

Usage:
    python geocode_restaurants.py input_file.xlsx output_file.xlsx YOUR_API_KEY
    
    Or set API key as environment variable:
    export GOOGLE_MAPS_API_KEY="your_api_key_here"
    python geocode_restaurants.py input_file.xlsx output_file.xlsx
"""

import pandas as pd
import googlemaps
import sys
import os
import time
from typing import Dict, Tuple, Optional
from datetime import datetime


def get_google_maps_client(api_key: Optional[str] = None) -> googlemaps.Client:
    """
    Initialize Google Maps client with API key.
    
    Args:
        api_key: Google Maps API key (optional, can use env variable)
        
    Returns:
        Initialized Google Maps client
    """
    if api_key is None:
        api_key = os.environ.get('GOOGLE_MAPS_API_KEY')
    
    if not api_key:
        raise ValueError(
            "Google Maps API key is required. Either:\n"
            "1. Pass it as command line argument: python script.py input.xlsx output.xlsx YOUR_API_KEY\n"
            "2. Set environment variable: export GOOGLE_MAPS_API_KEY='your_api_key'"
        )
    
    return googlemaps.Client(key=api_key)


def geocode_restaurant(
    gmaps: googlemaps.Client,
    provider_name: str,
    city: str,
    state: str,
    pincode: float,
    retry_count: int = 3
) -> Dict[str, any]:
    """
    Geocode a restaurant using Google Maps API.
    
    Args:
        gmaps: Google Maps client
        provider_name: Name of the restaurant/provider
        city: City name
        state: State name
        pincode: Pincode
        retry_count: Number of retries for failed requests
        
    Returns:
        Dictionary with lat, long, formatted_address, and place_id
    """
    result = {
        'lat': None,
        'long': None,
        'formatted_address': None,
        'place_id': None,
        'google_maps_link': None
    }
    
    # Skip if essential data is missing
    if pd.isna(provider_name) or pd.isna(city):
        return result
    
    # Clean and prepare the search query
    provider_name_clean = str(provider_name).strip()
    city_clean = str(city).strip()
    state_clean = str(state).strip() if pd.notna(state) else ''
    pincode_str = str(int(pincode)) if pd.notna(pincode) else ''
    
    # Build search queries in order of specificity
    search_queries = []
    
    # Query 1: Restaurant name + city + state + pincode
    if pincode_str and state_clean:
        search_queries.append(f"{provider_name_clean}, {city_clean}, {state_clean}, {pincode_str}, India")
    
    # Query 2: Restaurant name + city + state
    if state_clean:
        search_queries.append(f"{provider_name_clean}, {city_clean}, {state_clean}, India")
    
    # Query 3: Restaurant name + city + pincode
    if pincode_str:
        search_queries.append(f"{provider_name_clean}, {city_clean}, {pincode_str}, India")
    
    # Query 4: Restaurant name + city
    search_queries.append(f"{provider_name_clean}, {city_clean}, India")
    
    # Try each query
    for attempt in range(retry_count):
        for query in search_queries:
            try:
                # Call Google Maps Geocoding API
                geocode_result = gmaps.geocode(query)
                
                if geocode_result and len(geocode_result) > 0:
                    # Get the first (best) result
                    location = geocode_result[0]
                    geometry = location.get('geometry', {})
                    location_data = geometry.get('location', {})
                    
                    # Extract data
                    lat = location_data.get('lat')
                    lng = location_data.get('lng')
                    formatted_address = location.get('formatted_address')
                    place_id = location.get('place_id')
                    
                    if lat and lng:
                        result['lat'] = round(lat, 6)
                        result['long'] = round(lng, 6)
                        result['formatted_address'] = formatted_address
                        result['place_id'] = place_id
                        result['google_maps_link'] = f"https://www.google.com/maps/place/?q=place_id:{place_id}"
                        
                        return result
                
                # Add delay to respect API rate limits
                time.sleep(0.1)
                
            except googlemaps.exceptions.ApiError as e:
                print(f"API Error for '{query}': {e}")
                time.sleep(1)  # Wait before retry
            except Exception as e:
                print(f"Unexpected error for '{query}': {e}")
                time.sleep(0.5)
        
        # If no results, wait before retrying
        if attempt < retry_count - 1:
            time.sleep(2)
    
    return result


def process_restaurants(
    input_file: str,
    output_file: str,
    api_key: Optional[str] = None,
    batch_size: int = 100,
    save_interval: int = 500
) -> None:
    """
    Process restaurants file and add geocoding data.
    
    Args:
        input_file: Path to input Excel file
        output_file: Path to output Excel file
        api_key: Google Maps API key
        batch_size: Number of rows to process in each batch
        save_interval: Save progress after this many rows
    """
    # Validate input file
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Input file not found: {input_file}")
    
    print(f"{'='*70}")
    print(f"Google Maps Restaurant Geocoding")
    print(f"{'='*70}")
    print(f"Input file:  {input_file}")
    print(f"Output file: {output_file}")
    print(f"Started at:  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}\n")
    
    # Initialize Google Maps client
    print("Initializing Google Maps API client...")
    gmaps = get_google_maps_client(api_key)
    print("✓ Google Maps client initialized\n")
    
    # Read input file
    print("Reading input file...")
    df = pd.read_excel(input_file)
    total_rows = len(df)
    print(f"✓ Loaded {total_rows:,} rows\n")
    
    # Add new columns if they don't exist
    new_columns = ['lat', 'long', 'formatted_address', 'place_id', 'google_maps_link']
    for col in new_columns:
        if col not in df.columns:
            df[col] = None
    
    # Identify columns
    provider_col = 'Provider Name'
    city_col = 'Seller City'
    state_col = 'State'
    pincode_col = 'Seller Pincode'
    
    # Check required columns
    required_cols = [provider_col, city_col]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Required column '{col}' not found in input file")
    
    # Process restaurants
    print("Starting geocoding process...")
    print(f"Processing {total_rows:,} restaurants...\n")
    
    start_time = time.time()
    processed = 0
    successful = 0
    failed = 0
    
    for idx, row in df.iterrows():
        # Skip if already has coordinates
        if pd.notna(row.get('lat')):
            processed += 1
            successful += 1
            continue
        
        # Get restaurant data
        provider_name = row[provider_col]
        city = row[city_col]
        state = row.get(state_col)
        pincode = row.get(pincode_col)
        
        # Geocode
        try:
            result = geocode_restaurant(gmaps, provider_name, city, state, pincode)
            
            # Update dataframe
            df.at[idx, 'lat'] = result['lat']
            df.at[idx, 'long'] = result['long']
            df.at[idx, 'formatted_address'] = result['formatted_address']
            df.at[idx, 'place_id'] = result['place_id']
            df.at[idx, 'google_maps_link'] = result['google_maps_link']
            
            if result['lat'] is not None:
                successful += 1
            else:
                failed += 1
            
        except Exception as e:
            print(f"Error processing row {idx}: {e}")
            failed += 1
        
        processed += 1
        
        # Progress update
        if processed % 50 == 0:
            elapsed = time.time() - start_time
            rate = processed / elapsed if elapsed > 0 else 0
            remaining = (total_rows - processed) / rate if rate > 0 else 0
            
            print(f"Progress: {processed:,}/{total_rows:,} ({processed/total_rows*100:.1f}%) | "
                  f"Success: {successful:,} | Failed: {failed:,} | "
                  f"Rate: {rate:.1f} rows/sec | "
                  f"ETA: {remaining/60:.1f} min")
        
        # Save intermediate results
        if processed % save_interval == 0:
            temp_file = output_file.replace('.xlsx', f'_temp_{processed}.xlsx')
            df.to_excel(temp_file, index=False)
            print(f"  → Intermediate save: {temp_file}")
        
        # Rate limiting
        time.sleep(0.05)  # 20 requests per second max
    
    # Save final output
    print(f"\nSaving final results to: {output_file}")
    df.to_excel(output_file, index=False)
    
    # Calculate statistics
    elapsed_time = time.time() - start_time
    
    print(f"\n{'='*70}")
    print(f"RESULTS")
    print(f"{'='*70}")
    print(f"Total rows processed:     {processed:,}")
    print(f"Successfully geocoded:    {successful:,} ({successful/total_rows*100:.1f}%)")
    print(f"Failed to geocode:        {failed:,} ({failed/total_rows*100:.1f}%)")
    print(f"Processing time:          {elapsed_time/60:.1f} minutes")
    print(f"Average rate:             {processed/elapsed_time:.1f} rows/second")
    print(f"Output saved to:          {output_file}")
    print(f"Completed at:             {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}")
    
    # Show sample results
    print(f"\nSample of geocoded data:")
    sample_cols = [provider_col, city_col, 'lat', 'long', 'google_maps_link']
    sample_df = df[df['lat'].notna()][sample_cols].head(5)
    print(sample_df.to_string(index=False))


def main():
    """Main function to handle command line arguments."""
    if len(sys.argv) < 3:
        print("Usage: python geocode_restaurants.py <input_file.xlsx> <output_file.xlsx> [API_KEY]")
        print("\nExample:")
        print("  python geocode_restaurants.py restaurants.xlsx restaurants_geocoded.xlsx YOUR_API_KEY")
        print("\nOr set API key as environment variable:")
        print("  export GOOGLE_MAPS_API_KEY='your_api_key'")
        print("  python geocode_restaurants.py restaurants.xlsx restaurants_geocoded.xlsx")
        print("\n" + "="*70)
        print("Getting Google Maps API Key:")
        print("="*70)
        print("1. Go to: https://console.cloud.google.com/")
        print("2. Create a new project or select existing one")
        print("3. Enable 'Geocoding API' from API Library")
        print("4. Go to 'Credentials' and create an API Key")
        print("5. (Optional) Restrict the API key to Geocoding API only")
        print("="*70)
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    api_key = sys.argv[3] if len(sys.argv) > 3 else None
    
    try:
        process_restaurants(input_file, output_file, api_key)
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
