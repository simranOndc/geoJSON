#!/usr/bin/env python3
"""
Enhanced Google Maps Geocoding Script for Restaurant Locations

This script uses Google Maps Geocoding API and Places API to fetch comprehensive
restaurant data including coordinates, status, timings, address, and maps links.

Requirements:
    pip install pandas openpyxl googlemaps requests

Usage:
    python geocode_restaurants_enhanced.py input_file.xlsx output_file.xlsx YOUR_API_KEY
    
    Or set API key as environment variable:
    export GOOGLE_MAPS_API_KEY="your_api_key_here"
    python geocode_restaurants_enhanced.py input_file.xlsx output_file.xlsx
"""

import sys
import os
import time
from typing import Dict, Optional
from datetime import datetime

# Check for required packages
try:
    import pandas as pd
except ImportError:
    print("❌ Error: pandas not installed")
    print("Please run: pip install pandas openpyxl")
    sys.exit(1)

try:
    import googlemaps
except ImportError:
    print("❌ Error: googlemaps not installed")
    print("Please run: pip install googlemaps")
    print("\nFull installation command:")
    print("pip install pandas openpyxl googlemaps")
    sys.exit(1)


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


def format_opening_hours(opening_hours: dict) -> str:
    """
    Format opening hours from Google Places API response.
    
    Args:
        opening_hours: Opening hours dict from Places API
        
    Returns:
        Formatted string of opening hours
    """
    if not opening_hours:
        return None
    
    # Get weekday text (human-readable format)
    weekday_text = opening_hours.get('weekday_text', [])
    if weekday_text:
        return ' | '.join(weekday_text)
    
    return None


def get_business_status(business_status: str) -> str:
    """
    Translate Google's business status to readable format.
    
    Args:
        business_status: Status from Google Places API
        
    Returns:
        Human-readable status
    """
    status_map = {
        'OPERATIONAL': 'Open',
        'CLOSED_TEMPORARILY': 'Temporarily Closed',
        'CLOSED_PERMANENTLY': 'Permanently Closed'
    }
    
    return status_map.get(business_status, business_status if business_status else 'Unknown')


def geocode_restaurant_enhanced(
    gmaps: googlemaps.Client,
    provider_name: str,
    city: str,
    state: str,
    pincode: float,
    retry_count: int = 2
) -> Dict[str, any]:
    """
    Geocode a restaurant and fetch detailed information using Google Maps APIs.
    
    Args:
        gmaps: Google Maps client
        provider_name: Name of the restaurant/provider
        city: City name
        state: State name
        pincode: Pincode
        retry_count: Number of retries for failed requests
        
    Returns:
        Dictionary with comprehensive restaurant data
    """
    result = {
        'lat': None,
        'long': None,
        'Restaurant_Status': None,
        'Store_Timings': None,
        'Address': None,
        'Google_Maps_Link': None,
        'Place_ID': None
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
    
    # Try each query
    for attempt in range(retry_count):
        for query in search_queries:
            try:
                # First, use Find Place to get place_id
                find_result = gmaps.find_place(
                    input=query,
                    input_type='textquery',
                    fields=['place_id', 'geometry']
                )
                
                if find_result and find_result.get('candidates'):
                    place_data = find_result['candidates'][0]
                    place_id = place_data.get('place_id')
                    
                    if place_id:
                        # Now get detailed place information
                        try:
                            place_details = gmaps.place(
                                place_id=place_id,
                                fields=[
                                    'name',
                                    'geometry',
                                    'formatted_address',
                                    'business_status',
                                    'opening_hours',
                                    'place_id'
                                ]
                            )
                            
                            if place_details and place_details.get('status') == 'OK':
                                place_info = place_details.get('result', {})
                                
                                # Extract location
                                geometry = place_info.get('geometry', {})
                                location = geometry.get('location', {})
                                lat = location.get('lat')
                                lng = location.get('lng')
                                
                                if lat and lng:
                                    # Round to 6 decimal places
                                    result['lat'] = round(lat, 6)
                                    result['long'] = round(lng, 6)
                                    
                                    # Get address
                                    result['Address'] = place_info.get('formatted_address')
                                    
                                    # Get business status
                                    business_status = place_info.get('business_status')
                                    result['Restaurant_Status'] = get_business_status(business_status)
                                    
                                    # Get opening hours
                                    opening_hours = place_info.get('opening_hours')
                                    result['Store_Timings'] = format_opening_hours(opening_hours)
                                    
                                    # Store place_id
                                    result['Place_ID'] = place_id
                                    
                                    # Create Google Maps link
                                    result['Google_Maps_Link'] = f"https://www.google.com/maps/place/?q=place_id:{place_id}"
                                    
                                    return result
                        
                        except Exception as e:
                            print(f"  Error fetching place details: {e}")
                            # Continue to next query
                
                # Add delay to respect API rate limits
                time.sleep(0.05)
                
            except googlemaps.exceptions.ApiError as e:
                print(f"  API Error for '{query}': {e}")
                time.sleep(1)
            except Exception as e:
                print(f"  Unexpected error for '{query}': {e}")
                time.sleep(0.5)
        
        # If no results, wait before retrying
        if attempt < retry_count - 1:
            time.sleep(2)
    
    return result


def process_restaurants(
    input_file: str,
    output_file: str,
    api_key: Optional[str] = None,
    save_interval: int = 500,
    start_row: int = 0
) -> None:
    """
    Process restaurants file and add comprehensive geocoding data.
    
    Args:
        input_file: Path to input Excel file
        output_file: Path to output Excel file
        api_key: Google Maps API key
        save_interval: Save progress after this many rows
        start_row: Row to start processing from (for resume capability)
    """
    # Validate input file
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Input file not found: {input_file}")
    
    print(f"{'='*80}")
    print(f"Enhanced Google Maps Restaurant Geocoding")
    print(f"{'='*80}")
    print(f"Input file:  {input_file}")
    print(f"Output file: {output_file}")
    print(f"Started at:  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}\n")
    
    # Initialize Google Maps client
    print("Initializing Google Maps API client...")
    gmaps = get_google_maps_client(api_key)
    print("✓ Google Maps client initialized\n")
    
    # Read input file
    print("Reading input file...")
    df = pd.read_excel(input_file)
    total_rows = len(df)
    print(f"✓ Loaded {total_rows:,} rows\n")
    
    # Add new columns ONLY if they don't exist (don't overwrite existing data!)
    new_columns = [
        'lat',
        'long',
        'Restaurant_Status',
        'Store_Timings',
        'Address',
        'Google_Maps_Link',
        'Place_ID'
    ]
    
    for col in new_columns:
        if col not in df.columns:
            df[col] = None  # Only add column if it doesn't exist
    
    # Identify source columns
    provider_col = 'Provider Name'
    city_col = 'Seller City'
    state_col = 'State'
    pincode_col = 'Seller Pincode'
    
    # Check required columns
    required_cols = [provider_col, city_col, pincode_col]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Required column '{col}' not found in input file")
    
    # Check how many already have data
    already_geocoded = df[(df['lat'].notna()) & (df['long'].notna())].shape[0]
    needs_geocoding = total_rows - already_geocoded
    
    print(f"{'='*80}")
    print(f"DATA ANALYSIS")
    print(f"{'='*80}")
    print(f"Total restaurants:           {total_rows:,}")
    print(f"Already geocoded:            {already_geocoded:,} ({already_geocoded/total_rows*100:.1f}%)")
    print(f"Need to be geocoded:         {needs_geocoding:,} ({needs_geocoding/total_rows*100:.1f}%)")
    print(f"{'='*80}\n")
    
    if needs_geocoding == 0:
        print("✓ All restaurants already have geocoding data!")
        print(f"✓ Output file: {output_file}")
        return
    
    # Process restaurants
    print(f"Starting geocoding process from row {start_row}...")
    print(f"Will process only restaurants without existing data...\n")
    
    start_time = time.time()
    processed = 0
    successful = 0
    failed = 0
    skipped = 0
    
    for idx in range(start_row, total_rows):
        row = df.iloc[idx]
        
        # Skip if already has coordinates - check both by index and column name
        current_lat = df.at[idx, 'lat']
        current_lon = df.at[idx, 'long']
        
        if pd.notna(current_lat) and pd.notna(current_lon) and current_lat is not None and current_lon is not None:
            skipped += 1
            processed += 1
            if processed % 100 == 0:
                print(f"  ⏭️  Skipped row {idx} (already has data)")
            continue
        
        # Get restaurant data
        provider_name = row[provider_col]
        city = row[city_col]
        state = row.get(state_col)
        pincode = row.get(pincode_col)
        
        # Show current restaurant being processed
        if processed % 10 == 0 or processed < 5:
            print(f"[{idx}] Processing: {provider_name[:50]}...")
        
        # Geocode
        try:
            result = geocode_restaurant_enhanced(gmaps, provider_name, city, state, pincode)
            
            # Update dataframe
            df.at[idx, 'lat'] = result['lat']
            df.at[idx, 'long'] = result['long']
            df.at[idx, 'Restaurant_Status'] = result['Restaurant_Status']
            df.at[idx, 'Store_Timings'] = result['Store_Timings']
            df.at[idx, 'Address'] = result['Address']
            df.at[idx, 'Google_Maps_Link'] = result['Google_Maps_Link']
            df.at[idx, 'Place_ID'] = result['Place_ID']
            
            if result['lat'] is not None:
                successful += 1
                if processed % 10 == 0 or processed < 5:
                    print(f"  ✓ Success: {result['lat']}, {result['long']} | Status: {result['Restaurant_Status']}")
            else:
                failed += 1
                if processed % 10 == 0 or processed < 5:
                    print(f"  ✗ Failed to geocode")
            
        except Exception as e:
            print(f"  ✗ Error processing row {idx}: {e}")
            failed += 1
        
        processed += 1
        
        # Progress update
        if processed % 50 == 0:
            elapsed = time.time() - start_time
            rate = processed / elapsed if elapsed > 0 else 0
            remaining = (total_rows - start_row - processed) / rate if rate > 0 else 0
            
            print(f"\n{'='*80}")
            print(f"Progress: {processed:,}/{total_rows-start_row:,} ({processed/(total_rows-start_row)*100:.1f}%)")
            print(f"Success: {successful:,} | Failed: {failed:,} | Skipped: {skipped:,}")
            print(f"Rate: {rate:.2f} rows/sec | ETA: {remaining/60:.1f} minutes")
            print(f"{'='*80}\n")
        
        # Save intermediate results
        if processed % save_interval == 0:
            print(f"\n💾 Auto-saving progress...")
            df.to_excel(output_file, index=False)
            print(f"  ✓ Saved to: {output_file}")
            print(f"  Last processed row: {idx}")
            print(f"  You can resume from row {idx+1} if needed\n")
        
        # Rate limiting - Google allows 50 requests per second
        time.sleep(0.05)
    
    # Save final output
    print(f"\n{'='*80}")
    print(f"Saving final results...")
    df.to_excel(output_file, index=False)
    print(f"✓ Saved to: {output_file}")
    
    # Calculate statistics
    elapsed_time = time.time() - start_time
    
    # Get final count of geocoded restaurants
    final_geocoded = df[(df['lat'].notna()) & (df['long'].notna())].shape[0]
    
    print(f"\n{'='*80}")
    print(f"FINAL RESULTS")
    print(f"{'='*80}")
    print(f"Total rows in file:       {total_rows:,}")
    print(f"Total geocoded now:       {final_geocoded:,} ({final_geocoded/total_rows*100:.1f}%)")
    print(f"")
    print(f"This run:")
    print(f"  Rows processed:         {processed:,}")
    print(f"  Newly geocoded:         {successful:,}")
    print(f"  Failed:                 {failed:,}")
    print(f"  Skipped (had data):     {skipped:,}")
    print(f"  Success rate:           {successful/processed*100 if processed > 0 else 0:.1f}%")
    print(f"")
    print(f"Processing time:          {elapsed_time/60:.1f} minutes")
    print(f"Average rate:             {processed/elapsed_time if elapsed_time > 0 else 0:.2f} rows/second")
    print(f"Output saved to:          {output_file}")
    print(f"Completed at:             {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}")
    
    # Show sample results
    print(f"\n{'='*80}")
    print(f"SAMPLE OF GEOCODED DATA (First 5 successful entries)")
    print(f"{'='*80}")
    sample_cols = [provider_col, city_col, 'lat', 'long', 'Restaurant_Status', 'Address']
    sample_df = df[df['lat'].notna()][sample_cols].head(5)
    for idx, row in sample_df.iterrows():
        print(f"\n{row[provider_col]}")
        print(f"  Location: {row['lat']}, {row['long']}")
        print(f"  Status: {row['Restaurant_Status']}")
        print(f"  Address: {row['Address'][:80]}..." if row['Address'] and len(row['Address']) > 80 else f"  Address: {row['Address']}")
    
    print(f"\n{'='*80}")


def main():
    """Main function to handle command line arguments."""
    if len(sys.argv) < 3:
        print("="*80)
        print("Enhanced Google Maps Restaurant Geocoding Script")
        print("="*80)
        print("\nUsage:")
        print("  python geocode_restaurants_enhanced.py <input_file.xlsx> <output_file.xlsx> [API_KEY] [START_ROW]")
        print("\nExamples:")
        print("  python geocode_restaurants_enhanced.py restaurants.xlsx output.xlsx YOUR_API_KEY")
        print("  python geocode_restaurants_enhanced.py restaurants.xlsx output.xlsx YOUR_API_KEY 1000")
        print("\nOr set API key as environment variable:")
        print("  export GOOGLE_MAPS_API_KEY='your_api_key'")
        print("  python geocode_restaurants_enhanced.py restaurants.xlsx output.xlsx")
        print("\n" + "="*80)
        print("What This Script Fetches:")
        print("="*80)
        print("  • lat (6 decimal accuracy)")
        print("  • long (6 decimal accuracy)")
        print("  • Restaurant Status (Open/Closed/Permanently Closed)")
        print("  • Store Timings (Weekly schedule)")
        print("  • Full Address")
        print("  • Google Maps Link")
        print("  • Place ID")
        print("\n" + "="*80)
        print("Getting Google Maps API Key:")
        print("="*80)
        print("1. Go to: https://console.cloud.google.com/")
        print("2. Create a new project or select existing one")
        print("3. Enable these APIs:")
        print("   - Geocoding API")
        print("   - Places API")
        print("4. Go to 'Credentials' and create an API Key")
        print("5. (Optional) Restrict the API key to these APIs only")
        print("\n" + "="*80)
        print("API Costs (as of 2024):")
        print("="*80)
        print("  • Places API (Find Place):    $0.017 per request")
        print("  • Places API (Place Details): $0.017 per request")
        print("  • Total per restaurant:       ~$0.034")
        print("  • For 25,000 restaurants:     ~$850")
        print("  • Google gives $200 free credit monthly")
        print("="*80)
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    api_key = sys.argv[3] if len(sys.argv) > 3 else None
    start_row = int(sys.argv[4]) if len(sys.argv) > 4 else 0
    
    try:
        process_restaurants(input_file, output_file, api_key, start_row=start_row)
    except KeyboardInterrupt:
        print("\n\n⚠️  Processing interrupted by user!")
        print("You can resume by running the script again with the same output file.")
        print("Already processed data has been saved.")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()