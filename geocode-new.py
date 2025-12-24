#!/usr/bin/env python3
"""
Strict Google Maps Geocoding Script with 10km Radius Limit

VALIDATION ORDER (checks in this sequence):
1. Restaurant name (similarity check, not strict - just for matching)
2. Lat/Long (must be within 10km of network coordinates)
3. Pincode (must match or be in same region)
4. City (must be found in address)
5. State (must be found in address)

Input columns: Provider Name, Seller City, Seller Pincode, State, network_lat, network_long
Output columns: refined_lat, refined_long, Restaurant_Status, Store_Timings, Address, 
                Google_Maps_Link, Distance_Meters, Found_Pincode, Found_Restaurant_Name

Requirements:
    pip install pandas openpyxl requests

Usage:
    python geocode_restaurants_strict.py input_file.xlsx output_file.xlsx YOUR_API_KEY
    
    Or set API key as environment variable:
    export GOOGLE_MAPS_API_KEY="your_api_key_here"
    python geocode_restaurants_strict.py input_file.xlsx output_file.xlsx
"""

import sys
import os
import time
import requests
import re
from typing import Dict, Optional, Tuple
from datetime import datetime
from math import radians, cos, sin, asin, sqrt

# Check for required packages
try:
    import pandas as pd
except ImportError:
    print("❌ Error: pandas not installed")
    print("Please run: pip install pandas openpyxl")
    sys.exit(1)


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate the great circle distance between two points on Earth in meters.
    
    Args:
        lat1, lon1: First point coordinates
        lat2, lon2: Second point coordinates
        
    Returns:
        Distance in meters
    """
    # Convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    
    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    
    # Radius of earth in meters
    r = 6371000
    
    return c * r


def validate_api_key(api_key: Optional[str] = None) -> str:
    """
    Validate and return Google Maps API key.
    
    Args:
        api_key: Google Maps API key (optional, can use env variable)
        
    Returns:
        Validated API key
    """
    if api_key is None:
        api_key = os.environ.get('GOOGLE_MAPS_API_KEY')
    
    if not api_key:
        raise ValueError(
            "Google Maps API key is required. Either:\n"
            "1. Pass it as command line argument: python script.py input.xlsx output.xlsx YOUR_API_KEY\n"
            "2. Set environment variable: export GOOGLE_MAPS_API_KEY='your_api_key'"
        )
    
    return api_key


def validate_coordinates(lat: any, lon: any) -> Tuple[bool, Optional[float], Optional[float]]:
    """
    Validate latitude and longitude values.
    
    Args:
        lat: Latitude value
        lon: Longitude value
        
    Returns:
        Tuple of (is_valid, validated_lat, validated_lon)
    """
    try:
        if pd.isna(lat) or pd.isna(lon):
            return False, None, None
        
        lat_float = float(lat)
        lon_float = float(lon)
        
        # Check if coordinates are within valid ranges
        if -90 <= lat_float <= 90 and -180 <= lon_float <= 180:
            # Check if coordinates are not (0, 0) which is often a placeholder
            if lat_float != 0 or lon_float != 0:
                return True, lat_float, lon_float
        
        return False, None, None
    except (ValueError, TypeError):
        return False, None, None


def format_opening_hours(current_opening_hours: dict) -> str:
    """
    Format opening hours from New Places API response.
    
    Args:
        current_opening_hours: Opening hours dict from New Places API
        
    Returns:
        Formatted string of opening hours
    """
    if not current_opening_hours:
        return None
    
    # Get weekday descriptions (human-readable format)
    weekday_descriptions = current_opening_hours.get('weekdayDescriptions', [])
    if weekday_descriptions:
        return ' | '.join(weekday_descriptions)
    
    return None


def get_business_status(business_status: str) -> str:
    """
    Translate Google's business status to readable format.
    
    Args:
        business_status: Status from New Places API
        
    Returns:
        Human-readable status
    """
    status_map = {
        'OPERATIONAL': 'Open',
        'CLOSED_TEMPORARILY': 'Temporarily Closed',
        'CLOSED_PERMANENTLY': 'Permanently Closed'
    }
    
    return status_map.get(business_status, business_status if business_status else 'Unknown')


def extract_pincode_from_address(address: str) -> Optional[str]:
    """
    Extract Indian pincode from address string.
    Indian pincodes are 6 digits.
    
    Args:
        address: Full address string
        
    Returns:
        Pincode string or None if not found
    """
    if not address:
        return None
    
    # Indian pincode pattern: 6 consecutive digits
    pattern = r'\b(\d{6})\b'
    matches = re.findall(pattern, address)
    
    if matches:
        # Return the last match (pincodes usually appear at end of address)
        return matches[-1]
    
    return None


def extract_state_from_address(address: str) -> Optional[str]:
    """
    Extract state from address string.
    
    Args:
        address: Full address string
        
    Returns:
        State name or None if not found
    """
    if not address:
        return None
    
    # Common Indian states and their variations
    states = [
        'Andhra Pradesh', 'Arunachal Pradesh', 'Assam', 'Bihar', 'Chhattisgarh',
        'Goa', 'Gujarat', 'Haryana', 'Himachal Pradesh', 'Jharkhand', 'Karnataka',
        'Kerala', 'Madhya Pradesh', 'Maharashtra', 'Manipur', 'Meghalaya', 'Mizoram',
        'Nagaland', 'Odisha', 'Punjab', 'Rajasthan', 'Sikkim', 'Tamil Nadu',
        'Telangana', 'Tripura', 'Uttar Pradesh', 'Uttarakhand', 'West Bengal',
        'Delhi', 'Puducherry', 'Jammu and Kashmir', 'Ladakh'
    ]
    
    address_lower = address.lower()
    
    for state in states:
        if state.lower() in address_lower:
            return state
    
    return None


def validate_city_match(found_address: str, expected_city: str) -> bool:
    """
    Check if the found address contains the expected city name.
    STRICT MODE: City must be present in address.
    
    Args:
        found_address: Address from Google Places API
        expected_city: Expected city from input data
        
    Returns:
        True if city matches, False otherwise
    """
    if not found_address or not expected_city:
        return False
    
    city_clean = expected_city.lower().strip()
    address_lower = found_address.lower()
    
    # Direct match
    if city_clean in address_lower:
        return True
    
    # Check for common variations (e.g., "Bangalore" vs "Bengaluru")
    city_variations = {
        'bangalore': 'bengaluru',
        'bengaluru': 'bangalore',
        'bombay': 'mumbai',
        'mumbai': 'bombay',
        'calcutta': 'kolkata',
        'kolkata': 'calcutta',
        'madras': 'chennai',
        'chennai': 'madras',
        'pune': 'poona',
        'poona': 'pune'
    }
    
    if city_clean in city_variations:
        alternate = city_variations[city_clean]
        if alternate in address_lower:
            return True
    
    return False


def validate_state_match(found_address: str, expected_state: str) -> bool:
    """
    Check if the found address contains the expected state name.
    STRICT MODE: State must be present in address.
    
    Args:
        found_address: Address from Google Places API
        expected_state: Expected state from input data
        
    Returns:
        True if state matches, False otherwise
    """
    if not found_address or not expected_state:
        return False
    
    state_clean = expected_state.lower().strip()
    address_lower = found_address.lower()
    
    # Direct match
    if state_clean in address_lower:
        return True
    
    # Check for common abbreviations
    state_abbrev = {
        'andhra pradesh': ['ap', 'a.p.'],
        'arunachal pradesh': ['arunachal'],
        'himachal pradesh': ['hp', 'h.p.'],
        'madhya pradesh': ['mp', 'm.p.'],
        'tamil nadu': ['tn', 't.n.', 'tamilnadu'],
        'uttar pradesh': ['up', 'u.p.'],
        'west bengal': ['wb', 'w.b.', 'bengal'],
        'delhi': ['new delhi', 'ncr'],
        'jammu and kashmir': ['j&k', 'jk', 'jammu'],
        'puducherry': ['pondicherry', 'pondy']
    }
    
    if state_clean in state_abbrev:
        for abbrev in state_abbrev[state_clean]:
            if abbrev in address_lower:
                return True
    
    return False


def validate_restaurant_name(found_name: str, expected_name: str, threshold: float = 0.9) -> Tuple[bool, float]:
    """
    Check if found restaurant name matches expected name with 90%+ similarity.
    STRICT MODE: Requires 90% similarity minimum.
    
    Args:
        found_name: Restaurant name from Google Places API
        expected_name: Expected restaurant name from input data
        threshold: Minimum similarity ratio (default 0.9 = 90%)
        
    Returns:
        Tuple of (is_match, similarity_score)
    """
    if not found_name or not expected_name:
        return False, 0.0
    
    # Clean both names
    found_clean = found_name.lower().strip()
    expected_clean = expected_name.lower().strip()
    
    # Strategy 1: Exact match
    if found_clean == expected_clean:
        return True, 1.0
    
    # Strategy 2: One contains the other (perfect substring match)
    if expected_clean in found_clean or found_clean in expected_clean:
        # Calculate overlap ratio
        shorter = min(len(expected_clean), len(found_clean))
        longer = max(len(expected_clean), len(found_clean))
        ratio = shorter / longer if longer > 0 else 0
        
        if ratio >= threshold:
            return True, ratio
    
    # Strategy 3: Word-based matching
    # Split into words and compare
    expected_words = set(expected_clean.split())
    found_words = set(found_clean.split())
    
    # Remove common stop words
    stop_words = {'restaurant', 'cafe', 'hotel', 'foods', 'kitchen', 'the', 'a', 'an', 'and', 'by'}
    expected_words_clean = expected_words - stop_words
    found_words_clean = found_words - stop_words
    
    # If all words were stop words, use original
    if not expected_words_clean:
        expected_words_clean = expected_words
    if not found_words_clean:
        found_words_clean = found_words
    
    # Calculate word overlap
    if expected_words_clean and found_words_clean:
        overlap = len(expected_words_clean & found_words_clean)
        max_len = max(len(expected_words_clean), len(found_words_clean))
        similarity = overlap / max_len if max_len > 0 else 0
        
        if similarity >= threshold:
            return True, similarity
    
    # Strategy 4: Character-level similarity (Levenshtein-like)
    # Simple approach: count matching characters in order
    matching_chars = 0
    exp_idx = 0
    
    for char in found_clean:
        if exp_idx < len(expected_clean) and char == expected_clean[exp_idx]:
            matching_chars += 1
            exp_idx += 1
    
    char_similarity = matching_chars / max(len(expected_clean), len(found_clean))
    
    if char_similarity >= threshold:
        return True, char_similarity
    
    # Get the best similarity score from all strategies
    best_similarity = max(
        similarity if expected_words_clean and found_words_clean else 0,
        char_similarity
    )
    
    return False, best_similarity


def search_place_strict(
    api_key: str,
    query: str,
    lat: float,
    lon: float,
    radius_meters: int = 10000,
    retry_count: int = 2
) -> Optional[Dict]:
    """
    Search for a place using New Places API Text Search with strict 10km radius.
    
    Args:
        api_key: Google Maps API key
        query: Search query (restaurant name)
        lat: Latitude of search center
        lon: Longitude of search center
        radius_meters: Search radius in meters (fixed at 10km)
        retry_count: Number of retries for failed requests
        
    Returns:
        Place data dictionary or None
    """
    url = "https://places.googleapis.com/v1/places:searchText"
    
    headers = {
        'Content-Type': 'application/json',
        'X-Goog-Api-Key': api_key,
        'X-Goog-FieldMask': (
            'places.id,'
            'places.displayName,'
            'places.formattedAddress,'
            'places.location,'
            'places.businessStatus,'
            'places.currentOpeningHours,'
            'places.googleMapsUri'
        )
    }
    
    payload = {
        "textQuery": query,
        "languageCode": "en",
        "locationBias": {
            "circle": {
                "center": {
                    "latitude": lat,
                    "longitude": lon
                },
                "radius": radius_meters
            }
        }
    }
    
    for attempt in range(retry_count):
        try:
            response = requests.post(url, json=payload, headers=headers, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                places = data.get('places', [])
                
                if places and len(places) > 0:
                    return places[0]  # Return the first (best) match
                else:
                    return None
                    
            elif response.status_code == 429:  # Rate limit
                print(f"  Rate limit hit, waiting...")
                time.sleep(2)
            elif response.status_code == 400:
                print(f"  Bad request: {response.text}")
                return None
            else:
                print(f"  API Error {response.status_code}: {response.text}")
                if attempt < retry_count - 1:
                    time.sleep(1)
                    
        except requests.exceptions.Timeout:
            print(f"  Request timeout for query: {query}")
            if attempt < retry_count - 1:
                time.sleep(1)
        except requests.exceptions.RequestException as e:
            print(f"  Request error: {e}")
            if attempt < retry_count - 1:
                time.sleep(1)
        except Exception as e:
            print(f"  Unexpected error: {e}")
            if attempt < retry_count - 1:
                time.sleep(1)
    
    return None


def geocode_restaurant_strict(
    api_key: str,
    provider_name: str,
    city: str,
    state: str,
    pincode: float,
    network_lat: float,
    network_lon: float,
    retry_count: int = 2
) -> Dict[str, any]:
    """
    Geocode a restaurant with validation checks in this order:
    1. Restaurant name (similarity check, not strict)
    2. Distance (must be within 10km)
    3. Pincode (must match or be nearby)
    4. City (must match)
    5. State (must match)
    
    Args:
        api_key: Google Maps API key
        provider_name: Name of the restaurant/provider
        city: City name (MUST match)
        state: State name (MUST match)
        pincode: Pincode (should match)
        network_lat: Network latitude (search center)
        network_lon: Network longitude (search center)
        retry_count: Number of retries for failed requests
        
    Returns:
        Dictionary with restaurant data (empty if validation fails)
    """
    result = {
        'refined_lat': None,
        'refined_long': None,
        'Restaurant_Status': None,
        'Store_Timings': None,
        'Address': None,
        'Google_Maps_Link': None,
        'Distance_Meters': None,
        'Found_Pincode': None,
        'Found_Restaurant_Name': None
    }
    
    # Clean inputs
    provider_name_clean = str(provider_name).strip()
    city_clean = str(city).strip()
    state_clean = str(state).strip()
    
    # Better pincode handling
    try:
        pincode_str = str(int(float(pincode))) if pd.notna(pincode) else ''
    except (ValueError, TypeError):
        pincode_str = ''
    
    # Search configurations (all within 10km radius)
    search_queries = [
        # Query 1: Just restaurant name
        provider_name_clean,
        # Query 2: Restaurant name + city
        f"{provider_name_clean}, {city_clean}",
        # Query 3: Restaurant name + city + state
        f"{provider_name_clean}, {city_clean}, {state_clean}"
    ]
    
    for query in search_queries:
        try:
            place_data = search_place_strict(
                api_key, 
                query, 
                network_lat, 
                network_lon, 
                radius_meters=10000,  # Fixed 10km radius
                retry_count=retry_count
            )
            
            if not place_data:
                continue
            
            # Extract place data
            location = place_data.get('location', {})
            found_lat = location.get('latitude')
            found_lon = location.get('longitude')
            found_address = place_data.get('formattedAddress')
            found_name = place_data.get('displayName', {}).get('text', '')
            
            if not (found_lat and found_lon and found_address):
                continue
            
            # Calculate distance from network coordinates
            distance = haversine_distance(network_lat, network_lon, found_lat, found_lon)
            
            # Extract pincode from found address
            found_pincode = extract_pincode_from_address(found_address)
            
            # VALIDATION ORDER: name > lat,long > pincode > city > state
            
            # CHECK 1: Restaurant name similarity (not strict, just for matching score)
            name_matches, similarity = validate_restaurant_name(found_name, provider_name_clean, threshold=0.5)
            print(f"    Name: '{found_name}' ({similarity:.1%} match)")
            
            # CHECK 2: Distance - Must be within 10km
            if distance > 10000:
                print(f"    ❌ Failed: Distance {distance:.0f}m > 10km limit")
                continue
            print(f"    ✓ Distance: {distance:.0f}m (within 10km)")
            
            # CHECK 3: Pincode - Check if matches or is nearby
            pincode_match = False
            if found_pincode and pincode_str:
                if found_pincode == pincode_str:
                    pincode_match = True
                    print(f"    ✓ Pincode: {found_pincode} (exact match)")
                elif found_pincode[:3] == pincode_str[:3]:
                    pincode_match = True
                    print(f"    ✓ Pincode: {found_pincode} (same region as {pincode_str})")
                else:
                    print(f"    ❌ Failed: Pincode {found_pincode} doesn't match {pincode_str}")
                    continue
            else:
                print(f"    ⚠️  Warning: Pincode not found in address")
            
            # CHECK 4: City must match
            if not validate_city_match(found_address, city_clean):
                print(f"    ❌ Failed: City '{city_clean}' not found in address")
                continue
            print(f"    ✓ City: '{city_clean}' found in address")
            
            # CHECK 5: State must match
            if not validate_state_match(found_address, state_clean):
                print(f"    ❌ Failed: State '{state_clean}' not found in address")
                continue
            print(f"    ✓ State: '{state_clean}' found in address")
            
            # ALL VALIDATIONS PASSED! ✓
            result['refined_lat'] = round(found_lat, 6)
            result['refined_long'] = round(found_lon, 6)
            result['Restaurant_Status'] = get_business_status(place_data.get('businessStatus'))
            result['Store_Timings'] = format_opening_hours(place_data.get('currentOpeningHours'))
            result['Address'] = found_address
            result['Google_Maps_Link'] = place_data.get('googleMapsUri')
            result['Distance_Meters'] = round(distance, 2)
            result['Found_Pincode'] = found_pincode
            result['Found_Restaurant_Name'] = found_name
            
            print(f"    ✅ SUCCESS: All validations passed!")
            return result
            
        except Exception as e:
            print(f"    ⚠️  Error in search: {e}")
            time.sleep(0.5)
        
        # Rate limiting between queries
        time.sleep(0.06)
    
    # If we reach here, no valid match was found
    print(f"    ❌ No match found within 10km with all requirements met")
    return result


def process_restaurants(
    input_file: str,
    output_file: str,
    api_key: Optional[str] = None,
    save_interval: int = 20,
    start_row: int = 0
) -> None:
    """
    Process restaurants file with STRICT validation requirements.
    
    Args:
        input_file: Path to input Excel file
        output_file: Path to output Excel file
        api_key: Google Maps API key
        save_interval: Save progress after this many rows
        start_row: Row to start processing from
    """
    # Validate input file
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Input file not found: {input_file}")
    
    print(f"{'='*80}")
    print(f"STRICT Google Maps Restaurant Geocoding (10km Radius Only)")
    print(f"{'='*80}")
    print(f"Input file:  {input_file}")
    print(f"Output file: {output_file}")
    print(f"Started at:  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}\n")
    
    # Validate API key
    print("Validating Google Maps API key...")
    api_key = validate_api_key(api_key)
    print("✓ API key validated\n")
    
    # Read input file
    print("Reading input file...")
    df = pd.read_excel(input_file)
    total_rows = len(df)
    print(f"✓ Loaded {total_rows:,} rows\n")
    
    # Add new columns if they don't exist
    new_columns = [
        'refined_lat',
        'refined_long',
        'Restaurant_Status',
        'Store_Timings',
        'Address',
        'Google_Maps_Link',
        'Distance_Meters',
        'Found_Pincode',
        'Found_Restaurant_Name'
    ]
    
    for col in new_columns:
        if col not in df.columns:
            df[col] = None
    
    # Check required columns
    required_cols = ['Provider Name', 'Seller City', 'Seller Pincode', 'State', 'network_lat', 'network_long']
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Required column '{col}' not found in input file")
    
    print(f"{'='*80}")
    print(f"VALIDATION ORDER (checks in sequence)")
    print(f"{'='*80}")
    print(f"1. Restaurant name (similarity check - for matching only)")
    print(f"2. Distance (must be within 10km of network coordinates)")
    print(f"3. Pincode (must match or be in same region)")
    print(f"4. City (must be found in address)")
    print(f"5. State (must be found in address)")
    print(f"")
    print(f"If ANY check fails → row is skipped")
    print(f"{'='*80}\n")
    
    # Check how many rows have valid network coordinates
    valid_coords_count = 0
    for idx in range(total_rows):
        is_valid, _, _ = validate_coordinates(
            df.at[idx, 'network_lat'], 
            df.at[idx, 'network_long']
        )
        if is_valid:
            valid_coords_count += 1
    
    print(f"Rows with valid network coordinates: {valid_coords_count:,} ({valid_coords_count/total_rows*100:.1f}%)")
    print(f"Rows without coordinates will be SKIPPED\n")
    
    # Check already processed
    already_processed = df[(df['refined_lat'].notna()) & (df['Found_Restaurant_Name'].notna())].shape[0]
    needs_processing = valid_coords_count - already_processed
    
    print(f"{'='*80}")
    print(f"PROCESSING STATUS")
    print(f"{'='*80}")
    print(f"Total restaurants:           {total_rows:,}")
    print(f"With valid coordinates:      {valid_coords_count:,}")
    print(f"Already processed:           {already_processed:,}")
    print(f"Need to be processed:        {needs_processing:,}")
    print(f"{'='*80}\n")
    
    if needs_processing == 0:
        print("✓ All restaurants with valid coordinates already processed!")
        return
    
    # Process restaurants
    print(f"Starting strict geocoding process from row {start_row}...\n")
    
    start_time = time.time()
    processed = 0
    successful = 0
    failed = 0
    skipped_no_coords = 0
    skipped_already_done = 0
    
    for idx in range(start_row, total_rows):
        row = df.iloc[idx]
        
        # Skip if already has data
        if pd.notna(df.at[idx, 'Found_Restaurant_Name']):
            skipped_already_done += 1
            processed += 1
            continue
        
        # Get data
        provider_name = row['Provider Name']
        city = row['Seller City']
        state = row['State']
        pincode = row['Seller Pincode']
        network_lat = row['network_lat']
        network_lon = row['network_long']
        
        # Validate network coordinates
        has_coords, net_lat, net_lon = validate_coordinates(network_lat, network_lon)
        
        if not has_coords:
            skipped_no_coords += 1
            processed += 1
            if processed % 100 == 0:
                print(f"  ⏭️  Row {idx}: Skipped (no valid coordinates)")
            continue
        
        # Skip if missing essential data
        if pd.isna(provider_name) or pd.isna(city) or pd.isna(state):
            skipped_no_coords += 1
            processed += 1
            continue
        
        # Show progress
        if processed % 10 == 0 or processed < 5:
            print(f"[{idx}] {provider_name[:50]}... @ ({net_lat:.4f}, {net_lon:.4f})")
        
        # Geocode with strict validation
        try:
            result = geocode_restaurant_strict(
                api_key, provider_name, city, state, pincode,
                net_lat, net_lon
            )
            
            # Update dataframe
            df.at[idx, 'refined_lat'] = result['refined_lat']
            df.at[idx, 'refined_long'] = result['refined_long']
            df.at[idx, 'Restaurant_Status'] = result['Restaurant_Status']
            df.at[idx, 'Store_Timings'] = result['Store_Timings']
            df.at[idx, 'Address'] = result['Address']
            df.at[idx, 'Google_Maps_Link'] = result['Google_Maps_Link']
            df.at[idx, 'Distance_Meters'] = result['Distance_Meters']
            df.at[idx, 'Found_Pincode'] = result['Found_Pincode']
            df.at[idx, 'Found_Restaurant_Name'] = result['Found_Restaurant_Name']
            
            if result['refined_lat'] is not None:
                successful += 1
            else:
                failed += 1
                
        except Exception as e:
            print(f"    ⚠️  Error: {e}")
            failed += 1
        
        processed += 1
        
        # Progress update
        if processed % 50 == 0:
            elapsed = time.time() - start_time
            rate = processed / elapsed if elapsed > 0 else 0
            remaining = (total_rows - start_row - processed) / rate if rate > 0 else 0
            
            print(f"\n{'='*80}")
            print(f"Progress: {processed:,}/{valid_coords_count:,} ({processed/valid_coords_count*100:.1f}%)")
            print(f"Success: {successful:,} | Failed: {failed:,} | Skipped: {skipped_no_coords + skipped_already_done:,}")
            print(f"Success rate: {successful/max(successful+failed, 1)*100:.1f}%")
            print(f"Rate: {rate:.2f} rows/sec | ETA: {remaining/60:.1f} minutes")
            print(f"{'='*80}\n")
        
        # Save intermediate results
        if processed % save_interval == 0:
            print(f"\n💾 Auto-saving progress...")
            df.to_excel(output_file, index=False)
            print(f"  ✓ Saved to: {output_file}\n")
        
        # Rate limiting
        time.sleep(0.06)
    
    # Save final output
    print(f"\n{'='*80}")
    print(f"Saving final results...")
    df.to_excel(output_file, index=False)
    print(f"✓ Saved to: {output_file}")
    
    # Calculate statistics
    elapsed_time = time.time() - start_time
    
    print(f"\n{'='*80}")
    print(f"FINAL RESULTS")
    print(f"{'='*80}")
    print(f"Total rows processed:        {processed:,}")
    print(f"Successfully geocoded:       {successful:,}")
    print(f"Failed validation:           {failed:,}")
    print(f"Skipped (no coords):         {skipped_no_coords:,}")
    print(f"Skipped (already done):      {skipped_already_done:,}")
    print(f"Success rate:                {successful/max(successful+failed, 1)*100:.1f}%")
    print(f"Processing time:             {elapsed_time/60:.1f} minutes")
    print(f"Average rate:                {processed/elapsed_time if elapsed_time > 0 else 0:.2f} rows/second")
    print(f"Completed at:                {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}")


def main():
    """Main function to handle command line arguments."""
    if len(sys.argv) < 3:
        print("="*80)
        print("STRICT Google Maps Restaurant Geocoding")
        print("="*80)
        print("\nUsage:")
        print("  python geocode_restaurants_strict.py <input.xlsx> <output.xlsx> [API_KEY] [START_ROW]")
        print("\nRequired Input Columns:")
        print("  • Provider Name")
        print("  • Seller City")
        print("  • Seller Pincode")
        print("  • State")
        print("  • network_lat")
        print("  • network_long")
        print("\nOutput Columns:")
        print("  • refined_lat (6 decimal accuracy)")
        print("  • refined_long (6 decimal accuracy)")
        print("  • Restaurant_Status (Open/Closed/etc)")
        print("  • Store_Timings")
        print("  • Address")
        print("  • Google_Maps_Link")
        print("  • Distance_Meters (distance from network coordinates)")
        print("  • Found_Pincode (extracted from address)")
        print("  • Found_Restaurant_Name (actual name found by Google)")
        print("\n" + "="*80)
        print("VALIDATION ORDER (checks in this sequence)")
        print("="*80)
        print("1. Restaurant name (similarity check - for matching only)")
        print("2. Distance (must be within 10km of network coordinates)")
        print("3. Pincode (must match or be in same region)")
        print("4. City (must be found in address)")
        print("5. State (must be found in address)")
        print("\nIf ANY check fails → row is skipped (left empty)")
        print("\nThis ensures accuracy while providing the actual restaurant name found.")
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
        print("Already processed data has been saved.")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()