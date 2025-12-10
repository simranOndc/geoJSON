"""
Optimized Batch Processor with Auto-Save and Resume Capability
This script saves progress after every batch, so you can stop and resume anytime
"""

import pandas as pd
import requests
import time
from pathlib import Path
import sys
from datetime import datetime
import json

# CONFIGURATION
API_CHOICE = "google"  # Options: "nominatim" (free) or "google"
GOOGLE_API_KEY = "YOUR_API_KEY"  # Only needed if using Google
BATCH_SIZE = 100  # Save progress after every 100 rows
AUTO_SAVE_INTERVAL = 50  # Save to disk after every 50 geocoding attempts

class GeocoderBatch:
    def __init__(self, input_file, api_choice="nominatim", google_key=None):
        self.input_file = Path(input_file)
        self.api_choice = api_choice
        self.google_key = google_key
        self.progress_file = self.input_file.parent / f"{self.input_file.stem}_progress.json"
        self.output_file = self.input_file.parent / f"{self.input_file.stem}_with_location.xlsx"
        
        # Load data
        print(f"Loading: {self.input_file}")
        self.df = pd.read_excel(self.input_file)
        
        # Add columns if needed
        for col in ['Latitude', 'Longitude', 'Google Maps Link']:
            if col not in self.df.columns:
                self.df[col] = None
        
        # Load or initialize progress
        self.progress = self.load_progress()
        
        print(f"Total rows: {len(self.df)}")
        print(f"Already processed: {self.progress['processed_count']}")
        print(f"Remaining: {len(self.df) - self.progress['processed_count']}\n")
    
    def load_progress(self):
        """Load progress from JSON file or create new"""
        if self.progress_file.exists():
            print(f"Found existing progress file: {self.progress_file}")
            with open(self.progress_file, 'r') as f:
                progress = json.load(f)
            print(f"Resuming from row {progress['last_row']}")
            return progress
        else:
            return {
                'last_row': 0,
                'processed_count': 0,
                'success_count': 0,
                'fail_count': 0,
                'started_at': datetime.now().isoformat(),
                'last_updated': None
            }
    
    def save_progress(self):
        """Save current progress to JSON file"""
        self.progress['last_updated'] = datetime.now().isoformat()
        with open(self.progress_file, 'w') as f:
            json.dump(self.progress, f, indent=2)
    
    def save_output(self):
        """Save current state to Excel"""
        print(f"  Saving to: {self.output_file}")
        self.df.to_excel(self.output_file, index=False)
    
    def geocode_nominatim(self, provider_name, pincode, city, state):
        """Geocode using Nominatim API"""
        try:
            query_parts = [str(provider_name)]
            if pd.notna(city): query_parts.append(str(city))
            if pd.notna(state): query_parts.append(str(state))
            if pd.notna(pincode): query_parts.append(str(int(pincode)))
            query_parts.append("India")
            
            url = "https://nominatim.openstreetmap.org/search"
            params = {
                'q': ", ".join(query_parts),
                'format': 'json',
                'limit': 1
            }
            headers = {'User-Agent': 'ONDC-Restaurant-Geocoder/1.0'}
            
            response = requests.get(url, params=params, headers=headers, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if data and len(data) > 0:
                    lat = data[0]['lat']
                    lon = data[0]['lon']
                    link = f"https://www.google.com/maps/search/?api=1&query={lat},{lon}"
                    return lat, lon, link
            
            return None, None, None
        except Exception as e:
            return None, None, None
    
    def geocode_google(self, provider_name, pincode, city, state):
        """Geocode using Google Maps API"""
        try:
            address_parts = [str(provider_name)]
            if pd.notna(city): address_parts.append(str(city))
            if pd.notna(state): address_parts.append(str(state))
            if pd.notna(pincode): address_parts.append(str(int(pincode)))
            address_parts.append("India")
            
            url = "https://maps.googleapis.com/maps/api/geocode/json"
            params = {
                'address': ", ".join(address_parts),
                'key': self.google_key
            }
            
            response = requests.get(url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if data['status'] == 'OK' and data['results']:
                    location = data['results'][0]['geometry']['location']
                    lat = location['lat']
                    lon = location['lng']
                    link = f"https://www.google.com/maps/search/?api=1&query={lat},{lon}"
                    return lat, lon, link
            
            return None, None, None
        except Exception as e:
            return None, None, None
    
    def geocode(self, provider_name, pincode, city, state):
        """Main geocoding function that routes to appropriate API"""
        if self.api_choice == "google":
            return self.geocode_google(provider_name, pincode, city, state)
        else:
            return self.geocode_nominatim(provider_name, pincode, city, state)
    
    def process(self):
        """Main processing loop"""
        start_row = self.progress['last_row']
        total_rows = len(self.df)
        
        print("="*60)
        print(f"Starting batch processing with {self.api_choice.upper()} API")
        print("="*60)
        print(f"Auto-save every {AUTO_SAVE_INTERVAL} rows")
        print(f"Press Ctrl+C to safely stop and save progress\n")
        
        try:
            for idx in range(start_row, total_rows):
                # Progress display
                if idx % 10 == 0:
                    pct = (idx / total_rows) * 100
                    print(f"\nProgress: {idx}/{total_rows} ({pct:.1f}%)")
                    print(f"Success: {self.progress['success_count']} | Failed: {self.progress['fail_count']}")
                
                row = self.df.iloc[idx]
                
                # Skip if already has data
                if pd.notna(self.df.at[idx, 'Latitude']):
                    self.progress['processed_count'] += 1
                    self.progress['success_count'] += 1
                    continue
                
                # Geocode
                provider = row['Provider Name']
                print(f"  [{idx}] {provider[:40]}...", end=" ")
                
                lat, lon, link = self.geocode(
                    provider,
                    row['Seller Pincode'],
                    row['Seller City'],
                    row['State']
                )
                
                if lat is not None:
                    self.df.at[idx, 'Latitude'] = lat
                    self.df.at[idx, 'Longitude'] = lon
                    self.df.at[idx, 'Google Maps Link'] = link
                    self.progress['success_count'] += 1
                    print("✓")
                else:
                    self.progress['fail_count'] += 1
                    print("✗")
                
                self.progress['processed_count'] += 1
                self.progress['last_row'] = idx + 1
                
                # Auto-save at intervals
                if (idx + 1) % AUTO_SAVE_INTERVAL == 0:
                    print(f"\n  📁 Auto-saving progress...")
                    self.save_progress()
                    self.save_output()
                
                # Respect rate limits
                if self.api_choice == "nominatim":
                    time.sleep(1.5)
                else:
                    time.sleep(0.1)
            
            # Final save
            print("\n\n" + "="*60)
            print("PROCESSING COMPLETE!")
            print("="*60)
            self.save_progress()
            self.save_output()
            
            # Clean up progress file since we're done
            if self.progress_file.exists():
                self.progress_file.unlink()
                print("Progress file removed (processing complete)")
            
        except KeyboardInterrupt:
            print("\n\n⚠️  Interrupted by user - Saving progress...")
            self.save_progress()
            self.save_output()
            print(f"✓ Progress saved. Resume anytime by running the script again.")
            print(f"✓ Partial results saved to: {self.output_file}")
            sys.exit(0)
        
        # Print summary
        print(f"\nTotal processed: {self.progress['processed_count']}")
        print(f"Successful: {self.progress['success_count']}")
        print(f"Failed: {self.progress['fail_count']}")
        if self.progress['processed_count'] > 0:
            success_rate = (self.progress['success_count'] / self.progress['processed_count']) * 100
            print(f"Success rate: {success_rate:.1f}%")
        print(f"\n✓ Output saved to: {self.output_file}")


if __name__ == "__main__":
    INPUT_FILE = "SELLER_LEVEL_DETAILS__1_.xlsx"
    
    print("="*60)
    print("ONDC SELLER LOCATION - BATCH PROCESSOR")
    print("="*60)
    print()
    
    if not Path(INPUT_FILE).exists():
        print(f"❌ Error: '{INPUT_FILE}' not found!")
        print("Place your Excel file in the same directory as this script.")
        sys.exit(1)
    
    # Validate API choice
    if API_CHOICE == "google" and GOOGLE_API_KEY == "YOUR_API_KEY_HERE":
        print("❌ Error: Google API key not configured!")
        print("Either:")
        print("  1. Set GOOGLE_API_KEY in the script, or")
        print("  2. Change API_CHOICE to 'nominatim' (free)")
        sys.exit(1)
    
    # Run processor
    processor = GeocoderBatch(INPUT_FILE, API_CHOICE, GOOGLE_API_KEY)
    processor.process()