#!/usr/bin/env python3
"""
Enhanced Isodistance/Isochrone Generator with HISTORICAL TRAFFIC PATTERNS
Features:
- FREE traffic modeling using historical patterns
- Time-of-day adjustments (rush hour vs off-peak)
- City-specific congestion factors
- Pincode/GPS-based granular adjustments
- Day-of-week patterns (weekday vs weekend)
- Weather season adjustments (monsoon vs dry)
- Learning from actual delivery data
- Generates ONE GeoJSON per provider containing multiple zones
- Parallel processing with progress tracking
"""

import requests
import json
import time
import os
import pandas as pd
import numpy as np
from typing import Dict, Any, Optional, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from datetime import datetime, time as dt_time
import pickle


class HistoricalTrafficModel:
    """
    Models traffic patterns using historical data and patterns.
    NO PAID APIs - Uses statistical modeling and known patterns.
    """
    
    def __init__(self, learning_data_path: Optional[str] = None):
        """
        Initialize with optional historical delivery data.
        
        Args:
            learning_data_path: Path to CSV with historical delivery data
                               Columns: pincode, hour, day_of_week, actual_time_mins, distance_km
        """
        self.learning_data_path = learning_data_path
        self.learned_patterns = {}
        
        if learning_data_path and os.path.exists(learning_data_path):
            self._load_learning_data()
        
        # Initialize default patterns
        self._initialize_default_patterns()
    
    def _initialize_default_patterns(self):
        """Initialize default traffic patterns for Indian cities."""
        
        # City-level base congestion factors (0-1, where 1 = no congestion)
        self.CITY_BASE_FACTORS = {
            # Metro cities (Tier 1)
            'mumbai': 0.45,      # Worst traffic in India
            'bangalore': 0.50,   # Very bad
            'delhi': 0.52,       # Very bad
            'ncr': 0.52,         # NCR region
            'gurgaon': 0.48,
            'noida': 0.55,
            'pune': 0.58,
            'hyderabad': 0.60,
            'chennai': 0.58,
            'kolkata': 0.55,
            
            # Tier 2 cities
            'ahmedabad': 0.65,
            'jaipur': 0.68,
            'lucknow': 0.70,
            'kochi': 0.70,
            'coimbatore': 0.72,
            'indore': 0.72,
            'nagpur': 0.73,
            'surat': 0.70,
            'visakhapatnam': 0.72,
            'bhopal': 0.73,
            
            # Tier 3 and smaller
            'tier3': 0.80,
            'default': 0.75
        }
        
        # Hour of day traffic multipliers (24 hours)
        # Values < 1.0 mean slower (more congestion)
        # Values = 1.0 mean normal
        self.HOURLY_FACTORS = {
            0: 0.95,   # Midnight
            1: 0.98,
            2: 0.99,
            3: 0.99,
            4: 0.98,
            5: 0.90,   # Early morning deliveries start
            6: 0.80,   # Morning starts
            7: 0.60,   # Heavy morning rush
            8: 0.50,   # Peak morning rush
            9: 0.55,   # Still busy
            10: 0.70,  # Easing up
            11: 0.75,  # Lunch prep
            12: 0.70,  # Lunch rush
            13: 0.75,  # Post lunch
            14: 0.80,  # Afternoon lull
            15: 0.75,  # Schools out
            16: 0.70,  # Pre-evening
            17: 0.60,  # Evening rush starts
            18: 0.50,  # Peak evening rush
            19: 0.55,  # Dinner time rush
            20: 0.60,  # Still busy
            21: 0.70,  # Winding down
            22: 0.80,  # Late night
            23: 0.90   # Quiet
        }
        
        # Day of week factors
        self.DAY_FACTORS = {
            0: 0.95,  # Monday
            1: 0.93,  # Tuesday
            2: 0.92,  # Wednesday
            3: 0.93,  # Thursday
            4: 0.90,  # Friday (worst)
            5: 1.00,  # Saturday (better)
            6: 1.05   # Sunday (best)
        }
        
        # Season/weather factors (India-specific)
        self.SEASON_FACTORS = {
            'monsoon': 0.75,      # June-Sept: Heavy rain slows traffic
            'winter': 0.95,       # Oct-Feb: Good conditions
            'summer': 0.90,       # Mar-May: Hot but ok
            'festival': 0.65      # Diwali, Holi, etc.
        }
        
        # Area type factors (can be inferred from pincode patterns)
        self.AREA_TYPE_FACTORS = {
            'cbd': 0.55,          # Central Business District
            'commercial': 0.65,   # Commercial areas
            'residential': 0.80,  # Residential
            'industrial': 0.75,   # Industrial
            'suburban': 0.85,     # Suburbs
            'rural': 0.95         # Rural/outskirts
        }
        
        # Metro city pincode patterns (for area type detection)
        self.PINCODE_PATTERNS = {
            'bangalore_cbd': ['560001', '560002', '560009'],  # MG Road, Commercial St
            'bangalore_commercial': ['560100', '560103', '560038'],  # Whitefield, Electronic City
            'delhi_cbd': ['110001', '110002', '110003'],  # Connaught Place
            'mumbai_cbd': ['400001', '400021', '400051'],  # Fort, BKC
            'chennai_cbd': ['600001', '600002'],  # George Town
            # Add more as needed
        }
    
    def _load_learning_data(self):
        """Load historical delivery data to improve predictions."""
        try:
            df = pd.read_csv(self.learning_data_path)
            
            # Required columns: pincode, hour, day_of_week, actual_time_mins, distance_km
            required_cols = ['pincode', 'hour', 'day_of_week', 'actual_time_mins', 'distance_km']
            if not all(col in df.columns for col in required_cols):
                print(f"⚠️  Learning data missing columns. Required: {required_cols}")
                return
            
            # Calculate actual speed for each delivery
            df['actual_speed_kmh'] = (df['distance_km'] / df['actual_time_mins']) * 60
            
            # Group by pincode, hour, day to get average speeds
            grouped = df.groupby(['pincode', 'hour', 'day_of_week']).agg({
                'actual_speed_kmh': 'mean',
                'actual_time_mins': 'count'  # Number of samples
            }).reset_index()
            
            grouped.columns = ['pincode', 'hour', 'day_of_week', 'avg_speed', 'sample_count']
            
            # Store learned patterns
            for _, row in grouped.iterrows():
                key = (str(row['pincode']), int(row['hour']), int(row['day_of_week']))
                self.learned_patterns[key] = {
                    'speed_kmh': row['avg_speed'],
                    'confidence': min(row['sample_count'] / 10, 1.0)  # Max confidence at 10 samples
                }
            
            print(f"✅ Loaded {len(self.learned_patterns)} learned traffic patterns")
            
        except Exception as e:
            print(f"⚠️  Could not load learning data: {e}")
    
    def detect_city_from_pincode(self, pincode: str) -> str:
        """Detect city from Indian pincode."""
        if not pincode or len(str(pincode)) < 3:
            return 'default'
        
        pincode_str = str(pincode)[:3]
        
        # Indian pincode mapping (first 3 digits indicate region)
        PINCODE_CITY_MAP = {
            '110': 'delhi', '111': 'delhi', '112': 'ncr', '121': 'gurgaon', '122': 'gurgaon',
            '201': 'noida', '202': 'noida',
            '400': 'mumbai', '401': 'mumbai', '421': 'mumbai', '422': 'mumbai',
            '560': 'bangalore', '562': 'bangalore',
            '600': 'chennai', '601': 'chennai',
            '500': 'hyderabad', '501': 'hyderabad',
            '411': 'pune', '412': 'pune',
            '380': 'ahmedabad',
            '700': 'kolkata', '711': 'kolkata',
            '302': 'jaipur',
            '682': 'kochi',
            '641': 'coimbatore',
            '226': 'lucknow',
            '452': 'indore',
            '440': 'nagpur',
            '395': 'surat',
            '530': 'visakhapatnam',
            '462': 'bhopal'
        }
        
        return PINCODE_CITY_MAP.get(pincode_str, 'default')
    
    def detect_area_type(self, pincode: str) -> str:
        """Detect area type (CBD, commercial, residential, etc.) from pincode."""
        pincode_str = str(pincode)
        
        # Check known CBD/commercial pincodes
        for pattern_type, pincodes in self.PINCODE_PATTERNS.items():
            if pincode_str in pincodes:
                if 'cbd' in pattern_type:
                    return 'cbd'
                elif 'commercial' in pattern_type:
                    return 'commercial'
        
        # Heuristic: Lower pincodes in a city are usually CBD/commercial
        # This is a generalization but works reasonably well
        city = self.detect_city_from_pincode(pincode_str)
        
        if city in ['mumbai', 'delhi', 'bangalore', 'chennai', 'kolkata']:
            pincode_int = int(pincode_str) if pincode_str.isdigit() else 0
            city_base = int(pincode_str[:3]) * 1000
            
            # First 20 pincodes in a metro are usually CBD/commercial
            if pincode_int < city_base + 20:
                return 'commercial'
            elif pincode_int < city_base + 50:
                return 'residential'
            else:
                return 'suburban'
        
        return 'residential'  # Default
    
    def get_current_season(self, date: Optional[datetime] = None) -> str:
        """Determine current season in India."""
        if date is None:
            date = datetime.now()
        
        month = date.month
        
        # Indian seasons
        if month in [6, 7, 8, 9]:
            return 'monsoon'
        elif month in [10, 11, 12, 1, 2]:
            return 'winter'
        else:  # 3, 4, 5
            return 'summer'
    
    def calculate_traffic_factor(
        self,
        pincode: str,
        hour: int,
        day_of_week: int,
        date: Optional[datetime] = None
    ) -> float:
        """
        Calculate combined traffic factor (0-1 scale).
        
        Returns:
            float: Traffic speed factor (1.0 = no congestion, 0.5 = 50% slower)
        """
        if date is None:
            date = datetime.now()
        
        # Check if we have learned data for this specific condition
        learned_key = (str(pincode), hour, day_of_week)
        if learned_key in self.learned_patterns:
            pattern = self.learned_patterns[learned_key]
            confidence = pattern['confidence']
            learned_factor = pattern['speed_kmh'] / 30  # Normalize to base speed of 30 kmh
            learned_factor = max(0.3, min(1.0, learned_factor))  # Clamp to reasonable range
            
            if confidence > 0.7:  # High confidence, use learned data
                return learned_factor
            # Otherwise, blend with default patterns
        else:
            confidence = 0
            learned_factor = 0
        
        # Calculate using default patterns
        city = self.detect_city_from_pincode(pincode)
        area_type = self.detect_area_type(pincode)
        season = self.get_current_season(date)
        
        # Base city factor
        city_factor = self.CITY_BASE_FACTORS.get(city, self.CITY_BASE_FACTORS['default'])
        
        # Hour of day factor
        hour_factor = self.HOURLY_FACTORS.get(hour, 1.0)
        
        # Day of week factor
        day_factor = self.DAY_FACTORS.get(day_of_week, 1.0)
        
        # Area type factor
        area_factor = self.AREA_TYPE_FACTORS.get(area_type, 0.80)
        
        # Season factor
        season_factor = self.SEASON_FACTORS.get(season, 1.0)
        
        # Combined factor (multiplicative)
        default_factor = city_factor * hour_factor * day_factor * area_factor * season_factor
        
        # Clamp to reasonable range (traffic never makes things faster, max 70% slowdown)
        default_factor = max(0.30, min(1.0, default_factor))
        
        # Blend learned and default if we have partial confidence
        if confidence > 0:
            final_factor = (learned_factor * confidence) + (default_factor * (1 - confidence))
        else:
            final_factor = default_factor
        
        return final_factor
    
    def adjust_distance_for_traffic(
        self,
        distance_km: float,
        pincode: str,
        hour: int,
        day_of_week: int,
        date: Optional[datetime] = None
    ) -> float:
        """
        Adjust requested distance based on traffic conditions.
        
        Example:
            Requested: 5 km zone
            Traffic factor: 0.6 (40% slower)
            Adjusted: 3 km (to maintain same travel time)
        """
        traffic_factor = self.calculate_traffic_factor(pincode, hour, day_of_week, date)
        adjusted_distance = distance_km * traffic_factor
        return round(adjusted_distance, 2)
    
    def adjust_time_for_traffic(
        self,
        time_minutes: int,
        pincode: str,
        hour: int,
        day_of_week: int,
        date: Optional[datetime] = None
    ) -> int:
        """
        Adjust travel time to get same coverage area.
        
        Example:
            Requested: 20 min zone
            Traffic factor: 0.6 (40% slower)
            Adjusted: 33 min (20 / 0.6) to cover same area
        """
        traffic_factor = self.calculate_traffic_factor(pincode, hour, day_of_week, date)
        
        # If traffic is bad (factor low), need more time to cover same area
        adjusted_time = time_minutes / traffic_factor if traffic_factor > 0 else time_minutes * 2
        
        return int(round(adjusted_time))
    
    def get_traffic_metadata(
        self,
        pincode: str,
        hour: int,
        day_of_week: int,
        date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """Get detailed traffic condition metadata."""
        if date is None:
            date = datetime.now()
        
        city = self.detect_city_from_pincode(pincode)
        area_type = self.detect_area_type(pincode)
        season = self.get_current_season(date)
        traffic_factor = self.calculate_traffic_factor(pincode, hour, day_of_week, date)
        
        # Classify traffic condition
        if traffic_factor >= 0.85:
            condition = 'Light'
        elif traffic_factor >= 0.70:
            condition = 'Moderate'
        elif traffic_factor >= 0.55:
            condition = 'Heavy'
        else:
            condition = 'Very Heavy'
        
        return {
            'city': city,
            'area_type': area_type,
            'season': season,
            'traffic_factor': round(traffic_factor, 3),
            'traffic_condition': condition,
            'speed_reduction_percent': round((1 - traffic_factor) * 100, 1),
            'day_of_week': ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'][day_of_week],
            'hour': hour,
            'has_learned_data': (str(pincode), hour, day_of_week) in self.learned_patterns
        }
    
    def export_model(self, filepath: str):
        """Export learned patterns for reuse."""
        data = {
            'learned_patterns': self.learned_patterns,
            'version': '1.0',
            'exported_at': datetime.now().isoformat()
        }
        
        with open(filepath, 'wb') as f:
            pickle.dump(data, f)
        
        print(f"✅ Exported traffic model to {filepath}")
    
    def import_model(self, filepath: str):
        """Import previously learned patterns."""
        try:
            with open(filepath, 'rb') as f:
                data = pickle.load(f)
            
            self.learned_patterns = data.get('learned_patterns', {})
            print(f"✅ Imported {len(self.learned_patterns)} learned patterns")
            
        except Exception as e:
            print(f"⚠️  Could not import model: {e}")


class TrafficAwareIsodistanceGenerator:
    """
    Generator that uses historical traffic model to adjust isochrones.
    """
    
    def __init__(self, traffic_model: Optional[HistoricalTrafficModel] = None):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'TrafficAwareIsodistanceGenerator/2.0',
            'Content-Type': 'application/json',
        })
        
        self.traffic_model = traffic_model or HistoricalTrafficModel()
        self.valhalla_url = "https://valhalla1.openstreetmap.de/isochrone"
        
        # Stats
        self.stats = {
            'generated': 0,
            'failed': 0,
            'traffic_adjusted': 0
        }
    
    def _valhalla_costing_from_mode(self, mode: str) -> str:
        """Map mode to Valhalla costing."""
        VALHALLA_MODES = {
            'walk': 'pedestrian',
            'walking': 'pedestrian',
            'pedestrian': 'pedestrian',
            'bike': 'bicycle',
            'bicycle': 'bicycle',
            'motorcycle': 'motorcycle',
            'car': 'auto',
            'auto': 'auto',
        }
        return VALHALLA_MODES.get(mode.lower(), 'motorcycle')
    
    def generate_traffic_aware_distance_zone(
        self,
        lat: float,
        lon: float,
        distance_km: float,
        pincode: str,
        mode: str = 'motorcycle',
        hour: Optional[int] = None,
        day_of_week: Optional[int] = None,
        date: Optional[datetime] = None,
        max_retries: int = 3
    ) -> Optional[Dict[str, Any]]:
        """
        Generate distance-based zone with traffic awareness.
        
        The distance is ADJUSTED based on traffic conditions to maintain
        realistic travel time expectations.
        """
        if date is None:
            date = datetime.now()
        if hour is None:
            hour = date.hour
        if day_of_week is None:
            day_of_week = date.weekday()
        
        # Get traffic metadata
        traffic_metadata = self.traffic_model.get_traffic_metadata(
            pincode, hour, day_of_week, date
        )
        
        # Adjust distance for traffic
        adjusted_distance = self.traffic_model.adjust_distance_for_traffic(
            distance_km, pincode, hour, day_of_week, date
        )
        
        costing = self._valhalla_costing_from_mode(mode)
        
        payload = {
            "locations": [{"lat": lat, "lon": lon}],
            "costing": costing,
            "contours": [{"distance": adjusted_distance}],
            "polygons": True,
            "denoise": 0.3,
            "generalize": 50,
        }
        
        for attempt in range(max_retries):
            try:
                response = self.session.post(
                    self.valhalla_url,
                    json=payload,
                    timeout=30
                )
                response.raise_for_status()
                
                data = response.json()
                
                # Add comprehensive metadata
                if 'features' in data:
                    for feature in data['features']:
                        feature['properties'].update({
                            'api': 'valhalla',
                            'traffic_aware': True,
                            'traffic_model': 'historical',
                            'requested_distance_km': distance_km,
                            'adjusted_distance_km': adjusted_distance,
                            **traffic_metadata
                        })
                
                self.stats['generated'] += 1
                self.stats['traffic_adjusted'] += 1
                return data
                
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(2)
                else:
                    self.stats['failed'] += 1
                    print(f"  Failed distance {distance_km}km: {e}")
                    return None
        
        return None
    
    def generate_traffic_aware_time_zone(
        self,
        lat: float,
        lon: float,
        time_minutes: int,
        pincode: str,
        mode: str = 'motorcycle',
        hour: Optional[int] = None,
        day_of_week: Optional[int] = None,
        date: Optional[datetime] = None,
        max_retries: int = 3
    ) -> Optional[Dict[str, Any]]:
        """
        Generate time-based zone WITHOUT traffic adjustment.
        
        Time zones naturally adapt to road conditions, so we don't adjust.
        But we add traffic metadata for context.
        """
        if date is None:
            date = datetime.now()
        if hour is None:
            hour = date.hour
        if day_of_week is None:
            day_of_week = date.weekday()
        
        # Get traffic metadata (for information only)
        traffic_metadata = self.traffic_model.get_traffic_metadata(
            pincode, hour, day_of_week, date
        )
        
        costing = self._valhalla_costing_from_mode(mode)
        
        payload = {
            "locations": [{"lat": lat, "lon": lon}],
            "costing": costing,
            "contours": [{"time": time_minutes}],
            "polygons": True,
            "denoise": 0.3,
            "generalize": 50,
        }
        
        for attempt in range(max_retries):
            try:
                response = self.session.post(
                    self.valhalla_url,
                    json=payload,
                    timeout=30
                )
                response.raise_for_status()
                
                data = response.json()
                
                # Add metadata
                if 'features' in data:
                    for feature in data['features']:
                        feature['properties'].update({
                            'api': 'valhalla',
                            'traffic_aware': False,  # Time zones don't need adjustment
                            'traffic_model': 'historical',
                            'time_minutes': time_minutes,
                            **traffic_metadata
                        })
                
                self.stats['generated'] += 1
                return data
                
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(2)
                else:
                    self.stats['failed'] += 1
                    print(f"  Failed time {time_minutes}min: {e}")
                    return None
        
        return None
    
    def generate_all_zones_for_provider(
        self,
        name: str,
        lat: float,
        lon: float,
        pincode: str,
        distances_km: List[float] = [3, 4, 5, 6],
        times_minutes: List[int] = [15, 20, 30],
        mode: str = 'motorcycle',
        hour: Optional[int] = None,
        day_of_week: Optional[int] = None,
        date: Optional[datetime] = None,
        quiet: bool = False
    ) -> Dict[str, Any]:
        """
        Generate all zones for a single provider with traffic awareness.
        """
        if date is None:
            date = datetime.now()
        if hour is None:
            hour = date.hour
        if day_of_week is None:
            day_of_week = date.weekday()
        
        if not quiet:
            traffic_info = self.traffic_model.get_traffic_metadata(
                pincode, hour, day_of_week, date
            )
            print(f"🏍️  Generating zones for '{name}'")
            print(f"   Location: {traffic_info['city'].title()} ({traffic_info['area_type']})")
            print(f"   Traffic: {traffic_info['traffic_condition']} "
                  f"({traffic_info['speed_reduction_percent']}% slower)")
            print(f"   Time: {traffic_info['day_of_week']} {hour}:00")
        
        all_features = []
        
        # Generate distance-based zones (WITH traffic adjustment)
        for distance in distances_km:
            result = self.generate_traffic_aware_distance_zone(
                lat, lon, distance, pincode, mode, hour, day_of_week, date
            )
            if result and 'features' in result:
                for feature in result['features']:
                    feature['properties'].update({
                        'provider_name': name,
                        'zone_type': 'distance',
                        'label': f"{distance}km"
                    })
                    all_features.append(feature)
                
                if not quiet:
                    adj_dist = feature['properties'].get('adjusted_distance_km', distance)
                    print(f"   ✅ {distance}km → {adj_dist}km (adjusted)")
            else:
                if not quiet:
                    print(f"   ❌ {distance}km FAILED")
        
        # Generate time-based zones (NO adjustment needed)
        for time_min in times_minutes:
            result = self.generate_traffic_aware_time_zone(
                lat, lon, time_min, pincode, mode, hour, day_of_week, date
            )
            if result and 'features' in result:
                for feature in result['features']:
                    feature['properties'].update({
                        'provider_name': name,
                        'zone_type': 'time',
                        'label': f"{time_min}min"
                    })
                    all_features.append(feature)
                
                if not quiet:
                    print(f"   ✅ {time_min}min")
            else:
                if not quiet:
                    print(f"   ❌ {time_min}min FAILED")
        
        # Create combined GeoJSON
        combined_geojson = {
            "type": "FeatureCollection",
            "metadata": {
                "provider_name": name,
                "center_lat": lat,
                "center_lon": lon,
                "pincode": pincode,
                "mode": mode,
                "total_zones": len(all_features),
                "distance_zones": distances_km,
                "time_zones": times_minutes,
                "generated_at": datetime.utcnow().isoformat() + 'Z',
                "generation_conditions": self.traffic_model.get_traffic_metadata(
                    pincode, hour, day_of_week, date
                )
            },
            "features": all_features
        }
        
        return combined_geojson


class ExcelBatchProcessor:
    """Batch process Excel file with traffic-aware zones."""
    
    def __init__(
        self,
        output_dir: str = "batch_output",
        max_workers: int = 5,
        traffic_model: Optional[HistoricalTrafficModel] = None,
        learning_data_path: Optional[str] = None
    ):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.max_workers = max_workers
        
        # Initialize traffic model
        if traffic_model:
            self.traffic_model = traffic_model
        else:
            self.traffic_model = HistoricalTrafficModel(learning_data_path)
        
        self.generator = TrafficAwareIsodistanceGenerator(self.traffic_model)
    
    def load_from_excel(
        self,
        excel_file: str,
        distances_km: List[float] = [3, 4, 5, 6],
        times_minutes: List[int] = [15, 20, 30],
        mode: str = 'motorcycle',
        hour: Optional[int] = None,
        day_of_week: Optional[int] = None
    ) -> Tuple[pd.DataFrame, List[Dict]]:
        """Load provider data from Excel file."""
        
        df = pd.read_excel(excel_file)
        
        required_cols = ['Provider Name', 'Provider ID', 'Latitude', 'Longitude', 'bpp id', 'Seller Pincode']
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")
        
        print(f"📊 Loaded Excel file: {excel_file}")
        print(f"   Total rows: {len(df)}")
        
        # Filter rows with valid coordinates
        df_valid = df.dropna(subset=['Latitude', 'Longitude', 'Seller Pincode']).copy()
        print(f"   Valid rows: {len(df_valid)}")
        
        # Determine time conditions
        if hour is None or day_of_week is None:
            now = datetime.now()
            hour = now.hour if hour is None else hour
            day_of_week = now.weekday() if day_of_week is None else day_of_week
        
        # Create tasks
        tasks = []
        for idx, row in df_valid.iterrows():
            pincode = str(int(row['Seller Pincode']))
            
            tasks.append({
                'excel_idx': idx,
                'name': str(row['Provider Name']),
                'provider_id': str(row['Provider ID']),
                'bpp_id': str(row['bpp id']),
                'pincode': pincode,
                'lat': float(row['Latitude']),
                'lon': float(row['Longitude']),
                'distances_km': distances_km,
                'times_minutes': times_minutes,
                'mode': mode,
                'hour': hour,
                'day_of_week': day_of_week
            })
        
        print(f"\n📋 Configuration:")
        print(f"   Providers to process: {len(tasks)}")
        print(f"   Distance zones: {distances_km} km (will be traffic-adjusted)")
        print(f"   Time zones: {times_minutes} min")
        print(f"   Mode: {mode}")
        print(f"   Time conditions: {['Mon','Tue','Wed','Thu','Fri','Sat','Sun'][day_of_week]} {hour}:00")
        
        # Show sample traffic conditions
        if tasks:
            sample = tasks[0]
            traffic_info = self.traffic_model.get_traffic_metadata(
                sample['pincode'], hour, day_of_week
            )
            print(f"\n🚦 Sample Traffic Conditions:")
            print(f"   City: {traffic_info['city'].title()}")
            print(f"   Area: {traffic_info['area_type']}")
            print(f"   Condition: {traffic_info['traffic_condition']}")
            print(f"   Speed reduction: {traffic_info['speed_reduction_percent']}%")
        
        return df, tasks
    
    def process_single_provider(
        self,
        task: Dict,
        task_num: int,
        total_tasks: int
    ) -> Dict:
        """Process a single provider with traffic-aware zones."""
        start_time = time.time()
        
        try:
            # Generate all zones
            combined_geojson = self.generator.generate_all_zones_for_provider(
                name=task['name'],
                lat=task['lat'],
                lon=task['lon'],
                pincode=task['pincode'],
                distances_km=task['distances_km'],
                times_minutes=task['times_minutes'],
                mode=task['mode'],
                hour=task['hour'],
                day_of_week=task['day_of_week'],
                quiet=True
            )
            
            if not combined_geojson['features']:
                raise Exception("No zones generated successfully")
            
            # Create filename
            def clean_string(s):
                return str(s).replace(' ', '_').replace(',', '').replace('/', '_').replace(':', '').replace('?', '')
            
            bpp_id_clean = clean_string(task['bpp_id'])
            provider_name_clean = clean_string(task['name'])
            provider_id_clean = clean_string(task['provider_id'])
            pincode_clean = clean_string(task['pincode'])
            
            filename = f"{bpp_id_clean}+{provider_name_clean}+{provider_id_clean}+{pincode_clean}.geojson"
            filepath = self.output_dir / filename
            
            with open(filepath, 'w') as f:
                json.dump(combined_geojson, f, indent=2)
            
            elapsed = round(time.time() - start_time, 2)
            
            return {
                'status': 'success',
                'excel_idx': task['excel_idx'],
                'name': task['name'],
                'filepath': str(filepath),
                'geojson': combined_geojson,
                'zones_count': len(combined_geojson['features']),
                'elapsed': elapsed,
                'error': None
            }
            
        except Exception as e:
            elapsed = round(time.time() - start_time, 2)
            return {
                'status': 'failed',
                'excel_idx': task['excel_idx'],
                'name': task['name'],
                'filepath': None,
                'geojson': None,
                'zones_count': 0,
                'elapsed': elapsed,
                'error': str(e)
            }
    
    def process_batch(
        self,
        tasks: List[Dict],
        parallel: bool = True
    ) -> List[Dict]:
        """Process all providers with progress tracking."""
        results = []
        total_tasks = len(tasks)
        
        print(f"\n{'='*70}")
        print(f"PROCESSING {total_tasks} PROVIDERS WITH TRAFFIC AWARENESS")
        print(f"Parallel: {parallel} | Workers: {self.max_workers if parallel else 1}")
        print(f"{'='*70}\n")
        
        if parallel and self.max_workers > 1:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {
                    executor.submit(
                        self.process_single_provider,
                        task,
                        i + 1,
                        total_tasks
                    ): task for i, task in enumerate(tasks)
                }
                
                for future in as_completed(futures):
                    result = future.result()
                    results.append(result)
                    
                    completed = len(results)
                    percent = (completed / total_tasks) * 100
                    status_icon = "✅" if result['status'] == 'success' else "❌"
                    zones_info = f"({result['zones_count']} zones)" if result['status'] == 'success' else ""
                    
                    print(f"[{completed}/{total_tasks} - {percent:.0f}%] {status_icon} "
                          f"{result['name']} {zones_info} ({result['elapsed']}s)")
        else:
            for i, task in enumerate(tasks):
                result = self.process_single_provider(task, i + 1, total_tasks)
                results.append(result)
                
                completed = len(results)
                percent = (completed / total_tasks) * 100
                status_icon = "✅" if result['status'] == 'success' else "❌"
                zones_info = f"({result['zones_count']} zones)" if result['status'] == 'success' else ""
                
                print(f"[{completed}/{total_tasks} - {percent:.0f}%] {status_icon} "
                      f"{result['name']} {zones_info} ({result['elapsed']}s)")
        
        return results
    
    def add_results_to_excel(
        self,
        df: pd.DataFrame,
        results: List[Dict],
        output_excel: str
    ) -> pd.DataFrame:
        """Add results back to Excel."""
        
        print(f"\n{'='*70}")
        print("ADDING RESULTS TO EXCEL")
        print(f"{'='*70}")
        
        # Add columns
        df['zones_geojson'] = None
        df['zones_file'] = None
        df['zones_count'] = None
        df['processing_status'] = None
        df['traffic_adjusted'] = None
        
        for result in results:
            idx = result['excel_idx']
            
            if result['status'] == 'success':
                df.at[idx, 'zones_geojson'] = json.dumps(result['geojson'])
                df.at[idx, 'zones_file'] = result['filepath']
                df.at[idx, 'zones_count'] = result['zones_count']
                df.at[idx, 'processing_status'] = 'success'
                df.at[idx, 'traffic_adjusted'] = 'yes'
            else:
                df.at[idx, 'processing_status'] = f"failed: {result['error']}"
                df.at[idx, 'traffic_adjusted'] = 'no'
        
        df.to_excel(output_excel, index=False, engine='openpyxl')
        
        successful = sum(1 for r in results if r['status'] == 'success')
        print(f"✅ Updated Excel saved: {output_excel}")
        print(f"   Successful: {successful}/{len(results)}")
        
        return df
    
    def save_summary(
        self,
        results: List[Dict],
        filename: str = "batch_summary.json"
    ) -> Dict:
        """Save processing summary."""
        
        successful = sum(1 for r in results if r['status'] == 'success')
        failed = sum(1 for r in results if r['status'] == 'failed')
        total_time = sum(r['elapsed'] for r in results)
        total_zones = sum(r['zones_count'] for r in results if r['status'] == 'success')
        
        summary = {
            'total_providers': len(results),
            'successful': successful,
            'failed': failed,
            'total_zones_generated': total_zones,
            'total_time': round(total_time, 2),
            'average_time_per_provider': round(total_time / len(results), 2) if results else 0,
            'traffic_model_stats': {
                'learned_patterns': len(self.traffic_model.learned_patterns),
                'generator_stats': self.generator.stats
            },
            'results': results
        }
        
        summary_path = self.output_dir / filename
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)
        
        print(f"\n{'='*70}")
        print("BATCH PROCESSING SUMMARY")
        print(f"{'='*70}")
        print(f"Total providers: {summary['total_providers']}")
        print(f"Successful: {successful} ✅")
        print(f"Failed: {failed} ❌")
        print(f"Total zones generated: {total_zones}")
        print(f"Total time: {summary['total_time']:.2f}s")
        print(f"Average time per provider: {summary['average_time_per_provider']:.2f}s")
        print(f"\n🚦 Traffic Model:")
        print(f"   Learned patterns: {len(self.traffic_model.learned_patterns)}")
        print(f"   Traffic-adjusted zones: {self.generator.stats['traffic_adjusted']}")
        print(f"Summary saved: {summary_path}")
        print(f"{'='*70}\n")
        
        return summary


def batch_process_from_excel_with_traffic(
    excel_file: str,
    output_excel: str = None,
    output_dir: str = "batch_output",
    distances_km: List[float] = [3, 4, 5, 6],
    times_minutes: List[int] = [15, 20, 30],
    mode: str = 'motorcycle',
    max_workers: int = 5,
    hour: Optional[int] = None,
    day_of_week: Optional[int] = None,
    learning_data_path: Optional[str] = None
) -> Dict:
    """
    Batch process providers with TRAFFIC AWARENESS using historical patterns.
    
    Args:
        excel_file: Path to Excel file
        output_excel: Output Excel file path
        output_dir: Directory for GeoJSON files
        distances_km: Distance zones in km (will be traffic-adjusted)
        times_minutes: Time zones in minutes
        mode: Transportation mode
        max_workers: Number of parallel workers
        hour: Hour of day (0-23), defaults to current hour
        day_of_week: Day of week (0=Monday, 6=Sunday), defaults to today
        learning_data_path: Optional path to historical delivery data CSV
    """
    if output_excel is None:
        input_path = Path(excel_file)
        output_excel = str(input_path.parent / f"{input_path.stem}_with_traffic_zones{input_path.suffix}")
    
    processor = ExcelBatchProcessor(
        output_dir=output_dir,
        max_workers=max_workers,
        learning_data_path=learning_data_path
    )
    
    df, tasks = processor.load_from_excel(
        excel_file,
        distances_km=distances_km,
        times_minutes=times_minutes,
        mode=mode,
        hour=hour,
        day_of_week=day_of_week
    )
    
    if not tasks:
        print("❌ No valid providers found")
        return {}
    
    results = processor.process_batch(tasks, parallel=True)
    df_updated = processor.add_results_to_excel(df, results, output_excel)
    summary = processor.save_summary(results)
    
    return summary


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Generate traffic-aware isochrones using historical patterns (FREE)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Current time conditions
  python script.py --excel providers.xlsx
  
  # Specific time (evening rush hour)
  python script.py --excel providers.xlsx --hour 19 --day 4
  
  # With historical learning data
  python script.py --excel providers.xlsx --learning-data deliveries.csv
  
  # Custom distances (will be traffic-adjusted)
  python script.py --excel providers.xlsx --distances 3 5 7
  
  # Weekend conditions
  python script.py --excel providers.xlsx --day 6
        """
    )
    
    parser.add_argument('--excel', '-e', required=True,
                        help='Excel file with provider data')
    parser.add_argument('--output-excel', '-oe',
                        help='Output Excel file')
    parser.add_argument('--output-dir', '-od', default='batch_output_traffic',
                        help='Output directory for GeoJSON files')
    parser.add_argument('--distances', '-d', type=float, nargs='*', default=[3, 4, 5, 6],
                        help='Distance zones in km (will be traffic-adjusted)')
    parser.add_argument('--times', '-t', type=int, nargs='*', default=[15, 20, 30],
                        help='Time zones in minutes (default: 15 20 30)')
    parser.add_argument('--mode', '-m', default='motorcycle',
                        choices=['motorcycle', 'car', 'bike', 'walk'],
                        help='Transportation mode')
    parser.add_argument('--workers', '-w', type=int, default=5,
                        help='Number of parallel workers')
    parser.add_argument('--hour', type=int, choices=range(24),
                        help='Hour of day (0-23), defaults to current')
    parser.add_argument('--day', type=int, choices=range(7),
                        help='Day of week (0=Mon, 6=Sun), defaults to today')
    parser.add_argument('--learning-data', '-l',
                        help='Path to CSV with historical delivery data')
    
    args = parser.parse_args()
    
    print("🚦 Starting traffic-aware batch processing...")
    print(f"Excel file: {args.excel}")
    print(f"Mode: {args.mode}")
    
    if args.learning_data:
        print(f"Learning from: {args.learning_data}")
    
    print()
    
    batch_process_from_excel_with_traffic(
        excel_file=args.excel,
        output_excel=args.output_excel,
        output_dir=args.output_dir,
        distances_km=args.distances,
        times_minutes=args.times,
        mode=args.mode,
        max_workers=args.workers,
        hour=args.hour,
        day_of_week=args.day,
        learning_data_path=args.learning_data
    )