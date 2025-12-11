#!/usr/bin/env python3
"""
Enhanced Isodistance/Isochrone Generator - One GeoJSON per Provider
Features:
- Generates ONE GeoJSON per provider containing multiple zones
- Supports both distance-based (3,4,5,6 km) and time-based (10,20 min) zones
- Motorcycle mode
- Parallel processing with progress tracking
"""

import requests
import json
import time
import os
import pandas as pd
from typing import Dict, Any, Optional, List
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from datetime import datetime


class IsodistanceGenerator:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(
            {
                'User-Agent': 'IsodistanceGenerator/4.0',
                'Content-Type': 'application/json',
            }
        )

    def generate_distance_zone_valhalla(
        self,
        lat: float,
        lon: float,
        distance_km: float,
        mode: str = 'motorcycle',
        max_retries: int = 3
    ) -> Optional[Dict[str, Any]]:
        """Generate distance-based zone using Valhalla API."""
        VALHALLA_MODES = {
            'walk': 'pedestrian',
            'walking': 'pedestrian',
            'bike': 'bicycle',
            'bicycle': 'bicycle',
            'motorcycle': 'motorcycle',
            'car': 'auto',
            'auto': 'auto',
        }

        api_url = "https://valhalla1.openstreetmap.de/isochrone"

        payload = {
            "locations": [{"lat": lat, "lon": lon}],
            "costing": VALHALLA_MODES[mode.lower()],
            "contours": [{"distance": distance_km}],
            "polygons": True,
            "denoise": 0.3,
            "generalize": 50,
        }

        for attempt in range(max_retries):
            try:
                response = self.session.post(api_url, json=payload, timeout=30)
                response.raise_for_status()
                return response.json()
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(2)
                else:
                    print(f"  Failed distance {distance_km}km: {e}")
        
        return None

    def generate_time_zone_valhalla(
        self,
        lat: float,
        lon: float,
        time_minutes: int,
        mode: str = 'motorcycle',
        max_retries: int = 3
    ) -> Optional[Dict[str, Any]]:
        """Generate time-based zone using Valhalla API.
        
        Note: Valhalla expects time in MINUTES (not seconds) for isochrones.
        """
        VALHALLA_MODES = {
            'walk': 'pedestrian',
            'walking': 'pedestrian',
            'bike': 'bicycle',
            'bicycle': 'bicycle',
            'motorcycle': 'motorcycle',
            'car': 'auto',
            'auto': 'auto',
        }

        api_url = "https://valhalla1.openstreetmap.de/isochrone"

        # Valhalla API uses minutes for time contours
        payload = {
            "locations": [{"lat": lat, "lon": lon}],
            "costing": VALHALLA_MODES[mode.lower()],
            "contours": [{"time": time_minutes}],  # minutes
            "polygons": True,
            "denoise": 0.0,
            "generalize": 50,
        }

        for attempt in range(max_retries):
            try:
                response = self.session.post(api_url, json=payload, timeout=30)
                response.raise_for_status()
                return response.json()
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(2)
                else:
                    print(f"  Failed time {time_minutes}min: {e}")
        
        return None

    def generate_all_zones_for_provider(
        self,
        name: str,
        lat: float,
        lon: float,
        distances_km: List[float] = [3, 4, 5, 6],
        times_minutes: List[int] = [15, 20, 30],
        mode: str = 'motorcycle',
        quiet: bool = False
    ) -> Dict[str, Any]:
        """Generate all zones for a single provider and combine into one GeoJSON."""
        
        if not quiet:
            print(f"🏍️  Generating zones for '{name}'")
            print(f"   Distances: {distances_km} km")
            print(f"   Times: {times_minutes} min")
        
        all_features = []
        
        # Generate distance-based zones
        for distance in distances_km:
            result = self.generate_distance_zone_valhalla(lat, lon, distance, mode)
            if result and 'features' in result:
                for feature in result['features']:
                    feature['properties'].update({
                        'provider_name': name,
                        'zone_type': 'distance',
                        'distance_km': distance,
                        'mode': mode,
                        'center_lat': lat,
                        'center_lon': lon,
                        'label': f"{distance}km"
                    })
                    all_features.append(feature)
                if not quiet:
                    print(f"   ✅ {distance}km")
            else:
                if not quiet:
                    print(f"   ❌ {distance}km FAILED")
        
        # Generate time-based zones
        for time_min in times_minutes:
            result = self.generate_time_zone_valhalla(lat, lon, time_min, mode)
            if result and 'features' in result:
                for feature in result['features']:
                    feature['properties'].update({
                        'provider_name': name,
                        'zone_type': 'time',
                        'time_minutes': time_min,
                        'mode': mode,
                        'center_lat': lat,
                        'center_lon': lon,
                        'label': f"{time_min}min"
                    })
                    all_features.append(feature)
                if not quiet:
                    print(f"   ✅ {time_min}min")
            else:
                if not quiet:
                    print(f"   ❌ {time_min}min FAILED")
        
        # Combine all features into one GeoJSON
        combined_geojson = {
            "type": "FeatureCollection",
            "metadata": {
                "provider_name": name,
                "center_lat": lat,
                "center_lon": lon,
                "mode": mode,
                "total_zones": len(all_features),
                "distance_zones": distances_km,
                "time_zones": times_minutes,
                "generated_at": datetime.utcnow().isoformat() + 'Z'
            },
            "features": all_features
        }
        
        return combined_geojson


class ExcelBatchProcessor:
    """Batch process Excel file - one GeoJSON per provider with all zones."""
    
    def __init__(self, output_dir: str = "batch_output", max_workers: int = 5):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.max_workers = max_workers
        self.generator = IsodistanceGenerator()
        
    def load_from_excel(
        self,
        excel_file: str,
        distances_km: List[float] = [3, 4, 5, 6],
        times_minutes: List[int] = [10, 20],
        mode: str = 'motorcycle'
    ) -> tuple[pd.DataFrame, List[Dict]]:
        """Load provider data from Excel file."""
        
        df = pd.read_excel(excel_file)
        
        required_cols = ['Provider Name', 'Provider ID', 'lat', 'long', 'bpp id', 'Seller Pincode']
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")
        
        print(f"📊 Loaded Excel file: {excel_file}")
        print(f"   Total rows: {len(df)}")
        
        # Filter rows with valid coordinates
        df_valid = df.dropna(subset=['lat', 'long']).copy()
        print(f"   Valid coordinates: {len(df_valid)}")
        
        # Create ONE task per provider
        tasks = []
        for idx, row in df_valid.iterrows():
            tasks.append({
                'excel_idx': idx,
                'name': str(row['Provider Name']),
                'provider_id': str(row['Provider ID']),
                'bpp_id': str(row['bpp id']),
                'pincode': str(int(row['Seller Pincode'])) if pd.notna(row['Seller Pincode']) else 'NA',
                'lat': float(row['lat']),
                'lon': float(row['long']),
                'distances_km': distances_km,
                'times_minutes': times_minutes,
                'mode': mode
            })
        
        print(f"\n📋 Configuration:")
        print(f"   Providers to process: {len(tasks)}")
        print(f"   Distance zones per provider: {distances_km} km")
        print(f"   Time zones per provider: {times_minutes} min")
        print(f"   Mode: {mode}")
        print(f"   Total zones per provider: {len(distances_km) + len(times_minutes)}")
        print(f"   Total zones across all providers: {len(tasks) * (len(distances_km) + len(times_minutes))}")
        
        return df, tasks
    
    def process_single_provider(
        self,
        task: Dict,
        task_num: int,
        total_tasks: int
    ) -> Dict:
        """Process a single provider - generate all zones."""
        start_time = time.time()
        
        try:
            # Generate all zones for this provider
            combined_geojson = self.generator.generate_all_zones_for_provider(
                name=task['name'],
                lat=task['lat'],
                lon=task['lon'],
                distances_km=task['distances_km'],
                times_minutes=task['times_minutes'],
                mode=task['mode'],
                quiet=True
            )
            
            # Check if any zones were generated
            if not combined_geojson['features']:
                raise Exception("No zones generated successfully")
            
            # Create filename: bpp_id+Provider_Name+Provider_ID+Pincode.geojson
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
        print(f"PROCESSING {total_tasks} PROVIDERS")
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
        
        # Add columns for GeoJSON and file path
        df['zones_geojson'] = None
        df['zones_file'] = None
        df['zones_count'] = None
        df['processing_status'] = None
        
        for result in results:
            idx = result['excel_idx']
            
            if result['status'] == 'success':
                df.at[idx, 'zones_geojson'] = json.dumps(result['geojson'])
                df.at[idx, 'zones_file'] = result['filepath']
                df.at[idx, 'zones_count'] = result['zones_count']
                df.at[idx, 'processing_status'] = 'success'
            else:
                df.at[idx, 'processing_status'] = f"failed: {result['error']}"
        
        # Save updated Excel
        df.to_excel(output_excel, index=False, engine='openpyxl')
        
        successful = sum(1 for r in results if r['status'] == 'success')
        print(f"✅ Updated Excel saved: {output_excel}")
        print(f"   Added columns: zones_geojson, zones_file, zones_count, processing_status")
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
        print(f"Summary saved: {summary_path}")
        print(f"{'='*70}\n")
        
        return summary


def batch_process_from_excel(
    excel_file: str,
    output_excel: str = None,
    output_dir: str = "batch_output",
    distances_km: List[float] = [3, 4, 5, 6],
    times_minutes: List[int] = [10, 20],
    mode: str = 'motorcycle',
    max_workers: int = 5
) -> Dict:
    """
    Batch process providers from Excel - one GeoJSON per provider.
    
    Args:
        excel_file: Path to Excel file
        output_excel: Output Excel file path
        output_dir: Directory for GeoJSON files
        distances_km: Distance zones in km
        times_minutes: Time zones in minutes
        mode: Transportation mode
        max_workers: Number of parallel workers
    """
    if output_excel is None:
        input_path = Path(excel_file)
        output_excel = str(input_path.parent / f"{input_path.stem}_with_zones{input_path.suffix}")
    
    processor = ExcelBatchProcessor(output_dir=output_dir, max_workers=max_workers)
    df, tasks = processor.load_from_excel(
        excel_file, 
        distances_km=distances_km,
        times_minutes=times_minutes,
        mode=mode
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
        description='Generate isodistance/isochrone zones for providers (1 GeoJSON per provider)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Default: 3,4,5,6km + 10,20min zones for motorcycle
  python batch_processing_excel.py --excel providers.xlsx
  
  # Custom distances only
  python batch_processing_excel.py --excel providers.xlsx --distances 3 5 7
  
  # Custom times only (no distances)
  python batch_processing_excel.py --excel providers.xlsx --distances --times 5 10 15
  
  # Both custom
  python batch_processing_excel.py --excel providers.xlsx --distances 3 5 --times 10 20 30
  
  # Different mode
  python batch_processing_excel.py --excel providers.xlsx --mode car
        """
    )
    
    parser.add_argument('--excel', '-e', required=True,
                        help='Excel file with provider data')
    parser.add_argument('--output-excel', '-oe',
                        help='Output Excel file')
    parser.add_argument('--output-dir', '-od', default='batch_output',
                        help='Output directory for GeoJSON files')
    parser.add_argument('--distances', '-d', type=float, nargs='*', default=[3, 4, 5, 6],
                        help='Distance zones in km (default: 3 4 5 6)')
    parser.add_argument('--times', '-t', type=int, nargs='*', default=[10, 20],
                        help='Time zones in minutes (default: 10 20)')
    parser.add_argument('--mode', '-m', default='motorcycle',
                        choices=['motorcycle', 'car', 'bike', 'walk'],
                        help='Transportation mode (default: motorcycle)')
    parser.add_argument('--workers', '-w', type=int, default=5,
                        help='Number of parallel workers (default: 5)')
    
    args = parser.parse_args()
    
    print("🏍️  Starting batch processing...")
    print(f"Excel file: {args.excel}")
    print(f"Mode: {args.mode}")
    print()
    
    batch_process_from_excel(
        excel_file=args.excel,
        output_excel=args.output_excel,
        output_dir=args.output_dir,
        distances_km=args.distances,
        times_minutes=args.times,
        mode=args.mode,
        max_workers=args.workers
    )