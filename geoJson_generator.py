#!/usr/bin/env python3
"""
Smart Resume Isodistance Generator
Features:
- Skips rows that already have zones_file populated in Excel
- Skips files that already exist in output directory
- True incremental processing - only process what's needed
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
                'User-Agent': 'IsodistanceGenerator/5.0',
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

    def generate_all_zones_for_provider(
        self,
        name: str,
        lat: float,
        lon: float,
        distances_km: List[float] = [3, 4, 5, 6],
        mode: str = 'motorcycle',
        quiet: bool = False
    ) -> Dict[str, Any]:
        """Generate all distance zones for a single provider and combine into one GeoJSON."""
        
        if not quiet:
            print(f"🏍️  Generating zones for '{name}'")
            print(f"   Distances: {distances_km} km")
        
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
                "generated_at": datetime.utcnow().isoformat() + 'Z'
            },
            "features": all_features
        }
        
        return combined_geojson


class SmartBatchProcessor:
    """Smart batch processor - skips rows already processed in Excel AND files that exist."""
    
    def __init__(self, output_dir: str = "batch_output", max_workers: int = 10):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.max_workers = max_workers
        self.generator = IsodistanceGenerator()
        
    def generate_filename(self, task: Dict) -> str:
        """Generate consistent filename for a provider."""
        def clean_string(s):
            return str(s).replace(' ', '_').replace(',', '').replace('/', '_').replace(':', '').replace('?', '').replace('\\', '_')
        
        bpp_id_clean = clean_string(task['bpp_id'])
        provider_name_clean = clean_string(task['name'])
        provider_id_clean = clean_string(task['provider_id'])
        pincode_clean = clean_string(task['pincode'])
        
        filename = f"{bpp_id_clean}+{provider_name_clean}+{provider_id_clean}+{pincode_clean}.geojson"
        return filename
    
    def file_exists(self, task: Dict) -> bool:
        """Check if GeoJSON file already exists for this provider."""
        filename = self.generate_filename(task)
        filepath = self.output_dir / filename
        return filepath.exists()
    
    def is_already_processed_in_excel(self, row) -> bool:
        """Check if this row already has zones_file populated in Excel."""
        if 'zones_file' in row.index:
            zones_file = row['zones_file']
            if pd.notna(zones_file) and zones_file != '':
                # Check if the file actually exists
                if Path(zones_file).exists():
                    return True
        return False
    
    def load_from_excel(
        self,
        excel_file: str,
        distances_km: List[float] = [3, 4, 5, 6],
        mode: str = 'motorcycle',
        skip_existing: bool = True
    ) -> tuple[pd.DataFrame, List[Dict], List[Dict], List[Dict]]:
        """Load provider data from Excel file using network_lat and network_long."""
        
        # Try to read Excel with openpyxl engine
        try:
            df = pd.read_excel(excel_file, engine='openpyxl')
        except Exception as e:
            print(f"❌ Error reading Excel file: {e}")
            print("💡 Tip: Make sure the file is a valid .xlsx file")
            raise
        
        required_cols = ['Provider Name', 'Provider ID', 'network_lat', 'network_long', 'bpp id', 'Seller Pincode']
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")
        
        print(f"📊 Loaded Excel file: {excel_file}")
        print(f"   Total rows: {len(df)}")
        
        # Filter rows with valid network coordinates
        df_valid = df.dropna(subset=['network_lat', 'network_long']).copy()
        print(f"   Valid network coordinates: {len(df_valid)}")
        
        # Create ONE task per provider
        all_tasks = []
        skipped_excel = []  # Skipped because already in Excel
        skipped_file = []   # Skipped because file exists
        
        for idx, row in df_valid.iterrows():
            task = {
                'excel_idx': idx,
                'name': str(row['Provider Name']),
                'provider_id': str(row['Provider ID']),
                'bpp_id': str(row['bpp id']),
                'pincode': str(int(row['Seller Pincode'])) if pd.notna(row['Seller Pincode']) else 'NA',
                'lat': float(row['network_lat']),
                'lon': float(row['network_long']),
                'distances_km': distances_km,
                'mode': mode
            }
            
            if skip_existing:
                # First check: Is it already processed in Excel?
                if self.is_already_processed_in_excel(row):
                    skipped_excel.append(task)
                    continue
                
                # Second check: Does the file exist?
                if self.file_exists(task):
                    skipped_file.append(task)
                    continue
            
            # Not skipped - needs processing
            all_tasks.append(task)
        
        print(f"\n📋 Configuration:")
        print(f"   Total providers with coordinates: {len(df_valid)}")
        print(f"   Already in Excel (zones_file populated): {len(skipped_excel)} ⏭️")
        print(f"   File exists but not in Excel: {len(skipped_file)} 📁")
        print(f"   To be processed: {len(all_tasks)} 🔄")
        print(f"   Distance zones per provider: {distances_km} km")
        print(f"   Mode: {mode}")
        print(f"   Skip existing: {skip_existing}")
        
        if len(skipped_excel) + len(skipped_file) > 0:
            total_skipped = len(skipped_excel) + len(skipped_file)
            print(f"\n💡 Resume mode: Skipping {total_skipped} already processed providers")
            print(f"   ⏭️  In Excel: {len(skipped_excel)}")
            print(f"   📁 File exists: {len(skipped_file)}")
        
        return df, all_tasks, skipped_excel, skipped_file
    
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
                mode=task['mode'],
                quiet=True
            )
            
            # Check if any zones were generated
            if not combined_geojson['features']:
                raise Exception("No zones generated successfully")
            
            # Save to file
            filename = self.generate_filename(task)
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
        
        if total_tasks == 0:
            print("\n✅ All providers already processed! Nothing to do.")
            return results
        
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
        skipped_excel: List[Dict],
        skipped_file: List[Dict],
        output_excel: str
    ) -> pd.DataFrame:
        """Add results back to Excel, including skipped files."""
        
        print(f"\n{'='*70}")
        print("UPDATING EXCEL FILE")
        print(f"{'='*70}")
        
        # Add columns if they don't exist
        if 'zones_file' not in df.columns:
            df['zones_file'] = None
        if 'zones_count' not in df.columns:
            df['zones_count'] = None
        if 'processing_status' not in df.columns:
            df['processing_status'] = None
        
        # Add newly processed results
        for result in results:
            idx = result['excel_idx']
            
            if result['status'] == 'success':
                df.at[idx, 'zones_file'] = result['filepath']
                df.at[idx, 'zones_count'] = result['zones_count']
                df.at[idx, 'processing_status'] = 'success'
            else:
                df.at[idx, 'processing_status'] = f"failed: {result['error']}"
        
        # Mark file-exists-but-not-in-excel as processed
        for task in skipped_file:
            idx = task['excel_idx']
            filename = self.generate_filename(task)
            filepath = self.output_dir / filename
            
            # Only update if not already marked
            if pd.isna(df.at[idx, 'zones_file']) or df.at[idx, 'zones_file'] == '':
                df.at[idx, 'zones_file'] = str(filepath)
                df.at[idx, 'processing_status'] = 'file_exists_added_to_excel'
                
                # Try to get zone count from existing file
                try:
                    with open(filepath, 'r') as f:
                        geojson = json.load(f)
                        df.at[idx, 'zones_count'] = len(geojson.get('features', []))
                except:
                    pass
        
        # skipped_excel items already have their data in the Excel file, no update needed
        
        # Save updated Excel
        try:
            df.to_excel(output_excel, index=False, engine='openpyxl')
        except Exception as e:
            print(f"❌ Error saving Excel file: {e}")
            raise
        
        successful = sum(1 for r in results if r['status'] == 'success')
        print(f"✅ Updated Excel saved: {output_excel}")
        print(f"   Columns: zones_file, zones_count, processing_status")
        print(f"   Newly processed: {successful}/{len(results)}")
        print(f"   Already in Excel: {len(skipped_excel)}")
        print(f"   Files added to Excel: {len(skipped_file)}")
        
        return df
    
    def save_summary(
        self,
        results: List[Dict],
        skipped_excel: List[Dict],
        skipped_file: List[Dict],
        filename: str = "batch_summary.json"
    ) -> Dict:
        """Save processing summary."""
        
        successful = sum(1 for r in results if r['status'] == 'success')
        failed = sum(1 for r in results if r['status'] == 'failed')
        total_time = sum(r['elapsed'] for r in results)
        total_zones = sum(r['zones_count'] for r in results if r['status'] == 'success')
        
        summary = {
            'newly_processed': len(results),
            'already_in_excel': len(skipped_excel),
            'file_exists_added': len(skipped_file),
            'total_providers': len(results) + len(skipped_excel) + len(skipped_file),
            'successful': successful,
            'failed': failed,
            'total_zones_generated': total_zones,
            'total_time': round(total_time, 2),
            'average_time_per_provider': round(total_time / len(results), 2) if results else 0,
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'results': results
        }
        
        summary_path = self.output_dir / filename
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)
        
        print(f"\n{'='*70}")
        print("BATCH PROCESSING SUMMARY")
        print(f"{'='*70}")
        print(f"Total providers: {summary['total_providers']}")
        print(f"Already in Excel: {len(skipped_excel)} ⏭️")
        print(f"File exists (added to Excel): {len(skipped_file)} 📁")
        print(f"Newly processed: {len(results)} 🔄")
        print(f"  └─ Successful: {successful} ✅")
        print(f"  └─ Failed: {failed} ❌")
        print(f"Total zones generated: {total_zones}")
        print(f"Total time: {summary['total_time']:.2f}s")
        if results:
            print(f"Average time per provider: {summary['average_time_per_provider']:.2f}s")
        print(f"Summary saved: {summary_path}")
        print(f"{'='*70}\n")
        
        return summary


def smart_batch_process(
    excel_file: str,
    output_excel: str = None,
    output_dir: str = "batch_output",
    distances_km: List[float] = [3, 4, 5, 6],
    mode: str = 'motorcycle',
    max_workers: int = 10,
    skip_existing: bool = True
) -> Dict:
    """
    Smart batch processing - only processes rows that need it.
    Checks both Excel file and output directory.
    
    Args:
        excel_file: Path to Excel file (requires network_lat, network_long)
        output_excel: Output Excel file path
        output_dir: Directory for GeoJSON files
        distances_km: Distance zones in km
        mode: Transportation mode
        max_workers: Number of parallel workers
        skip_existing: Skip rows already processed
    """
    if output_excel is None:
        input_path = Path(excel_file)
        output_excel = str(input_path.parent / f"{input_path.stem}_with_zones{input_path.suffix}")
    
    processor = SmartBatchProcessor(output_dir=output_dir, max_workers=max_workers)
    df, tasks, skipped_excel, skipped_file = processor.load_from_excel(
        excel_file, 
        distances_km=distances_km,
        mode=mode,
        skip_existing=skip_existing
    )
    
    if not tasks and not skipped_excel and not skipped_file:
        print("❌ No valid providers found")
        return {}
    
    results = processor.process_batch(tasks, parallel=True)
    df_updated = processor.add_results_to_excel(df, results, skipped_excel, skipped_file, output_excel)
    summary = processor.save_summary(results, skipped_excel, skipped_file)
    
    return summary


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Smart isodistance generator - only processes pending rows',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Default: Skip rows already processed in Excel AND files that exist
  python smart_batch_processing.py --excel providers.xlsx
  
  # Force reprocess everything
  python smart_batch_processing.py --excel providers.xlsx --no-skip-existing
  
  # Custom distances
  python smart_batch_processing.py --excel providers.xlsx --distances 3 5 7
        """
    )
    
    parser.add_argument('--excel', '-e', required=True,
                        help='Excel file with provider data')
    parser.add_argument('--output-excel', '-oe',
                        help='Output Excel file')
    parser.add_argument('--output-dir', '-od', default='batch_output',
                        help='Output directory for GeoJSON files')
    parser.add_argument('--distances', '-d', type=float, nargs='+', default=[3, 4, 5, 6],
                        help='Distance zones in km (default: 3 4 5 6)')
    parser.add_argument('--mode', '-m', default='motorcycle',
                        choices=['motorcycle', 'car', 'bike', 'walk'],
                        help='Transportation mode')
    parser.add_argument('--workers', '-w', type=int, default=5,
                        help='Number of parallel workers')
    parser.add_argument('--no-skip-existing', action='store_true',
                        help='Reprocess all files')
    
    args = parser.parse_args()
    
    print("🏍️  Smart batch processing - only pending rows...")
    print(f"Excel file: {args.excel}")
    print(f"Mode: {args.mode}")
    print()
    
    smart_batch_process(
        excel_file=args.excel,
        output_excel=args.output_excel,
        output_dir=args.output_dir,
        distances_km=args.distances,
        mode=args.mode,
        max_workers=args.workers,
        skip_existing=not args.no_skip_existing
    )