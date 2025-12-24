#!/usr/bin/env python3
"""
Organize Batch Output Files (Comprehensive City Coverage)
Copies GeoJSON files from batch_output/ to organized_output/ with structure:
organized_output/
  └── {bpp_id}/
      └── {city}/
          └── {filename}.geojson

Features:
- Comprehensive city mapping covering 100+ cities
- Only copies NEW files (incremental processing)
- Non-destructive (original files untouched)
- Creates folders only for cities that have files
"""

import os
import shutil
import json
from pathlib import Path
from typing import Dict, List, Optional, Set
from datetime import datetime


class ComprehensiveCityMapper:
    """Comprehensive pincode to city mapping for India."""
    
    # Major metro cities with multiple prefixes
    METRO_CITIES = {
        '110': 'Delhi', '100': 'Delhi',
        '560': 'Bangalore', '562': 'Bangalore', '561': 'Bangalore',
        '600': 'Chennai', '601': 'Chennai', '602': 'Chennai', '603': 'Chennai',
        '400': 'Mumbai', '401': 'Mumbai',
        '500': 'Hyderabad', '501': 'Hyderabad', '502': 'Hyderabad',
        '411': 'Pune', '412': 'Pune', '410': 'Pune',
        '700': 'Kolkata', '711': 'Kolkata', '712': 'Kolkata', '713': 'Kolkata',
    }
    
    # Tier 1 & Tier 2 cities
    MAJOR_CITIES = {
        # Gujarat
        '380': 'Ahmedabad', '382': 'Ahmedabad',
        '390': 'Vadodara', '391': 'Vadodara',
        '360': 'Rajkot', '361': 'Rajkot',
        '395': 'Surat', '394': 'Surat',
        '396': 'Vapi',
        
        # Rajasthan
        '302': 'Jaipur', '303': 'Jaipur',
        '342': 'Jodhpur',
        '324': 'Kota',
        '313': 'Udaipur',
        '305': 'Ajmer',
        
        # Uttar Pradesh
        '226': 'Lucknow', '227': 'Lucknow',
        '208': 'Kanpur', '209': 'Kanpur',
        '221': 'Varanasi',
        '211': 'Allahabad', '212': 'Allahabad',
        '282': 'Agra', '283': 'Agra',
        '250': 'Meerut', '251': 'Meerut',
        '201': 'Ghaziabad', '202': 'Ghaziabad',
        '281': 'Mathura',
        '247': 'Haridwar',
        '210': 'Aligarh',
        '273': 'Gorakhpur',
        '243': 'Bareilly',
        '244': 'Moradabad',
        
        # Maharashtra
        '422': 'Nashik', '423': 'Nashik',
        '440': 'Nagpur', '441': 'Nagpur',
        '431': 'Aurangabad',
        '416': 'Kolhapur',
        '421': 'Thane', '420': 'Thane',
        '403': 'Goa', '402': 'Goa',
        
        # Karnataka
        '570': 'Mysore', '571': 'Mysore',
        '575': 'Mangalore', '574': 'Mangalore',
        '590': 'Belgaum', '591': 'Belgaum',
        '580': 'Hubli', '581': 'Hubli',
        
        # Tamil Nadu
        '641': 'Coimbatore', '642': 'Coimbatore',
        '625': 'Madurai', '626': 'Madurai',
        '620': 'Tiruchirappalli', '621': 'Tiruchirappalli',
        '627': 'Tirunelveli',
        '636': 'Salem',
        '638': 'Erode',
        '632': 'Vellore',
        
        # Andhra Pradesh & Telangana
        '520': 'Vijayawada', '521': 'Vijayawada',
        '530': 'Visakhapatnam', '531': 'Visakhapatnam',
        '517': 'Tirupati',
        '522': 'Guntur',
        
        # Kerala
        '695': 'Thiruvananthapuram',
        '682': 'Kochi', '683': 'Kochi',
        '673': 'Kozhikode',
        '680': 'Thrissur',
        '686': 'Kottayam',
        '691': 'Kollam',
        
        # Madhya Pradesh
        '452': 'Indore', '453': 'Indore',
        '462': 'Bhopal',
        '474': 'Gwalior',
        '482': 'Jabalpur',
        
        # Punjab & Haryana
        '141': 'Ludhiana', '142': 'Ludhiana',
        '143': 'Amritsar',
        '144': 'Jalandhar',
        '147': 'Patiala',
        '160': 'Chandigarh',
        '121': 'Faridabad', '122': 'Faridabad',
        '124': 'Rohtak',
        '125': 'Hisar',
        '131': 'Sonipat',
        '132': 'Karnal',
        '134': 'Ambala',
        
        # Bihar & Jharkhand
        '800': 'Patna', '801': 'Patna',
        '834': 'Ranchi', '835': 'Ranchi',
        '831': 'Jamshedpur',
        '826': 'Dhanbad',
        
        # Chhattisgarh
        '492': 'Raipur', '493': 'Raipur',
        '490': 'Durg',
        '495': 'Bilaspur',
        
        # Odisha
        '751': 'Bhubaneswar', '752': 'Bhubaneswar',
        '753': 'Cuttack',
        
        # West Bengal
        '734': 'Siliguri',
        '743': 'Nadia',
        
        # Northeast
        '781': 'Guwahati', '782': 'Guwahati',
        '793': 'Shillong',
        '796': 'Aizawl',
        '799': 'Agartala',
        
        # Jammu & Kashmir
        '190': 'Srinagar',
        '180': 'Jammu', '181': 'Jammu',
        
        # Uttarakhand
        '248': 'Dehradun', '249': 'Dehradun',
        
        # Himachal Pradesh
        '171': 'Shimla',
        '176': 'Dharamshala',
        
        # Pondicherry
        '605': 'Pondicherry',
        
        # Additional coverage for common prefixes in your data
        '101': 'Noida',
        '102': 'Delhi_NCR',
        '120': 'Delhi_NCR',
        '123': 'Gurgaon',
        '200': 'UP_West',
        '203': 'Bulandshahr',
        '204': 'UP_West',
        '222': 'UP_East',
        '242': 'UP_West',
        '245': 'UP_West',
        '246': 'UP_North',
        '260': 'MP_North',
        '261': 'MP_North',
        '263': 'UP_Central',
        '271': 'UP_East',
        '280': 'UP_Central',
        '284': 'UP_Central',
        '300': 'Rajasthan_North',
        '301': 'Alwar',
        '311': 'Rajasthan_West',
        '321': 'Rajasthan_East',
        '331': 'Rajasthan_North',
        '332': 'Rajasthan_North',
        '359': 'Gujarat_West',
        '362': 'Gujarat_West',
        '363': 'Gujarat_West',
        '364': 'Gujarat_West',
        '370': 'Gujarat_Kutch',
        '383': 'Gujarat_North',
        '384': 'Gujarat_North',
        '385': 'Gujarat_North',
        '387': 'Gujarat_Central',
        '388': 'Gujarat_Central',
        '392': 'Gujarat_South',
        '413': 'Maharashtra_Central',
        '414': 'Maharashtra_Central',
        '415': 'Maharashtra_West',
        '424': 'Maharashtra_North',
        '425': 'Maharashtra_North',
        '442': 'Maharashtra_East',
        '443': 'Maharashtra_East',
        '444': 'Maharashtra_East',
        '445': 'Maharashtra_East',
        '450': 'MP_West',
        '455': 'MP_West',
        '456': 'MP_West',
        '461': 'MP_Central',
        '464': 'MP_Central',
        '470': 'MP_East',
        '473': 'MP_South',
        '480': 'MP_East',
        '481': 'MP_East',
        '485': 'MP_East',
        '486': 'MP_East',
        '490': 'Durg',
        '491': 'Chhattisgarh_North',
        '496': 'Chhattisgarh_North',
        '497': 'Chhattisgarh_North',
        '503': 'Telangana_North',
        '505': 'Telangana_North',
        '506': 'Telangana_East',
        '507': 'Telangana_East',
        '509': 'Telangana_West',
        '511': 'AP_East',
        '515': 'AP_South',
        '516': 'AP_South',
        '518': 'AP_South',
        '523': 'AP_East',
        '524': 'AP_East',
        '533': 'AP_East',
        '534': 'AP_East',
        '535': 'AP_East',
        '540': 'AP_North',
        '563': 'Karnataka_East',
        '566': 'Karnataka_North',
        '572': 'Karnataka_South',
        '573': 'Karnataka_South',
        '576': 'Karnataka_West',
        '577': 'Karnataka_Central',
        '582': 'Karnataka_North',
        '583': 'Karnataka_North',
        '584': 'Karnataka_North',
        '585': 'Karnataka_North',
        '586': 'Karnataka_North',
        '587': 'Karnataka_North',
        '606': 'TN_South',
        '607': 'TN_South',
        '609': 'TN_South',
        '612': 'TN_Central',
        '613': 'TN_Central',
        '614': 'TN_Central',
        '622': 'TN_South',
        '623': 'TN_South',
        '624': 'TN_South',
        '628': 'TN_South',
        '629': 'TN_South',
        '631': 'TN_North',
        '635': 'TN_West',
        '637': 'TN_West',
        '639': 'TN_West',
        '643': 'TN_West',
        '650': 'TN_Nicobar',
        '670': 'Kerala_North',
        '671': 'Kerala_North',
        '676': 'Kerala_Central',
        '678': 'Kerala_Central',
        '679': 'Kerala_Central',
        '688': 'Kerala_South',
        '689': 'Kerala_South',
        '690': 'Kerala_South',
        '721': 'WB_South',
        '722': 'WB_South',
        '731': 'WB_North',
        '732': 'WB_North',
        '733': 'WB_North',
        '735': 'WB_North',
        '736': 'WB_North',
        '737': 'WB_North',
        '741': 'WB_South',
        '742': 'WB_South',
        '754': 'Odisha_East',
        '756': 'Odisha_West',
        '757': 'Odisha_West',
        '760': 'Odisha_South',
        '767': 'Odisha_South',
        '768': 'Odisha_North',
        '769': 'Odisha_North',
        '770': 'Odisha_Central',
        '783': 'Assam_Central',
        '784': 'Assam_North',
        '785': 'Assam_East',
        '786': 'Assam_West',
        '791': 'Meghalaya_East',
        '795': 'Manipur',
        '797': 'Nagaland',
        '802': 'Bihar_Central',
        '803': 'Bihar_South',
        '804': 'Bihar_South',
        '812': 'Bihar_West',
        '814': 'Bihar_West',
        '821': 'Bihar_South',
        '823': 'Bihar_Central',
        '824': 'Bihar_Central',
        '825': 'Bihar_East',
        '827': 'Jharkhand_North',
        '828': 'Jharkhand_East',
        '829': 'Jharkhand_Central',
        '832': 'Jharkhand_South',
        '841': 'Bihar_North',
        '842': 'Bihar_North',
        '844': 'Bihar_North',
        '845': 'Bihar_North',
        '846': 'Bihar_North',
        '847': 'Bihar_North',
        '851': 'Bihar_East',
        '854': 'Bihar_East',
        '860': 'Andaman_Nicobar',
        '996': 'International',  # Often used for test data
    }
    
    def __init__(self):
        # Combine all mappings
        self.pincode_map = {**self.METRO_CITIES, **self.MAJOR_CITIES}
    
    def get_city(self, pincode: str) -> str:
        """Get city from pincode with comprehensive mapping."""
        if not pincode or pincode == 'NA':
            return 'Unknown'
        
        # Try first 3 digits
        prefix = str(pincode)[:3]
        
        if prefix in self.pincode_map:
            return self.pincode_map[prefix]
        
        # Fallback: Try state-level grouping for unmapped codes
        first_digit = prefix[0]
        state_groups = {
            '1': 'Delhi_NCR_Region',
            '2': 'UP_Region',
            '3': 'Rajasthan_Gujarat',
            '4': 'Maharashtra_Goa',
            '5': 'South_India',
            '6': 'Tamil_Nadu_Region',
            '7': 'East_India',
            '8': 'Northeast_Bihar',
        }
        
        return state_groups.get(first_digit, 'Other')


class BatchOutputOrganizer:
    """Organize batch output files with comprehensive city mapping."""
    
    def __init__(
        self, 
        source_dir: str = "batch_output",
        target_dir: str = "organized_output"
    ):
        self.source_dir = Path(source_dir)
        self.target_dir = Path(target_dir)
        self.city_mapper = ComprehensiveCityMapper()
        self.stats = {
            'total_source_files': 0,
            'already_organized': 0,
            'newly_copied': 0,
            'skipped': 0,
            'errors': 0,
            'bpp_ids': set(),
            'cities': set()
        }
        self.organized_files_cache = set()
    
    def parse_filename(self, filename: str) -> Optional[Dict[str, str]]:
        """Parse filename: bpp_id+provider_name+provider_id+pincode.geojson"""
        if not filename.endswith('.geojson'):
            return None
        
        name_parts = filename[:-8]
        parts = name_parts.split('+')
        
        if len(parts) != 4:
            return None
        
        bpp_id, provider_name, provider_id, pincode = parts
        city = self.city_mapper.get_city(pincode)
        
        return {
            'bpp_id': bpp_id,
            'provider_name': provider_name,
            'provider_id': provider_id,
            'pincode': pincode,
            'city': city,
            'original_filename': filename
        }
    
    def build_organized_files_cache(self):
        """Build cache of already organized files."""
        if not self.target_dir.exists():
            return
        
        print("🔍 Scanning existing organized files...")
        
        for geojson_file in self.target_dir.rglob('*.geojson'):
            self.organized_files_cache.add(geojson_file.name)
        
        print(f"   Found {len(self.organized_files_cache)} files already organized")
    
    def is_already_organized(self, filename: str) -> bool:
        """Check if file is already organized."""
        return filename in self.organized_files_cache
    
    def get_target_path(self, parsed: Dict[str, str]) -> Path:
        """Get target path for organized file."""
        return self.target_dir / parsed['bpp_id'] / parsed['city'] / parsed['original_filename']
    
    def copy_file(self, source_path: Path, dry_run: bool = False) -> bool:
        """Copy a single file to organized structure."""
        try:
            parsed = self.parse_filename(source_path.name)
            
            if not parsed:
                self.stats['skipped'] += 1
                return False
            
            if self.is_already_organized(parsed['original_filename']):
                self.stats['already_organized'] += 1
                return False
            
            target_path = self.get_target_path(parsed)
            
            self.stats['bpp_ids'].add(parsed['bpp_id'])
            self.stats['cities'].add(parsed['city'])
            
            if dry_run:
                print(f"  [DRY RUN] Would copy: {source_path.name}")
                print(f"           Target: {target_path.relative_to(self.target_dir)}")
                self.stats['newly_copied'] += 1
                return True
            
            target_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(str(source_path), str(target_path))
            self.organized_files_cache.add(parsed['original_filename'])
            
            print(f"  ✅ {parsed['bpp_id']}/{parsed['city']}/{parsed['original_filename']}")
            self.stats['newly_copied'] += 1
            return True
            
        except Exception as e:
            print(f"  ❌ Error copying {source_path.name}: {e}")
            self.stats['errors'] += 1
            return False
    
    def organize_new_files(self, dry_run: bool = False, verbose: bool = True) -> Dict:
        """Copy only new files from source to organized directory."""
        if not self.source_dir.exists():
            print(f"❌ Source directory not found: {self.source_dir}")
            return self.stats
        
        if not dry_run:
            self.target_dir.mkdir(parents=True, exist_ok=True)
        
        self.build_organized_files_cache()
        
        source_files = list(self.source_dir.glob('*.geojson'))
        self.stats['total_source_files'] = len(source_files)
        
        if self.stats['total_source_files'] == 0:
            print(f"ℹ️  No .geojson files found in {self.source_dir}")
            return self.stats
        
        print(f"\n{'='*70}")
        print(f"ORGANIZING NEW BATCH OUTPUT FILES")
        if dry_run:
            print("🔍 DRY RUN MODE - No files will be copied")
        print(f"{'='*70}")
        print(f"Source: {self.source_dir}")
        print(f"Target: {self.target_dir}")
        print(f"Total source files: {self.stats['total_source_files']}")
        print(f"Already organized: {len(self.organized_files_cache)}")
        print()
        
        processed = 0
        for source_path in source_files:
            if self.is_already_organized(source_path.name):
                continue
            
            processed += 1
            if verbose:
                print(f"[{processed}] Processing: {source_path.name}")
            
            self.copy_file(source_path, dry_run=dry_run)
        
        print(f"\n{'='*70}")
        print("ORGANIZATION SUMMARY")
        print(f"{'='*70}")
        print(f"Source files: {self.stats['total_source_files']}")
        print(f"Already organized: {self.stats['already_organized']} ⏭️")
        print(f"Newly copied: {self.stats['newly_copied']} ✅")
        print(f"Skipped (invalid): {self.stats['skipped']} ⚠️")
        print(f"Errors: {self.stats['errors']} ❌")
        
        if self.stats['newly_copied'] > 0:
            print(f"\n📊 New organization:")
            print(f"   BPP IDs involved: {len(self.stats['bpp_ids'])}")
            print(f"   Cities involved: {len(self.stats['cities'])}")
            
            if len(self.stats['cities']) > 0:
                print(f"\n   Cities:")
                for city in sorted(self.stats['cities']):
                    city_files = len(list(self.target_dir.rglob(f"*/{city}/*.geojson")))
                    print(f"     - {city}: {city_files} total files")
        
        total_organized = len(self.organized_files_cache) + self.stats['newly_copied']
        print(f"\n📁 Total files in organized directory: {total_organized}")
        print(f"{'='*70}\n")
        
        return self.stats
    
    def create_index(self, output_file: str = "organization_index.json"):
        """Create JSON index mapping bpp_id > city > files."""
        if not self.target_dir.exists():
            print("⚠️  Target directory doesn't exist, skipping index creation")
            return None
        
        index = {
            'metadata': {
                'created_at': datetime.utcnow().isoformat() + 'Z',
                'source_directory': str(self.source_dir),
                'target_directory': str(self.target_dir),
                'total_files': 0,
                'total_bpp_ids': 0,
                'total_cities': 0
            },
            'structure': {}
        }
        
        total_files = 0
        
        for bpp_dir in sorted(self.target_dir.iterdir()):
            if not bpp_dir.is_dir() or bpp_dir.name.startswith('.'):
                continue
            
            bpp_id = bpp_dir.name
            index['structure'][bpp_id] = {}
            
            for city_dir in sorted(bpp_dir.iterdir()):
                if not city_dir.is_dir() or city_dir.name.startswith('.'):
                    continue
                
                city = city_dir.name
                files = sorted([f.name for f in city_dir.glob('*.geojson')])
                
                if files:
                    index['structure'][bpp_id][city] = {
                        'file_count': len(files),
                        'files': files
                    }
                    total_files += len(files)
        
        index['metadata']['total_files'] = total_files
        index['metadata']['total_bpp_ids'] = len(index['structure'])
        index['metadata']['total_cities'] = len(set(
            city for bpp in index['structure'].values() for city in bpp.keys()
        ))
        
        index_path = self.target_dir / output_file
        with open(index_path, 'w') as f:
            json.dump(index, f, indent=2)
        
        print(f"📄 Index created: {index_path}")
        print(f"   Total files: {total_files}")
        print(f"   BPP IDs: {index['metadata']['total_bpp_ids']}")
        print(f"   Cities: {index['metadata']['total_cities']}")
        
        return index
    
    def create_summary_report(self, output_file: str = "organization_report.txt"):
        """Create human-readable summary report."""
        if not self.target_dir.exists():
            return
        
        report_path = self.target_dir / output_file
        
        with open(report_path, 'w') as f:
            f.write("="*70 + "\n")
            f.write("BATCH OUTPUT ORGANIZATION REPORT\n")
            f.write("="*70 + "\n\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Source: {self.source_dir}\n")
            f.write(f"Target: {self.target_dir}\n\n")
            
            bpp_summary = {}
            
            for bpp_dir in sorted(self.target_dir.iterdir()):
                if not bpp_dir.is_dir() or bpp_dir.name.startswith('.'):
                    continue
                
                bpp_id = bpp_dir.name
                bpp_summary[bpp_id] = {}
                
                for city_dir in sorted(bpp_dir.iterdir()):
                    if not city_dir.is_dir() or city_dir.name.startswith('.'):
                        continue
                    
                    city = city_dir.name
                    file_count = len(list(city_dir.glob('*.geojson')))
                    bpp_summary[bpp_id][city] = file_count
            
            f.write(f"Total BPP IDs: {len(bpp_summary)}\n")
            f.write(f"Total Cities: {len(set(city for bpp in bpp_summary.values() for city in bpp.keys()))}\n")
            f.write(f"Total Files: {sum(sum(cities.values()) for cities in bpp_summary.values())}\n\n")
            
            f.write("="*70 + "\n")
            f.write("BREAKDOWN BY BPP ID\n")
            f.write("="*70 + "\n\n")
            
            for bpp_id in sorted(bpp_summary.keys()):
                total_files = sum(bpp_summary[bpp_id].values())
                f.write(f"\n{bpp_id} ({total_files} files)\n")
                f.write("-" * len(bpp_id) + "\n")
                
                for city in sorted(bpp_summary[bpp_id].keys()):
                    f.write(f"  {city}: {bpp_summary[bpp_id][city]} files\n")
        
        print(f"📊 Report created: {report_path}")


def organize_batch_output(
    source_dir: str = "batch_output",
    target_dir: str = "organized_output",
    dry_run: bool = False,
    verbose: bool = True,
    create_index: bool = True,
    create_report: bool = True
):
    """
    Main function to organize batch output files.
    
    Args:
        source_dir: Source directory with flat files
        target_dir: Target directory for organized structure
        dry_run: Preview mode
        verbose: Print detailed progress
        create_index: Create JSON index
        create_report: Create text summary
    """
    organizer = BatchOutputOrganizer(source_dir, target_dir)
    stats = organizer.organize_new_files(dry_run=dry_run, verbose=verbose)
    
    if not dry_run and stats['newly_copied'] > 0:
        if create_index:
            organizer.create_index()
        if create_report:
            organizer.create_summary_report()
    
    return stats


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Organize batch output files with comprehensive city mapping',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Preview organization
  python organize_batch_output_comprehensive.py --dry-run
  
  # Organize new files
  python organize_batch_output_comprehensive.py
  
  # Custom directories
  python organize_batch_output_comprehensive.py --source batch_output --target organized_output
  
  # Quiet mode
  python organize_batch_output_comprehensive.py --quiet

Features:
- Comprehensive city mapping (100+ cities)
- Only creates folders for cities with files
- Incremental processing (skips existing)
- Non-destructive (originals untouched)
        """
    )
    
    parser.add_argument('--source', '-s', default='batch_output',
                        help='Source directory (default: batch_output)')
    parser.add_argument('--target', '-t', default='organized_output',
                        help='Target directory (default: organized_output)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Preview without copying')
    parser.add_argument('--quiet', '-q', action='store_true',
                        help='Minimal output')
    parser.add_argument('--no-index', action='store_true',
                        help='Skip creating index')
    parser.add_argument('--no-report', action='store_true',
                        help='Skip creating report')
    
    args = parser.parse_args()
    
    print("📁 Batch Output Organizer (Comprehensive)")
    print(f"Source: {args.source}")
    print(f"Target: {args.target}\n")
    
    organize_batch_output(
        source_dir=args.source,
        target_dir=args.target,
        dry_run=args.dry_run,
        verbose=not args.quiet,
        create_index=not args.no_index,
        create_report=not args.no_report
    )