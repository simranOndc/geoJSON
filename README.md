# Smart Isodistance Zone Generator - API Documentation

## API Used

### Valhalla Isochrone API
**Endpoint:** `https://valhalla1.openstreetmap.de/isochrone`

**Method:** POST

**Description:** Generates realistic delivery/travel zones based on actual road networks and routing data.

---

## Input Parameters

### 1. Excel File Requirements

| Column Name | Type | Description | Example |
|------------|------|-------------|---------|
| `Provider Name` | String | Restaurant/store name | "Dominos Pizza" |
| `Provider ID` | String | Unique provider identifier | "DOM123" |
| `network_lat` | Float | Latitude coordinate | 28.6139 |
| `network_long` | Float | Longitude coordinate | 77.2090 |
| `bpp id` | String | Business platform provider ID | "ondc-bpp-123" |
| `Seller Pincode` | String/Integer | Store pincode | "110001" |

### 2. Command Line Arguments

| Parameter | Flag | Type | Default | Description |
|-----------|------|------|---------|-------------|
| `excel` | `-e`, `--excel` | String | **Required** | Path to input Excel file |
| `output_excel` | `-oe`, `--output-excel` | String | `[input]_with_zones.xlsx` | Path for output Excel file |
| `output_dir` | `-od`, `--output-dir` | String | `batch_output` | Directory for GeoJSON files |
| `distances` | `-d`, `--distances` | Float[] | `[3, 4, 5, 6]` | Distance zones in kilometers |
| `mode` | `-m`, `--mode` | String | `motorcycle` | Transportation mode |
| `workers` | `-w`, `--workers` | Integer | `5` | Number of parallel workers |
| `skip_existing` | `--no-skip-existing` | Boolean | `True` | Skip already processed rows |

### 3. Valhalla API Request Payload
```json
{
  "locations": [
    {
      "lat": 28.6139,
      "lon": 77.2090
    }
  ],
  "costing": "motorcycle",
  "contours": [
    {
      "distance": 3
    }
  ],
  "polygons": true,
  "denoise": 0.3,
  "generalize": 50
}
```

**Payload Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `locations` | Array | Array of coordinate objects (lat, lon) |
| `costing` | String | Transportation mode: `motorcycle`, `auto` (car), `bicycle`, `pedestrian` |
| `contours` | Array | Array of distance objects in kilometers |
| `polygons` | Boolean | Return polygon geometry (true) |
| `denoise` | Float | Smoothing factor (0-1) |
| `generalize` | Integer | Polygon simplification in meters |

---

## Output Parameters

### 1. GeoJSON File (per provider)

**Filename Format:** `[bpp_id]+[provider_name]+[provider_id]+[pincode].geojson`

**Structure:**
```json
{
  "type": "FeatureCollection",
  "metadata": {
    "provider_name": "Dominos Pizza",
    "center_lat": 28.6139,
    "center_lon": 77.2090,
    "mode": "motorcycle",
    "total_zones": 4,
    "distance_zones": [3, 4, 5, 6],
    "generated_at": "2024-12-24T10:30:00Z"
  },
  "features": [
    {
      "type": "Feature",
      "geometry": {
        "type": "Polygon",
        "coordinates": [[...]]
      },
      "properties": {
        "provider_name": "Dominos Pizza",
        "zone_type": "distance",
        "distance_km": 3,
        "mode": "motorcycle",
        "center_lat": 28.6139,
        "center_lon": 77.2090,
        "label": "3km"
      }
    }
  ]
}
```

### 2. Updated Excel File

**New Columns Added:**

| Column Name | Type | Description | Example |
|-------------|------|-------------|---------|
| `zones_file` | String | Path to generated GeoJSON file | `batch_output/ondc-bpp-123+Dominos_Pizza+DOM123+110001.geojson` |
| `zones_count` | Integer | Number of zones generated | `4` |
| `processing_status` | String | Processing result | `success` or `failed: [error]` |

### 3. Summary JSON File

**Filename:** `batch_output/batch_summary.json`
```json
{
  "newly_processed": 600,
  "already_in_excel": 300,
  "file_exists_added": 50,
  "total_providers": 950,
  "successful": 595,
  "failed": 5,
  "total_zones_generated": 2380,
  "total_time": 1800.50,
  "average_time_per_provider": 3.0,
  "timestamp": "2024-12-24T10:30:00Z",
  "results": [
    {
      "status": "success",
      "excel_idx": 0,
      "name": "Dominos Pizza",
      "filepath": "batch_output/ondc-bpp-123+Dominos_Pizza+DOM123+110001.geojson",
      "zones_count": 4,
      "elapsed": 2.3,
      "error": null
    }
  ]
}
```

**Summary Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `newly_processed` | Integer | Number of providers processed in this run |
| `already_in_excel` | Integer | Providers skipped (already had zones_file) |
| `file_exists_added` | Integer | Existing files added to Excel |
| `total_providers` | Integer | Total providers in Excel |
| `successful` | Integer | Successfully processed count |
| `failed` | Integer | Failed processing count |
| `total_zones_generated` | Integer | Total zone polygons created |
| `total_time` | Float | Total processing time in seconds |
| `average_time_per_provider` | Float | Average time per provider in seconds |
| `timestamp` | String | ISO 8601 timestamp |
| `results` | Array | Detailed results for each provider |

---

## Transportation Mode Mapping

| Input Mode | Valhalla API Mode |
|------------|-------------------|
| `motorcycle` | `motorcycle` |
| `car` | `auto` |
| `auto` | `auto` |
| `bike` | `bicycle` |
| `bicycle` | `bicycle` |
| `walk` | `pedestrian` |
| `walking` | `pedestrian` |

---

## Example Usage
```bash
# Basic usage - all defaults
python smart_batch_processing.py --excel providers.xlsx

# Custom distances and mode
python smart_batch_processing.py --excel providers.xlsx --distances 2 5 8 --mode car

# More parallel workers for faster processing
python smart_batch_processing.py --excel providers.xlsx --workers 10

# Force reprocess everything
python smart_batch_processing.py --excel providers.xlsx --no-skip-existing
```