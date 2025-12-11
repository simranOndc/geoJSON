
# 📍 Isochrone GeoJSONs for Restaurant on the network

=======

* **Delhi**
* **Bangalore**
* **Chennai**

---

## 📏 Parameters Used

### **Distance-Based Isochrones**

* **3 km**
* **4 km**
* **5 km**
* **6 km**

### **Time-Based Isochrones**

* **10 minutes**
* **20 minutes**

### **Travel Mode**

* **Motorcycle**

---

## 📂 Repository Structure

```
/batch_output
   ├── bpp_id+provider_name+provider_id+pincode1
   ├── bpp_id+provider_name+provider_id+pincode2
   └── ... GeoJSON output files

/batch_output.py
   └── Script used to generate GeoJSONs via the Valhalla API
```

---

## 🧩 About the Code

The included Python script generates isodistance and isochrone polygons using routing engines and exports them as GeoJSON files.
All final outputs are stored inside the **`batch_output/`** directory.

---

# 🎯 Traffic Model — What the Script Considers

Your isochrone/isodistance zones are generated with multiple **traffic-aware adjustments**, making them significantly more realistic.

---

## ✅ **City-Level Congestion Factors**

Used based on pincode → auto-detection of city:

| City      | Speed Factor | Interpretation         |
| --------- | ------------ | ---------------------- |
| Mumbai    | 0.45         | 55% slower             |
| Bangalore | 0.50         | 50% slower             |
| Delhi     | 0.52         | 48% slower             |
| Chennai   | 0.58         | 42% slower             |
| Others    | Custom       | Based on configuration |

---

## ⏰ **Hour-of-Day Traffic**

Reflects typical 24-hour traffic patterns:

* **7–9 AM:** 0.50–0.60 (Peak morning rush)
* **12–1 PM:** 0.70 (Lunch rush)
* **6–9 PM:** 0.50–0.60 (Dinner/Evening peak)
* **Late night:** 0.90–0.99 (Light traffic)

---

## 📅 **Day-of-Week Impact**

* **Weekdays:** 0.90–0.95 (Busier)
* **Friday:** 0.90 (Worst)
* **Saturday:** 1.00 (Better)
* **Sunday:** 1.05 (Best)

---

## 🏙️ **Area Type Adjustments**

* **CBD:** 0.55 (Most congested)
* **Commercial:** 0.65
* **Residential:** 0.80
* **Suburban:** 0.85

---

## 🌦️ **Seasonal Adjustment**

* **Monsoon (Jun–Sep):** 0.75
* **Winter (Oct–Feb):** 0.95
* **Summer (Mar–May):** 0.90

---

## 📘 Optional: Historical Learning

If you upload **delivery history CSV**, the engine auto-learns:

* Actual deliverable speeds per **pincode + hour + day**
* Overrides default factors
* Improves zone accuracy over time

---

# 📊 Output Generated Per Restaurant

Each restaurant gets **one GeoJSON** containing **7 computed zones**:

### **Distance-Based Zones (Traffic Adjusted)**

* 3 km
* 4 km
* 5 km
* 6 km

Each of these gets **adjusted** based on traffic factor.

### **Time-Based Zones (Automatically Realistic)**

* 15 minutes
* 20 minutes
* 30 minutes

No adjustments required — Valhalla naturally considers road speeds.

---

## 🗂️ File Naming Format

```
{bpp_id}+{provider_name}+{provider_id}+{pincode}.geojson
```

### Example

```
seller.tipplr.in+Paradise_Biryani+PB123+560001.geojson
```

---

## 📄 Sample GeoJSON Structure

```json
{
  "type": "FeatureCollection",
  "metadata": {
    "provider_name": "Paradise Biryani",
    "center_lat": 12.9716,
    "center_lon": 77.5946,
    "pincode": "560001",
    "mode": "motorcycle",
    "total_zones": 7,
    "distance_zones": [3, 4, 5, 6],
    "time_zones": [15, 20, 30],
    "generated_at": "2024-12-11T10:30:00Z",
    "generation_conditions": {
      "city": "bangalore",
      "area_type": "commercial",
      "season": "winter",
      "traffic_factor": 0.175,
      "traffic_condition": "Very Heavy",
      "speed_reduction_percent": 82.5,
      "day_of_week": "Friday",
      "hour": 19,
      "has_learned_data": false
    }
  },
  "features": [
    {
      "type": "Feature",
      "properties": {
        "provider_name": "Paradise Biryani",
        "zone_type": "distance",
        "label": "3km",
        "api": "valhalla",
        "traffic_aware": true,
        "traffic_model": "historical",
        "requested_distance_km": 3,
        "adjusted_distance_km": 0.53,
        "city": "bangalore",
        "area_type": "commercial",
        "traffic_condition": "Very Heavy",
        "speed_reduction_percent": 82.5
      },
      "geometry": {
        "type": "Polygon",
        "coordinates": [[ ... ]]
      }
    }
    // + 6 more zones
  ]
}
```

---

# 🔧 How Traffic Adjustment Works

### Example: **Bangalore, Friday, 7 PM (Dinner Rush)**

```
City Factor       = 0.50
Hour Factor       = 0.55
Day Factor        = 0.90
Area Type Factor  = 0.65
Season Factor     = 0.95
```

### Combined Factor

```
Combined = 0.50 × 0.55 × 0.90 × 0.65 × 0.95
         = 0.153
```

This means **84.7% slower traffic** than free-flow conditions.

### Adjusted Distance Outputs

| Requested | Adjusted |
| --------- | -------- |
| 3 km      | 0.46 km  |
| 4 km      | 0.61 km  |
| 5 km      | 0.77 km  |
| 6 km      | 0.92 km  |

### Time Zones (Unaffected)

* 15 min
* 20 min
* 30 min

Valhalla adapts naturally using travel-time routing.

---
