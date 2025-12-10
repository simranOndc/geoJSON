# 📍 Isochrone GeoJSONs for Restaurant on the network


* **Delhi**
* **Bangalore**
* **Chennai**

## 📏 Parameters Used

### **Distance-based Isochrones**

* **3 km**
* **4 km**
* **5 km**
* **6 km**

### **Time-based Isochrones**

* **10 minutes**
* **20 minutes**

### **Travel Mode**

* **Motorcycle**

## 📂 Repository Structure

```
/batch_output
   ├── bpp_id+provider_name+provider_id+pincode1
   ├── bpp_id+provider_name+provider_id+pincode2
   └── ... GeoJSON output files

/batch_output.py
   └── code used to generate the GeoJSONs via "ValHalla" API
```

## 🧩 About the Code

The enclosed script generates isochrone and isodistance polygons using routing engines and exports them as GeoJSON files.
All final outputs are available in the **`batch_output`** folder.

---
