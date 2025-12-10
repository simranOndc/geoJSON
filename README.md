# 🚀 Quick Start Guide - Google Maps Geocoding

## Step-by-Step Instructions

### 1️⃣ Get Google Maps API Key (5 minutes)

1. Go to https://console.cloud.google.com/
2. Create a new project or select existing one
3. Click **"Enable APIs & Services"**
4. Search for **"Geocoding API"** and click **Enable**
5. Go to **"Credentials"** → **"Create Credentials"** → **"API Key"**
6. Copy your API key (looks like: `AIzaSyD...`)

**Important**: Enable billing (you get $200 free credit/month)

---

### 2️⃣ Install Requirements (1 minute)

```bash
pip install pandas openpyxl requests
```

---

### 3️⃣ Test Your API Key (1 minute)

```bash
python test_api.py YOUR_API_KEY
```

**Expected output:**
```
✅ SUCCESS - All tests passed!
Your API key is working correctly.
```

---

### 4️⃣ Run Geocoding on Your File (30-60 minutes)

```bash
python geocode_restaurants_http.py SELLER_LEVEL_DETAILS__1_.xlsx output_with_coords.xlsx YOUR_API_KEY
```

**What happens:**
- Script processes all 25,708 restaurants
- Adds lat, long, and Google Maps links
- Shows progress every 25 rows
- Auto-saves every 500 rows
- Takes ~40-50 minutes

**Live progress:**
```
Progress: 500/25,708 (1.9%) | Success: 485 | Failed: 15 | Rate: 10.1/sec | ETA: 41.7 min
```

---

### 5️⃣ Check Your Results

Open `output_with_coords.xlsx` and you'll see new columns:
- **lat** - Latitude (e.g., 13.082700)
- **long** - Longitude (e.g., 80.270700)
- **formatted_address** - Full address from Google
- **place_id** - Unique place identifier
- **google_maps_link** - Clickable link (e.g., https://www.google.com/maps/place/?q=place_id:ChIJ...)

---

## 📊 What to Expect

### Success Rate
- **Expected**: 95-97% success rate
- **25,708 restaurants** = ~24,500 successfully geocoded
- **Failed**: ~1,200 (generic names, closed places, etc.)

### Cost
- **Your job**: 25,708 requests = ~$128
- **Free tier**: $200/month credit
- **Result**: Completely FREE! ✨

### Time
- **Processing**: ~40-50 minutes for all 25,708 rows
- **Rate**: ~10 rows/second (with safety margins)

---

## 🛟 Common Issues & Solutions

### Issue 1: "REQUEST_DENIED"
**Fix**: Enable Geocoding API and set up billing
1. Go to https://console.cloud.google.com/apis/library
2. Search "Geocoding API" → Enable
3. Go to Billing → Set up billing account

### Issue 2: Script is slow
**Normal**: 10 rows/sec is expected (rate limiting)
**Why**: Prevents hitting Google API limits

### Issue 3: Some restaurants failed
**Normal**: 3-5% failure rate is expected
**Reasons**: Generic names, closed places, typos
**Action**: Review failed ones manually

### Issue 4: Script interrupted
**Solution**: Just run it again with same output file
**Why**: It will resume from where it stopped

---

## 💡 Pro Tips

1. **Run overnight** - Let it complete without interruption
2. **Stable internet** - Use wired connection if possible
3. **Check samples** - Verify first 100 results make sense
4. **Clean data first** - Fix obvious typos in restaurant names

---

## 📞 Need Help?

### Check these first:
1. ✅ API key is correct (test with test_api.py)
2. ✅ Geocoding API is enabled
3. ✅ Billing is set up
4. ✅ Internet connection is stable

### Still stuck?
- Read the full README_GOOGLE_MAPS.md
- Check Google Maps documentation
- Verify your input file format

---

## 📁 Files Included

1. **geocode_restaurants_http.py** - Main script (use this)
2. **geocode_restaurants.py** - Alternative (needs googlemaps library)
3. **test_api.py** - Test your API key
4. **README_GOOGLE_MAPS.md** - Complete documentation
5. **QUICKSTART.md** - This file

---

## ✨ Example Command

```bash
# Complete workflow
pip install pandas openpyxl requests
python test_api.py YOUR_API_KEY
python geocode_restaurants_http.py SELLER_LEVEL_DETAILS__1_.xlsx restaurants_geocoded.xlsx YOUR_API_KEY
```

That's it! You'll have accurate coordinates and Google Maps links for all your restaurants.

---

**Time to complete**: 5 mins setup + 45 mins processing = **50 minutes total**  
**Cost**: **FREE** (within free tier)  
**Result**: 25,708 restaurants with accurate GPS coordinates! 🎉
