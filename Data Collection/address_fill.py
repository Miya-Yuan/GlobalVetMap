limport pandas as pd
import requests
import time
import os

# === CONFIGURATION ===
BASE_DIR = r"C:\Users\myuan\Desktop\VetMap_Data"   # root folder with ISO subfolders
load_dotenv()
USER_AGENT = os.getenv("OSM_USER_AGENT")  # required by Nominatim
SLEEP_SECONDS = 1  # Nominatim limit = 1 request/sec

# === REVERSE GEOCODING ===
def reverse_geocode(lat, lon):
    url = "https://nominatim.openstreetmap.org/reverse"
    params = {"lat": lat, "lon": lon, "format": "jsonv2", "addressdetails": 1}
    try:
        r = requests.get(url, params=params, headers={"User-Agent": USER_AGENT}, timeout=10)
        if r.status_code == 200:
            data = r.json().get("address", {})
            return ", ".join(filter(None, [
                data.get("road"),
                data.get("house_number"),
                data.get("postcode"),
                data.get("city") or data.get("town") or data.get("village"),
                data.get("country")
            ]))
    except Exception as e:
        print(f"⚠️ Error at {lat}, {lon}: {e}")
    return None

# === PROCESS A SINGLE FILE ===
def process_csv(csv_path):
    print(f"\n🔍 Processing {csv_path}")
    df = pd.read_csv(csv_path)

    if not {"Latitude", "Longitude", "Address"}.issubset(df.columns):
        print(f"⚠️ Skipping {csv_path}: missing required columns")
        return

    missing_mask = df["Address"].isna() | df["Address"].astype(str).str.strip().eq("")
    print(f"   → {missing_mask.sum()} rows missing addresses")

    for idx, row in df[missing_mask].iterrows():
        lat, lon = row["Latitude"], row["Longitude"]
        if pd.notna(lat) and pd.notna(lon):
            addr = reverse_geocode(lat, lon)
            if addr:
                df.at[idx, "Address"] = addr
                print(f"   ✅ Row {idx} filled: {addr}")
            else:
                print(f"   ⚠️ Row {idx}: no address found")
            time.sleep(SLEEP_SECONDS)

    # Save updated file (overwrite)
    df.to_csv(csv_path, index=False)
    print(f"💾 Saved updates to {csv_path}")

# === WALK THROUGH ALL SUBFOLDERS ===
def process_all():
    for root, _, files in os.walk(BASE_DIR):
        for file in files:
            if file.endswith("_VP_OSM.csv") and len(file.split("_")[0]) == 3:
                csv_path = os.path.join(root, file)
                process_csv(csv_path)

if __name__ == "__main__":
    process_all()


