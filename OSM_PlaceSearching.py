import geopandas as gpd
from shapely.geometry import box
import pandas as pd
import time
from datetime import timedelta
import os
import requests
import random

# === CONFIGURATION ===
USER_AGENT = "MyGeocoderScript/1.0 (mingyangy656@gmail.com)"
COUNTRY_DIR = "VUT"
BASE_DIR = "C:/Users/myuan/Desktop/VetMap_Data"
SHP_DIR = "C:/Users/myuan/Desktop/Data/shapefile/country"

# Input files
COUNTRY_SHP_PATH = os.path.join(SHP_DIR, COUNTRY_DIR, f"{COUNTRY_DIR}1_nr.shp")
TILE_SIZE_DEG = 0.25   

# Output files — keep separate
OSM_PROGRESS_FILE = os.path.join(BASE_DIR, COUNTRY_DIR, "OSM", f"progress_{COUNTRY_DIR}.csv")
OSM_OUTPUT_FILE = os.path.join(BASE_DIR, COUNTRY_DIR, "OSM", f"{COUNTRY_DIR}_VP_OSM.csv")
os.makedirs(os.path.dirname(OSM_PROGRESS_FILE), exist_ok=True)

# Overpass mirrors
OVERPASS_MIRRORS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.openstreetmap.ru/api/interpreter",
    "https://overpass.osm.ch/api/interpreter",
]
MAX_RETRIES = 5
BACKOFF_BASE = 2.0  # seconds

# === BBOX GENERATION WITHIN SHAPEFILE ===
def query_osm_veterinary_bbox(south, west, north, east):
    query = f"""
    [out:json][timeout:60];
    (
      node["amenity"="veterinary"]({south},{west},{north},{east});
      way["amenity"="veterinary"]({south},{west},{north},{east});
      relation["amenity"="veterinary"]({south},{west},{north},{east});
    );
    out center;
    """
    headers = {"User-Agent": USER_AGENT}

    for attempt in range(1, MAX_RETRIES + 1):
        mirror = OVERPASS_MIRRORS[(attempt - 1) % len(OVERPASS_MIRRORS)]
        try:
            r = requests.post(mirror, data=query, headers=headers, timeout=90)
            if r.status_code == 429:
                wait = BACKOFF_BASE * attempt + random.uniform(0, 1)
                time.sleep(wait)
                continue
            if r.status_code >= 500:
                wait = BACKOFF_BASE * attempt + random.uniform(0, 1)
                time.sleep(wait)
                continue
            r.raise_for_status()
            data = r.json()
            return data.get("elements", [])
        except Exception:
            wait = BACKOFF_BASE * attempt + random.uniform(0, 1)
            time.sleep(wait)
            continue

    print(f"❌ Overpass query failed after {MAX_RETRIES} attempts for bbox ({south},{west},{north},{east})")
    return []


def OSM_Place():
    start_time = time.time()

    # 1) Load country shapefile and unify geometry (WGS84)
    print(f"🔍 Loading shapefile for country: {COUNTRY_DIR}")
    gdf = gpd.read_file(COUNTRY_SHP_PATH).to_crs(epsg=4326)
    polygon = gdf.geometry.union_all()

    # 2) Build bounding box tiles
    minx, miny, maxx, maxy = polygon.bounds
    tiles = []
    y = miny
    while y < maxy:
        x = minx
        while x < maxx:
            tile = box(x, y, x + TILE_SIZE_DEG, y + TILE_SIZE_DEG)
            if polygon.intersects(tile):
                tiles.append(tile)
            x += TILE_SIZE_DEG
        y += TILE_SIZE_DEG
    print(f"🧩 {len(tiles)} tiles generated.")

    # 3) Ensure progress file exists with headers
    if not os.path.exists(OSM_PROGRESS_FILE):
        pd.DataFrame(columns=[
            "country_code", "osm_id", "name", "address", "latitude", "longitude", "website",
            "south", "west", "north", "east"
        ]).to_csv(OSM_PROGRESS_FILE, index=False)

    # Load already processed tiles
    df_progress = pd.read_csv(OSM_PROGRESS_FILE)
    processed_tiles = set(zip(df_progress["south"], df_progress["west"],
                              df_progress["north"], df_progress["east"]))

    # Track seen OSM ids to avoid duplicates during data collection
    seen_ids = set(df_progress["osm_id"].dropna().unique())

    # 4) Iterate over tiles
    for idx, tile in enumerate(tiles, start=1):
        south, west, north, east = tile.bounds
        tile_id = (south, west, north, east)
        if tile_id in processed_tiles:
            continue

        print(f"[{idx}/{len(tiles)}] 🔎 OSM search in bbox ({south:.3f},{west:.3f},{north:.3f},{east:.3f})...")
        elements = query_osm_veterinary_bbox(south, west, north, east)

        new_entries = []
        for el in elements:
            osm_id = f"{el['type']}/{el['id']}"
            if osm_id in seen_ids:
                continue
            seen_ids.add(osm_id)

            tags = el.get("tags", {})
            name = tags.get("name")
            website = tags.get("website") or tags.get("contact:website")
            address_parts = [tags.get("addr:street"), tags.get("addr:housenumber"),
                             tags.get("addr:postcode"), tags.get("addr:city")]
            address = ", ".join([p for p in address_parts if p])
            center = el.get("center") or {"lat": el.get("lat"), "lon": el.get("lon")}

            new_entries.append({
                "country_code": COUNTRY_DIR,
                "osm_id": osm_id,
                "name": name,
                "address": address,
                "latitude": center.get("lat"),
                "longitude": center.get("lon"),
                "website": website,
                "south": south, "west": west, "north": north, "east": east
            })

        # Append results immediately
        if new_entries:
            pd.DataFrame(new_entries).to_csv(OSM_PROGRESS_FILE, mode="a", header=False, index=False)

        # Always mark the tile as processed, even if no results
        pd.DataFrame([{
            "country_code": COUNTRY_DIR, "osm_id": None, "name": None, "address": None,
            "latitude": None, "longitude": None, "website": None,
            "south": south, "west": west, "north": north, "east": east
        }]).to_csv(OSM_PROGRESS_FILE, mode="a", header=False, index=False)
        time.sleep(2)  # API pacing

    # 5) Finalize: normalize, dedup, select columns robustly
    df_progress = pd.read_csv(OSM_PROGRESS_FILE)
    df_final = df_progress.dropna(subset=["osm_id"]).drop_duplicates(subset=["osm_id"])
    if not df_final.empty:
        df_final = df_final[["name", "address", "latitude", "longitude", "website"]]
        df_final.columns = [col.capitalize() for col in df_final.columns]
        df_final[['Name', 'Address']] = df_final[['Name', 'Address']].replace(r'^\s*$', pd.NA, regex=True)
        df_final = df_final.dropna(subset=['Name', 'Address'], how='all')
    else:
        df_final = pd.DataFrame(columns=["Name", "Address", "Latitude", "Longitude", "Website"])

    df_final.to_csv(OSM_OUTPUT_FILE, index=False)

    total_time = time.time() - start_time
    print(f"\n✅ Finished! {len(df_final)} unique vet clinics saved to {OSM_OUTPUT_FILE}.")
    print(f"⏱️ Total runtime: {timedelta(seconds=int(total_time))}")

if __name__ == "__main__":
    OSM_Place()
