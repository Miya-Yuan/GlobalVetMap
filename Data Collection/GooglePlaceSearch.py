import os
import time
import random
import re
from datetime import timedelta
from typing import Dict, Any, List, Optional, Tuple, Set
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from geopy.distance import geodesic
import googlemaps
from dotenv import load_dotenv

# === CONFIGURATION ===
load_dotenv()
API_KEYS = [
    os.getenv("GOOGLE_API_KEY_1"),
    os.getenv("GOOGLE_API_KEY_2"),
    os.getenv("GOOGLE_API_KEY_3"),
]
API_KEYS = [k for k in API_KEYS if k]  # remove Nones
if not API_KEYS:
    raise RuntimeError("No API keys found. Set GOOGLE_API_KEY_1 (and _2, _3...) in your environment.")

# Per-second pacing targets (tune based on observed error rate/quota)
NEARBY_QPS_TARGET = 5.0     # global pacing for places_nearby
DETAILS_QPS_TARGET = 2.0    # pacing for place details
BASE_JITTER = 0.15          # seconds of small jitter between calls

COUNTRY_DIR = "AGO"
BASE_DIR = "C:/Users/myuan/Desktop/VetMap_Data"
SHP_DIR = "C:/Users/myuan/Desktop/Data/shapefile/country"

COUNTRY_SHP_PATH = os.path.join(SHP_DIR, COUNTRY_DIR, "AGO1_nr.shp") 
SEARCH_RADIUS = 10000  # in meters
GRID_SPACING_KM = 10  # grid spacing in kilometers
LANGUAGE = "en"               # response language
PLACE_TYPE = "veterinary_care"
ALWAYS_FETCH_WEBSITE = True
# === OUTPUT ===
PROGRESS_FILE = os.path.join(BASE_DIR, COUNTRY_DIR, "GM/progress_AGO.csv")
FINAL_OUTPUT_FILE = os.path.join(BASE_DIR, COUNTRY_DIR, "GM/AGO_VP_GM.csv")
DEDUP_OUTPUT_FILE = os.path.join(BASE_DIR, COUNTRY_DIR, "GM/AGO_VP_GM_dedup.csv")
# Restrict fields for Details (reduces quota and failure surface)
DETAIL_FIELDS = ["business_status", "types", "website"]

# =========================
# ====== UTILITIES ========
# =========================

class KeyRotator:
    """
    Round-robin API key rotator. Creates a fresh googlemaps.Client for each call.
    """
    def __init__(self, keys: List[str]):
        self.keys = keys
        self.i = 0
    def client(self) -> googlemaps.Client:
        key = self.keys[self.i]
        self.i = (self.i + 1) % len(self.keys)
        return googlemaps.Client(key=key)

class Pacer:
    """
    Simple pacer to approximate QPS limits by sleeping between calls.
    """
    def __init__(self, qps: float):
        self.min_interval = 1.0 / max(qps, 0.1)
        self.last_ts = 0.0
    def wait(self):
        now = time.time()
        delta = now - self.last_ts
        if delta < self.min_interval:
            time.sleep(self.min_interval - delta)
        # add light jitter to avoid synchronization effects
        time.sleep(random.uniform(0, BASE_JITTER))
        self.last_ts = time.time()

def backoff_sleep(attempt: int, base: float = 0.8, cap: float = 10.0):
    """
    Exponential backoff with jitter. attempt starts at 0.
    """
    delay = min(cap, base * (2 ** attempt)) + random.uniform(0, 0.3)
    time.sleep(delay)

# =========================
# === GEOMETRY & GRID  ===
# =========================

def load_country_polygon(shp_path: str):
    """
    Load shapefile and return a unified polygon in EPSG:4326.
    Uses union_all() for widest Shapely compatibility.
    """
    gdf = gpd.read_file(shp_path).to_crs(epsg=4326)
    try:
        polygon = gdf.geometry.union_all()
    except Exception:
        polygon = gdf.union_all()
    return polygon

def generate_grid_in_shape(polygon, spacing_km: float = 10.0) -> List[Tuple[float, float]]:
    """
    Generate a latitude/longitude grid inside a polygon using geodesic steps.
    Rounded to 5 decimal places to reduce float noise.
    """
    minx, miny, maxx, maxy = polygon.bounds
    lat = miny
    points = []
    while lat <= maxy:
        lng = minx
        while lng <= maxx:
            pt = Point(lng, lat)
            if polygon.contains(pt):
                points.append((round(lat, 5), round(lng, 5)))
            # Move east by spacing_km
            lng = geodesic(kilometers=spacing_km).destination((lat, lng), 90).longitude
        # Move north by spacing_km
        lat = geodesic(kilometers=spacing_km).destination((lat, minx), 0).latitude
    return points

# =========================
# ==== PLACES QUERIES =====
# =========================

key_rotator = KeyRotator(API_KEYS)
nearby_pacer = Pacer(NEARBY_QPS_TARGET)
details_pacer = Pacer(DETAILS_QPS_TARGET)

def places_nearby_once(lat: float, lng: float, radius: int) -> Dict[str, Any]:
    """
    Single Nearby Search call with pacing and a fresh client.
    """
    nearby_pacer.wait()
    gmaps_client = key_rotator.client()
    return gmaps_client.places_nearby(
        location=(lat, lng),
        radius=radius,
        type=PLACE_TYPE,
        language=LANGUAGE
    )

def places_nearby_page(page_token: str) -> Dict[str, Any]:
    """
    Fetch a subsequent page. Only send page_token (best practice).
    """
    nearby_pacer.wait()
    gmaps_client = key_rotator.client()
    return gmaps_client.places_nearby(page_token=page_token, language=LANGUAGE)

def place_details(place_id: str) -> Dict[str, Any]:
    """
    Lean Place Details call restricted to needed fields.
    """
    details_pacer.wait()
    gmaps_client = key_rotator.client()
    return gmaps_client.place(place_id=place_id, language=LANGUAGE, fields=DETAIL_FIELDS)

def call_with_retries(call_fn, max_attempts: int = 6, treat_invalid_as_retry: bool = False) -> Dict[str, Any]:
    """
    Wrap Google calls with retry/backoff for transient statuses:
    - OVER_QUERY_LIMIT, UNKNOWN_ERROR
    - INVALID_REQUEST (optionally, e.g., next_page_token not ready)
    - 5xx handled by googlemaps client as ApiError (caught generically)
    """
    last_exc = None
    for attempt in range(max_attempts):
        try:
            resp = call_fn()
            status = resp.get("status", "OK")
            if status in ("OK",):
                return resp
            if status in ("OVER_QUERY_LIMIT", "UNKNOWN_ERROR"):
                backoff_sleep(attempt)
                continue
            if status in ("INVALID_REQUEST",) and treat_invalid_as_retry:
                # Happens when next_page_token isn't ready yet
                backoff_sleep(attempt)
                continue
            # Non-retriable statuses
            return resp
        except Exception as e:
            last_exc = e
            backoff_sleep(attempt)
    if last_exc:
        raise last_exc
    return {"status": "ERROR", "results": []}

def nearby_with_pagination(lat: float, lng: float, radius: int) -> List[Dict[str, Any]]:
    """
    Robust Nearby Search:
    - Retries the first page.
    - Polls for the next_page_token with retries until it's ready (INVALID_REQUEST → wait).
    - Max 3 pages total (Google cap).
    """
    results: List[Dict[str, Any]] = []

    first = call_with_retries(lambda: places_nearby_once(lat, lng, radius))
    results.extend(first.get("results", []))
    page_token = first.get("next_page_token")
    pages_fetched = 1

    # Up to two more pages
    while page_token and pages_fetched < 3:
        # Token propagation delay: treat INVALID_REQUEST as retriable
        page = call_with_retries(lambda: places_nearby_page(page_token), treat_invalid_as_retry=True)
        status = page.get("status", "OK")
        if status != "OK":
            # If still not OK after retries, stop pagination gracefully
            break
        results.extend(page.get("results", []))
        page_token = page.get("next_page_token")
        pages_fetched += 1

    return results
# =========================
# ====== MAIN LOGIC =======
# =========================

def scrape_vet_clinics_with_resume():
    start_time = time.time()

    print(f"Loading country polygon for: {COUNTRY_DIR}")
    polygon = load_country_polygon(COUNTRY_SHP_PATH)

    print("Generating grid points ...")
    grid_points = generate_grid_in_shape(polygon, spacing_km=GRID_SPACING_KM)
    print(f"Grid points inside {COUNTRY_DIR}: {len(grid_points)}")

    # Resume support
    if os.path.exists(PROGRESS_FILE):
        df_progress = pd.read_csv(PROGRESS_FILE)
        processed_coords: Set[Tuple[float, float]] = set(zip(df_progress["grid_lat"], df_progress["grid_lng"]))
        all_results: List[Dict[str, Any]] = df_progress.to_dict(orient="records")
        print(f"⏸️ Resuming: {len(processed_coords)} points already processed.")
    else:
        processed_coords = set()
        all_results = []

    # Dedup set of seen place_ids; also track which place_ids already have website (=> Details likely done)
    seen_place_ids: Set[str] = {str(r["place_id"]) for r in all_results if pd.notna(r.get("place_id"))}
    detailed_place_ids: Set[str] = {
        str(r["place_id"]) for r in all_results
        if pd.notna(r.get("place_id")) and pd.notna(r.get("website"))
    }

    for idx, (lat, lng) in enumerate(grid_points, start=1):
        if (lat, lng) in processed_coords:
            continue

        print(f"[{idx}/{len(grid_points)}] 🔎 Nearby ({lat:.5f}, {lng:.5f}) ...")
        try:
            nearby_results = nearby_with_pagination(lat, lng, SEARCH_RADIUS)
        except Exception as e:
            print(f"Nearby error at ({lat}, {lng}): {e}")
            nearby_results = []

        for place in nearby_results:
            place_id = str(place.get("place_id"))
            if not place_id:
                continue
            if place_id in seen_place_ids:
                # Already captured from some other grid cell
                continue
            seen_place_ids.add(place_id)

            # Prefer values from Nearby to avoid a Details call
            nearby_status = place.get("business_status")
            nearby_types = place.get("types") or []
            website = None

            # --- Always fetch website if available in Google data ---
            # Call Details for every unique place exactly once (resume-safe via detailed_place_ids)
            if (ALWAYS_FETCH_WEBSITE) and (place_id not in detailed_place_ids):
                try:
                    d = call_with_retries(lambda: place_details(place_id))
                    if d.get("status") == "OK":
                        res = d.get("result", {}) or {}
                        # capture website if present
                        website = res.get("website")  # may be None if Google has no website
                        # fill missing status/types from Details (don’t overwrite Nearby values if already present)
                        if nearby_status is None:
                            nearby_status = res.get("business_status")
                        if not nearby_types:
                            nearby_types = res.get("types") or nearby_types
                        detailed_place_ids.add(place_id)
                    else:
                        # Details returned a logical error; proceed with Nearby-only fields
                        pass
                except Exception as e:
                    print(f"Details error for {place_id}: {e}")

            entry = {
                "country_code": COUNTRY_DIR,
                "place_id": place_id,
                "name": place.get("name"),
                "address": place.get("vicinity"),
                "latitude": (place.get("geometry") or {}).get("location", {}).get("lat"),
                "longitude": (place.get("geometry") or {}).get("location", {}).get("lng"),
                "business_status": nearby_status,
                "types": nearby_types,
                "website": website,  # will be None if Google has no website value
                "grid_lat": lat,
                "grid_lng": lng
            }
            all_results.append(entry)

        # Persist progress after each grid point (resume-safe)
        pd.DataFrame(all_results).to_csv(PROGRESS_FILE, index=False)

    # ---------- Final post-processing ----------
    df_all = pd.DataFrame(all_results)
    # Drop places without IDs or coordinates
    df_all = df_all[pd.notna(df_all["place_id"])]
    df_all = df_all.drop_duplicates(subset=["place_id"])

    # Separate operational vs non-operational BEFORE filtering
    df_oper = df_all[df_all["business_status"] == "OPERATIONAL"].copy()
    df_nonoper = df_all[df_all["business_status"] != "OPERATIONAL"].copy()

    # Choose columns and format
    cols = ["name", "address", "latitude", "longitude", "website"]
    df_oper = df_oper[cols]
    df_nonoper = df_nonoper[cols]
    df_oper.columns = [c.capitalize() for c in df_oper.columns]
    df_nonoper.columns = [c.capitalize() for c in df_nonoper.columns]

    df_oper.to_csv(FINAL_OUTPUT_FILE, index=False)
    df_nonoper.to_csv(DEDUP_OUTPUT_FILE, index=False)

    total_time = time.time() - start_time
    print(f"\n✅ Finished. {len(df_oper)} operational vet clinics → {FINAL_OUTPUT_FILE}")
    print(f"{len(df_nonoper)} non-operational/other → {DEDUP_OUTPUT_FILE}")
    print(f"⏱️ Total runtime: {timedelta(seconds=int(total_time))}")

# ========= RUN =========
if __name__ == "__main__":
    scrape_vet_clinics_with_resume()
