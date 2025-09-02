# pip install geopandas shapely pandas requests python-dotenv geopy
import os
import time
import math
import requests
import pandas as pd
import geopandas as gpd
from shapely.geometry import box
from shapely.ops import union_all
from geopy.distance import geodesic
from dotenv import load_dotenv

# =========================
# ====== CONFIG ===========
# =========================
load_dotenv()

API_KEY = os.getenv("GOOGLE_PLACES_KEY_1")
if not API_KEY:
    raise RuntimeError("Missing GOOGLE_PLACES_KEY_1 in environment/.env")

# --- Shapefile & selection ---
COUNTRY_DIR = "AFG"
BASE_DIR = "C:/Users/myuan/Desktop/VetMap_Data"
SHP_DIR = "C:/Users/myuan/Desktop/Data/shapefile/country" 
COUNTRY_SHP_PATH = os.path.join(SHP_DIR, COUNTRY_DIR, f"{COUNTRY_DIR}1_nr.shp")
TARGET_COUNTRY = "Afghanistan"                    # match NAME_0 in your shapefile
ADMIN_FIELD = "NAME_1"                            # admin1 name column in your shapefile

# --- Places API (New) Essentials-only ---
SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"
TEXT_QUERY = "veterinarian"
PLACE_TYPE = "veterinary_care"
LANGUAGE = "en"

# Essentials-only fields (+ nextPageToken for pagination) — includes businessStatus for filtering
FIELD_MASK = (
    "places.displayName,places.formattedAddress,places.location,places.placeId,"
    "places.businessStatus,nextPageToken"
)

# --- Pagination & pacing ---
MAX_PAGES_PER_TILE = 3       # up to 3 pages (~60 results) per tile
PAGE_SLEEP = 2.5             # wait for nextPageToken warm-up
DAILY_CALL_CAP = 8000        # hard cap on API calls (safety)
TIMEOUT_S = 30

# --- Tiling controls (slice admin bbox into a grid of tiles) ---
# --- Tiling over polygon bbox ---
TARGET_TILE_KM = 50          # target tile size (~ km per side)
MAX_ROWS = 5                 # clamp rows per admin1 bbox
MAX_COLS = 5                 # clamp cols per admin1 bbox
MIN_TILE_KM = 15             # tiny bbox → 1 tile
PAD_DEG = 0.0                # no outward padding (we clip to polygon)
MIN_OVERLAP_FRAC = 0.10      # require ≥10% of a tile to overlap polygon
STRICT_INSIDE = True         # require tile-rect centroid inside polygon (reduces leakage)

# --- Output ---
GM_OPEN_CSV   = os.path.join(BASE_DIR, COUNTRY_DIR, f"GM/{COUNTRY_DIR}_VP_GM.csv")
GM_CLOSED_CSV = os.path.join(BASE_DIR, COUNTRY_DIR, f"GM/{COUNTRY_DIR}_VP_GM_closed.csv")

# --- Networking session ---
SESSION = requests.Session()

# =========================
# ====== UTILITIES ========
# =========================
def km_extent_from_bounds(bounds):
    """Compute bbox width/height in km using geodesic distance."""
    minx, miny, maxx, maxy = bounds
    mid_lat = (miny + maxy) / 2.0
    width_km  = geodesic((mid_lat, minx), (mid_lat, maxx)).km
    height_km = geodesic((miny, minx), (maxy, minx)).km
    return width_km, height_km

def make_grid_over_bbox(bounds, target_tile_km=50, max_rows=5, max_cols=5, min_tile_km=15):
    """
    Build an axis-aligned grid (as shapely boxes) over a bbox.
    Row/col counts are derived from bbox size and clamped by max_rows/cols.
    """
    minx, miny, maxx, maxy = bounds
    width_km, height_km = km_extent_from_bounds(bounds)

    # Small bbox: single tile
    if width_km <= min_tile_km and height_km <= min_tile_km:
        return [box(minx, miny, maxx, maxy)]

    rows = max(1, min(max_rows, math.ceil(height_km / max(1, target_tile_km))))
    cols = max(1, min(max_cols, math.ceil(width_km  / max(1, target_tile_km))))

    lat_edges = [miny + (maxy - miny) * i / rows for i in range(rows + 1)]
    lon_edges = [minx + (maxx - minx) * j / cols for j in range(cols + 1)]

    cells = []
    for i in range(rows):
        for j in range(cols):
            tminy, tmaxy = lat_edges[i], lat_edges[i+1]
            tminx, tmaxx = lon_edges[j], lon_edges[j+1]
            cells.append(box(tminx, tminy, tmaxx, tmaxy))
    return cells

def rect_from_polygon(poly):
    """Return the tight Places API rectangle (low/high) for a polygon's bounds."""
    minx, miny, maxx, maxy = poly.bounds
    return {
        "low":  {"latitude": float(miny - PAD_DEG), "longitude": float(minx - PAD_DEG)},
        "high": {"latitude": float(maxy + PAD_DEG), "longitude": float(maxx + PAD_DEG)},
    }

def search_text_essentials(rectangle, page_token=None):
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": API_KEY,
        "X-Goog-FieldMask": FIELD_MASK,
    }
    payload = {
        "textQuery": TEXT_QUERY,
        "placeType": PLACE_TYPE,
        "languageCode": LANGUAGE,
        "locationRestriction": {"rectangle": rectangle},
    }
    if page_token:
        payload["pageToken"] = page_token

    resp = SESSION.post(SEARCH_URL, headers=headers, json=payload, timeout=TIMEOUT_S)
    if resp.status_code == 429:
        # Basic backoff on rate-limit
        time.sleep(2.0)
    resp.raise_for_status()
    return resp.json()

# -------- Pagination helpers with early-stop rules --------
def extract_and_route_places(data, admin_name, seen_place_ids, rows_open, rows_not_open):
    """
    Parse a page, apply businessStatus routing, and return:
      - page_ids: list of all placeIds present on THIS page (used for duplicate ratio tests)
      - appended: count of NEW rows appended to outputs (global de-dup is respected)
    """
    page_ids = []
    appended = 0

    for p in data.get("places", []):
        pid = p.get("placeId")
        if not pid:
            continue
        page_ids.append(pid)  # collect IDs regardless of global dup status

        if pid in seen_place_ids:
            continue  # global de-dup across tiles/provinces

        name = (p.get("displayName") or {}).get("text")
        addr = p.get("formattedAddress")
        loc  = p.get("location") or {}
        lat  = loc.get("latitude")
        lon  = loc.get("longitude")
        status = p.get("businessStatus")  # Essentials field

        if not (name and (lat is not None) and (lon is not None)):
            continue

        seen_place_ids.add(pid)

        row_out = {
            "Name": name,
            "Address": addr,
            "Latitude": lat,
            "Longitude": lon,
            "Province": admin_name
        }

        if status == "OPERATIONAL":
            rows_open.append(row_out)
        else:
            rows_not_open.append(row_out)

        appended += 1

    return page_ids, appended

# =========================
# ========= MAIN ==========
# =========================
def GooglePlace():
    # 1) Load admin1 polygons for target country
    gdf = gpd.read_file(COUNTRY_SHP_PATH)

    # Ensure EPSG:4326 (lat/lon). 
    if gdf.crs is None or (getattr(gdf.crs, "to_epsg", lambda: None)() != 4326):
        gdf = gdf.to_crs(epsg=4326)

    # Required columns
    for col in ["NAME_0", ADMIN_FIELD, "geometry"]:
        if col not in gdf.columns:
            raise ValueError(f"Shapefile is missing required column: {col}")

    # Filter to the chosen country and dissolve to one geometry per admin1
    gdf = gdf[gdf["NAME_0"] == TARGET_COUNTRY].copy()
    if gdf.empty:
        raise ValueError(f"No records found for NAME_0 == '{TARGET_COUNTRY}' in {COUNTRY_SHP_PATH}")
    gdf = gdf.dissolve(by=ADMIN_FIELD, as_index=False)

    seen_place_ids = set()  # global dedupe across all tiles
    rows_open = []
    rows_not_open = []
    calls_used = 0

    # 2) Iterate provinces; tile their bbox; query each tile
    for _, row in gdf.iterrows():
        if calls_used >= DAILY_CALL_CAP:
            print(f"[STOP] Daily call cap reached ({DAILY_CALL_CAP}).")
            break

        admin_name = str(row[ADMIN_FIELD])
        poly = row.geometry
        if poly.is_empty:
            continue

        # Normalize to a single polygonal geometry
        poly = union_all([poly])

        # Generate bbox grid
        bounds = poly.bounds
        cells = make_grid_over_bbox(
            bounds,
            target_tile_km=TARGET_TILE_KM,
            max_rows=MAX_ROWS,
            max_cols=MAX_COLS,
            min_tile_km=MIN_TILE_KM,
        )

        # Keep only cells with meaningful overlap; optional centroid-inside filter
        valid_tiles = []
        for cell in cells:
            inter = poly.intersection(cell)
            if inter.is_empty:
                continue
            if inter.area / cell.area < MIN_OVERLAP_FRAC:
                continue
            if STRICT_INSIDE:
                rect = rect_from_polygon(inter)
                cx = (rect["low"]["longitude"] + rect["high"]["longitude"]) / 2.0
                cy = (rect["low"]["latitude"]  + rect["high"]["latitude"])  / 2.0
                # point-in-polygon via zero-size box
                if not poly.contains(box(cx, cy, cx, cy)):
                    continue
            valid_tiles.append(inter)

        print(f"Admin1: {admin_name} — tiles kept: {len(valid_tiles)}")

        for t_idx, inter in enumerate(valid_tiles, 1):
            if calls_used >= DAILY_CALL_CAP:
                break

            rect = rect_from_polygon(inter)
            print(f"  Tile {t_idx}/{len(valid_tiles)}")

            # ----------------------
            # Page 1 (always fetch)
            # ----------------------
            data = search_text_essentials(rect, page_token=None)
            calls_used += 1

            page1_ids, _ = extract_and_route_places(
                data, admin_name, seen_place_ids, rows_open, rows_not_open
            )
            page1_count = len(data.get("places", []))
            next_token = data.get("nextPageToken")

            # RULE 1: If page 1 returns < 20 results, no pages 2–3 exist → stop this tile.
            if page1_count < 20 or not next_token or calls_used >= DAILY_CALL_CAP:
                time.sleep(0.1)
                continue  # move to next tile

            # ----------------------
            # Page 2 (conditional)
            # ----------------------
            time.sleep(PAGE_SLEEP)  # token warm-up
            data = search_text_essentials(rect, page_token=next_token)
            calls_used += 1

            page2_ids, _ = extract_and_route_places(
                data, admin_name, seen_place_ids, rows_open, rows_not_open
            )
            page2_count = len(data.get("places", []))
            next_token = data.get("nextPageToken")

            # RULE 2: If >80% of page 2 are duplicates of page 1 → stop (skip page 3).
            if page2_count > 0:
                dups_with_p1 = sum(1 for pid in page2_ids if pid in set(page1_ids))
                dup_ratio = dups_with_p1 / page2_count
                if dup_ratio > 0.80 or not next_token or calls_used >= DAILY_CALL_CAP:
                    time.sleep(0.1)
                    continue  # move to next tile
            
            # RULE 3: If page 2 returns < 20 results → stop (skip page 3).
            if page2_count < 20 or not next_token or calls_used >= DAILY_CALL_CAP:
                time.sleep(0.1)
                continue

            # ----------------------
            # Page 3 (last chance)
            # ----------------------
            time.sleep(PAGE_SLEEP)
            data = search_text_essentials(rect, page_token=next_token)
            calls_used += 1

            _page3_ids, _ = extract_and_route_places(
                data, admin_name, seen_place_ids, rows_open, rows_not_open
            )
            # No further token; API caps at page 3.
            time.sleep(0.1)

        time.sleep(0.1)  # pacing between provinces

    # 3) Save outputs (BusinessStatus is intentionally omitted)
    pd.DataFrame(rows_open).drop_duplicates().to_csv(GM_OPEN_CSV, index=False)
    pd.DataFrame(rows_not_open).drop_duplicates().to_csv(GM_CLOSED_CSV, index=False)

    print(f"\n✅ Open (OPERATIONAL): {len(rows_open)}")
    print(f"✅ Not open (non-OPERATIONAL): {len(rows_not_open)}")
    print(f"📊 Approx API calls used: {calls_used}")

if __name__ == "__main__":
    GooglePlace()
