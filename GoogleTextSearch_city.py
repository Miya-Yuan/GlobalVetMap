import os
import time
import math
import requests
import pandas as pd
from dotenv import load_dotenv

# =========================
# ====== CONFIG ===========
# =========================
load_dotenv()
API_KEY = os.getenv("GOOGLE_PLACES_KEY_1")
if not API_KEY:
    raise RuntimeError("Missing GOOGLE_PLACES_KEY_1 in environment/.env")

BASE_DIR = "C:/Users/myuan/Desktop/VetMap_Data"
CITY_CSV = "C:/Users/myuan/Desktop/Data/cities.csv"   
COUNTRY_DIR = "AUS"        # change per run
FILTER_ADMIN2 = None       # e.g., "Queensland" or ["Queensland","Victoria"]; keep None for all

# Places API (New) — Essentials only
SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"
PLACE_TYPE = "veterinary_care"
LANGUAGE = "en"
FIELD_MASK = "places.displayName,places.formattedAddress,places.location,places.placeId,nextPageToken"  # Essentials only

# Pagination / pacing / safety
MAX_PAGES_PER_CITY = 3       # up to ~60 results per city (3 pages × ~20)
PAGE_SLEEP = 2.5             # token warm-up
DAILY_CALL_CAP = 8000        # hard stop to control spend
TIMEOUT_S = 30
SESSION = requests.Session()

# Output
GM_OPEN_CSV   = os.path.join(BASE_DIR, COUNTRY_DIR, f"GM/{COUNTRY_DIR}_VP_GM.csv")
GM_CLOSED_CSV = os.path.join(BASE_DIR, COUNTRY_DIR, f"GM/{COUNTRY_DIR}_VP_GM_closed.csv")

# =========================
# === POP → RADIUS (m) ====
# =========================
# Piecewise-population mapping (simple, predictable).
MIN_RADIUS_M = 10_000   # 10 km floor
MAX_RADIUS_M = 80_000   # 80 km cap (API may limit very large radii; keep conservative)

def radius_from_population(pop):
    """
    Return a radius (meters) based on city population.
    Adjust buckets to your country density.
    """
    if pop is None or pd.isna(pop) or pop <= 0:
        return 20_000  # default 20 km when population missing
    pop = float(pop)
    if pop < 20_000:
        r = 15_000   # small towns
    elif pop < 100_000:
        r = 25_000
    elif pop < 300_000:
        r = 35_000
    elif pop < 1_000_000:
        r = 50_000
    else:
        r = 65_000   # large metros
    return max(MIN_RADIUS_M, min(MAX_RADIUS_M, int(r)))

# Optional: logarithmic variant (commented)
# def radius_from_population(pop):
#     if pop is None or pd.isna(pop) or pop <= 0:
#         return 20_000
#     # base 15 km + 8 km * log10(pop), clamped
#     r = 15_000 + 8_000 * math.log10(pop)
#     return max(MIN_RADIUS_M, min(MAX_RADIUS_M, int(r)))

# =========================
# ===== API call (NEW) ====
# =========================
SESSION = requests.Session()

def search_text_essentials(query, lat, lon, radius_m, page_token=None):
    """
    Essentials-only Text Search with circular locationBias (no Details, no contact/atmosphere fields).
    """
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": API_KEY,
        "X-Goog-FieldMask": FIELD_MASK,
    }
    payload = {
        "textQuery": query,
        "placeType": PLACE_TYPE,
        "languageCode": LANGUAGE,
        "locationBias": {
            "circle": {
                "center": {"latitude": float(lat), "longitude": float(lon)},
                "radius": int(radius_m)
            }
        }
    }
    if page_token:
        payload["pageToken"] = page_token

    resp = SESSION.post(SEARCH_URL, headers=headers, json=payload, timeout=TIMEOUT_S)
    if resp.status_code == 429:
        # Simple backoff on rate limiting
        time.sleep(2.0)
    resp.raise_for_status()
    return resp.json()

# =========================
# === Page parse & route ==
# =========================
def extract_and_route_places(data, city, country, radius_m, seen_place_ids, rows_open, rows_not_open):
    """
    Parse one page, apply businessStatus routing, and return:
      - page_ids: IDs present on THIS page (for duplicate-ratio checks)
      - appended: count of NEW rows appended (global de-dup enforced)
    """
    page_ids = []
    appended = 0
    for p in data.get("places", []):
        pid = p.get("placeId")
        if not pid:
            continue
        page_ids.append(pid)  # collect regardless of global duplication

        if pid in seen_place_ids:
            continue  # global de-dup across all cities

        name = (p.get("displayName") or {}).get("text")
        addr = p.get("formattedAddress")
        loc  = p.get("location") or {}
        plat = loc.get("latitude")
        plon = loc.get("longitude")
        status = p.get("businessStatus")  # Essentials field

        if not (name and (plat is not None) and (plon is not None)):
            continue

        seen_place_ids.add(pid)

        # Build row WITHOUT BusinessStatus (per requirement)
        row_out = {
            "Name": name,
            "Address": addr,
            "Latitude": plat,
            "Longitude": plon,
            "City": city,
            "Country": country,
            "RadiusM": int(radius_m)
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
    df = pd.read_csv(CITY_CSV)

    # Filter to country
    df = df[df["ISO3"] == COUNTRY_DIR].copy()
    # Optionally filter to admin2
    if FILTER_ADMIN2 is not None:
        if isinstance(FILTER_ADMIN2, str):
            df = df[df["admin2"] == FILTER_ADMIN2]
        else:
            df = df[df["admin2"].isin(FILTER_ADMIN2)]

    # Basic sanity
    df = df.dropna(subset=["lat", "lon"])
    if df.empty:
        print("No city rows after filtering.")
        return

    seen = set()         # global dedupe by placeId
    rows_open = []
    rows_not_open = []
    calls_used = 0

    for _, row in df.iterrows():
        if calls_used >= DAILY_CALL_CAP:
            print(f"[STOP] Daily call cap reached ({DAILY_CALL_CAP}).")
            break

        city = str(row["name"]).strip()
        country = str(row["country"]).strip()
        lat = float(row["lat"])
        lon = float(row["lon"])
        pop = row.get("population", None)

        radius_m = radius_from_population(pop)
        query = f"veterinarian in {city}, {country}"

        print(f"[{calls_used}] {city}, {country} — radius {radius_m/1000:.0f} km")
        # ----------------------
        # Page 1 (always fetch)
        # ----------------------
        data = search_text_essentials(query, lat, lon, radius_m, page_token=None)
        calls_used += 1

        page1_ids, _ = extract_and_route_places(
            data, city, country, radius_m, seen, rows_open, rows_not_open
        )
        page1_count = len(data.get("places", []))
        next_token = data.get("nextPageToken")

        # RULE 1: If page 1 has < 20 results (or no token), stop this city.
        if page1_count < 20 or not next_token or calls_used >= DAILY_CALL_CAP:
            time.sleep(0.1)
            continue

        # ----------------------
        # Page 2 (conditional)
        # ----------------------
        time.sleep(PAGE_SLEEP)  # token warm-up
        data = search_text_essentials(query, lat, lon, radius_m, page_token=next_token)
        calls_used += 1

        page2_ids, _ = extract_and_route_places(
            data, city, country, radius_m, seen, rows_open, rows_not_open
        )
        page2_count = len(data.get("places", []))
        next_token = data.get("nextPageToken")

        # RULE 2: If page 2 has < 20 results (or no token), stop (skip page 3).
        if page2_count < 20 or not next_token or calls_used >= DAILY_CALL_CAP:
            time.sleep(0.1)
            continue

        # RULE 3: If >80% of page 2 are duplicates of page 1, stop (skip page 3).
        if page2_count > 0:
            dups_with_p1 = sum(1 for pid in page2_ids if pid in set(page1_ids))
            dup_ratio = dups_with_p1 / page2_count
            if dup_ratio > 0.80:
                time.sleep(0.1)
                continue

        # ----------------------
        # Page 3 (last chance)
        # ----------------------
        time.sleep(PAGE_SLEEP)
        data = search_text_essentials(query, lat, lon, radius_m, page_token=next_token)
        calls_used += 1

        _page3_ids, _ = extract_and_route_places(
            data, city, country, radius_m, seen, rows_open, rows_not_open
        )
        # No further token after page 3
        time.sleep(0.1)
        
    # Save outputs (BusinessStatus intentionally omitted)
    pd.DataFrame(rows_open).drop_duplicates().to_csv(GM_OPEN_CSV, index=False)
    pd.DataFrame(rows_not_open).drop_duplicates().to_csv(GM_CLOSED_CSV, index=False)

    print(f"\n✅ Open (OPERATIONAL): {len(rows_open)")
    print(f"✅ Not open (non-OPERATIONAL): {len(rows_not_open)}")
    print(f"📊 Approx API calls used: {calls_used}")

if __name__ == "__main__":
    GooglePlace()
