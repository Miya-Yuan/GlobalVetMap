import os
import time
import requests
import socket
import pandas as pd
import numpy as np
import unicodedata
import tldextract
from urllib.parse import urlparse
import geopandas as gpd
from rapidfuzz import fuzz, process
from rapidfuzz.process import cdist
from shapely.geometry import Point
from sklearn.cluster import DBSCAN

# === CONFIGURATION ===
BASE_DIR = "C:/Users/myuan/Desktop/VetMap_Data"
COUNTRY_DIR = "CHE"
KEYWORD_DIR = "C:/Users/myuan/Desktop/VetMap/Keyword"
SHP_DIR = "C:/Users/myuan/Desktop/Data/shapefile/country"
# --- INPUT FILES ---
website_csv = os.path.join(BASE_DIR, "merged_output.csv")
google_csv = os.path.join(BASE_DIR, COUNTRY_DIR, f"GM/{COUNTRY_DIR}_VP_GM.csv")
closed_csv = os.path.join(BASE_DIR, COUNTRY_DIR, f"GM/{COUNTRY_DIR}_VP_GM_dedup.csv")
bad_words_csv = os.path.join(KEYWORD_DIR, "nonclinic_keywords.csv")
shapefile_path = os.path.join(SHP_DIR, COUNTRY_DIR, f"{COUNTRY_DIR}1_nr.shp")
# --- OUTPUT FILES ---
text_matched_csv = os.path.join(BASE_DIR, COUNTRY_DIR, "VP_text_matched.csv")
cleaned_google_csv = os.path.join(BASE_DIR, COUNTRY_DIR, f"GM/{COUNTRY_DIR}_VP_GM_cleaned.csv")
cleaned_website_csv = os.path.join(BASE_DIR, COUNTRY_DIR, "merged_output_cleaned.csv")
GEOCODING_OUTPUT_PATH = os.path.join(BASE_DIR, COUNTRY_DIR, "VP_geocoded.csv")
DEDUPED_OUTPUT_PATH = os.path.join(BASE_DIR, COUNTRY_DIR, "VP_cleaned.csv")
NON_CLINIC_PATH = os.path.join(BASE_DIR, COUNTRY_DIR, f"{COUNTRY_DIR}_Not_Clinic.csv")
DUPLICATES_PATH = os.path.join(BASE_DIR, COUNTRY_DIR, f"{COUNTRY_DIR}_Duplicated_Rows.csv")
# --- VARIABLES ---
load_dotenv()
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY_1")
USER_AGENT = os.getenv("OSM_USER_AGENT")
SAVE_INTERVAL = 10
DISTANCE_THRESHOLD_METERS = 50
EARTH_RADIUS_METERS = 6371000
similarity_threshold = 85
name_col = "Name"
address_col = "Address"
nonclinic_keywords_df = pd.read_csv(bad_words_csv, encoding='utf-8')
nonclinic_keywords = set(k.lower().strip() for k in nonclinic_keywords_df['Keyword'].dropna())

non_html_extensions = (
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp",
    ".pdf", ".doc", ".docx", ".xls", ".xlsx",
    ".zip", ".rar", ".mp4", ".mp3", ".avi", ".mov"
)
# ===================================================================
# === CLEAN GOOGLE MAP DATA AS BENCHMARK ===
# ===================================================================

def normalize_text(text):
    return unicodedata.normalize("NFKD", str(text)).encode("ascii", "ignore").decode().lower().strip()

def keyword_match(name, keywords, threshold=85):
    name_norm = normalize_text(name)
    return any(
        (fuzz.partial_ratio(keyword, name_norm) >= threshold and keyword in name_norm)
        for keyword in (normalize_text(k) for k in keywords)
    )
# ===================================================================
# === DISTANCE DEDUPLICATION ===
# ===================================================================

# --- DEDUPLICATE USING DBSCAN CLUSTERING ---
def merge_cluster_rows(group):
    merged = {}

    # Pick the Google base row if it exists
    if "Source" in group.columns:
        google_rows = group[group["Source"].astype(str).str.contains("google", case=False, na=False)]
        google_base = google_rows.iloc[0] if not google_rows.empty else None
    else:
        google_base = None

    for col in group.columns:
        if col in ["cluster"]:  # skip helper col
            continue

        non_nulls = group[col].dropna().astype(str).unique()
        # --- Special handling for Specialization ---
        if col == "Specialization":
            if google_base is not None and pd.notna(google_base.get(col)):
                google_val = str(google_base[col]).strip()
                # concatenate Google + other values, remove duplicates
                all_vals = set(non_nulls.tolist() + [google_val])
                merged[col] = ", ".join(sorted(all_vals))
            elif len(non_nulls) > 0:
                merged[col] = ", ".join(non_nulls)
            else:
                merged[col] = None
            continue
        # --- Default for all other columns ---
        if google_base is not None and pd.notna(google_base.get(col)) and str(google_base[col]).strip().lower() not in ["nan", "none", "null", ""]:
            merged[col] = google_base[col]
        else:
            merged[col] = non_nulls[0] if len(non_nulls) > 0 else None

    return pd.Series(merged)

def deduplicate_with_dbscan(df):
    coords_rad = np.radians(df[['Latitude', 'Longitude']].values)
    db = DBSCAN(
        eps=DISTANCE_THRESHOLD_METERS / EARTH_RADIUS_METERS,
        min_samples=1,
        metric='haversine'
    ).fit(coords_rad)

    df['cluster'] = db.labels_
    # --- Save duplicates ---
    cluster_counts = df['cluster'].value_counts()
    duplicate_clusters = cluster_counts[cluster_counts > 1].index
    duplicates_df = df[df['cluster'].isin(duplicate_clusters)].copy()
    if not duplicates_df.empty:
        duplicates_df.to_csv(DUPLICATES_PATH, index=False)
    # --- Merge clusters with Google-first preference ---
    deduped_df = (
        df.groupby('cluster', group_keys=False, sort=False)
          .apply(merge_cluster_rows, include_groups=False)
          .reset_index(drop=True)
    )

    # Drop helper columns
    deduped_df.drop(columns=['cluster', 'Source'], inplace=True, errors='ignore')
    return deduped_df

def preprocess_google_data():
    bad_words = nonclinic_keywords

    google_df = pd.read_csv(google_csv, index_col=False)
    google_df["Source"] = "Google"
    google_df = google_df.dropna(subset=[name_col, address_col, 'Latitude', 'Longitude'], how="any")
    # --- Separate non-clinic rows ---
    nonclinic_google = google_df[google_df[name_col].apply(lambda x: keyword_match(x, bad_words))]
    clinic_google = google_df[~google_df[name_col].apply(lambda x: keyword_match(x, bad_words))]
    # Save non-clinic rows
    if not nonclinic_google.empty:
        nonclinic_google.to_csv(NON_CLINIC_PATH, mode='w', index=False)  # overwrite on first write
    deduped_google_df = deduplicate_with_dbscan(clinic_google)
    return deduped_google_df

# ===================================================================
# === TEXT MATCH DEDUPLICATION ===
# ===================================================================
# Function to validate and normalize website URLs
def clean_website_url(url):
    if not isinstance(url, str):
        return None
    url = url.strip().lower()
    if url in ("", "none", "null", "nan"):
        return None
    if not (url.startswith("http://") or url.startswith("https://")):
        return None

    try:
        parsed = urlparse(url)
        hostname = parsed.hostname
        if not hostname:
            return None
        socket.gethostbyname(hostname)  # DNS resolution

        # TLD filtering
        ext = tldextract.extract(hostname)
        suffix = ext.suffix
        if suffix != "ch" and len(suffix) == 2:
            return None  # Foreign ccTLD (non-Swiss)

        # If it's a downloadable file, return root homepage
        if parsed.path.endswith(non_html_extensions):
            return f"{parsed.scheme}://{hostname}/"

        return url
    except Exception:
        return None

# Main function
def text_match_dedup(deduped_google_df):
    website_df = pd.read_csv(website_csv, index_col=False)
    bad_words = nonclinic_keywords

    website_df = website_df.dropna(subset=[name_col, address_col], how="all")
    nonclinic_web = website_df[website_df[name_col].apply(lambda x: keyword_match(x, bad_words))]
    website_df = website_df[~website_df[name_col].apply(lambda x: keyword_match(x, bad_words))]
    
    # --- Clean Website URLs without dropping rows ---
    if 'Website' in website_df.columns:
        pre_clean_valid_web = website_df['Website'].notna().sum()
        website_df['Website'] = website_df['Website'].apply(clean_website_url)
        website_df.to_csv(cleaned_website_csv, index=False)
        post_clean_valid_web = website_df['Website'].notna().sum()
        print(f"Website DF: Website values before cleaning = {pre_clean_valid_web}, after cleaning = {post_clean_valid_web}")

    if 'Website' in deduped_google_df.columns:
        pre_clean_valid_google = deduped_google_df['Website'].notna().sum()
        deduped_google_df['Website'] = deduped_google_df['Website'].apply(clean_website_url)
        deduped_google_df.to_csv(cleaned_google_csv, index=False)
        post_clean_valid_google = deduped_google_df['Website'].notna().sum()
        print(f"Google DF: Website values before cleaning = {pre_clean_valid_google}, after cleaning = {post_clean_valid_google}")

    # --- Reset index to ensure consistent row indexing for drop ---
    website_df = website_df.reset_index(drop=True)
    deduped_google_df = deduped_google_df.reset_index(drop=True)

    # --- Combine name + address for fuzzy matching ---
    website_df["combined"] = website_df[name_col].astype(str) + " " + website_df[address_col].astype(str)
    deduped_google_df["combined"] = deduped_google_df[name_col].astype(str) + " " + deduped_google_df[address_col].astype(str)

    # --- Fuzzy match using token sort ratio ---
    scores = cdist(website_df["combined"].tolist(), deduped_google_df["combined"].tolist(), scorer=fuzz.token_sort_ratio)
    matched_google_indices = set()
    matched_website_indices = set()
    merged_rows = []

    for i, (web_idx, row_web) in enumerate(website_df.iterrows()):
        best_idx = np.argmax(scores[i])
        best_score = scores[i][best_idx]
        if best_score >= similarity_threshold and best_idx not in matched_google_indices:
            row_google = deduped_google_df.iloc[best_idx].copy()
            for col in ['Specialization', 'Website']:
                if col in website_df.columns:
                    if col not in row_google or pd.isna(row_google[col]):
                        row_google[col] = row_web.get(col, None)
            merged_rows.append(row_google)
            matched_google_indices.add(best_idx)
            matched_website_indices.add(web_idx)

    unmatched_website_df = website_df.drop(index=matched_website_indices)
    unmatched_google_df = deduped_google_df.drop(index=matched_google_indices)
    for col in website_df.columns:
        if col not in unmatched_google_df.columns:
            unmatched_google_df[col] = None

    matched_df = pd.DataFrame(merged_rows)
    text_matched_df = pd.concat([matched_df, unmatched_website_df, unmatched_google_df], ignore_index=True)
    text_matched_df.drop_duplicates(subset=[name_col, address_col], inplace=True)

    # --- Remove known closed clinics ---
    if os.path.exists(closed_csv):
        closed_df = pd.read_csv(closed_csv, index_col=False)
        closed_df.dropna(subset=[name_col, address_col], inplace=True)
        closed_df["combined"] = closed_df[name_col].astype(str) + " " + closed_df[address_col].astype(str)
        closed_combined = closed_df["combined"].tolist()

        text_matched_df["combined"] = text_matched_df[name_col].astype(str) + " " + text_matched_df[address_col].astype(str)
        closed_addresses = closed_df[address_col].astype(str).tolist()
        indices_to_remove = set()

        for idx, row in text_matched_df.iterrows():
            name = str(row.get(name_col, ""))
            address = str(row.get(address_col, ""))
            combined = f"{name} {address}"

            match = process.extractOne(combined, closed_combined, scorer=fuzz.token_sort_ratio)
            if match and match[1] >= 85:
                indices_to_remove.add(idx)
                continue

            address_match = process.extractOne(address, closed_addresses, scorer=fuzz.token_sort_ratio)
            if address_match and address_match[1] >= 90:
                indices_to_remove.add(idx)

        text_matched_df.drop(index=indices_to_remove, inplace=True)
        text_matched_df.drop(columns=["combined"], inplace=True, errors="ignore")
    else:
        print(f"⚠️ Closed clinics file not found: {closed_csv}. Skipping closed-clinic removal.")

    text_matched_df.to_csv(text_matched_csv, index=False)
    return text_matched_df

# ===================================================================
# === GEOCODING ===
# ===================================================================

# === Geocoding Functions ===
def build_query(row):
    if pd.isna(row.get('Address')):
        return None
    parts = [str(row['Address'])]
    if 'PLZ' in row and pd.notna(row['PLZ']):
        parts.append(str(row['PLZ']))
    if 'ORT' in row and pd.notna(row['ORT']):
        parts.append(str(row['ORT']))
    return ', '.join(parts)

def geocode_osm(query):
    url = "https://nominatim.openstreetmap.org/search"
    params = {'q': query, 'format': 'json', 'limit': 1}
    headers = {'User-Agent': USER_AGENT}
    try:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        if data:
            return float(data[0]['lat']), float(data[0]['lon'])
    except Exception as e:
        print(f"❌ OSM error for '{query}': {e}")
    return None, None

def geocode_google(query, api_key):
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {'address': query, 'key': api_key}
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        result = response.json()
        if result['status'] == 'OK':
            location = result['results'][0]['geometry']['location']
            return location['lat'], location['lng']
    except Exception as e:
        print(f"❌ Google error for '{query}': {e}")
    return None, None

# === Main geocoding loop ===
def geocode_dataframe(df):
    final_path = GEOCODING_OUTPUT_PATH
    partial_path = GEOCODING_OUTPUT_PATH.replace(".csv", "_partial.csv")

    # === Resume logic ===
    if os.path.exists(partial_path):
        print(f"🔄 Resuming from partial save: {partial_path}")
        df = pd.read_csv(partial_path)
    elif os.path.exists(final_path):
        print(f"✅ Final geocoded file already exists: {final_path}")
        return pd.read_csv(final_path)

    if 'Latitude' not in df.columns:
        df['Latitude'] = None
    if 'Longitude' not in df.columns:
        df['Longitude'] = None

     # --- Identify rows that need geocoding ---
    rows_to_geocode = df[df['Latitude'].isna() | df['Longitude'].isna()]
    total_to_geocode = len(rows_to_geocode)

    geocoded_count = 0

    for idx, row in rows_to_geocode.iterrows():
        query = build_query(row)
        if not query:
            continue

        lat, lon = geocode_osm(query)
        # Fallback to Google if OSM failed
        if (lat is None or lon is None) and GOOGLE_API_KEY:
            lat, lon = geocode_google(query, GOOGLE_API_KEY)
        
        if lat is not None and lon is not None:
            df.at[idx, 'Latitude'] = lat
            df.at[idx, 'Longitude'] = lon

        geocoded_count += 1
        time.sleep(1)

        if geocoded_count % SAVE_INTERVAL == 0:
            percent = (geocoded_count / total_to_geocode) * 100
            df.to_csv(partial_path, index=False)
            print(f"💾 Progress saved ({geocoded_count}/{total_to_geocode} rows, {percent:.1f}%) → {partial_path}")

    # Final save
    df.to_csv(final_path, index=False)
    print(f"✅ Final geocoded file saved: {final_path}")

    if os.path.exists(partial_path):
        os.remove(partial_path)
        print(f"🗑️ Removed partial file: {partial_path}")

    return df

# --- FILTER BY COUNTRY BORDER ---
def filter_by_country_border(df, shapefile_path):
    print("Filtering practices within country border...")
    country_shape = gpd.read_file(shapefile_path)
    if country_shape.crs != 'EPSG:4326':
        country_shape = country_shape.to_crs('EPSG:4326')

    gdf = gpd.GeoDataFrame(df, geometry=[Point(xy) for xy in zip(df['Longitude'], df['Latitude'])], crs='EPSG:4326')
    country_union = country_shape.union_all()
    filtered_gdf = gdf[gdf.geometry.within(country_union)]
    return pd.DataFrame(filtered_gdf.drop(columns='geometry'))

# --- MAIN DEDUPLICATION FUNCTION ---
def vp_dedup():
    print("Data Cleaning Start:")
    deduped_google_df = preprocess_google_data()
    df_text_matched = text_match_dedup(deduped_google_df)
    df_geocoded = geocode_dataframe(df_text_matched)
    df_geo_filtered = filter_by_country_border(df_geocoded, shapefile_path)
    geo_deduped_df = deduplicate_with_dbscan(df_geo_filtered)
    # --- Final cleanup: drop helper columns ---
    helper_cols = ["cluster", "Source", "combined"]
    geo_deduped_df = geo_deduped_df.drop(columns=[c for c in helper_cols if c in geo_deduped_df.columns])

    geo_deduped_df.to_csv(DEDUPED_OUTPUT_PATH, index=False)
    print("✅ File after distance deduplication saved.")
# --- MAIN EXECUTION ---
if __name__ == "__main__":
    vp_dedup()





