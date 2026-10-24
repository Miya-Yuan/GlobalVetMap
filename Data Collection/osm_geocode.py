
import pandas as pd
import pycountry
import sqlite3
from pathlib import Path
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

# ============================================================
# 1. FILE PATHS
# ============================================================

root_folder = Path(r"C:\Users\myuan\Desktop\Data_VetMap")

input_file = root_folder / "vets_web_combined.csv"
output_file = root_folder / "vets_web_geocoded.csv"

# Persistent cache prevents repeated API calls for the same address.
cache_file = root_folder / "geocoding_cache.sqlite"

# ============================================================
# 2. INITIALIZE NOMINATIM
# ============================================================

# Nominatim requires an identifying User-Agent.
geolocator = Nominatim(
    user_agent="vetmap_research_geocoder/1.0 (mingyang.yuan@uzh.ch)",
    timeout=15
)

# Keep requests below the public API limit of 1 request/second.
# This should only be used for small, occasional jobs.
geocode = RateLimiter(
    geolocator.geocode,
    min_delay_seconds=1.1,
    max_retries=0,
    swallow_exceptions=False
)

# ============================================================
# 3. READ INPUT DATA
# ============================================================

df = pd.read_csv(
    input_file,
    dtype=str,
    encoding="utf-8-sig",
    keep_default_na=False
)

df.columns = df.columns.str.strip()

# Confirm required columns exist.
required_columns = {"Address", "Country"}

if not required_columns.issubset(df.columns):
    raise ValueError(
        "Input CSV must contain Address and Country columns."
    )

print(f"Total records: {len(df)}")

# Prepare output columns while preserving all original columns.
df["Latitude"] = ""
df["Longitude"] = ""
df["Geocoding_Status"] = ""

# ============================================================
# 4. CREATE PERSISTENT GEOCODING CACHE
# ============================================================

conn = sqlite3.connect(str(cache_file))

conn.execute("""
    CREATE TABLE IF NOT EXISTS geocoding_cache (
        country TEXT NOT NULL,
        address TEXT NOT NULL,
        latitude TEXT,
        longitude TEXT,
        status TEXT NOT NULL,
        PRIMARY KEY (country, address)
    )
""")

conn.commit()

# ============================================================
# 5. CONVERT ISO3 TO ISO2
# ============================================================

# Nominatim uses ISO2 codes for its country filter.
# Example: AUT -> AT, DEU -> DEU's ISO2 equivalent DE,
# CHE -> CH.

def iso3_to_iso2(country_code):
    country_code = str(country_code).strip().upper()

    try:
        country = pycountry.countries.get(alpha_3=country_code)

        if country is not None:
            return country.alpha_2.lower()

    except (LookupError, AttributeError):
        pass

    return None


# ============================================================
# 6. GEOCODE AN ADDRESS
# ============================================================

def geocode_address(address, country):
    """
    Geocode a single address using Nominatim.

    Address: Street/city/postcode information.
    Country: ISO3 code from input CSV.

    Returns latitude, longitude and geocoding status.
    """

    address = str(address).strip()
    country = str(country).strip().upper()

    # Skip records without addresses.
    if not address:
        return "", "", "missing_address"

    # Convert ISO3 country code to ISO2.
    country_iso2 = iso3_to_iso2(country)

    if country_iso2 is None:
        return "", "", "invalid_country"

    # Look for an existing cached result.
    cached = conn.execute(
        """
        SELECT latitude, longitude, status
        FROM geocoding_cache
        WHERE country = ? AND address = ?
        """,
        (country, address)
    ).fetchone()

    if cached is not None:
        return cached

    # Request geocoding from OpenStreetMap Nominatim.
    try:
        location = geocode(
            query=address,
            country_codes=country_iso2,
            exactly_one=True,
            language="en"
        )

        if location is not None:
            latitude = str(location.latitude)
            longitude = str(location.longitude)
            status = "matched"

        else:
            latitude = ""
            longitude = ""
            status = "not_found"

        # Cache both matches and genuine no-match results.
        conn.execute(
            """
            INSERT OR REPLACE INTO geocoding_cache
            (country, address, latitude, longitude, status)
            VALUES (?, ?, ?, ?, ?)
            """,
            (country, address, latitude, longitude, status)
        )

        conn.commit()

        return latitude, longitude, status

    except Exception as e:
        print(f"Geocoding error: {address} ({country}): {e}")

        # Do not cache API errors so they can be retried later.
        return "", "", "error"


# ============================================================
# 7. GEOCODE ALL RECORDS
# ============================================================

try:
    for index, row in df.iterrows():

        address = row["Address"]
        country = row["Country"]

        latitude, longitude, status = geocode_address(
            address,
            country
        )

        df.at[index, "Latitude"] = latitude
        df.at[index, "Longitude"] = longitude
        df.at[index, "Geocoding_Status"] = status

        # Progress information every 100 records.
        if (index + 1) % 100 == 0:
            print(
                f"Processed {index + 1}/{len(df)} records"
            )

            # Save intermediate results to protect progress.
            df.to_csv(
                output_file,
                index=False,
                encoding="utf-8-sig"
            )

except KeyboardInterrupt:
    print("\nInterrupted. Saving results obtained so far.")

finally:
    # Save the final or partially completed dataset.
    df.to_csv(
        output_file,
        index=False,
        encoding="utf-8-sig"
    )

    conn.close()

# ============================================================
# 8. SUMMARY
# ============================================================

print("\nGeocoding completed or interrupted.")
print(f"Total records: {len(df)}")

print("\nGeocoding status:")
print(df["Geocoding_Status"].value_counts(dropna=False))

print(f"\nOutput saved to: {output_file}")
print(f"Cache saved to: {cache_file}")
print("\nGeocoding data © OpenStreetMap contributors (ODbL).")

# ============================================================
# 9. SUMMARY OF FINAL OUTPUT FILE
# ============================================================

# Read the final saved CSV file
final_df = pd.read_csv(
    output_file,
    dtype=str,
    encoding="utf-8-sig",
    keep_default_na=False
)

print("\n" + "=" * 70)
print("FINAL DATASET SUMMARY")
print("=" * 70)

print(f"Total rows: {len(final_df):,}")
print(f"Total columns: {len(final_df.columns)}")
print(f"Total countries: {final_df['Country'].nunique()}")

# Generate column-level summary
summary_data = []

for col in final_df.columns:

    # Strip whitespace and identify empty values
    values = final_df[col].astype(str).str.strip()
    missing_mask = values.eq("")

    total = len(values)
    missing = missing_mask.sum()
    non_missing = total - missing

    # Count distinct non-empty values
    unique = values[~missing_mask].nunique()

    # Calculate missing percentage
    missing_pct = (
        (missing / total) * 100
        if total > 0 else 0
    )

    summary_data.append({
        "Column": col,
        "Data_Type": str(final_df[col].dtype),
        "Total_Rows": total,
        "Non_Empty": non_missing,
        "Missing": missing,
        "Missing_Percent": round(missing_pct, 2),
        "Unique_Values": unique
    })

# Create summary dataframe
summary_df = pd.DataFrame(summary_data)

# Display all columns in the console
print("\nCOLUMN-LEVEL SUMMARY")
print(summary_df.to_string(index=False))

# Save the summary as a separate CSV file
summary_file = root_folder / "vets_web_column_summary.csv"

summary_df.to_csv(
    summary_file,
    index=False,
    encoding="utf-8-sig"
)

print(f"\nColumn summary saved to: {summary_file}")

# ============================================================
# 10. GEOCODING SUCCESS SUMMARY
# ============================================================

print("\n" + "=" * 70)
print("GEOCODING QUALITY SUMMARY")
print("=" * 70)

status_counts = final_df["Geocoding_Status"].value_counts()

print("\nGeocoding status:")
print(status_counts.to_string())

total_records = len(final_df)

matched_records = (
    final_df["Geocoding_Status"] == "matched"
).sum()

success_rate = (
    matched_records / total_records * 100
    if total_records > 0 else 0
)

print(f"\nSuccessfully geocoded: {matched_records:,}")
print(f"Total records: {total_records:,}")
print(f"Geocoding success rate: {success_rate:.2f}%")

# ============================================================
# 11. COUNTRY-LEVEL GEOCODING SUMMARY
# ============================================================

country_summary = (
    final_df.groupby("Country")
    .agg(
        Total_Records=("Country", "size"),
        Geocoded=(
            "Geocoding_Status",
            lambda x: (x == "matched").sum()
        )
    )
    .reset_index()
)

country_summary["Not_Geocoded"] = (
    country_summary["Total_Records"]
    - country_summary["Geocoded"]
)

country_summary["Success_Rate_Percent"] = (
    country_summary["Geocoded"]
    / country_summary["Total_Records"] * 100
).round(2)

print("\nCOUNTRY-LEVEL SUMMARY")
print(country_summary.to_string(index=False))

# Save country-level summary
country_summary_file = (
    root_folder / "vets_web_country_summary.csv"
)

country_summary.to_csv(
    country_summary_file,
    index=False,
    encoding="utf-8-sig"
)

print(f"\nCountry summary saved to: {country_summary_file}")