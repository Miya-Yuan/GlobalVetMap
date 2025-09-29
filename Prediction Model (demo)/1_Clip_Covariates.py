import geopandas as gpd
import rioxarray
import rasterio
import numpy as np
import os

# === INPUTS ===
BASE_DIR = "C:/Users/myuan/Desktop/Data/Covariates"
SHP_DIR = "C:/Users/myuan/Desktop/Data/shapefile/country"
RASTER_CLIP_DIR = os.path.join(BASE_DIR, "clipped_rasters")
os.makedirs(RASTER_CLIP_DIR, exist_ok=True)

shapefiles = {
    "CHE": os.path.join(SHP_DIR, "CHE/CHE1_nr.shp"),  # Switzerland shapefile
    "AUT": os.path.join(SHP_DIR, "AUT/AUT1_nr.shp")   # Austria shapefile
}

rasters = [
    os.path.join(BASE_DIR, "ca_v4.tif"),
    os.path.join(BASE_DIR, "ch_v4.tif"),
    os.path.join(BASE_DIR, "pg_v4.tif"),
    os.path.join(BASE_DIR, "sh_v4.tif"),
    os.path.join(BASE_DIR, "pop_density_2015_10k.tif"),
    os.path.join(BASE_DIR, "urban_rural_2018_10k.tif"),
    os.path.join(BASE_DIR, "world_settlement_footprint.tif"),
    os.path.join(BASE_DIR, "gdp.grd"),   # GDP
    os.path.join(BASE_DIR, "acc.grd")    # Travel time
]

# === LOOP OVER COUNTRIES & RASTERS ===
for country_name, shp_path in shapefiles.items():
    # Load shapefile and merge polygons
    country = gpd.read_file(shp_path)
    country_union = country.union_all()
    country = gpd.GeoDataFrame(geometry=[country_union], crs=country.crs)

    for raster_path in rasters:
        raster_name = os.path.splitext(os.path.basename(raster_path))[0]
        out_raster = os.path.join(RASTER_CLIP_DIR, f"{raster_name}_{country_name}.tif")

        # Open raster
        da = rioxarray.open_rasterio(raster_path, masked=True).squeeze()

        # Mask out 0 values as nodata
        da = da.where(da != 0)

        # Reproject boundary to match raster CRS
        if country.crs != da.rio.crs:
            country = country.to_crs(da.rio.crs)

        # Clip raster
        clipped = da.rio.clip(country.geometry, country.crs, drop=True)

        # Save as GeoTIFF
        clipped.rio.to_raster(out_raster)

        print(f"\n✅ Saved {out_raster}")

        # === INSPECT PROPERTIES ===
        with rasterio.open(out_raster) as src:
            res = src.res
            arr = src.read(1, masked=True)
            vmin, vmax, mean = float(arr.min()), float(arr.max()), float(arr.mean())

            print(f"--- {raster_name} ({country_name}) ---")
            print(f"CRS: {src.crs}")
            print(f"Resolution: {res}")
            print(f"Value range: min={vmin:.3f}, max={vmax:.3f}, mean={mean:.3f}")

            # Heuristic checks
            if abs(res[0] - 0.08333) < 0.001 and abs(res[1] - 0.08333) < 0.001:
                print("✔ Pixel size ~10 km (0.08333°) → matches paper resolution.")
            else:
                print("⚠ Pixel size not 0.08333° → may need resampling.")

            if vmax < 10:
                print("✔ Values likely log10-transformed (range below 10).")
            else:
                print("⚠ Values look raw (not log10).")
