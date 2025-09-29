import geopandas as gpd
import rasterio
import pandas as pd
import os

# === INPUTS ===
BASE_DIR = "C:/Users/myuan/Desktop/Data/Covariates"
CLINIC_GRID_DIR = os.path.join(BASE_DIR, "grids_with_clinics")
RASTER_CLIP_DIR = os.path.join(BASE_DIR, "clipped_rasters")
COVARIATE_GRID_DIR = os.path.join(BASE_DIR, "grids_with_covariates")
os.makedirs(COVARIATE_GRID_DIR, exist_ok=True)

grid_files = {
    "CHE": os.path.join(CLINIC_GRID_DIR, "CHE_grid_with_clinics_EPSG4326.gpkg"), 
    "AUT": os.path.join(CLINIC_GRID_DIR, "AUT_grid_with_clinics_EPSG4326.gpkg")
}
rasters = {
    "CHE":{
        "ca": os.path.join(RASTER_CLIP_DIR, "ca_v4_CHE.tif"),           
        "ch": os.path.join(RASTER_CLIP_DIR, "ch_v4_CHE.tif"),           
        "pg": os.path.join(RASTER_CLIP_DIR, "pg_v4_CHE.tif"),           
        "sh": os.path.join(RASTER_CLIP_DIR, "sh_v4_CHE.tif"),           
        "pop": os.path.join(RASTER_CLIP_DIR, "pop_density_2015_10k_CHE.tif"),
        "urban": os.path.join(RASTER_CLIP_DIR, "urban_rural_2018_10k_CHE.tif"),
        "wsf": os.path.join(RASTER_CLIP_DIR, "world_settlement_footprint_CHE.tif"),
        "gdp": os.path.join(RASTER_CLIP_DIR, "gdp_CHE.tif"),
        "acc": os.path.join(RASTER_CLIP_DIR, "acc_CHE.tif")
    },
    "AUT":{
        "ca": os.path.join(RASTER_CLIP_DIR, "ca_v4_AUT.tif"),           
        "ch": os.path.join(RASTER_CLIP_DIR, "ch_v4_AUT.tif"),           
        "pg": os.path.join(RASTER_CLIP_DIR, "pg_v4_AUT.tif"),           
        "sh": os.path.join(RASTER_CLIP_DIR, "sh_v4_AUT.tif"),           
        "pop": os.path.join(RASTER_CLIP_DIR, "pop_density_2015_10k_AUT.tif"),
        "urban": os.path.join(RASTER_CLIP_DIR, "urban_rural_2018_10k_AUT.tif"),
        "wsf": os.path.join(RASTER_CLIP_DIR, "world_settlement_footprint_AUT.tif"),
        "gdp": os.path.join(RASTER_CLIP_DIR, "gdp_AUT.tif"),
        "acc": os.path.join(RASTER_CLIP_DIR, "acc_AUT.tif")
    },
}

# === PROCESS EACH COUNTRY SEPARATELY ===
for code, grid_file in grid_files.items():
    print(f"\n🔹 Processing {code} ...")

    # Load grid
    grid = gpd.read_file(grid_file)

    # --- STEP 1: Settlement mask ---
    with rasterio.open(rasters[code]["wsf"]) as src:
        coords = [(geom.centroid.x, geom.centroid.y) for geom in grid.geometry]
        vals = [v[0] if v is not None else 0 for v in src.sample(coords)]
    grid["settlement"] = vals

    # Keep only settlement cells
    grid = grid[grid["settlement"] > 0].copy()
    print(f"  Remaining cells after settlement mask: {len(grid)}")

    # --- STEP 2: Recompute centroids after masking ---
    coords = [(geom.centroid.x, geom.centroid.y) for geom in grid.geometry]

    # --- STEP 3: Sample covariates ---
    for cov_name, raster_path in rasters[code].items():
        if cov_name == "wsf":  # skip, already used
            continue

        with rasterio.open(raster_path) as src:
            vals = [v[0] if v is not None else 0 for v in src.sample(coords)]
        grid[cov_name] = vals
    
    # --- STEP 3b: Enforce data types ---
    # Integer-like
    if "clinic_count" in grid.columns:
        grid["clinic_count"] = grid["clinic_count"].astype("int32")
    if "settlement" in grid.columns:
        grid["settlement"] = grid["settlement"].astype("int32")
    if "urban" in grid.columns:
        grid["urban"] = grid["urban"].astype("int32")

    # Continuous covariates
    for col in ["ca", "ch", "pg", "sh", "pop", "gdp", "acc"]:
        if col in grid.columns:
            grid[col] = grid[col].astype("float32").round(2)

    # --- STEP 4: Quick Quality Check ---
    print("\n📊 Quality Check for", code)
    for col in ["clinic_count", "settlement", "urban", "ca", "ch", "pg", "sh", "pop", "gdp", "acc"]:
        if col in grid.columns:
            vals = grid[col].dropna()
            print(f"  {col:12s} → dtype={grid[col].dtype}, "
                  f"min={vals.min():.2f}, max={vals.max():.2f}, mean={vals.mean():.2f}")

    # --- STEP 5: Save results ---
    df = pd.DataFrame(grid.drop(columns="geometry"))
    out_csv = os.path.join(COVARIATE_GRID_DIR, f"{code}_grid_with_covariates_EPSG4326.csv")
    df.to_csv(out_csv, index=False, float_format="%.2f")
    print(f"  ✅ Saved {out_csv}")
