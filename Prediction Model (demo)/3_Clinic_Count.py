import geopandas as gpd
import pandas as pd
import os

# === INPUT FILES ===
CSV_DIR = "C:/Users/myuan/Desktop/VetMap_Data"
GRID_DIR = "C:/Users/myuan/Desktop/Data/Covariates/grids"
CLINIC_GRID_DIR = "C:/Users/myuan/Desktop/Data/Covariates/grids_with_clinics"
os.makedirs(CLINIC_GRID_DIR, exist_ok=True)

clinic_files = {
    "CHE": os.path.join(CSV_DIR, "CHE/VP_team.csv"),
    "AUT": os.path.join(CSV_DIR, "AUT/VP_filtered_team.csv")
}

grid_files = {
    "CHE": {
        "EPSG4326": os.path.join(GRID_DIR, "CHE_grid_10km_EPSG4326.gpkg"),
        "EPSG3035": os.path.join(GRID_DIR, "CHE_grid_10km_EPSG3035.gpkg")
    },
    "AUT": {
        "EPSG4326": os.path.join(GRID_DIR, "AUT_grid_10km_EPSG4326.gpkg"),
        "EPSG3035": os.path.join(GRID_DIR, "AUT_grid_10km_EPSG3035.gpkg")
    }
}

# === LOOP OVER COUNTRIES ===
for code, clinic_file in clinic_files.items():
    print(f"\n🔹 Processing {code} ...")

    # Load clinics in EPSG:4326 (raw coordinates)
    clinics = pd.read_csv(clinic_file)
    gdf_clinics = gpd.GeoDataFrame(
        clinics,
        geometry=gpd.points_from_xy(clinics["Longitude"], clinics["Latitude"]),
        crs="EPSG:4326"
    )

    # --- Run twice: once for each CRS ---
    for crs, grid_path in grid_files[code].items():
        print(f"   ➡️ Matching clinics to {crs} grid ...")

        # Load grid
        grid = gpd.read_file(grid_path)

        # Ensure same CRS for join
        gdf = gdf_clinics.to_crs(grid.crs)

        # Spatial join (clinics → nearest grid cell)
        join = gpd.sjoin_nearest(
            gdf, grid,
            how="left",
            max_distance=None  # no cutoff; every clinic gets assigned
        )

        # Count clinics per cell
        counts = join.groupby("cell_id").size().reset_index(name="clinic_count")

        # Merge back into grid
        grid_with_counts = grid.merge(counts, on="cell_id", how="left").fillna({"clinic_count": 0})

        # Ensure integer counts
        grid_with_counts["clinic_count"] = grid_with_counts["clinic_count"].astype(int)

        # Sanity check
        total_clinics = len(gdf)
        total_counted = int(grid_with_counts["clinic_count"].sum())
        print(f"      Clinics in CSV: {total_clinics}")
        print(f"      Sum of clinic_count in grid: {total_counted}")

        if total_clinics != total_counted:
            raise ValueError(f"❌ Mismatch for {code}-{crs}: {total_clinics} vs {total_counted}")
        else:
            print(f"      ✅ Counts match exactly")

        # Save
        out_file = os.path.join(CLINIC_GRID_DIR, f"{code}_grid_with_clinics_{crs}.gpkg")
        if os.path.exists(out_file):
            os.remove(out_file)
        grid_with_counts.to_file(out_file, driver="GPKG")
        print(f"      ✅ Saved {out_file}")
