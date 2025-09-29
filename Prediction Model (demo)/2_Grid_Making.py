import geopandas as gpd
import pandas as pd
from shapely.geometry import box
import numpy as np
import os

# === INPUTS ===
SHP_DIR = "C:/Users/myuan/Desktop/Data/shapefile/country"
shapefiles = {
    "CHE": os.path.join(SHP_DIR, "CHE/CHE1_nr.shp"),
    "AUT": os.path.join(SHP_DIR, "AUT/AUT1_nr.shp")
}

GRID_DIR = "C:/Users/myuan/Desktop/Data/Covariates/grids"
os.makedirs(GRID_DIR, exist_ok=True)

# === FUNCTION TO BUILD GRID ===
def make_grid(boundary_gdf, country_code, GRID_DIR):
    # Reproject to equal-area CRS (EPSG:3035)
    boundary_proj = boundary_gdf.to_crs(epsg=3035)

    # Build 10 km grid
    minx, miny, maxx, maxy = boundary_proj.total_bounds
    grid_size = 10000  # 10 km in meters

    cols = np.arange(minx, maxx, grid_size)
    rows = np.arange(miny, maxy, grid_size)

    polygons = [box(x, y, x + grid_size, y + grid_size) for x in cols for y in rows]
    grid = gpd.GeoDataFrame({"geometry": polygons}, crs=boundary_proj.crs)

    # Clip to boundary
    grid_clipped = gpd.overlay(grid, boundary_proj, how="intersection")

    # Add unique ID
    grid_clipped["cell_id"] = [f"{country_code}_{i+1}" for i in range(len(grid_clipped))]

    # Save EPSG:3035 version
    path_3035 = os.path.join(GRID_DIR, f"{country_code}_grid_10km_EPSG3035.gpkg")
    grid_clipped.to_file(path_3035, driver="GPKG")

    # Save EPSG:4326 version
    grid_4326 = grid_clipped.to_crs(epsg=4326)
    path_4326 = os.path.join(GRID_DIR, f"{country_code}_grid_10km_EPSG4326.gpkg")
    grid_4326.to_file(path_4326, driver="GPKG")

    print(f"✅ {country_code}: saved {len(grid_clipped)} grid cells")
    return grid_clipped

# === LOOP OVER COUNTRIES ===
country_grids = []
for code, shp_path in shapefiles.items():
    country = gpd.read_file(shp_path)
    country_union = country.union_all()
    boundary_gdf = gpd.GeoDataFrame(geometry=[country_union], crs=country.crs)

    grid_clipped = make_grid(boundary_gdf, code, GRID_DIR)
    country_grids.append(grid_clipped)
