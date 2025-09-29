import geopandas as gpd
import matplotlib.pyplot as plt
from libpysal.weights import Queen
from esda import Moran_Local
import rasterio
from rasterio.features import rasterize
from rasterio.transform import from_origin
import numpy as np
import pandas as pd
import os

BASE_DIR = "C:/Users/myuan/Desktop/Data/Covariates"
CLINIC_GRID_DIR = "grids_with_clinics"
CHE_COUNT_PATH = os.path.join(BASE_DIR, CLINIC_GRID_DIR, "CHE_grid_with_clinics_EPSG3035.gpkg")
CHE_LISA_PATH = os.path.join(BASE_DIR, CLINIC_GRID_DIR, "CHE_grid_LISA.gpkg")
CHE_LMI_PATH = os.path.join(BASE_DIR, CLINIC_GRID_DIR, "CHE_localMoranI.tif")
CHE_LMIP_PATH = os.path.join(BASE_DIR, CLINIC_GRID_DIR, "CHE_localMoranPval.tif")
CHE_LMICL_PATH = os.path.join(BASE_DIR, CLINIC_GRID_DIR, "CHE_localMoranCluster.tif")
CHE_LISAcsv_PATH = os.path.join(BASE_DIR, CLINIC_GRID_DIR, "CHE_LISA_summary.csv")
CHE_LISApng_PATH = os.path.join(BASE_DIR, CLINIC_GRID_DIR, "CHE_LISA_clusters.png")
# -------------------------------------------------------------------
# 1. Load Switzerland grid with clinic counts
# -------------------------------------------------------------------
# Use EPSG:3035 version (projected, uniform grid in meters)
gdf = gpd.read_file(CHE_COUNT_PATH)

# Make sure vet counts are numeric
if "clinic_count" not in gdf.columns:
    raise ValueError("Column 'clinic_count' not found in dataset.")
gdf["clinic_count"] = gdf["clinic_count"].fillna(0).astype(int)

# -------------------------------------------------------------------
# 2. Build Queen contiguity weights
# -------------------------------------------------------------------
w = Queen.from_dataframe(gdf)
w.transform = "R"  # row-standardized

# -------------------------------------------------------------------
# 3. Compute Local Moran’s I
# -------------------------------------------------------------------
lisa = Moran_Local(gdf["clinic_count"], w)

# Attach results
gdf["local_I"] = lisa.Is
gdf["p_value"] = lisa.p_sim
gdf["q"] = lisa.q

# Classify cluster types
def cluster_label(row):
    if row["p_value"] > 0.05:
        return "Not significant"
    elif row["q"] == 1:
        return "High-High"
    elif row["q"] == 2:
        return "Low-Low"
    elif row["q"] == 3:
        return "High-Low"
    elif row["q"] == 4:
        return "Low-High"
gdf["cluster"] = gdf.apply(cluster_label, axis=1)

# Integer mapping for clusters
cluster_map = {
    "Not significant": 0,
    "High-High": 1,
    "Low-Low": 2,
    "High-Low": 3,
    "Low-High": 4
}
gdf["cluster_int"] = gdf["cluster"].map(cluster_map)

# -------------------------------------------------------------------
# 4. Save results as GeoPackage
# -------------------------------------------------------------------
gdf.to_file(CHE_LISA_PATH, driver="GPKG")

# -------------------------------------------------------------------
# 5. Export Local Moran’s I as GeoTIFF rasters
# -------------------------------------------------------------------
# Function to rasterize an attribute
def rasterize_attribute(gdf, attribute, out_file, cell_size=10000, add_colortable=False):
    """
    Rasterize a GeoDataFrame attribute to GeoTIFF.
    Handles floats with NaN and integers with -9999 as NoData automatically.
    """
    bounds = gdf.total_bounds  # [xmin, ymin, xmax, ymax]
    xmin, ymin, xmax, ymax = bounds
    width = int((xmax - xmin) / cell_size)
    height = int((ymax - ymin) / cell_size)
    transform = from_origin(xmin, ymax, cell_size, cell_size)

    shapes = ((geom, value) for geom, value in zip(gdf.geometry, gdf[attribute]))
    # Decide dtype
    if np.issubdtype(gdf[attribute].dtype, np.floating):
        dtype = "float32"
        nodata_val = np.nan
    else:
        dtype = "int32"
        nodata_val = -9999

    with rasterio.open(
        out_file,
        "w",
        driver="GTiff",
        height=height,
        width=width,
        count=1,
        dtype=dtype,
        crs=gdf.crs,
        transform=transform,
        nodata=nodata_val if dtype == "int32" else None
    ) as dst:
        burned = rasterize(
            shapes,
            out_shape=(height, width),
            transform=transform,
            fill=nodata_val,
            dtype=dtype
        )
        dst.write(burned, 1)

        # Add color table if requested (for cluster raster)
        if add_colortable and attribute == "cluster_int":
            colormap = {
                0: (200, 200, 200, 255),  # Not significant (gray)
                1: (255,   0,   0, 255),  # High-High (red)
                2: (  0,   0, 255, 255),  # Low-Low (blue)
                3: (255, 255,   0, 255),  # High-Low (yellow)
                4: (  0, 255,   0, 255)   # Low-High (green)
            }
            dst.write_colormap(1, colormap)

# Export Local Moran’s I statistic
rasterize_attribute(gdf, "local_I", CHE_LMI_PATH)

# Export p-values
rasterize_attribute(gdf, "p_value", CHE_LMIP_PATH)

# Export cluster 
rasterize_attribute(gdf, "cluster_int", CHE_LMICL_PATH)

# -------------------------------------------------------------------
# 6. Quantitative summary of cluster results
# -------------------------------------------------------------------
summary = gdf["cluster"].value_counts().rename_axis("Cluster").reset_index(name="Cell count")
summary["Percent of cells"] = (summary["Cell count"] / len(gdf) * 100).round(2)

print("\n=== Local Moran’s I Cluster Summary (Switzerland) ===")
print(summary)

summary.to_csv(CHE_LISAcsv_PATH, index=False)

# -------------------------------------------------------------------
# 7. Quick visualization
# -------------------------------------------------------------------
fig, ax = plt.subplots(1, 1, figsize=(10, 10))
gdf.plot(column="cluster", categorical=True, legend=True, ax=ax,
         cmap="Set1", edgecolor="grey", linewidth=0.2)
ax.set_title("Local Moran’s I Clusters of Vet Clinics (Switzerland)", fontsize=14)
# Save to PNG before showing
plt.savefig(CHE_LISApng_PATH, dpi=300, bbox_inches="tight")
plt.show()
