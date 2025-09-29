### Step 1: Raster Clipping (1_Clip_Covariates.py)
=================================

Overview:

This script clips global raster covariates (e.g., climate, population, GDP, accessibility) to the national boundaries of Switzerland (CHE) and Austria (AUT).  
It ensures each raster is aligned with the study area, masked correctly, and saved as country-specific GeoTIFFs for later modeling.

What it does:
1. Load each country shapefile and merge polygons into one boundary.  
2. For each raster:
   - Open raster with `rioxarray`.  
   - Mask out 0 values (set to `NoData`).  
   - Reproject boundary shapefile to raster CRS.  
   - Clip raster to the country boundary.  
   - Save clipped raster as GeoTIFF in `clipped_rasters/`. 
3. Inspect properties of each clipped raster:
   - CRS  
   - Resolution  
   - Value range (min, max, mean)  
   - Heuristic checks for pixel size (≈10 km) and log10-transformation
4. Save outputs.

INPUT files:
1. National boundaries:
  - `CHE1_nr.shp` (Switzerland)  
  - `AUT1_nr.shp` (Austria)
2. Covariate rasters (global or continental coverage):
  - `ca_v4.tif` (cattle density)  
  - `ch_v4.tif` (chicken density)
  - `pg_v4.tif` (pig density)
  - `sh_v4.tif` (sheep density)
  - `pop_density_2015_10k.tif` (human population density)  
  - `urban_rural_2018_10k.tif` (urban–rural classification)  
  - `world_settlement_footprint.tif` (settlement mask)  
  - `gdp.grd` (GDP per capita)  
  - `acc.grd` (travel time to cities) 

OUTPUT file:
1. Clipped raster files, e.g.:  
  - `ca_v4_CHE.tif`  
  - `ca_v4_AUT.tif`  
  - (same for all covariates)  
----------------------
### Step 2: Grid Generation (2_Grid_Making.py)
=================================

Overview:

This script generates **10 km × 10 km spatial grids** for Switzerland (CHE) and Austria (AUT).  
Each grid is clipped to the national boundary, assigned a unique cell ID, and saved in both **EPSG:3035 (equal-area, meters)** and **EPSG:4326 (geographic, degrees)** formats.  
These grids are later used as the spatial framework for counting clinics and attaching covariates.

What it does:
1. Load each country’s shapefile and unify polygons into a single boundary.
2. Reproject the boundary to **EPSG:3035** (meter-based, equal-area). 
3. Build a rectangular grid with 10 km cell size covering the bounding box of the country.
4. Clip the grid to the national boundary.
5. Assign a unique ID to each cell (`CHE_1`, `CHE_2`, …, `AUT_1`, `AUT_2`, …).
6. Save results in both CRS formats:  
   - EPSG:3035 → for modeling (area-preserving, consistent adjacency).  
   - EPSG:4326 → for compatibility with covariates stored in degrees.  

INPUT files:
1. National boundaries:
  - `CHE1_nr.shp` (Switzerland)  
  - `AUT1_nr.shp` (Austria)

OUTPUT file:
1. Grid files, e.g.:  
  - `CHE_grid_10km_EPSG3035.gpkg`  
  - `CHE_grid_10km_EPSG4326.gpkg`
  - `AUT_grid_10km_EPSG3035.gpkg`  
  - `AUT_grid_10km_EPSG4326.gpkg`
----------------------
### Step 3: Clinic-to-Grid Assignment (3_Clinic_Count.py)
=================================

Overview:

This script assigns veterinary clinics (from CSV files) to the nearest **10 × 10 km grid cells** for Switzerland (CHE) and Austria (AUT).  
It ensures each clinic is counted exactly once and produces gridded datasets with a new column `clinic_count`.  
Outputs are generated in both **EPSG:4326** (degrees) and **EPSG:3035** (meters) to support later modeling workflows.  

What it does:
1. Load clinic CSV and convert to a GeoDataFrame (`Longitude`, `Latitude` → geometry). 
2. For each CRS (EPSG:4326 and EPSG:3035):
   - Reproject clinics to grid CRS.  
   - Assign each clinic to the **nearest grid cell** using spatial join (`sjoin_nearest`).  
   - Count the number of clinics per cell (`clinic_count`).  
   - Merge counts back into the grid; cells without clinics are assigned `0`.  
   - Verify total clinics match between CSV and gridded counts (sanity check).
3. Save the enriched grid (with `clinic_count`) as a new GeoPackage. 

INPUT files:
1. Clinic location files (CSV, raw coordinates in EPSG:4326):
   - `CHE/VP_team.csv` (Switzerland)
   - `AUT/VP_filtered_team.csv` (Austria)
2. Grids (GeoPackage format, already created in Step 2):
   - `*_grid_10km_EPSG4326.gpkg`  
   - `*_grid_10km_EPSG3035.gpkg` 

OUTPUT file:
1. Grid with clinic counts files, e.g.:  
  - `CHE_grid_with_clinics_EPSG3035.gpkg`  
  - `CHE_grid_with_clinics_EPSG4326.gpkg`
  - `AUT_grid_with_clinics_EPSG3035.gpkg`  
  - `AUT_grid_with_clinics_EPSG4326.gpkg`
----------------------
### Step 4: Grid Covariate Extraction (4_Extract_Covariates.py)
=================================

Overview:

This script enriches 10 × 10 km grid cells with **covariates** derived from raster datasets (environmental, demographic, economic, and accessibility).  
It assigns values from raster covariates to each grid cell centroid and outputs a cleaned CSV for modeling.

What it does:
1. Load the **grid with clinics** files.  
2. Sample `wsf` raster at grid centroids → create a `settlement` mask.
3. Keep only cells with `settlement > 0`.
4. Recompute centroids for masked grid cells.
5. Sample all remaining covariate rasters at centroids.
6. Enforce proper data types:  
   - `clinic_count`, `settlement`, `urban` → **integer**.  
   - Continuous covariates (`ca`, `ch`, `pg`, `sh`, `pop`, `gdp`, `acc`) → **float**, rounded to 2 decimals.
7. Quick quality check: prints min, max, mean for each covariate.
8. Save covariate-enriched grid to **CSV** (no geometry, EPSG:4326). 

The workflow ensures:
- Veterinary clinic counts are preserved (`clinic_count`).
- Only cells with **settlements** (from World Settlement Footprint) are retained.
- Covariates are typed correctly (integer or float).
- Results are exported as **CSV** (EPSG:4326, lat/long) for further modeling in R/INLA.

INPUT files:
1. Grid with clinic counts files, e.g.:  
  - `CHE_grid_with_clinics_EPSG3035.gpkg`  
  - `CHE_grid_with_clinics_EPSG4326.gpkg`
  - `AUT_grid_with_clinics_EPSG3035.gpkg`  
  - `AUT_grid_with_clinics_EPSG4326.gpkg`
2. Clipped raster files, e.g.:  
  - `ca_v4_CHE.tif`  
  - `ca_v4_AUT.tif`  
  - (same for all covariates) 

OUTPUT file:
1. Grid with covariates files, e.g.:   
  - `CHE_grid_with_covariates_EPSG4326.gpkg`
  - `AUT_grid_with_covariates_EPSG4326.gpkg`
----------------------
### Step 5: Spatial Autocorrelation Check: Local Moran's I (LISA) Clustering Analysis (5_Spatial_Autocorrelation.py)
=================================

Overview:

This script detects **spatial autocorrelation clusters** of veterinary clinics across Switzerland at a 10 × 10 km grid resolution.  
Using **Local Moran’s I (LISA)**, it identifies *hotspots* and *coldspots* of veterinary clinic presence, quantifies their significance, and exports both **vector** and **raster** outputs for visualization and modeling.

What it does:
1. Load Switzerland’s grid and ensure `clinic_count` is numeric.
2. Construct **Queen contiguity weights** (each cell shares borders with neighbors). 
3. Run **Local Moran’s I** using PySAL. For each grid cell:
   - Local Moran’s I value  
   - p-value (permutation test)  
   - cluster type (High-High, Low-Low, High-Low, Low-High, or not significant) 
4. Save enriched grid as GeoPackage.
5. Export results as GeoTIFFs (10 km resolution, EPSG:3035)
6. Create a CSV summary with cluster counts and percentages.
7. Plot and export a map of clusters.

INPUT files:
1. Grid with clinic counts files, e.g.:  
  - `CHE_grid_with_clinics_EPSG3035.gpkg`  
  - `CHE_grid_with_clinics_EPSG4326.gpkg`
  - `AUT_grid_with_clinics_EPSG3035.gpkg`  
  - `AUT_grid_with_clinics_EPSG4326.gpkg`

OUTPUT file:
1. GeoPackage: `CHE_grid_LISA.gpkg` (grid cells + attributes). Columns include:
  - `clinic_count` – observed clinics per cell  
  - `local_I` – Local Moran’s I statistic  
  - `p_value` – significance (permutation test)  
  - `cluster` – cluster label  
  - `cluster_int` – numeric encoding of cluster type
2. Raster:
   - `CHE_localMoranI.tif` – continuous Local Moran’s I  
   - `CHE_localMoranPval.tif` – p-values  
   - `CHE_localMoranCluster.tif` – cluster map (integer codes + color table)
   
   Cluster color codes:
   - **0 (gray):** Not significant  
   - **1 (red):** High-High  
   - **2 (blue):** Low-Low  
   - **3 (yellow):** High-Low  
   - **4 (green):** Low-High  
3. `CHE_LISA_summary.csv` – quantitative breakdown of cluster types (% of grid cells)
4. `CHE_LISA_clusters.png` – quick visualization of clusters
----------------------
### Step 6: Mesh and SPDE Model Creation (6_Mesh_SPDE.R)
=================================

Overview:

This script builds the computational structure for spatial modeling in Switzerland.
It creates a triangular mesh and defines an **SPDE (Stochastic Partial Differential Equation)** model to represent the spatial random effect in the regression model.
The mesh + SPDE captures spatial autocorrelation between grid cells, ensuring that unmeasured geographic clustering is properly modeled in Step 7 (regression fitting).

What it does:
1. Load Swiss boundary: reproject to EPSG3035, convert to inla.sp2segment() format. 
2. Build triangulated mesh: construct mesh across Switzerland with buffer extension.
3. Define SPDE model: matern covariance function, alpha = 2.
4. Save outputs.

> Model Specification:

The mesh approximates a **Gaussian Random Field (GRF)** using the SPDE approach with a Matern covariance function.
Parameter used:
1. Mesh construction:
  - `max.edge = c(20e3, 50e3)` -> max triangule edge length inside vs outside boundary (20km vs 50km).
  - `cutoff = 5e3` -> merge nodes within 5 km.
  - `offset = c(50e3, 100e3)` -> buffer outside boundary (50-100km).
2. SPDE priors:
  - `prior.range = c(50e3, 0.5)` -> P(range < 50km) = 0.5
  - `prior.sigma = c(1, 0.01)` -> P(sigma > 1) = 0.01

INPUT files:
1. `CHE1_nr.shp`

OUTPUT file:
1. Triangulated mesh: `CHE_mesh.rds` (defines the triangulated domain for the SPDE)
2. Triangulated mesh with nodes and triangles layers: `CHE_mesh.gpkg`
3. Triangulated mesh plot for documentation: `CHE_mesh.png`
4. SPDE model object: `CHE_spde_model.rds` (spatial random effect in Step 7)
5. SPDE parameters for transparency: `CHE_spde_params.json`
----------------------
### Step 7: Model Fitting with inlabru (7_Model_Create.R)
=================================

Overview:

This script fits a spatial regression model to Swiss veterinary clinic data, using the inlabru/INLA framework.

It combines:
1. Poisson regression for clinic counts per 10 * 10km grid cell.
2. Fixed effects: environmental and socio-economic covariates.
3. Random effect: a Gaussian Random Field (GRF) estimated via the Stochastic Partial Differential Equation (SPDE) approach, to capture spatial autocorrelation.

What it does:
1. Load data: Swiss grid and covariates, merge datasets by cell_id; Ensure covariates are numeric, impute missing value with column means.
2. Load mesh and SPDE model: import mesh and SPDE.
3. Define inlabru components: intercept; Covariates (settlement, ca, ch, pg, sh, pop, urban, gdp, acc); SPDE random effect.
4. Fit model with bru(): Poisson (family); merged Swiss grid and covariates; cell area (exposure).
5. Save outputs.

> Model specification

The response variable (clinic counts) is modeled using a **Log-Gaussian Cox process** approximation via a Poisson likelihood:

$$
y_i \sim \text{Poisson}(\lambda_i \cdot A_i)
$$

with the log-link function:

$$
\log(\lambda_i) = \beta_0 + \beta_1 \cdot \text{settlement}_i + \cdots + \beta_k \cdot \text{acc}_i + u(s_i)
$$

**Where:**

- $y_i$ = observed clinic count in grid cell *i*  
- $A_i$ = grid cell area (exposure)  
- $\lambda_i$ = expected clinic intensity  
- $\beta$ = regression coefficients for covariates  
- $u(s_i)$ = spatial random effect at location $s_i$, modeled via the SPDE mesh  
  
INPUT files:
1. `CHE_grid_with_clinics_EPSG3035.gpkg`
2. `CHE_grid_with_covariates_EPSG4326.csv`
3. `CHE_mesh.rds`
4. `CHE_spde_model.rds`

OUTPUT file:
1. Model object: `CHE_bru_fit.rds`
2. Fixed effects (coefficients) table: `CHE_bru_fixed_effects.csv` (quantify influence of each covariate)
3. Model fit stats (DIC, WAIC, CPO): `CHE_bru_model_fit.csv` (access goodness of fit and compare models)
4. Predictions per grid cell: `CHE_bru_predictions.gpkg` (expected number of clinic per grid cell, with uncertainty bounds -> mean, df, 95%CI)
5. Spatial random effect estimates: `CHE_bru_spatial_effect.csv` (captures clustering not explained by covariates)
----------------------
