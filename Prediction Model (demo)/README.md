### Step 6: Raster Clipping (1_Clip_Covariates.py)
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
5. Save outputs.

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
