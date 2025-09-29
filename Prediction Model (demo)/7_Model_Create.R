# -------------------------------------------------------------------
# 7. Model fitting with Switzerland data (Log-Gaussian Poisson regression)
# Compatible with inlabru v2.8.0 (uses predictor())
#
# Response: clinic counts per 10 × 10 km grid cell
# Predictors: covariates stored separately in CSV (EPSG:4326 values)
# Random effect: Gaussian Random Field via SPDE mesh (EPSG:3035)
# Inference: INLA via inlabru (Integrated Nested Laplace Approximation)
# -------------------------------------------------------------------
library(INLA)
library(inlabru)
library(sf)
library(data.table)
# -------------------------------------------------------------------
# 1. Define paths
# -------------------------------------------------------------------
base_dir <- "C:/Users/myuan/Desktop/Data/Covariates"
grid_path  <- file.path(base_dir, "grids_with_clinics", "CHE_grid_with_clinics_EPSG3035.gpkg")
cov_path   <- file.path(base_dir, "grids_with_covariates", "CHE_grid_with_covariates_EPSG4326.csv")
mesh_rds   <- file.path(base_dir, "model", "CHE_mesh.rds")
spde_rds   <- file.path(base_dir, "model", "CHE_spde_model.rds")

# Outputs
bru_fit_rds  <- file.path(base_dir, "model", "CHE_inlabru_fit.rds")
preds_gpkg   <- file.path(base_dir, "model", "CHE_inlabru_predictions.gpkg")
resid_gpkg   <- file.path(base_dir, "model", "CHE_inlabru_residuals.gpkg")
fixed_csv    <- file.path(base_dir, "model", "CHE_inlabru_fixed_effects.csv")
spatial_csv  <- file.path(base_dir, "model", "CHE_inlabru_spatial_effect.csv")

# -------------------------------------------------------------------
# 2. Load Swiss grid with clinic counts
# -------------------------------------------------------------------
gdf_che <- st_read(grid_path)

if (!"cell_id" %in% names(gdf_che)) {
  gdf_che$cell_id <- 1:nrow(gdf_che)
}

# -------------------------------------------------------------------
# 3. Load covariates (CSV, EPSG:4326 but join by ID)
# -------------------------------------------------------------------
cov_che <- fread(cov_path)

if (!"cell_id" %in% names(cov_che)) {
  stop("❌ Covariates file must contain 'cell_id' to join with grid.")
}

gdf_che <- merge(gdf_che, cov_che, by = "cell_id")

# -------------------------------------------------------------------
# 4. Clean response variable (fix duplicate clinic_count columns)
# -------------------------------------------------------------------
if ("clinic_count.x" %in% names(gdf_che)) {
  gdf_che$clinic_count <- gdf_che$clinic_count.x
  gdf_che$clinic_count.x <- NULL
}
if ("clinic_count.y" %in% names(gdf_che)) {
  gdf_che$clinic_count.y <- NULL
}

# -------------------------------------------------------------------
# 5. Prepare covariates
# -------------------------------------------------------------------
gdf_nogeo <- st_drop_geometry(gdf_che)

covariates <- gdf_nogeo[, c("settlement", "ca", "ch", "pg",
                            "sh", "pop", "urban", "gdp", "acc")]

covariates <- as.data.frame(lapply(covariates, function(x) as.numeric(x)))
colnames(covariates) <- c("settlement", "ca", "ch", "pg", "sh",
                          "pop", "urban", "gdp", "acc")

# Impute missing values
for (col in names(covariates)) {
  if (any(is.na(covariates[[col]]))) {
    covariates[[col]][is.na(covariates[[col]])] <- mean(covariates[[col]], na.rm = TRUE)
  }
}
gdf_che[, names(covariates)] <- covariates

# -------------------------------------------------------------------
# 6. Load mesh and SPDE model
# -------------------------------------------------------------------
mesh_che <- readRDS(mesh_rds)
spde_che <- readRDS(spde_rds)

# Add coordinates and cell area
coords <- st_coordinates(st_centroid(gdf_che))
gdf_che$coordx <- coords[, 1]
gdf_che$coordy <- coords[, 2]
gdf_che$cellarea <- as.numeric(st_area(gdf_che))

grid_df <- as.data.frame(gdf_che)

# -------------------------------------------------------------------
# 7. Define components and fit model (inlabru)
# -------------------------------------------------------------------
# Ensure response is named "count"
grid_df$count <- grid_df$clinic_count

components <- ~ Intercept(1) +
  settlement + ca + ch + pg + sh + pop + urban + gdp + acc +
  SPDE(main = cbind(coordx, coordy), model = spde_che)

# Define likelihood
likelihood <- inlabru::like(
  formula = count ~ .,
  family = "poisson",
  data = grid_df,
  E = grid_df$cellarea  
)

cat("\n📌 Fitting inlabru model (v2.13.0) with SPDE + covariates\n")

bru_fit <- bru(
  components = components,
  lik = likelihood,
  options = list(verbose = TRUE)
)

# -------------------------------------------------------------------
# 8. Save fitted model
# -------------------------------------------------------------------
saveRDS(bru_fit, bru_fit_rds)

# -------------------------------------------------------------------
# 9. Export results
# -------------------------------------------------------------------
# 9a. Fixed effects
fixed_effects <- bru_fit$summary.fixed
write.csv(fixed_effects, fixed_csv, row.names = TRUE)

# 9b. Predictions: intensity (λ) and expected counts (λ * A)
preds <- predict(
  bru_fit,
  newdata = grid_df,
  formula = ~ exp(Intercept + settlement + ca + ch + pg + sh +
                    pop + urban + gdp + acc + SPDE),
  n.samples = 1000
)

# Intensity λ_i
gdf_che$lambda_mean <- preds$mean
gdf_che$lambda_sd   <- preds$sd
gdf_che$lambda_q025 <- preds$q0.025
gdf_che$lambda_q975 <- preds$q0.975

# Expected counts λ_i * A_i
gdf_che$expected_mean <- gdf_che$lambda_mean * gdf_che$cellarea
gdf_che$expected_sd   <- gdf_che$lambda_sd   * gdf_che$cellarea
gdf_che$expected_q025 <- gdf_che$lambda_q025 * gdf_che$cellarea
gdf_che$expected_q975 <- gdf_che$lambda_q975 * gdf_che$cellarea

st_write(gdf_che, preds_gpkg, delete_dsn = TRUE)

# 9c. Residuals
gdf_che$resid_raw <- gdf_che$clinic_count - gdf_che$expected_mean
gdf_che$resid_pearson <- gdf_che$resid_raw / sqrt(gdf_che$expected_mean)

st_write(gdf_che, resid_gpkg, delete_dsn = TRUE)

# 9d. Spatial random effect
spatial_effect <- predict(
  bru_fit,
  newdata = grid_df,
  formula = ~ SPDE,
  n.samples = 1000
)
write.csv(spatial_effect, spatial_csv, row.names = FALSE)

# -------------------------------------------------------------------
# 10. Report
# -------------------------------------------------------------------
cat("✅ Fitted inlabru v2.13.0 model for Switzerland\n")
cat("   Model saved to:", bru_fit_rds, "\n")
cat("   Fixed effects saved to:", fixed_csv, "\n")
cat("   Predictions saved to:", preds_gpkg, "\n")
cat("   Residuals saved to:", resid_gpkg, "\n")
cat("   Spatial effect saved to:", spatial_csv, "\n")
