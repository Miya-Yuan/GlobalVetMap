# -------------------------------------------------------------------
# Model fitting with Switzerland data (Log-Gaussian Poisson regression)
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
preds_gpkg <- file.path(base_dir, "model", "CHE_inlabru_predictions.gpkg")
bru_rds    <- file.path(base_dir, "model", "CHE_inlabru_model.rds")
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

# Add coordinates and area
coords <- st_coordinates(st_centroid(gdf_che))
gdf_che$coordx <- coords[, 1]
gdf_che$coordy <- coords[, 2]
gdf_che$cellarea <- as.numeric(st_area(gdf_che))

grid_df <- as.data.frame(gdf_che)

# -------------------------------------------------------------------
# 7. Define components and fit model (inlabru)
# -------------------------------------------------------------------
# Ensure response is named “count” (inlabru expects the response in the data frame)
grid_df$count <- grid_df$clinic_count

# Define model components, with explicit intercept + covariates + SPDE
components <- count ~ 
  Intercept(1) +
  settlement +
  ca +
  ch +
  pg +
  sh +
  pop +
  urban +
  gdp +
  acc +
  SPDE(main = cbind(coordx, coordy), model = spde_che)

cat("\n📌 Fitting inlabru model with SPDE + covariates:",
    paste(c("settlement","ca","ch","pg","sh","pop","urban","gdp","acc"), collapse = " + "), "\n")

bru_fit <- bru(
  components = components,
  family = "poisson",
  data = grid_df,
  E = grid_df$cellarea,
  options = list(verbose = TRUE)
)

# -------------------------------------------------------------------
# 8. Save the fitted model
# -------------------------------------------------------------------
bru_rds <- file.path(base_dir, "model", "CHE_bru_fit.rds")
saveRDS(bru_fit, bru_rds)

# -------------------------------------------------------------------
# 9. Export fixed effects, model fit, spatial effects
# -------------------------------------------------------------------
# 9a. Fixed effects
fixed_effects <- bru_fit$summary.fixed
fixed_csv <- file.path(base_dir, "model", "CHE_bru_fixed_effects.csv")
write.csv(fixed_effects, fixed_csv, row.names = TRUE)

# 9b. Model fit criteria (safe extraction, since some may be NULL)
fit_csv <- file.path(base_dir, "model", "CHE_bru_model_fit.csv")
DIC  <- if (!is.null(bru_fit$dic)) bru_fit$dic$dic else NA
WAIC <- if (!is.null(bru_fit$waic)) bru_fit$waic$waic else NA
CPO  <- if (!is.null(bru_fit$cpo)) mean(-log(bru_fit$cpo$cpo), na.rm = TRUE) else NA

model_fit <- data.frame(DIC = DIC, WAIC = WAIC, CPO = CPO)
write.csv(model_fit, fit_csv, row.names = FALSE)

# 9c. Predictions (posterior means, sd, quantiles)
preds_gpkg <- file.path(base_dir, "model", "CHE_bru_predictions.gpkg")

predictions <- predict(
  bru_fit,
  newdata = grid_df,
  formula = ~ count,
  n.samples = 1000
)

gdf_che$pred_mean  <- predictions$mean
gdf_che$pred_sd    <- predictions$sd
gdf_che$pred_q025  <- predictions$q0.025
gdf_che$pred_q975  <- predictions$q0.975

if (file.exists(preds_gpkg)) file.remove(preds_gpkg)
st_write(gdf_che, preds_gpkg)

# 9d. Spatial random effect (latent field)
spatial_effect <- predict(
  bru_fit,
  newdata = grid_df,
  formula = ~ SPDE,
  n.samples = 1000
)
spatial_csv <- file.path(base_dir, "model", "CHE_bru_spatial_effect.csv")
write.csv(spatial_effect, spatial_csv, row.names = FALSE)

# -------------------------------------------------------------------
# 10. Report
# -------------------------------------------------------------------
cat("✅ Fitted inlabru model for Switzerland\n")
cat("   Model saved to:", bru_rds, "\n")
cat("   Fixed effects saved to:", fixed_csv, "\n")
cat("   Fit stats saved to:", fit_csv, "\n")
cat("   Predictions saved to:", preds_gpkg, "\n")
cat("   Spatial effect saved to:", spatial_csv, "\n")
