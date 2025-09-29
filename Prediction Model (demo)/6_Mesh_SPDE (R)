# Purpose:
#   - Account for spatial autocorrelation detected in Swiss clinic data.
#   - Build a triangulated mesh covering Switzerland to approximate a 
#     continuous Gaussian Random Field (GRF).
#   - Define an SPDE (Stochastic Partial Differential Equation) model 
#     on this mesh, which will later be used as a spatial random effect 
#     in regression modeling.
# Outcome:
#   The mesh and SPDE model define the computational structure for adding 
#   a spatial random effect to the Swiss model. This ensures spatial 
#   autocorrelation is properly captured when fitting predictive models.
# -------------------------------------------------------------------
library(INLA)
library(sf)
library(jsonlite)
# -------------------------------------------------------------------
# 1. Define paths
# -------------------------------------------------------------------
shp_dir <- "C:/Users/myuan/Desktop/Data/shapefile/country/CHE"
out_dir <- "C:/Users/myuan/Desktop/Data/Covariates/model"

# Input shapefile (Switzerland boundary)
shapefile_path <- file.path(shp_dir, "CHE1_nr.shp")

# Output files
mesh_rds   <- file.path(out_dir, "CHE_mesh.rds")
spde_rds   <- file.path(out_dir, "CHE_spde_model.rds")
mesh_plot  <- file.path(out_dir, "CHE_mesh.png")
mesh_gpkg  <- file.path(out_dir, "CHE_mesh.gpkg")
spde_json  <- file.path(out_dir, "CHE_spde_params.json")

# -------------------------------------------------------------------
# 2. Load Switzerland boundary shapefile
# -------------------------------------------------------------------
che_boundary <- st_read(shapefile_path)

# Reproject to EPSG:3035 (meter-based CRS for mesh construction)
che_boundary <- st_transform(che_boundary, 3035)

# Convert boundary to INLA format
che_boundary_inla <- inla.sp2segment(as_Spatial(che_boundary))

# -------------------------------------------------------------------
# 3. Build mesh on Switzerland
# -------------------------------------------------------------------
mesh_che <- inla.mesh.2d(
  boundary = che_boundary_inla,
  max.edge = c(20e3, 50e3),   # 20 km inside, 50 km outside buffer
  cutoff = 5e3,               # merge close points
  offset = c(50e3, 100e3)     # 50–100 km buffer
)

# -------------------------------------------------------------------
# 4. Define SPDE model (Gaussian Random Field with Matern covariance)
# -------------------------------------------------------------------
spde_che <- inla.spde2.pcmatern(
  mesh = mesh_che,
  alpha = 2,
  prior.range = c(50e3, 0.5),   # P(range < 50 km) = 0.5
  prior.sigma = c(1, 0.01)      # P(sigma > 1) = 0.01
)

# -------------------------------------------------------------------
# 5. Save R outputs
# -------------------------------------------------------------------
saveRDS(mesh_che, mesh_rds)
saveRDS(spde_che, spde_rds)

# -------------------------------------------------------------------
# 6. Save mesh as GeoPackage
# -------------------------------------------------------------------
nodes <- mesh_che$loc
nodes_sf <- st_as_sf(data.frame(id = 1:nrow(nodes), x = nodes[,1], y = nodes[,2]),
                     coords = c("x","y"), crs = 3035)

tri <- mesh_che$graph$tv
tri_polys <- lapply(1:nrow(tri), function(i) {
  verts <- nodes[tri[i,], ]
  st_polygon(list(rbind(verts, verts[1,]))) # close polygon
})
tris_sf <- st_sf(id = 1:length(tri_polys),
                 geometry = st_sfc(tri_polys, crs = 3035))

if (file.exists(mesh_gpkg)) file.remove(mesh_gpkg) # overwrite if exists
st_write(nodes_sf, mesh_gpkg, layer = "nodes")
st_write(tris_sf,  mesh_gpkg, layer = "triangles", append = TRUE)

# -------------------------------------------------------------------
# 7. Save SPDE priors/parameters as JSON
# -------------------------------------------------------------------
spde_params <- list(
  alpha = 2,
  prior_range = c(50000, 0.5),  # (range, probability)
  prior_sigma = c(1, 0.01)      # (sigma, probability)
)

write_json(spde_params, spde_json, pretty = TRUE)

# -------------------------------------------------------------------
# 8. Report and plot
# -------------------------------------------------------------------
cat("✅ Mesh created for Switzerland\n")
cat("   Number of vertices:", mesh_che$n, "\n")
cat("   Saved mesh RDS:", mesh_rds, "\n")
cat("   Saved SPDE RDS:", spde_rds, "\n")
cat("   Saved mesh GeoPackage:", mesh_gpkg, "\n")
cat("   Saved SPDE JSON:", spde_json, "\n")
cat("   Mesh plot saved:", mesh_plot, "\n")

png(mesh_plot, width=1200, height=1200, res=150)
plot(mesh_che, main="SPDE Mesh for Switzerland (EPSG:3035)")
plot(st_geometry(che_boundary), add=TRUE, col="red")
dev.off()
