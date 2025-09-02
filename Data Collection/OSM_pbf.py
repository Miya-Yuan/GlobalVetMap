import osmium
import pandas as pd
import geopandas as gpd
import os

# === CONFIGURATION ===
BASE_DIR = "C:/Users/myuan/Desktop/VetMap_Data"
SHP_DIR = "C:/Users/myuan/Desktop/Data/shapefile/country"
# Path to continent PBF
OSM_PBF_PATH = "C:/Users/myuan/Downloads/asia-latest.osm.pbf"  
# Select continent here
CONTINENT = "ASIA"
# === ISO sets by continent ===
EUROPE_ISO = {
    "ALB","AND","AUT","BEL","BGR","BIH","CHE","CYP","CZE","DEU","DNK","ESP","EST","FIN","FRA",
    "GBR","GRC","HRV","HUN","IRL","ISL","ITA","KOS","LIE","LTU","LUX","LVA","MCO","MDA","MKD",
    "MLT","MNE","NLD","NOR","POL","PRT","ROU","SMR","SRB","SVK","SVN","SWE","UKR","VAT"
}
AFRICA_ISO = {
    "DZA","AGO","BEN","BWA","BFA","BDI","CPV","CMR","CAF","TCD","COM","COG","COD","DJI","EGY",
    "GNQ","ERI","SWZ","ETH","GAB","GMB","GHA","GIN","GNB","CIV","KEN","LSO","LBR","LBY","MDG",
    "MWI","MLI","MRT","MUS","MYT","MAR","MOZ","NAM","NER","NGA","REU","RWA","STP","SEN","SYC",
    "SLE","SOM","ZAF","SSD","SDN","TZA","TGO","TUN","UGA","ESH","ZMB","ZWE"
}
ASIA_ISO = {
    "AFG","ARM","AZE","BHR","BGD","BTN","BRN","KHM","CHN","CYP","GEO","HKG","IND","IDN","IRN",
    "IRQ","ISR","JPN","JOR","KAZ","KWT","KGZ","LAO","LBN","MAC","MYS","MDV","MNG","MMR","NPL",
    "PRK","OMN","PAK","PSE","PHL","QAT","SAU","SGP","KOR","LKA","SYR","TWN","TJK","THA","TLS",
    "TUR","TKM","ARE","UZB","VNM","YEM"
}
NORTH_AMERICA_ISO = {
    "AIA","ATG","BHS","BRB","BLZ","BMU","CAN","CYM","CRI","CUB","CUW","DMA","DOM","SLV","GRL",
    "GRD","GLP","GTM","HTI","HND","JAM","MTQ","MEX","MSR","ANT","KNA","LCA","MAF","SPM","VCT",
    "TTO","USA","VIR"
}
CENTRAL_AMERICA_ISO = {
    "BHS","BLZ","CRI","CUB","SLV","GTM","HTI","DOM","HND","JAM","NIC","PAN"   
}
SOUTH_AMERICA_ISO = {
    "ARG","BOL","BRA","CHL","COL","ECU","GUY","PRY","PER","SUR","URY","VEN","FLK","GUF"
}
OCEANIA_ISO = {
    "ASM","AUS","COK","FJI","PYF","GUM","KIR","MHL","FSM","NRU","NCL","NZL","NIU","NFK","MNP",
    "PLW","PNG","PCN","WSM","SLB","TKL","TON","TUV","VUT","WLF"
}

# Map continents to ISO sets
CONTINENT_MAP = {
    "EUROPE": EUROPE_ISO,
    "AFRICA": AFRICA_ISO,
    "ASIA": ASIA_ISO,
    "NORTH_AMERICA": NORTH_AMERICA_ISO,
    "CENTRAL_AMERICA": CENTRAL_AMERICA_ISO,
    "SOUTH_AMERICA": SOUTH_AMERICA_ISO,
    "OCEANIA": OCEANIA_ISO,
}
# Pick ISO set for chosen continent
ISO_SET = CONTINENT_MAP[CONTINENT]

# === HANDLER ===
class VetHandler(osmium.SimpleHandler):
    def __init__(self):
        super().__init__()
        self.rows = []

    def node(self, n):
        if 'amenity' in n.tags and n.tags['amenity'] == 'veterinary':
            self.rows.append(self._make_entry("node", n.id, n.tags, 
                                              n.location.lat if n.location else None,
                                              n.location.lon if n.location else None))

    def way(self, w):
        if 'amenity' in w.tags and w.tags['amenity'] == 'veterinary':
            if w.nodes:
                lat = sum(n.lat for n in w.nodes if n.location) / len(w.nodes)
                lon = sum(n.lon for n in w.nodes if n.location) / len(w.nodes)
            else:
                lat, lon = None, None
            self.rows.append(self._make_entry("way", w.id, w.tags, lat, lon))

    def relation(self, r):
        if 'amenity' in r.tags and r.tags['amenity'] == 'veterinary':
            self.rows.append(self._make_entry("relation", r.id, r.tags, None, None))

    def _make_entry(self, obj_type, obj_id, tags, lat, lon):
        return {
            "osm_id": f"{obj_type}/{obj_id}",
            "name": tags.get("name"),
            "address": self._compose_address(tags),
            "latitude": lat,
            "longitude": lon,
            "website": tags.get("website") or tags.get("contact:website"),
        }

    def _compose_address(self, tags):
        parts = [
            tags.get("addr:street"),
            tags.get("addr:housenumber"),
            tags.get("addr:postcode"),
            tags.get("addr:city"),
        ]
        return ", ".join([p for p in parts if p])

# === MAIN ===
def extract_vets_by_country():
    print(f"🔍 Reading PBF extract: {OSM_PBF_PATH}")
    handler = VetHandler()
    handler.apply_file(OSM_PBF_PATH, locations=True)
    print(f"✅ Parsed {len(handler.rows)} raw vet entries. Deduplicating...")
    
    df = pd.DataFrame(handler.rows)
    if df.empty:
        print("⚠️ No vet entries found in this PBF.")
        return

    summary = {}  # store counts for each country
    # Add country column based on shapefile check
    for iso in sorted(ISO_SET):
        shp_folder = os.path.join(SHP_DIR, iso)
        if not os.path.isdir(shp_folder):
            print(f"⏭️ Skipping {iso} (no shapefile folder found)")
            continue
        # Spatial filter using shapefile
        shp_path = os.path.join(shp_folder, f"{iso}1_nr.shp")
        if not os.path.exists(shp_path):
            print(f"⏭️ Skipping {iso} (no shapefile file found)")
            continue
        
        output_file = os.path.join(BASE_DIR, iso, "OSM", f"{iso}_VP_OSM.csv")
        os.makedirs(os.path.dirname(output_file), exist_ok=True)

        country_gdf = gpd.read_file(shp_path).to_crs(epsg=4326)
        poly = country_gdf.geometry.union_all()

        # Keep only rows with valid coordinates
        df_coords = df.dropna(subset=["latitude", "longitude"])

        # Build GeoDataFrame correctly
        gdf_points = gpd.GeoDataFrame(
            df_coords,
            geometry=gpd.points_from_xy(df_coords["longitude"], df_coords["latitude"]),
            crs="EPSG:4326"
        )

        inside = gdf_points[gdf_points.within(poly)]
        df_out = inside.drop(columns="geometry")

        # Deduplicate
        df_out = df_out.drop_duplicates(subset=["osm_id"])
        df_out = df_out[["name", "address", "latitude", "longitude", "website"]]
        df_out.columns = [c.capitalize() for c in df_out.columns]
        df_out[["Name", "Address"]] = df_out[["Name", "Address"]].replace(r"^\s*$", pd.NA, regex=True)
        df_out = df_out.dropna(subset=["Name", "Address"], how="all")

        # Save
        df_out.to_csv(output_file, index=False)
        count = len(df_out)
        summary[iso] = count
        print(f"💾 {iso}: {len(df_out)} vet clinics saved to {output_file}")

    # Print summary
    print(f"\n📊 Summary of {CONTINENT} countries processed:")
    for iso, count in summary.items():
        print(f"{iso}: {count} clinics")


if __name__ == "__main__":
    extract_vets_by_country()
