import geopandas as gpd
from pathlib import Path

# Path to this script
HERE = Path(__file__).resolve().parent

# Input SHP
shp_path = HERE / "FieldPoints32_2018.shp"

# Output CSV
csv_path = HERE / "FieldPoints32_2018.csv"


# Read shapefile
gdf = gpd.read_file(shp_path)

# OPTIONAL: convert geometry to WKT so it fits cleanly in CSV
gdf["geometry"] = gdf.geometry.to_wkt()

# Write to CSV
gdf.to_csv(csv_path, index=False)

print(f"Saved CSV to: {csv_path}")