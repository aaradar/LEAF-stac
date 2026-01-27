"""
Simple test script for leaf_wrapper.py region dictionary creation
Tests both KML and Shapefile inputs with start=1, end=5
"""

from pathlib import Path
import json
from source.leaf_wrapper import regions_from_kml

# Get absolute paths
base_dir = Path(__file__).parent
kml_path = base_dir / "AfforestationSItesFixed.kml"
shp_path = base_dir / "Sample Points" / "FieldPoints32_2018.shp"

# Test KML
print("="*60)
print("KML REGIONS (positions 1-5)")
print("="*60)

try:
    kml_regions = regions_from_kml(str(kml_path), start=1, end=5)
    print(f"[OK] Loaded {len(kml_regions)} KML regions")
    print(json.dumps(kml_regions, indent=2))
except Exception as e:
    print(f"[ERROR] {e}")

# Test SHP with 300m buffer
print("="*60)
print("SHAPEFILE REGIONS (positions 1-5, 300m buffer)")
print("="*60)

try:
    shp_regions = regions_from_kml(str(shp_path), start=1, end=5, spatial_buffer_m=300)
    print(f"[OK] Loaded {len(shp_regions)} SHP regions")
    print(json.dumps(shp_regions, indent=2))
except Exception as e:
    print(f"[ERROR] {e}")

