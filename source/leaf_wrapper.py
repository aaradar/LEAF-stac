import geopandas as gpd
from pathlib import Path   # <-- add this
from typing import Dict, List, Union
from shapely.geometry import mapping

#############################################################################################################
# Description: This function reads a KML file containing multiple polygon features and converts them
#              into a LEAF-compatible region dictionary. The output dictionary maps user-defined
#              region names to GeoJSON-like Polygon objects, which can be directly passed to
#              ProdParams['regions'] for mosaic generation.
#
#############################################################################################################
def regions_from_kml(kml_file, start=5,  end=7, prefix="region"):
    """
    Load a KML and return a dict of polygon regions:
    {
        'region1': {...},
        'region2': {...},
        ...
    }
    """
    if start < 1 or end < start:
        raise ValueError("Invalid start or end values. 'start' must be >= 1 and 'end' must be >= 'start'.")
    
    wrapper = LeafWrapper(kml_file).load()
    regions_dict = wrapper.to_region_dict()  # keys = TARGET_FID

    out = {}
    for i in range(start, min(end, len(regions_dict)) + 1):
        region_data = regions_dict[i]
        out[f"{prefix}{i}"] = {
            "type": "Polygon",
            "coordinates": region_data["coordinates"],
        }

    return out

class LeafWrapper:
    def __init__(self, polygon_file):
        self.polygon_file = Path(polygon_file)
        self.gdf = None

    def load(self):
        """Load the polygon file into a GeoDataFrame"""
        if not self.polygon_file.exists():
            raise FileNotFoundError(f"Polygon file not found: {self.polygon_file}")

        ext = self.polygon_file.suffix.lower()
        if ext == ".kml":
            self.gdf = gpd.read_file(self.polygon_file, driver="KML")
        else:
            self.gdf = gpd.read_file(self.polygon_file)

        if self.gdf.empty:
            raise ValueError("Loaded polygon file contains no geometries.")
        return self
        
    def to_region_dict(self, use_target_fid: bool = True) -> Dict[int, Dict]:
        """
        Convert loaded geometries to dict suitable for ProdParams['regions'].
        Keys: TARGET_FID or index.
        Values: {'coordinates': [...], 'start_date': ..., 'end_date': ...}
        Handles Polygon and MultiPolygon geometries.
        """
        if self.gdf is None:
            raise ValueError("No polygon file loaded. Call `.load()` first.")
    
        regions = {}
        for idx, row in self.gdf.iterrows():
            key = int(row["TARGET_FID"]) if use_target_fid and "TARGET_FID" in row else idx
    
            geom = row.geometry
            if geom.is_empty:
                continue
    
            coords = []
    
            if geom.geom_type == "Polygon":
                # single polygon
                ring_coords = [list(pt) for pt in geom.exterior.coords]
                if ring_coords[0] != ring_coords[-1]:
                    ring_coords.append(ring_coords[0])
                coords.append(ring_coords)
    
            elif geom.geom_type == "MultiPolygon":
                # multiple polygons
                for poly in geom.geoms:
                    ring_coords = [list(pt) for pt in poly.exterior.coords]
                    if ring_coords[0] != ring_coords[-1]:
                        ring_coords.append(ring_coords[0])
                    coords.append(ring_coords)
    
            else:
                raise ValueError(f"Unsupported geometry type: {geom.geom_type}")
    
            start_date = row.get("AsssD_1")
            end_date   = row.get("AsssD_2")
    
            regions[key] = {
                "coordinates": coords,
                "start_date": start_date,
                "end_date": end_date
            }
    
        return regions
