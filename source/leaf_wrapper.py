import geopandas as gpd
from pathlib import Path
from typing import Dict, List, Union, Optional
from shapely.geometry import mapping
from shapely.geometry import Polygon as ShapelyPolygon
from shapely.ops import unary_union, transform

#############################################################################################################
# Description: This function reads a KML or Shapefile containing polygon/point features and converts them
#              into a LEAF-compatible region dictionary. For shapefiles with Point geometries, a spatial
#              buffer can be applied to convert points to polygons. The output dictionary maps user-defined
#              region names to GeoJSON-like Polygon objects, which can be directly passed to
#              ProdParams['regions'] for mosaic generation.
# 
# Revision history:  2025-January-19  Alexander Radar  Initial creation
#############################################################################################################
def regions_from_kml(kml_file, start=14, end=14, prefix="region", spatial_buffer_m=None):
    """
    Load a KML or Shapefile and return a dict of polygon regions by file ID.
    
    Selects regions at positions [start:end+1] from the sorted file.
    Output keys use the actual file IDs (TARGET_FID, point ID, etc.) for identification.
    
    Parameters:
    -----------
    kml_file : str or Path
        Path to KML or Shapefile
    start : int
        Starting position (0-based, inclusive). Example: start=0 selects the 1st region
    end : int
        Ending position (0-based, inclusive). Example: end=5 selects up to the 6th region
    prefix : str
        Prefix for region names in output
    spatial_buffer_m : float or None
        Buffer size in meters to apply to geometries
    
    Returns:
    --------
    dict
        Regions keyed as "{prefix}{file_id}". Example:
        {'region20': {...}, 'region39': {...}, 'region45': {...}}
    
    Example:
        # File has 10 regions with IDs 20, 39, 45, 47, 52, 53, 57, 61, ...
        regions = regions_from_kml('file.kml', start=1, end=5)
        # Returns: {'region39': {...}, 'region45': {...}, 'region47': {...}, 'region52': {...}, 'region53': {...}}
    """
    if start < 0 or (end is not None and end < start):
        raise ValueError("Invalid start or end values. 'start' must be >= 0 and 'end' must be >= 'start'.")
    
    wrapper = LeafWrapper(kml_file, spatial_buffer_m=spatial_buffer_m).load()
    regions_dict = wrapper.to_region_dict()
    
    # Get sorted keys and select by position
    sorted_keys = sorted(regions_dict.keys())
    # Handle end=None → go to end of file
    if end is None:
        selected_keys = sorted_keys[start:]
    else:
        selected_keys = sorted_keys[start : end + 1]
    
    if not selected_keys:
        raise ValueError(f"No regions found in range [{start}:{end+1}]. File has {len(sorted_keys)} regions.")
    
    
    out = {}
    for key in selected_keys:
        region_data = regions_dict[key]
        file_id = region_data.get("r_id", key)
        coords = region_data.get("coordinates", [])

        if not coords: 
            continue # nothing to output

        # coords is guaranteed to be: [[ring]]
        out[f"{prefix}{file_id}"] = {
            "type": "Polygon",
            "coordinates": coords,
        }

    return out



#############################################################################################################
# Class: LeafWrapper
#
# Description:
#  ------------
#  Wrapper class to load KML or Shapefile polygon/point data into a GeoDataFrame,
#  optionally apply a spatial buffer to point or polygon geometries, and convert
#  the data into a dictionary format compatible with LEAF mosaic generation.
#
#  Features:
#    - Load KML (.kml) or Shapefile (.shp) files
#    - Drop Z coordinates (altitude) to ensure 2D polygons
#    - Apply metric buffer to points or polygons
#    - Convert MultiPolygon geometries to single polygons or bounding boxes
#    - Generate a dictionary suitable for ProdParams['regions']
#
#############################################################################################################
class LeafWrapper:
    def __init__(self, polygon_file, spatial_buffer_m=None):
        self.polygon_file = Path(polygon_file)
        self.gdf = None
        self.spatial_buffer_m = spatial_buffer_m

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
        
        #  Drop Z coordinates if present
        self.gdf["geometry"] = self.gdf.geometry.apply(drop_z) 

        # Apply spatial buffer if specified
        if self.spatial_buffer_m is not None:
            self._apply_buffer()
        
        return self
    
    def _apply_buffer(self):
        """Apply spatial buffer to geometries, converting to appropriate CRS if needed"""
        if self.gdf is None:
            raise ValueError("No data loaded")
        
        original_crs = self.gdf.crs
        
        # Convert to EPSG:3979 (Canada Atlas Lambert) for accurate metric buffering
        if self.gdf.crs is None:
            print("Warning: No CRS defined, assuming EPSG:4326")
            self.gdf = self.gdf.set_crs("EPSG:4326")
        
        # Reproject to EPSG:3979 for buffering
        gdf_projected = self.gdf.to_crs("EPSG:3979")
        
        # Apply buffer
        gdf_projected['geometry'] = gdf_projected.geometry.buffer(self.spatial_buffer_m)
        
        # Convert back to original CRS (or WGS84 if none was set)
        target_crs = original_crs if original_crs is not None else "EPSG:4326"
        self.gdf = gdf_projected.to_crs(target_crs)
        
        print(f"Applied {self.spatial_buffer_m}m buffer to geometries")
        
    def to_region_dict(self, use_target_fid: bool = True) -> Dict[int, Dict]:
        """
        Convert loaded geometries into a LEAF-compatible region dictionary.

        Actions performed:
        -----------------
        - Converts all geometries to EPSG:4326
        - Handles Polygon and MultiPolygon geometries
        - Optionally merges MultiPolygons into a single bounding-box polygon
        - Generates a dictionary keyed by TARGET_FID or other ID columns

        Parameters:
        -----------
        use_target_fid : bool
            If True, use "TARGET_FID" column as the dictionary key (if available)

        Returns:
        --------
        regions : dict
            Dictionary mapping region IDs to GeoJSON-like Polygon data:
            Example: {'region20': {"r_id": 20, "coordinates": [[...]]}, ...}

        Raises:
        -------
        ValueError : If GeoDataFrame is not loaded
        """
        if self.gdf is None:
            raise ValueError("No polygon file loaded. Call `.load()` first.")
    
        # Convert to geographic coordinates (EPSG:4326) for output
        gdf_geo = self.gdf.to_crs("EPSG:4326")
        
        regions = {}
        for idx, row in gdf_geo.iterrows():
            # Priority: TARGET_FID > SiteID > pointid > OBJECTID > index
            key = None
            if use_target_fid and "TARGET_FID" in gdf_geo.columns:
                key = int(row["TARGET_FID"])
            elif "SiteID" in gdf_geo.columns:
                key = int(row["SiteID"])
            elif "pointid" in gdf_geo.columns:
                key = int(row["pointid"])
            elif "OBJECTID" in gdf_geo.columns:
                key = int(row["OBJECTID"])
            elif "id" in gdf_geo.columns:
                key = row["id"]   # keep as string
            else:
                # Fallback to 0-based index
                key = idx if isinstance(idx, int) else 0
    
            geom = row.geometry
            if geom.is_empty:
                continue
    
            coords = []
    
            
            if geom.geom_type == "Polygon":
                ring = [list(pt) for pt in geom.exterior.coords]
                if ring[0] != ring[-1]:
                    ring.append(ring[0])
                coords.append([ring])   # wrap polygon consistently

            elif geom.geom_type == "MultiPolygon":
                for poly in geom.geoms:
                    ring = [list(pt) for pt in poly.exterior.coords]
                    if ring[0] != ring[-1]:
                        ring.append(ring[0])
                    coords.append([ring])  # wrap polygon

            # If there are multiple polygon entries, compute one bounding-box polygon (axis-aligned).
            if len(coords) > 1:
                shapely_polys = []
                for poly in coords:
                    # each `poly` is [[ring]] (you only keep the exterior ring)
                    ring = poly[0]
                    p = ShapelyPolygon(ring)
                    if not p.is_valid:
                        p = p.buffer(0)
                    if not p.is_empty:
                        shapely_polys.append(p)

                if shapely_polys:
                    merged = unary_union(shapely_polys)
                    minx, miny, maxx, maxy = merged.bounds

                    # Build bbox ring (closed)
                    bbox_ring = [
                        [minx, miny],
                        [maxx, miny],
                        [maxx, maxy],
                        [minx, maxy],
                        [minx, miny],
                    ]

                    # Replace coords with a single polygon: [[bbox_ring]]
                    coords = [[bbox_ring]]

            # then proceed to package `regions[key]` as you already do:
            #start_date = row.get("AsssD_1")
            #end_date   = row.get("AsssD_2")

            regions[key] = {
                "r_id": key,
                "coordinates": coords[0],
            # "start_date": start_date,
            # "end_date": end_date
            }

    
        return regions


#############################################################################################################
# Function: drop_z
#
# Description:
#  ------------
#  Removes the Z coordinate (altitude) from a Shapely geometry if present.
#  Converts 3D coordinates (x, y, z) to 2D (x, y), which is required for LEAF regions.
#  If the geometry has no Z coordinate, it is returned unchanged.
#############################################################################################################
def drop_z(geom):
    """Remove the Z coordinate (altitude) from a Shapely geometry"""
    if geom.has_z:
        return transform(lambda x, y, z=None: (x, y), geom)
    return geom