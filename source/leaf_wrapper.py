import geopandas as gpd
from pathlib import Path
from typing import Dict, List, Union, Optional
from shapely.geometry import mapping

#############################################################################################################
# Description: This function reads a KML or Shapefile containing polygon/point features and converts them
#              into a LEAF-compatible region dictionary. For shapefiles with Point geometries, a spatial
#              buffer can be applied to convert points to polygons. The output dictionary maps user-defined
#              region names to GeoJSON-like Polygon objects, which can be directly passed to
#              ProdParams['regions'] for mosaic generation.
#
#############################################################################################################
def regions_from_kml(kml_file, start=0, end=2, prefix="region", spatial_buffer_m=None):
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
    if start < 0 or end < start:
        raise ValueError("Invalid start or end values. 'start' must be >= 0 and 'end' must be >= 'start'.")
    
    wrapper = LeafWrapper(kml_file, spatial_buffer_m=spatial_buffer_m).load()
    regions_dict = wrapper.to_region_dict()
    
    # Get sorted keys and select by position
    sorted_keys = sorted(regions_dict.keys())
    selected_keys = sorted_keys[start : end + 1]
    
    if not selected_keys:
        raise ValueError(f"No regions found in range [{start}:{end+1}]. File has {len(sorted_keys)} regions.")
    
    out = {}
    for key in selected_keys:
        region_data = regions_dict[key]
        # Use the actual ID/key from the file (TARGET_FID, point ID, etc.)
        out[f"{prefix}{region_data['id']}"] = {
            "type": "Polygon",
            "coordinates": region_data["coordinates"],
        }

    return out


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
        Convert loaded geometries to dict suitable for ProdParams['regions'].
        Keys: TARGET_FID if available, otherwise index (0-based).
        Values: {'coordinates': [...], 'start_date': ..., 'end_date': ...}
        Handles Polygon and MultiPolygon geometries.
        All coordinates are converted to EPSG:4326 (geographic).
        """
        if self.gdf is None:
            raise ValueError("No polygon file loaded. Call `.load()` first.")
    
        # Convert to geographic coordinates (EPSG:4326) for output
        gdf_geo = self.gdf.to_crs("EPSG:4326")
        
        regions = {}
        for idx, row in gdf_geo.iterrows():
            # Priority: TARGET_FID > SiteID > index
            key = None
            if use_target_fid and "TARGET_FID" in gdf_geo.columns:
                key = int(row["TARGET_FID"])
            elif "SiteID" in gdf_geo.columns:
                key = int(row["SiteID"])
            elif "OBJECTID" in gdf_geo.columns:
                key = int(row["OBJECTID"])
            else:
                # Fallback to 0-based index
                key = idx if isinstance(idx, int) else 0
    
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
                raise ValueError(f"Unsupported geometry type: {geom.geom_type}. Only Polygon and MultiPolygon are supported after buffering.")
    
            start_date = row.get("AsssD_1")
            end_date   = row.get("AsssD_2")
    
            regions[key] = {
                "id": key,
                "coordinates": coords,
                "start_date": start_date,
                "end_date": end_date
            }
    
        return regions
'''
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

'''
