# leaf_wrapper.py - Region Dictionary Generator

Native support for defining mosaic regions using **KML polygon files** and **Shapefiles**. Automatically converts geometries to LEAF-compatible dictionaries for mosaic production.

---

## What's New

**New Files:**
- `source/leaf_wrapper.py` — Region conversion module
- `AfforestationSItesFixed.kml` — Example KML file with afforestation polygons
- `Sample Points/FieldPoints32_2018.shp` — Example Shapefile with point geometries
- `production_kml_tiles.ipynb` — KML-driven production example
- `test_leaf_wrapper.py` — Output validation tests for both KML and SHP

**Updated Files:**
- `Production.py` — Automatic KML/SHP detection
- `requirements.txt` — Two-step GeoPandas installation

---

## Installation

**Step 1: Core dependencies**
```bash
conda install -c conda-forge click==8.1.7 dask==2024.5.2 dask-jobqueue==0.9.0 numpy==1.24.4 odc-geo==0.4.8 odc-stac==0.3.10 pandas==2.2.3 psutil==5.9.8 pyproj==3.6.1 pystac-client==0.8.2 rasterio==1.3.10 Requests==2.32.3 rioxarray==0.15.6 stackstac==0.5.1 tqdm==4.66.4 urllib3==2.3.0 xarray==2024.6.0 "bokeh!=3.0.*,>=2.4.2" gdal
```

**Step 2: GeoPandas and spatial libraries**
```bash
conda install -c conda-forge geopandas shapely pyogrio packaging
```

---

## Quick Usage

### Extract Regions from KML/SHP

```python
from source.leaf_wrapper import regions_from_kml

# Load KML regions at positions 1-5
regions = regions_from_kml(
    'AfforestationSItesFixed.kml',
    start=1,
    end=5
)
# Returns: {'region39': {...}, 'region45': {...}, 'region47': {...}, 'region52': {...}, 'region53': {...}}

# Load shapefile points with 300m buffer
regions = regions_from_kml(
    'field_points.shp',
    start=0,
    end=4,
    spatial_buffer_m=300,
    prefix="site"
)
# Returns: {'site12113': {...}, 'site12114': {...}, ...}
```

### Automatic Detection in MosaicProduction

```python
from source.LEAFProduction import MosaicProduction

ProdParams = {
    'regions': 'AfforestationSItesFixed.kml',  # File path automatically detected
    'year': 2023,
    'months': [5, 6, 7, 8, 9, 10],
    'resolution': 20,
    'projection': 'EPSG:3979'
}

mosaic = MosaicProduction(ProdParams, CompParams)
```

When `regions` is a `.kml` or `.shp` file path, it's automatically converted to a region dictionary. For SHP files with points, a 300m square buffer is applied.

---

## Function Reference

### `regions_from_kml()`

```python
regions_from_kml(kml_file, start=0, end=2, prefix="region", spatial_buffer_m=None) -> dict
```

**Parameters:**
- `kml_file` (str/Path): Path to KML or Shapefile
- `start` (int): Starting position (0-based, inclusive). Default: 0
- `end` (int): Ending position (0-based, inclusive). Default: 2
- `prefix` (str): Key prefix in output. Default: "region"
- `spatial_buffer_m` (float): Buffer size in meters for Point geometries. Default: None

**Returns:** Dictionary with keys `{prefix}{region_id}` and GeoJSON Polygon values

**Example Output:**
```json
{
  "region39": {
    "type": "Polygon",
    "coordinates": [[[-80.287, 44.353], [-80.286, 44.353], ...]]
  }
}
```

---

## LeafWrapper Class

Handles file loading and geometry conversion.

```python
from source.leaf_wrapper import LeafWrapper

wrapper = LeafWrapper('regions.kml', spatial_buffer_m=300).load()
regions = wrapper.to_region_dict()
```

**Methods:**
- `.load()` — Load file and apply buffering if specified
- `.to_region_dict()` — Convert to LEAF format (outputs EPSG:4326 coordinates)

---

## Key Features

**Multiple ID fields** — Detects TARGET_FID, SiteID, OBJECTID automatically  
**Geographic output** — All coordinates in EPSG:4326 (WGS84)  
**Point buffering** — Creates square bounding boxes for Point geometries  
**Position-based selection** — Select regions by index range, not ID  
**Metadata preservation** — Extracts start_date, end_date from attributes  

---

## Running Notebooks

### production_kml_tiles.ipynb (RECOMMENDED)
```bash
jupyter notebook production_kml_tiles.ipynb
```
Fully configured KML-driven mosaic production with Dask local cluster.

### production.ipynb
```bash
jupyter notebook production.ipynb
```
Standard workflow with automatic KML/SHP region detection.

---

## Testing

Validate region extraction before production:

```bash
python test_leaf_wrapper.py
```

Tests both KML and Shapefile outputs for:
- Correct file ID keys (e.g., region39, region12113)
- Geographic coordinates (EPSG:4326)
- Valid GeoJSON Polygon structure
- Proper ring closure

---

## Supported Formats

| Format | Geometry | ID Priority | Buffer |
|--------|----------|-------------|--------|
| KML | Polygon/MultiPolygon | TARGET_FID | Yes |
| SHP | Point/Polygon | TARGET_FID → SiteID → OBJECTID | Yes (square bbox) |

---

## Error Handling

| Error | Solution |
|-------|----------|
| `FileNotFoundError` | Check path is absolute or correct relative to notebook |
| `ValueError: Invalid start/end` | Verify start <= end and start >= 0 |
| `ValueError: No regions found` | Increase `end` value |
| `ModuleNotFoundError: geopandas` | Run `pip install geopandas` |

---

## Coordinate Conversion

All output coordinates automatically converted to **EPSG:4326 (WGS84)**:
- KML → EPSG:4326
- Shapefile → EPSG:4326 
- Buffering → Performed in EPSG:3979, converted to EPSG:4326 

---

## Next Steps

1. **Test regions**: Run `test_leaf_wrapper.py` to validate file format
2. **Use in notebooks**: See `production_kml_tiles.ipynb` for full workflow
3. **Integrate**: Pass file path to `ProdParams['regions']` in Production.py
4. **Monitor**: View Dask Dashboard during execution (http://localhost:8787)
