KML Region Support for Mosaic Production Overview
This repo adds native support for defining mosaic processing regions using KML polygon files. Regions are automatically extracted, converted into LEAF-compatible dictionaries, and injected into the mosaic workflow without requiring manual region definitions.





New Module: leaf_wrapper.py

New files: 
afforestationSItesFixed.kml
production_kml_tiles.ipynb
LEAF_STAC_TEST.ipynb
test_regions_kml.py (unused)
updates_readme.txt

Updated files:
Production.py
eoMosiac.py (no changes, new comments, commented out functionality)






Requirements Update:
The requirements.txt file has been updated to document two installation commands:
Install core Python dependencies
Install geopandas and its system-level dependencies separately
This split may potentially avoid common installation issues with geopandas (Installation method that worked for me).
During development, the requirements were installed in two steps following this approach.


The LeafWrapper class (in leaf_wrapper.py)
provides a thin abstraction for reading polygon files (KML or other formats supported by GeoPandas) and converting them into LEAF-compatible region definitions.
Key Features
Reads KML afforestation documents
Loads polygons into a GeoDataFrame
Supports Polygon and MultiPolygon geometries
Generates a region dictionary keyed by TARGET_FID
Preserves temporal metadata from the source file

Output Structure
Each region contains:
{
    "coordinates": [...],
    "start_date": AsssD_1,
    "end_date": AsssD_2
}
Regions can be uniquely identified using the TARGET_FID attribute.






KML Helper Function: regions_from_kml (in leaf_wrapper.py)
A new helper function in production.py converts a KML file into a dictionary suitable for ProdParams["regions"].

Function Behavior
Loads the KML via LeafWrapper
Extracts regions using TARGET_FID
Allows selecting a subset of regions via index range
Outputs LEAF-compatible Polygon definitions

Example Output
{
    "region5": { "type": "Polygon", "coordinates": [...] },
    "region6": { "type": "Polygon", "coordinates": [...] }
}





Automatic KML Detection in MosaicProduction (production.py)
MosaicProduction now detects when ProdParams["regions"] is a string ending in .kml.
When detected, the KML is automatically converted before UsedParams is created.






Example Notebook
production_kml_tiles.ipynb
Fully configured example workflow
Uses the included afforestation KML file
Demonstrates tiled mosaic production driven entirely by KML regions
production.ipynb is also edited to manually call the kml file
Dask local environment should run when Jupyter file is ran