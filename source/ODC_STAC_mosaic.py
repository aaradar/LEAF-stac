from datetime import datetime, timedelta

import xarray as xr
import pystac_client
import odc.stac
from dask.diagnostics import ProgressBar

#from odc.geo.geobox import GeoBox
#from rasterio.enums import Resampling


#==================================================================================================
# define a spatial region around Ottawa
#==================================================================================================
ottawa_region = {
    'type': 'Polygon',
    'coordinates': [
       [
         [-76.120,45.184], 
         [-75.383,45.171],
         [-75.390,45.564], 
         [-76.105,45.568], 
         [-76.120,45.184]
       ]
    ]
}



#==================================================================================================
# define a temporal window
# Note: there are a number of different ways to a timeframe. For example, using datetime library or
#       simply a string such as "2020-06-01/2020-09-30"
#==================================================================================================
# Define a timeframe using datetime functions
year = 2020
month = 1

start_date = datetime(year, month, 1)
end_date   = start_date + timedelta(days=31)
timeframe  = start_date.strftime("%Y-%m-%d") + "/" + end_date.strftime("%Y-%m-%d")

# Define a timeframe using a string
#date_range = "2023-01-10/2023-01-20"



def get_resolution(xr_img_coll):
  # Inspect the first item's metadata
  first_item = xr_img_coll[0]
  #print(first_item.to_dict())
  #bands = first_item.assets.keys()
  #print("Available bands in the first item:", bands)

  # Look for the 'proj:transform' or 'gsd' properties to infer resolution  
  if 'proj:transform' in first_item.properties:
    transform = first_item.properties['proj:transform']
    x_resolution = transform[0]
    y_resolution = -transform[4]  # Typically negative
    print(f"Spatial resolution: {x_resolution} x {y_resolution} (in degrees or meters, depending on CRS)")
    return x_resolution
  elif 'gsd' in first_item.properties:
    gsd = first_item.properties['gsd']
    print(f"Spatial resolution: {gsd} (in meters)")
    return gsd
  else:
    print("<get_resolution> Resolution information not found in item metadata")
    return 20




#############################################################################################################
# Description: This function returns a collection of images from a specified catalog and collection based on
#              given spatial region, timeframe and filtering criteria. The returned image collection will be 
#              stored in a xarray.Dataset structure.
#
# Revision history:  2024-May-24  Lixin Sun  Initial creation
# 
#############################################################################################################
def get_STAC_IC(CatalogName, CollName, Region, ProjStr, StartStr, EndStr):
  # use publically available stac link such as
  catalog = pystac_client.Client.open(str(CatalogName)) 
  #catalog = pystac_client.Client.open("https://earth-search.aws.element84.com/v1") 

  # define a temporal window
  timeframe = str(StartStr) + '/' + str(EndStr)

  #==================================================================================================
  # Create a filter for the search based on metadata. The filtering params will depend upon the 
  # image collection we are using. e.g. in case of Sentine 2 L2A, we can use params such as: 
  #
  # eo:cloud_cover
  # s2:dark_features_percentage
  # s2:cloud_shadow_percentage
  # s2:vegetation_percentage
  # s2:water_percentage
  # s2:not_vegetated_percentage
  # s2:snow_ice_percentage, etc.
  # 
  #==================================================================================================
  filters = {"s2:cloud_shadow_percentage": {"lt": 0.9} }

  #==================================================================================================
  # Search and filter a image collection
  #==================================================================================================  
  search_IC = catalog.search(collections = [str(CollName)], 
                             intersects  = Region,                            
                             datetime    = timeframe, 
                             query       = filters,
                             limit       = 200)
  
  items = list(search_IC.items())
  print(f"Found: {len(items):d} datasets")
  
  #first_item = items[0].to_dict()
  #print(first_item)
  #resolu = get_resolution(items)

  '''
  # Spit out data as GeoJSON dictionary  
  print(search_IC.item_collection_as_dict())

  # loop through each item
  for item in search_IC.items_as_dicts():
    print(item)
    print('\n')
  '''
  #==================================================================================================
  # define a geobox for my region
  #==================================================================================================
  #dx = 3/3600  # 90m resolution
  #epsg = 4326
  #geobox = GeoBox.from_bbox(ottawa_region, crs=f"epsg:{epsg}", resolution=dx)

  # lazily combine items
  band_names = ['blue', 'green', 'red', 'nir08', 'swir16', 'swir22', 'scl']

  ds_xr = odc.stac.load(search_IC.items(),
                        bands  = band_names,
                        #groupby='solar_day',
                        chunks = {'time': 5, 'x': 1000, 'y': 1000},
                        crs = ProjStr, 
                        #geobox = ottawa_region,
                        resolution = 100)

  # actually load it
  with ProgressBar():
    ds_xr.load()

  return ds_xr




#############################################################################################################
# Description: This function attaches a score band to each image object in a given image collection.
#
# Revision history:  2024-May-24  Lixin Sun  Initial creation
# 
#############################################################################################################
def attach_score(xr_Img_coll):
  red = xr_Img_coll.red
  blu = xr_Img_coll.blue

  xr_Img_coll['score'] = red - blu

  return xr_Img_coll




'''
# define a mask for valid pixels (non-cloud)
def is_valid_pixel(data):
    # include only vegetated, not_vegitated, water, and snow
    return ((data > 3) & (data < 7)) | (data == 11)

ds_odc['valid'] = is_valid_pixel(ds_odc.scl)
ds_odc.valid.sum("time").plot()

# compute the masked median
rgb_median = (
    ds_odc[['red', 'green', 'blue']]
    .where(ds_odc.valid)
    .to_dataarray(dim="band")
    .transpose(..., "band")
    .median(dim="time")
)
(rgb_median / rgb_median.max() * 2).plot.imshow(rgb="band", figsize=(10, 8))
'''