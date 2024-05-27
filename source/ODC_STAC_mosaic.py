from datetime import datetime, timedelta

import xarray as xr
import pystac_client
import odc.stac
from dask.diagnostics import ProgressBar

#from odc.geo.geobox import GeoBox
#from rasterio.enums import Resampling

UNKNOWN_sensor = 0
LS5_sensor     = 5
LS7_sensor     = 7
LS8_sensor     = 8
LS9_sensor     = 9
LS_sensor      = 19
MAX_LS_CODE    = 20
S2A_sensor     = 21
S2B_sensor     = 22
S1B_sensor     = 41
S1B_sensor     = 42


MOD_sensor     = 50     # MODIS sensor
HLS_sensor     = 100    # Harmonized Landsat and Sentinel-2



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


tile55_922 = {
    'type': 'Polygon',
    'coordinates': [
       [
         [-77.6221, 47.5314], 
         [-73.8758, 46.7329],
         [-75.0742, 44.2113], 
         [-78.6303, 44.9569],
         [-77.6221, 47.5314]
       ]
    ]
}





#############################################################################################################
# Description: This function returns a boundary box [min(longs), min(lats), max(longs), max(lats)] based on 
#              a given geographic region defined by Lats/Longs.
#
# Revision history:  2024-May-27  Lixin Sun  Initial creation
# 
#############################################################################################################
def get_region_bbox(inRegion):
  coords = inRegion['coordinates'][0]
  nCoords = len(coords)
   
  longs = []
  lats  = []

  for i in range(nCoords):
    longs.append(coords[i][0]) 
    lats.append(coords[i][1])

  return [min(longs), min(lats), max(longs), max(lats)]




#############################################################################################################
# Description: This function divides the bbox associated with a given geographic region (defined by 
#              Lats/Longs) into a number of sub-bboxes
#
# Revision history:  2024-May-27  Lixin Sun  Initial creation
# 
#############################################################################################################
def divide_region(inRegion, nDivides):
  if nDivides <= 0:
    nDivides = 2

  # Obtain the bbox of the given geographic region 
  bbox = get_region_bbox(inRegion)
  left_lon = bbox[0]
  btom_Lat = bbox[1]

  lon_delta = (bbox[2] - left_lon)/nDivides
  lat_delta = (bbox[3] - btom_Lat)/nDivides

  sub_regions = []  
  for i in range(nDivides):    
    for j in range(nDivides):
      sub_region = []
      BL_lon = left_lon+i*lon_delta
      BL_lat = btom_Lat+j*lat_delta

      sub_region.append([BL_lon, BL_lat])
      sub_region.append([BL_lon+lon_delta, BL_lat])
      sub_region.append([BL_lon+lon_delta, BL_lat+lat_delta])
      sub_region.append([BL_lon, BL_lat+lat_delta])
      sub_region.append([BL_lon, BL_lat])
  
      sub_regions.append(sub_region)
  
  return sub_regions





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
  bands = first_item.assets.keys()
  print("Available bands in the first item:", bands)

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
def get_query_conditions(ssr_code, StartStr, EndStr):
  query_conds = {}
  
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
  query_conds['filters'] = {"s2:cloud_shadow_percentage": {"lt": 0.9} }

  if ssr_code > MAX_LS_CODE & ssr_code < MOD_sensor:
    query_conds['catalog']    = "https://earth-search.aws.element84.com/v1"
    query_conds['collection'] = "sentinel-2-l2a"
    query_conds['timeframe']  = str(StartStr) + '/' + str(EndStr)
    query_conds['bands']      = ['blue', 'green', 'red', 'nir08', 'swir16', 'swir22', 'scl']

  return query_conds





#############################################################################################################
# Description: This function returns a base image.
#
# Revision history:  2024-May-24  Lixin Sun  Initial creation
# 
#############################################################################################################
def get_base_Image(S2A_sensor, Region, ProjStr, Scale, StartStr, EndStr):
  # get all query conditions 
  query_conds = get_query_conditions(S2A_sensor, StartStr, EndStr)

  # use publically available stac link such as
  catalog = pystac_client.Client.open(str(query_conds['catalog'])) 

  #==================================================================================================
  # Search and filter a image collection
  #==================================================================================================  
  search_IC = catalog.search(collections = [str(query_conds['collection'])], 
                             intersects  = Region,                            
                             datetime    = str(query_conds['timeframe']), 
                             query       = query_conds['filters'],
                             limit       = 1)
  
  items = list(search_IC.items())
  print(f"Found: {len(items):d} datasets")
  
  #==================================================================================================
  # define a geobox for my region
  #==================================================================================================
  # lazily combine items
  mybbox = get_region_bbox(Region)
  print('<get_STAC_ImColl> The bbox of the given region = ', mybbox)

  ds_xr = odc.stac.load([items[0], items[1]],
                        bands  = query_conds['bands'],
                        chunks = {'x': 1000, 'y': 1000},
                        crs    = ProjStr, 
                        bbox   = mybbox,
                        resolution = Scale)

  # actually load it
  with ProgressBar():
    ds_xr.load()

  return ds_xr.isel(time=0)





#############################################################################################################
# Description: This function returns a collection of images from a specified catalog and collection based on
#              given spatial region, timeframe and filtering criteria. The returned image collection will be 
#              stored in a xarray.Dataset structure.
#
# Revision history:  2024-May-24  Lixin Sun  Initial creation
# 
#############################################################################################################
def get_STAC_ImColl(S2A_sensor, Region, ProjStr, Scale, StartStr, EndStr, GroupBy=True):
  # get all query conditions 
  query_conds = get_query_conditions(S2A_sensor, StartStr, EndStr)

  # use publically available stac link such as
  catalog = pystac_client.Client.open(str(query_conds['catalog'])) 

  #==================================================================================================
  # Search and filter a image collection
  #==================================================================================================  
  search_IC = catalog.search(collections = [str(query_conds['collection'])], 
                             intersects  = Region,                            
                             datetime    = str(query_conds['timeframe']), 
                             query       = query_conds['filters'],
                             limit       = 200)
  
  items = list(search_IC.items())
  print(f"Found: {len(items):d} datasets")
  
  #first_item = items[0].to_dict()
  #print(first_item)
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
  mybbox = get_region_bbox(Region)
  print('<get_STAC_ImColl> The bbox of the given region = ', mybbox)

  ds_xr = odc.stac.load(search_IC.items(),
                        bands  = query_conds['bands'],
                        groupby='solar_day',  #For lower latitude scenses, save a lot memories
                        chunks = {'x': 1000, 'y': 1000},
                        crs    = ProjStr, 
                        bbox   = mybbox,
                        resolution = Scale)

  return ds_xr  

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
def attach_score(inImg):
  red = inImg.red
  nir = inImg.nir08

  inImg['score'] = (nir - red)/(nir + red)

  return inImg





#############################################################################################################
# Description: This function returns a mosaic image for a sub-region
#
# Revision history:  2024-May-24  Lixin Sun  Initial creation
# 
#############################################################################################################
def get_sub_mosaic(S2A_sensor, SubRegion, ProjStr, Scale, StartStr, EndStr):
  # get all query conditions 
  query_conds = get_query_conditions(S2A_sensor, StartStr, EndStr)

  # use publically available stac link such as
  catalog = pystac_client.Client.open(str(query_conds['catalog'])) 

  #==================================================================================================
  # Search and filter a image collection
  #==================================================================================================  
  search_IC = catalog.search(collections = [str(query_conds['collection'])], 
                             intersects  = SubRegion,                            
                             datetime    = str(query_conds['timeframe']), 
                             query       = query_conds['filters'],
                             limit       = 200)
  
  items = list(search_IC.items())
  print(f"Found: {len(items):d} datasets")
    
  #==================================================================================================
  # lazily combine items
  #==================================================================================================
  mybbox = get_region_bbox(SubRegion)
  print('<get_STAC_ImColl> The bbox of the given region = ', mybbox)

  raw_IC = odc.stac.load(search_IC.items(),
                        bands  = query_conds['bands'],
                        groupby='solar_day',  #For lower latitude scenses, save a lot memories
                        chunks = {'x': 1000, 'y': 1000},
                        crs    = ProjStr, 
                        bbox   = mybbox,
                        resolution = Scale)

  # actually load it
  with ProgressBar():
    raw_IC.load()

  #==================================================================================================
  # Apply default pixel mask to each of the images
  #==================================================================================================
  scl = raw_IC.scl
  condition = (raw_IC > 0) & (scl != 3) & (scl != 8) & (scl != 9)  # & (scl != 10)
  masked_IC = raw_IC.where(condition)
  
  #==================================================================================================
  # Apply default pixel mask to each of the images
  #==================================================================================================
  scored_IC = attach_score(masked_IC)

  return scored_IC.isel(time=0)
  base_img = base_img.where(base_img.score > reindexed_sub_mosaic.score, base_img, reindexed_sub_mosaic)


  return masked_IC.median(dim='time')



#############################################################################################################
# Description: This function returns a mosaic image for a sub-region
#
# Revision history:  2024-May-24  Lixin Sun  Initial creation
# 
#############################################################################################################
def period_mosaic(S2A_sensor, Region, ProjStr, Scale, StartStr, EndStr):
  #==========================================================================================================
  # Create a base image that has full spatial dimensions of the specified region
  #==========================================================================================================
  base_img = get_base_Image(S2A_sensor, Region, ProjStr, Scale, StartStr, EndStr)
  
  base_img = attach_score(base_img)*0.0
  base_img = base_img.where(base_img > 0)
  
  #==========================================================================================================
  # Create individual sub-mosaic and combine it into base image based on score
  #==========================================================================================================
  sub_regions = divide_region(Region, 3)

  for sub_region in sub_regions:
    print('<period_mosaic> create a sub-mosaic for ', sub_region)
    sub_polygon = {'type': 'Polygon',  'coordinates': [sub_region] }

    sub_mosaic = get_sub_mosaic(S2A_sensor, sub_polygon, ProjStr, Scale, StartStr, EndStr)
    
    sub_mosaic = attach_score(sub_mosaic)
    sub_mosaic = sub_mosaic.where(sub_mosaic > 0)    

    #base_img = xr.merge([base_img, sub_mosaic], compat='override') # Didn't work
    base_img = base_img.combine_first(sub_mosaic)

  return base_img
  






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

