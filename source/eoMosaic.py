# import pystac_client
# import odc.stac
# import xarray as xr
# import dask.diagnostics as ddiag


# DestProj   = 'epsg:3979'   # Define a destination projection. Lat/lon EPSG code = epsg:4326. 
# geo_region = {'type': 'Polygon', 'coordinates': [[[-76.120,45.184], [-75.383,45.171], [-75.390,45.564], [-76.105,45.568], [-76.120,45.184]]]}
# #LatLon_bbox = eoUs.get_region_bbox(Region)
# xy_bbox    = eoUs.get_region_bbox(geo_region, DestProj)  # Obtain the bbox of a syudy area in destination coordinate system 

# catalog = pystac_client.Client.open("https://earth-search.aws.element84.com/v1")  # Connect to the STAC Catalog on AWS

# # Obtain the metadata on a collection of Sentinel-2 images
# collection = catalog.search(collections = ["sentinel-2-l2a"],                  # A specific collection name
#                             intersects  = geo_region,                          # Spatial region 
#                             datetime    = "2021-06-01/2021-06-16",             # Time window
#                             query       = {"eo:cloud_cover": {"lt": 85.0} },   # Filter criteria
#                             limit       = 100)                                 

# # Define how to download a set/subset of images
# xrDS = odc.stac.load(list(collection.items()),
#                       bands         = ['blue', 'green', 'red', 'nir08', 'swir16', 'swir22'],
#                       chunks        = {'x': 1000, 'y': 1000},
#                       crs           = DestProj,
#                       fail_on_error = False,
#                       resolution    = 20, 
#                       #bbox         = LatLon_bbox,
#                       x             = (xy_bbox[0], xy_bbox[2]),
#                       y             = (xy_bbox[3], xy_bbox[1]))
  
# # Actually load images into memory (stored in an Xarray.Dataset object) using DASK for parallel processing 
# with ddiag.ProgressBar():
#   xrDS.load()


# median  = xrDS.median(dim='time') 

# blu_img = xrDS['blue']
# grn_img = xrDS['green']

# max_SV = xr.apply_ufunc(np.maximum, blu_img, grn_img)

# time_values = xrDS.coords['time'].values  

# print(xrDS.data_vars)

# max_indices = xrDS[eoIM.pix_score].argmax(dim='time')
# mosaic      = xrDS.isel(time=max_indices)



import os
import math
import time
from datetime import datetime
import numpy as np
import pandas as pd
import requests
import xml.etree.ElementTree as ET

import xarray as xr
import pystac_client as psc
import odc.stac
import dask.diagnostics as ddiag
import dask
#from joblib import Parallel
#from joblib import delayed
import concurrent.futures

from collections import defaultdict

# certificate_path = "C:/Users/lsun/nrcan_azure_amazon.cer"
# if os.path.exists(certificate_path):
#   stac_api_io = psc.stac_api_io.StacApiIO()
#   stac_api_io.session.verify = certificate_path
#   print("stac_api_io.session.verify = {}".format(stac_api_io.session.verify))
# else:
#   print("Certificate file {} does not exist:".format(certificate_path))

odc.stac.configure_rio(cloud_defaults = True, GDAL_HTTP_UNSAFESSL = 'YES')

# Set the temporary directory for Dask
#dask.config.set({'temporary-directory': 'M:/Dask_tmp'})

# The two things must be noted:
# (1) this line must be used after "import odc.stac"
# (2) This line is necessary for exporting a xarray dataset object into separate GeoTiff files,
#     even it is not utilized directly
import rioxarray

import eoImage as eoIM
import eoUtils as eoUs
import eoParams as eoPM



#==================================================================================================
# define a spatial region around Ottawa
#==================================================================================================
# ottawa_region = {'type': 'Polygon', 'coordinates': [[[-76.120,45.184], [-75.383,45.171], [-75.390,45.564], [-76.105,45.568], [-76.120,45.184]]]}

# tile55_922 = {'type': 'Polygon', 'coordinates': [[[-77.6221, 47.5314], [-73.8758, 46.7329], [-75.0742, 44.2113], [-78.6303, 44.9569], [-77.6221, 47.5314]]]}




#==================================================================================================
# define a temporal window
# Note: there are a number of different ways to a timeframe. For example, using datetime library or
#       simply a string such as "2020-06-01/2020-09-30"
#==================================================================================================
# Define a timeframe using datetime functions
# year = 2020
# month = 1

# start_date = datetime(year, month, 1)
# end_date   = start_date + timedelta(days=31)
# timeframe  = start_date.strftime("%Y-%m-%d") + "/" + end_date.strftime("%Y-%m-%d")




#############################################################################################################
# Description: This function returns a collection of images from a specified catalog and collection based on
#              given spatial region, timeframe and filtering criteria. The returned image collection will be 
#              stored in a xarray.Dataset structure.
#
# Note: (1) It seems that you can retrieve catalog info on AWS Landsat collection, but cannot access assets.
#
# Revision history:  2024-May-24  Lixin Sun  Initial creation
# 
#############################################################################################################
def get_query_conditions(SsrData, StartStr, EndStr):
  ssr_code = SsrData['SSR_CODE']
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
  # For many other collections, the Microsoft Planetary Computer has a STAC server at 
  # https://planetarycomputer-staging.microsoft.com/api/stac/v1 (this info comes from 
  # https://www.matecdev.com/posts/landsat-sentinel-aws-s3-python.html)
  #==================================================================================================  
  if ssr_code > eoIM.MAX_LS_CODE and ssr_code < eoIM.MOD_sensor:
    query_conds['catalog']    = "https://earth-search.aws.element84.com/v1"
    query_conds['collection'] = "sentinel-2-l2a"
    query_conds['timeframe']  = str(StartStr) + '/' + str(EndStr)
    query_conds['bands']      = SsrData['ALL_BANDS'] + ['scl']
    query_conds['filters']    = {"eo:cloud_cover": {"lt": 85.0} }    

  elif ssr_code < eoIM.MAX_LS_CODE and ssr_code > 0:
    #query_conds['catalog']    = "https://landsatlook.usgs.gov/stac-server"
    #query_conds['collection'] = "landsat-c2l2-sr"
    query_conds['catalog']    = "https://earth-search.aws.element84.com/v1"
    query_conds['collection'] = "landsat-c2-l2"
    query_conds['timeframe']  = str(StartStr) + '/' + str(EndStr)
    #query_conds['bands']      = ['OLI_B2', 'OLI_B3', 'OLI_B4', 'OLI_B5', 'OLI_B6', 'OLI_B7', 'qa_pixel']
    query_conds['bands']      = ['blue', 'green', 'red', 'nir08', 'swir16', 'swir22', 'qa_pixel']
    query_conds['filters']    = {"eo:cloud_cover": {"lt": 85.0}}  
  elif ssr_code == eoIM.HLS_sensor:
    query_conds['catalog']    = "https://cmr.earthdata.nasa.gov/stac/LPCLOUD"
    query_conds['collection'] = "HLSL30.v2.0"
    query_conds['timeframe']  = str(StartStr) + '/' + str(EndStr)
    query_conds['bands']      = ['blue', 'green', 'red', 'nir08', 'swir16', 'swir22', 'qa_pixel']
    query_conds['filters']    = {"eo:cloud_cover": {"lt": 85.0}}  

  return query_conds




#############################################################################################################
# Description: This function returns average view angles (VZA and VAA) for a given STAC item/scene
#
# Revision history:  2024-Jul-23  Lixin Sun  Initial creation
# 
#############################################################################################################
def get_View_angles(StacItem):
  '''StacItem: a item obtained from the STAC catalog at AWS'''
  assets = dict(StacItem.assets.items())
  granule_meta = assets['granule_metadata']
  response = requests.get(granule_meta.href)
  response.raise_for_status()  # Check that the request was successful

  # Parse the XML content
  root = ET.fromstring(response.content)
  
  view_angles = {}
  elem = root.find(".//Mean_Viewing_Incidence_Angle[@bandId='8']")
  view_angles['vza'] = float(elem.find('ZENITH_ANGLE').text)
  view_angles['vaa'] = float(elem.find('AZIMUTH_ANGLE').text)    
  
  return view_angles
   




def display_meta_assets(stac_items):
  first_item = stac_items[0]

  print('<<<<<<< The assets associated with an item >>>>>>>\n' )
  for asset_key, asset in first_item.assets.items():
    #print(f"Band: {asset_key}, Description: {asset.title or 'No title'}")
    print(f"Asset key: {asset_key}, title: {asset.title}, href: {asset.href}")    

  print('<<<<<<< The meta data associated with an item >>>>>>>\n' )
  print("ID:", first_item.id)
  print("Geometry:", first_item.geometry)
  print("Bounding Box:", first_item.bbox)
  print("Datetime:", first_item.datetime)
  print("Properties:")

  for key, value in first_item.properties.items():
    print(f"  <{key}>: {value}")




#############################################################################################################
# Description: This function returns the results of searching a STAC catalog
#
# Revision history:  2024-May-24  Lixin Sun  Initial creation
#                    2024-Jul-12  Lixin Sun  Added a filter to retain only one image from the items with
#                                            identical timestamps.
#
#############################################################################################################
def search_STAC_Catalog(Region, Criteria, MaxImgs):
  '''
    Args:
      Region(): A spatial region;

  '''
  #==========================================================================================================
  # use publically available stac 
  #==========================================================================================================
  catalog = psc.client.Client.open(str(Criteria['catalog'])) 

  #==========================================================================================================
  # Search and filter a image collection
  #==========================================================================================================
  print('<search_STAC_Images> The given region = ', Region)
  stac_catalog = catalog.search(collections = [str(Criteria['collection'])], 
                                intersects  = Region,                           
                                datetime    = str(Criteria['timeframe']), 
                                query       = Criteria['filters'],
                                limit       = MaxImgs)        
    
  stac_items = list(stac_catalog.items())
    
  #==========================================================================================================
  # Ingest imaging geometry angles into each STAC item
  #==========================================================================================================
  stac_items, angle_time = ingest_Geo_Angles(stac_items)
  print('\n<search_STAC_Catalog> The total elapsed time for ingesting angles = %6.2f minutes'%(angle_time))

  return stac_items
    




#############################################################################################################
# Description: This function returns a list of unique tile names contained in a given "StatcItems" list.
#
# Revision history:  2024-Jul-17  Lixin Sun  Initial creation
# 
#############################################################################################################
def get_unique_tile_names(StacItems):
  '''
    Args:
      StacItems(List): A list of stac items. '''
  stac_items = list(StacItems)
  unique_names = []

  if len(stac_items) < 2:
    return unique_names  
  
  unique_names.append(stac_items[0].properties['grid:code'])  
  for item in stac_items:
    new_tile = item.properties['grid:code']
    found_flag = False
    for name in unique_names:
      if new_tile == name:
        found_flag = True
        break
    
    if found_flag == False:
      unique_names.append(new_tile)

  return unique_names 






#############################################################################################################
# Description: This function returns a list of unique STAC items by remaining only one item from those that 
#              share the same timestamp.
#
# Revision history:  2024-Jul-17  Lixin Sun  Initial creation
# 
#############################################################################################################
def get_unique_STAC_items(inSTACItems):
  '''
     Args:
        inSTACItems(): A given list of STAC items to be filtered based on timestamps.'''
 
  #==========================================================================================================
  # Retain only one image from the items with identical timestamps
  #==========================================================================================================
  # Create a dictionary to store items by their timestamp
  items_by_id = defaultdict(list)

  # Create a new dictionary with the core image ID as keys
  for item in inSTACItems:    
    tokens = str(item.id).split('_')   #core image ID
    id = tokens[0] + '_' + tokens[1] + '_' + tokens[2]
    items_by_id[id].append(item)
  
  # Iterate through the items and retain only one item per timestamp
  unique_items = []
  for id, item_group in items_by_id.items():
    # Assuming we keep the first item in each group
    unique_items.append(item_group[0])

  return unique_items





#############################################################################################################
# Description: This function returns a list of item names corresponding to a specified MGRS/Snetinel-2 tile.
#
# Note: this function is specifically for Sentinel-2 data, because other dataset might not have 'grid:code'
#       property.
#
# Revision history:  2024-Jul-17  Lixin Sun  Initial creation
# 
#############################################################################################################
def get_one_granule_items(StacItems, GranuleName):
  stac_items = list(StacItems)
  tile_items = []

  if len(stac_items) < 2:
    return tile_items  
  
  for item in stac_items:
    if GranuleName == item.properties['grid:code']:
      tile_items.append(item)

  return tile_items 




def ingest_Geo_Angles(StacItems):
  startT = time.time()
  #==========================================================================================================
  # Confirm the given item list is not empty
  #==========================================================================================================
  nItems = len(StacItems)
  if nItems < 1:
    return None
  
  def process_item(item):
    view_angles = get_View_angles(item)
    
    item.properties['sza'] = 90.0 - item.properties['view:sun_elevation']
    item.properties['saa'] = item.properties['view:sun_azimuth']
    item.properties['vza'] = view_angles['vza']
    item.properties['vaa'] = view_angles['vaa']
    
    return item
  
  # investigate if there is other library for parallel for this part
  out_items = []
  with concurrent.futures.ThreadPoolExecutor() as executor:
    futures = [executor.submit(process_item, item) for item in StacItems]
    for future in concurrent.futures.as_completed(futures):
      out_items.append(future.result())

  #==========================================================================================================
  # 
  #==========================================================================================================  
  #out_items = [] 
  #for item in StacItems:
  #  view_angles = get_View_angles(item)
    
  #  item.properties['sza'] = 90.0 - item.properties['view:sun_elevation']
  #  item.properties['saa'] = item.properties['view:sun_azimuth']
  #  item.properties['vza'] = view_angles['vza']
  #  item.properties['vaa'] = view_angles['vaa']

  #  out_items.append(item)
  
  endT   = time.time()
  totalT = (endT - startT)/60

  return out_items, totalT







#############################################################################################################
# Description: This function reads and returns a geometry angle database from a local .CSV file, which was
#              created using GEE (in ImgSet.py)
#
# Revision history:  2024-Jul-15  Lixin Sun  Initial creation
# 
#############################################################################################################
'''
def read_angleDB(DB_fullpath):
  if os.path.isfile(DB_fullpath) == False:
    return None
  
  def form_key(product_ID):
    #PRODUCT_ID: S2B_MSIL2A_20200801T182919_N0214_R027_T12VWK_20200801T223038
    tokens = str(product_ID).split('_')
    return tokens[0] + '_' + tokens[5][1:] + '_' + tokens[2][:8] + '_' + tokens[1][3:]

  angle_DB = pd.read_csv(DB_fullpath)
  ID_col_name = angle_DB.columns[0]
  ID_column = angle_DB.loc[:, ID_col_name]

  new_IDs = []
  for id in ID_column:
    new_IDs.append(form_key(id))
  
  angle_DB[ID_col_name] = new_IDs
  
  return angle_DB
  #print(angle_DB.head())
'''





#############################################################################################################
# Description: This function returns a base image that covers the entire spatial region od an interested area.
#
# Revision history:  2024-May-24  Lixin Sun  Initial creation
#                    2024-Jul-17  Lixin Sun  Modified so that only unique and filtered STAC items will be
#                                            returned 
#############################################################################################################
def get_base_Image(StacItems, Region, ProjStr, Scale, Criteria, ExtraBandCode):
  '''
     Args:
       StacItems(List): A list of STAC items searched for a study area and a time window;

  '''
  start_time = time.time()

  #==========================================================================================================
  # Load the first image based on the boundary box of ROI
  #==========================================================================================================
  xy_bbox  = eoUs.get_region_bbox(Region, ProjStr)

  ds_xr = odc.stac.load([StacItems[0]],
                        bands         = Criteria['bands'],
                        chunks        = {'x': 1000, 'y': 1000}, #2000
                        crs           = ProjStr, 
                        fail_on_error = False,
                        resolution    = Scale, 
                        x = (xy_bbox[0], xy_bbox[2]),
                        y = (xy_bbox[3], xy_bbox[1]))
  
  # actually load data into memory
  #with ddiag.ProgressBar():
  ds_xr.load()
  out_xrDS = ds_xr.isel(time=0).astype(np.float32)

  #==========================================================================================================
  # Attach necessary extra bands
  #==========================================================================================================
  band1 = Criteria['bands'][0]  
  out_xrDS[eoIM.pix_date]  = out_xrDS[band1]
  out_xrDS[eoIM.pix_score] = out_xrDS[band1]
  
  if int(ExtraBandCode) == eoIM.EXTRA_ANGLE:
    out_xrDS['cosSZA']  = out_xrDS[band1]
    out_xrDS['cosVZA']  = out_xrDS[band1]
    out_xrDS['cosRAA']  = out_xrDS[band1]

  #==========================================================================================================
  # Mask out all the pixels in each variable of "base_img", so they will treated as gap/missing pixels
  # This step is very import if "combine_first" function is used to merge granule mosaic into based image. 
  #==========================================================================================================
  out_xrDS = out_xrDS*0 + -10000.0
  #out_xrDS = out_xrDS.where(out_xrDS > 0)
  #ds_xr.load()

  stop_time = time.time() 
  
  return out_xrDS, (stop_time - start_time)/60





#############################################################################################################
# Description: This function returns reference bands for the blue and NIR bands.
#
# Revision history:  2024-May-28  Lixin Sun  Initial creation
# 
#############################################################################################################
def get_score_refers(ready_IC):
  #==========================================================================================================
  # create a median image from the ready Image collection
  #==========================================================================================================
  median_img = ready_IC.median(dim='time')    #.astype(np.float32)
  
  #==========================================================================================================
  # Extract separate bands from the median image, then calculate NDVI and modeled blue median band
  #==========================================================================================================
  blu = median_img.blue
  red = median_img.red
  nir = median_img.nir08
  sw2 = median_img.swir22
  
  NDVI = (nir - red)/(nir + red + 0.0001)  
  #print('\n\nNDVI = ', NDVI)
  model_blu = sw2*0.25
  
  #==========================================================================================================
  # Correct the blue band values of median mosaic for the pixels with NDVI values larger than 0.3
  #========================================================================================================== 
  condition = (model_blu > blu) | (NDVI < 0.3) | (sw2 < blu)
  median_img['blue'] = median_img['blue'].where(condition, other = model_blu)
  
  print('\n<get_score_refers> median image = ', median_img)

  return median_img





#############################################################################################################
# Description: This function attaches a score band to each image in a xarray Dataset object.
#
# Revision history:  2024-May-24  Lixin Sun  Initial creation
#                    2024-Jul-25  Lixin Sun  Parallelized the code using 'concurrent.futures' module.
#  
#############################################################################################################
def attach_score(SsrData, ready_IC, StartStr, EndStr):
  '''Attaches a score band to each image in a xarray Dataset object, an image collection equivalent in GEE.
     Args:
       SsrData(dictionary): A dictionary containing some metadata about a sensor;
       ready_ID(xarray.dataset): A xarray dataset object containing a set of STAC images/items;
       StartStr(string): A string representing the start date of a timeframe;
       EndStr(string): A string representing the end date of a timeframe.'''
  #print('<attach_score> ready IC = ', ready_IC)
  #print('\n\n<attach_score> ready IC after adding empty pixel score = ', ready_IC)
  #print('\n\n<attach_score> all pixel score layers in ready_IC = ', ready_IC[eoIM.pix_score])
  #==========================================================================================================
  # Create a median image for all spectral bands
  #==========================================================================================================
  start = time.time() 

  median     = get_score_refers(ready_IC)
  median_blu = median[SsrData['BLU']]
  median_nir = median[SsrData['NIR']]
  
  #==========================================================================================================
  # Define an internal function that can calculate time and spectral scores for each image in 'ready_IC'
  #==========================================================================================================
  midDate = datetime.strptime(eoUs.period_centre(StartStr, EndStr), "%Y-%m-%d")

  def image_score(i, T, ready_IC, midDate, SsrData, median_blu, median_nir):
    #print('\n<image_score> Start scorring for %2d'%(i))
    timestamp  = pd.Timestamp(T).to_pydatetime()
    time_score = get_time_score(timestamp, midDate, SsrData['SSR_CODE'])   
    
    img = ready_IC.isel(time=i)
    spec_score = get_spec_score(SsrData, img, median_blu, median_nir)    
    #print('\n<image_score> Stop scorring for %2d'%(i))

    return i, spec_score * time_score
  
  #==========================================================================================================
  # Parallelize the process of score calculations for every image in 'ready_IC'
  #==========================================================================================================
  #time_vals = list(ready_IC.time.values)
  # with concurrent.futures.ThreadPoolExecutor() as executor:
  #   futures = [executor.submit(image_score, i, T, ready_IC, midDate, SsrData, median_blu, median_nir) for i, T in enumerate(time_vals)]
    
  #   for future in concurrent.futures.as_completed(futures):
  #     i, score = future.result()
  #     ready_IC[eoIM.pix_score][i, :,:] = score

  for i, T in enumerate(ready_IC.time.values):    
    i, score_img = image_score(i, T, ready_IC, midDate, SsrData, median_blu, median_nir)    
    ready_IC[eoIM.pix_score][i, :,:] = score_img
  
  stop = time.time() 

  return ready_IC, (stop - start)/60.0




######################################################################################################
# Description: This function creates a map with all the pixels having an identical time score for a
#              given image. Time score is calculated based on the date gap between the acquisition
#              date of the given image and a reference date (midDate parameter), which normally is
#              the middle date of a time period (e.g., a peak growing season).
#
# Revision history:  2024-May-31  Lixin Sun  Initial creation
#
######################################################################################################
def get_time_score(ImgDate, MidDate, SsrCode):
  '''Return a time score image corresponding to a given image
  
     Args:
        ImgDate (datetime object): A given ee.Image object to be generated a time score image.
        MidData (datetime object): The centre date of a time period for a mosaic generation.
        SsrCode (int): The sensor type code. '''
  
  #==================================================================================================
  # Calculate the date difference between image date and a reference date  
  #==================================================================================================  
  date_diff = (ImgDate - MidDate).days  

  #==================================================================================================
  # Calculatr time score according to sensor type 
  #==================================================================================================
  std = 12 if int(SsrCode) > eoIM.MAX_LS_CODE else 16  

  return 1.0/math.exp(0.5 * pow(date_diff/std, 2))





#############################################################################################################
# Description: This function attaches a score band to each image within a xarray Dataset.
#
# Note:        The given "masked_IC" may be either an image collection with time dimension or a single image
#              without time dimension
#
# Revision history:  2024-May-24  Lixin Sun  Initial creation
# 
#############################################################################################################
def get_spec_score(SsrData, inImg, median_blu, median_nir):
  '''Attaches a score band to each image within a xarray Dataset
     Args:
       inImg(xarray dataset): a given single image;
       medianImg(xarray dataset): a given median image.'''
  '''
  min_val   = 0.01
  six_bands = SsrData['SIX_BANDS']

  for band in six_bands:
    inImg[band] = inImg[band].where(inImg > min_val, min_val)
  '''

  blu = inImg[SsrData['BLU']]
  grn = inImg[SsrData['GRN']]
  red = inImg[SsrData['RED']]
  nir = inImg[SsrData['NIR']]
  sw1 = inImg[SsrData['SW1']]
  sw2 = inImg[SsrData['SW2']]
  
  max_SV = xr.apply_ufunc(np.maximum, blu, grn)
  max_SW = xr.apply_ufunc(np.maximum, sw1, sw2)
  max_IR = xr.apply_ufunc(np.maximum, nir, max_SW)

  #==================================================================================================
  # Calculate scores assuming all the pixels are water
  #==================================================================================================
  water_score = max_SV/max_IR
  water_score = water_score.where(median_blu > blu, -1*water_score)
  #print('\n<attach_score> water_score = ', water_score)

  #==================================================================================================
  # Calculate scores assuming all the pixels are land
  #==================================================================================================
  #blu_pen = xr.apply_ufunc(np.abs, blu - median_blu)
  #nir_pen = xr.apply_ufunc(np.abs, nir - median_nir)
  blu_pen = blu - median_blu
  nir_pen = median_nir - nir

  #refer_blu = xr.apply_ufunc(np.maximum, sw2*0.25, red*0.5 + 0.8) 
  #STD_blu = xr.apply_ufunc(np.maximum, STD_blu, blu) 
  STD_blu = blu.where(blu > 0, 0) + 1.0 
  #STD_blu = blu.where(blu > 1.0, refer_blu)   
    
  land_score = (max_IR*100.0)/(STD_blu*100.0 + blu_pen + nir_pen)  

  return land_score.where((max_SV < max_IR) | (max_SW > 3.0), water_score)
  



#############################################################################################################
# Description: 
#
# Revision history:  2024-May-24  Lixin Sun  Initial creation
# 
#############################################################################################################
@dask.delayed
def chunk_mosaic(chunk, SsrData, StartStr, EndStr, GranuleItems, ExtraBandCode, time_values):
  # SsrData       = Packed_params['SsrData']
  # StartStr      = Packed_params['Start']
  # EndStr        = Packed_params['End']
  # GranuleItems  = Packed_params['Items']
  # ExtraBandCode = Packed_params['ExtraBandCode']

  #==========================================================================================================
  # Attach three layers, an empty 'score', acquisition DOY and 'time_index', to eath item/image in "xrDS" 
  #==========================================================================================================  
  #xrDS['time'] = pd.to_datetime(xrDS['time'].values)
  time_datetime = pd.to_datetime(time_values)
  doys = [date.timetuple().tm_yday for date in time_datetime]  #Calculate DOYs for every temporal point

  chunk[eoIM.pix_score] = chunk[SsrData['BLU']]*0
  chunk[eoIM.pix_date]  = xr.DataArray(np.array(doys, dtype='uint16'), dims=['time'])
  chunk['time_index']   = xr.DataArray(np.array(range(0, len(time_values)), dtype='uint8'), dims=['time'])
  
  #==========================================================================================================
  # Apply default pixel mask, rescaling gain and offset to each image in 'chunk'
  #==========================================================================================================
  chunk = eoIM.apply_default_mask(chunk, SsrData)
  chunk = eoIM.apply_gain_offset(chunk, SsrData, 100, False)

  #==========================================================================================================
  # Calculate compositing scores for every valid pixel in xarray dataset object (chunk)
  #==========================================================================================================
  chunk, score_time = attach_score(SsrData, chunk, StartStr, EndStr)

  print('<get_granule_mosaic> Complete pixel scoring, elapsed time = %6.2f minutes'%(score_time)) 

  #==========================================================================================================
  # Create a composite image based on compositing scores
  # Note: calling "fillna" function before invaking "argmax" function is very important!!!
  #==========================================================================================================
  chunk       = chunk.fillna(-0.0001)

  max_indices = chunk[eoIM.pix_score].argmax(dim='time')
  mosaic      = chunk.isel(time=max_indices)

  #elif extra_code == eoIM.EXTRA_NDVI:
  #  chunk = eoIM.attach_NDVIBand(chunk, SsrData)
  #==========================================================================================================
  # Attach an additional bands as necessary 
  #==========================================================================================================
  extra_code = int(ExtraBandCode)
  if extra_code == eoIM.EXTRA_ANGLE:
    mosaic = eoIM.attach_AngleBands(mosaic, GranuleItems)
    #mosaic = mosaic.drop_vars(['blue','scl'])    

  #==========================================================================================================
  # Remove 'time_index' and 'score' variables from submosaic 
  #==========================================================================================================
  mosaic = mosaic.drop_vars(['time_index'])

  mosaic = mosaic.where(mosaic[eoIM.pix_date] > 0)
  print('<get_granule_mosaic> Data variables of granule mosaic = ', mosaic.data_vars)

  return mosaic






#############################################################################################################
# Description: This function returns a mosaic image for a sub-region
#
# Note:        The baseBBox must be applied; otherwise, there will be artifact lines in the final mosaic
#              image between different granule mosaic images.
#
# Revision history:  2024-May-24  Lixin Sun  Initial creation
#                    2024-Jul-05  Lixin Sun  Added imaging angle bands to each item/image 
#
#############################################################################################################
def get_granule_mosaic(SsrData, GranuleItems, StartStr, EndStr, Bands, ProjStr, Scale, ExtraBandCode):
  '''
     Args:
       SsrData(Dictionary): Some meta data on a used satellite sensor;
       TileItems(List): A list of STAC items associated with a specific tile;
       ExtraBandCode(Int): An integer indicating if to attach extra bands to mosaic image.'''
  
  successful_items = []
  for item_ID in GranuleItems:
    try:
      one_DS = odc.stac.load([item_ID],
                              bands  = Bands,
                              chunks = {'x': 1000, 'y': 1000},
                              crs    = ProjStr, 
                              resolution = Scale)
      one_DS.load()

      successful_items.append(one_DS)
    except Exception as e:
      continue
  
  if not successful_items:
    return None
  
  xrDS = xr.concat(successful_items, dim='time')
  
  print("\n<get_granule_mosaic> Time Dimension Values:")
  time_values = xrDS.coords['time'].values  
  for t in time_values:
    print(t)


  # packed_params = {}
  # packed_params['SsrData'] = SsrData
  # packed_params['Start']   = StartStr
  # packed_params['End']     = EndStr
  # packed_params['Items']   = GranuleItems
  # packed_params['ExtraBandCode'] = ExtraBandCode

  results = xrDS.map_blocks(chunk_mosaic, args = [SsrData, StartStr, EndStr, GranuleItems, ExtraBandCode, time_values])

  granule_mosaic = results.compute()
  
  return granule_mosaic






def get_granule_mosaic_old(SsrData, GranuleItems, StartStr, EndStr, Bands, ProjStr, Scale, ExtraBandCode):
  '''
     Args:
       SsrData(Dictionary): Some meta data on a used satellite sensor;
       TileItems(List): A list of STAC items associated with a specific tile;
       ExtraBandCode(Int): An integer indicating if to attach extra bands to mosaic image.'''
  
  successful_items = []
  for item_ID in GranuleItems:
    try:
      one_DS = odc.stac.load([item_ID],
                              bands  = Bands,
                              chunks = {'x': 2000, 'y': 2000},
                              crs    = ProjStr, 
                              resolution = Scale)
      one_DS.load()

      successful_items.append(one_DS)
    except Exception as e:
      continue
  
  if not successful_items:
    return None
  
  xrDS = xr.concat(successful_items, dim='time')
  
  #==========================================================================================================
  # 
  #==========================================================================================================
  # xrDS = odc.stac.load([GranuleItems],
  #                      bands  = Bands,
  #                      chunks = {'x': 2000, 'y': 2000},
  #                      crs    = ProjStr, 
  #                      resolution = Scale)
  # xrDS.load()

  #print('\n<get_granule_mosaic> loaded xarray dataset:\n', xrDS) 

  print("\n<get_granule_mosaic> Time Dimension Values:")
  time_values = xrDS.coords['time'].values  
  for t in time_values:
    print(t)

  #==========================================================================================================
  # Attach three layers, an empty 'score', acquisition DOY and 'time_index', to eath item/image in "xrDS" 
  #==========================================================================================================  
  #xrDS['time'] = pd.to_datetime(xrDS['time'].values)
  time_datetime = pd.to_datetime(time_values)
  doys = [date.timetuple().tm_yday for date in time_datetime]  #Calculate DOYs for every temporal point

  xrDS[eoIM.pix_score] = xrDS[SsrData['BLU']]*0
  xrDS[eoIM.pix_date]  = xr.DataArray(np.array(doys, dtype='uint16'), dims=['time'])
  xrDS['time_index']   = xr.DataArray(np.array(range(0, len(time_values)), dtype='uint8'), dims=['time'])
  
  #==========================================================================================================
  # Apply default pixel mask, rescaling gain and offset to each image in 'xrDS'
  #==========================================================================================================
  xrDS = eoIM.apply_default_mask(xrDS, SsrData)
  xrDS = eoIM.apply_gain_offset(xrDS, SsrData, 100, False)

  #==========================================================================================================
  # Calculate compositing scores for every valid pixel in xarray dataset object (xrDS)
  #==========================================================================================================
  xrDS, score_time = attach_score(SsrData, xrDS, StartStr, EndStr)

  print('<get_granule_mosaic> Complete pixel scoring, elapsed time = %6.2f minutes'%(score_time)) 

  #==========================================================================================================
  # Create a composite image based on compositing scores
  # Note: calling "fillna" function before invaking "argmax" function is very important!!!
  #==========================================================================================================
  xrDS = xrDS.fillna(-0.0001)
  #xrDS.load()

  max_indices = xrDS[eoIM.pix_score].argmax(dim='time')
  mosaic      = xrDS.isel(time=max_indices)

  #elif extra_code == eoIM.EXTRA_NDVI:
  #  xrDS = eoIM.attach_NDVIBand(xrDS, SsrData)
  #==========================================================================================================
  # Attach an additional bands as necessary 
  #==========================================================================================================
  extra_code = int(ExtraBandCode)
  if extra_code == eoIM.EXTRA_ANGLE:
    mosaic = eoIM.attach_AngleBands(mosaic, GranuleItems)
    #mosaic = mosaic.drop_vars(['blue','scl'])    

  #==========================================================================================================
  # Remove 'time_index' and 'score' variables from submosaic 
  #==========================================================================================================
  mosaic = mosaic.drop_vars(['time_index'])

  mosaic = mosaic.where(mosaic[eoIM.pix_date] > 0)
  print('<get_granule_mosaic> Data variables of granule mosaic = ', mosaic.data_vars)

  return mosaic

  




#############################################################################################################
# Description: This function returns a composite image generated from images acquired over a specified time 
#              period.
# 
# Revision history:  2024-May-24  Lixin Sun  Initial creation
#                    2024-Jul-20  Lixin Sun  Modified to generate the final composite image tile by tile.
#############################################################################################################
def period_mosaic(inParams, ExtraBandCode):
  '''
    Args:
      inParams(dictionary): A dictionary containing all necessary execution parameters;
      ExtraBandCode(int): An integer indicating which kind of extra bands will be created as well.'''
  
  mosaic_start = time.time()  

  #==========================================================================================================
  # Confirm 'current_month' and 'current_tile' keys have valid values
  #==========================================================================================================
  StartStr, EndStr = eoPM.get_time_window(inParams)
  if StartStr == None or EndStr == None:
    print('\n<period_mosaic> Invalid time window was defined!!!')
    return None
  
  Region = eoPM.get_spatial_region(inParams)
  if Region == None:
    print('\n<period_mosaic> Invalid spatial region was defined!!!')
    return None
  
  #==========================================================================================================
  # Prepare other required parameters and query criteria
  #==========================================================================================================
  ProjStr = str(inParams['projection']) if 'projection' in inParams else 'EPSG:3979'
  Scale   = int(inParams['resolution']) if 'resolution' in inParams else 20

  SsrData  = eoIM.SSR_META_DICT[str(inParams['sensor']).upper()]
  criteria = get_query_conditions(SsrData, StartStr, EndStr)
  
  #==========================================================================================================
  # Search all the STAC items based on a spatial region and a time window
  # Note: (1) The third parameter (MaxImgs) for "search_STAC_Catalog" function cannot be too large. Otherwise,
  #           a server internal error will be triggered.
  #       (2) The imaging angles have been attached to each STAC item by "search_STAC_Catalog" function.
  #==========================================================================================================  
  stac_items = search_STAC_Catalog(Region, criteria, 100)

  print(f"\n<period_mosaic> A total of {len(stac_items):d} items were found.\n")
  display_meta_assets(stac_items)
  
  #mosaic = collection_mosaic(stac_items, inParams, ExtraBandCode)
  #==========================================================================================================
  # Create a base image that has full spatial dimensions covering ROI
  #==========================================================================================================
  base_img, used_time = get_base_Image(stac_items, Region, ProjStr, Scale, criteria, ExtraBandCode)
  
  print('\n<period_mosaic> based mosaic image = ', base_img)
  print('\n<<<<<<<<<< Complete generating base image, elapsed time = %6.2f minutes>>>>>>>>>'%(used_time))  
  
  #==========================================================================================================
  # Get a list of unique tile names and then loop through each unique tile to generate submosaic 
  #==========================================================================================================  
  unique_granules = get_unique_tile_names(stac_items)  #Get all unique tile names 
  print('\n<<<<<< The number of unique granule tiles = %d'%(len(unique_granules)))  
  print('\n<<<<<< The unique granule tiles = ', unique_granules) 

  #==========================================================================================================
  # Obtain the bbox in projected CRS system (x and y, rather than Lat and Lon)
  #==========================================================================================================  
  def mosaic_one_granule(granule_name, stac_items, SsrData, StartStr, EndStr, criteria, ProjStr, Scale):
    one_granule_items = get_one_granule_items(stac_items, granule_name)  # Extract a list of items based on an unique tile name
    filtered_items    = get_unique_STAC_items(one_granule_items)         # Remain only one item from those that share the same timestamp

    return get_granule_mosaic(SsrData, filtered_items, StartStr, EndStr, criteria['bands'], ProjStr, Scale, ExtraBandCode)

  #test_mosaic = mosaic_one_granule(unique_granules[10], stac_items, SsrData, StartStr, EndStr, criteria, ProjStr, Scale)
  #return test_mosaic
  count = 0
  for granule in unique_granules:
    granule_mosaic = mosaic_one_granule(granule, stac_items, SsrData, StartStr, EndStr, criteria, ProjStr, Scale)

    if granule_mosaic is not None:
      granule_mosaic = granule_mosaic.reindex_like(base_img)   # do not apply any value to "method" parameter, just default value
      mask = granule_mosaic[eoIM.pix_score] > base_img[eoIM.pix_score]
      for var in base_img.data_vars:
        base_img[var] = base_img[var].where(~mask, granule_mosaic[var], True)
      
      count += 1      
      print('\n<<<<<<<<<< Complete %2dth sub mosaic >>>>>>>>>'%(count))
      
  # cpu_cores = os.cpu_count()
  # print('\n\n The numb of CPU cores = ', cpu_cores)

  # with concurrent.futures.ThreadPoolExecutor(max_workers=cpu_cores) as executor:
  #   futures = [executor.submit(mosaic_one_granule, granule, stac_items, SsrData, StartStr, EndStr, criteria, ProjStr, Scale) for granule in unique_granules]
  #   count = 0
  #   for future in concurrent.futures.as_completed(futures):
  #     granule_mosaic = future.result()
  #     if granule_mosaic is not None:
  #       granule_mosaic = granule_mosaic.reindex_like(base_img)   # do not apply any value to "method" parameter, just default value
  #       mask = granule_mosaic[eoIM.pix_score] > base_img[eoIM.pix_score]
  #       for var in base_img.data_vars:
  #         base_img[var] = base_img[var].where(~mask, granule_mosaic[var], True)

  #       count += 1      
  #       print('\n<<<<<<<<<< Complete %2dth sub mosaic >>>>>>>>>'%(count))

  #==========================================================================================================
  # Mask out the pixels with negative date value
  #========================================================================================================== 
  mosaic = base_img.where(base_img[eoIM.pix_date] > 0)

  mosaic_stop = time.time()
  mosaic_time = (mosaic_stop - mosaic_start)/60
  print('\n\n<<<<<<<<<< The total elapsed time for generating the mosaic = %6.2f minutes>>>>>>>>>'%(mosaic_time))
  
  return mosaic





#############################################################################################################
# Description: This function exports the band images of a mosaic into separate GeoTiff files
#
# Revision history:  2024-May-24  Lixin Sun  Initial creation
# 
#############################################################################################################
def export_mosaic(inParams, inMosaic):
  '''
    This function exports the band images of a mosaic into separate GeoTiff files.

    Args:
      inParams(dictionary): A dictionary containing all required execution parameters;
      inMosaic(xrDS): A xarray dataset object containing mosaic images to be exported.'''
  
  #==========================================================================================================
  # Get all the parameters for exporting composite images
  #==========================================================================================================
  params = eoPM.get_mosaic_params(inParams)  

  #==========================================================================================================
  # Convert float pixel values to integers
  #==========================================================================================================
  mosaic_int = (inMosaic * 100.0).astype(np.int16)
  rio_mosaic = mosaic_int.rio.write_crs(params['projection'], inplace=True)  # Assuming WGS84 for this example

  #==========================================================================================================
  # Create a directory to store the output files
  #==========================================================================================================
  dir_path = params['out_folder']
  os.makedirs(dir_path, exist_ok=True)

  #==========================================================================================================
  # Create prefix filename
  #==========================================================================================================
  SsrData    = eoIM.SSR_META_DICT[str(params['sensor'])]   
  region_str = str(params['region_str'])
  period_str = str(params['time_str'])
 
  filePrefix = f"{SsrData['NAME']}_{region_str}_{period_str}"

  #==========================================================================================================
  # Create individual sub-mosaic and combine it into base image based on score
  #==========================================================================================================
  spa_scale    = params['resolution']
  export_style = str(params['export_style']).lower()
  
  if 'sepa' in export_style:
    for band in rio_mosaic.data_vars:
      out_img     = rio_mosaic[band]
      filename    = f"{filePrefix}_{band}_{spa_scale}m.tif"
      output_path = os.path.join(dir_path, filename)
      out_img.rio.to_raster(output_path)
  else:

    filename = f"{filePrefix}_mosaic_{spa_scale}m.tif"

    output_path = os.path.join(dir_path, filename)
    rio_mosaic.to_netcdf(output_path)





# params = {
#     'sensor': 'S2_SR',           # A sensor type string (e.g., 'S2_SR' or 'L8_SR' or 'MOD_SR')
#     'unit': 2,                   # A data unit code (1 or 2 for TOA or surface reflectance)    
#     'year': 2023,                # An integer representing image acquisition year
#     'nbYears': -1,               # positive int for annual product, or negative int for monthly product
#     'months': [8],               # A list of integers represening one or multiple monthes     
#     'tile_names': ['tile55_922'], # A list of (sub-)tile names (defined using CCRS' tile griding system) 
#     'prod_names': ['LAI', 'fCOVER', 'fAPAR', 'Albedo'],    #['mosaic', 'LAI', 'fCOVER', ]    
#     'resolution': 200,            # Exporting spatial resolution    
#     'out_folder': 'C:/Work_documents/LEAF_tile55_2023_922_Aug_200m_new',  # the folder name for exporting
#     'projection': 'EPSG:3979',    
#     #'start_date': '2022-06-15',
#     #'end_date': '2022-09-15'
# }

# mosaic = period_mosaic(params, eoIM.EXTRA_ANGLE)

# export_mosaic(params, mosaic)