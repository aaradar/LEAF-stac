import gc
import gc
import os
import sys
import dask
import sys
import dask
import math
import time
import uuid
import copy
import shutil
import logging
import asyncio
import odc.stac
import platform
import requests
import warnings
import functools
import subprocess
import numpy as np
import xarray as xr
import pandas as pd
import dask.array as da

from dateutil import parser
from dask import delayed
from pathlib import Path

import concurrent.futures
import pandas as pd
import dask.array as da
from dask import delayed
from pathlib import Path
import concurrent.futures
import pystac_client as psc
from datetime import datetime
from datetime import datetime
import dask.diagnostics as ddiag
import xml.etree.ElementTree as ET
import xml.etree.ElementTree as ET
from collections import defaultdict
from urllib3.exceptions import TimeoutError, ConnectionError
from dask.distributed import as_completed
from dask.distributed import Client, LocalCluster

odc.stac.configure_rio(cloud_defaults = True, GDAL_HTTP_UNSAFESSL = 'YES')
dask.config.set(**{'array.slicing.split_large_chunks': True})
dask.config.set(**{'array.slicing.split_large_chunks': True})

# The two things must be noted:
# (1) this line must be used after "import odc.stac"
# (2) This line is necessary for exporting a xarray dataset object into separate GeoTiff files,
#     even it is not utilized directly
import rioxarray
if str(Path(__file__).parents[0]) not in sys.path:
  sys.path.insert(0, str(Path(__file__).parents[0]))
if str(Path(__file__).parents[0]) not in sys.path:
  sys.path.insert(0, str(Path(__file__).parents[0]))


import eoImage as eoIM
import eoUtils as eoUs
import eoParams as eoPM
import eoTileGrids as eoTG


logging.basicConfig(level=logging.WARNING) 

# def get_query_conditions(SsrData, StartStr, EndStr, ClCover):
#   logging.basicConfig(level=logging.WARNING) 



# def get_query_conditions(SsrData, StartStr, EndStr, ClCover):
#   ssr_code = SsrData['SSR_CODE']
#   query_conds = {}
  
#   #==================================================================================================
#   # Create a filter for the search based on metadata. The filtering params will depend upon the 
#   # image collection we are using. e.g. in case of Sentine 2 L2A, we can use params such as: 
#   #
#   # eo:cloud_cover
#   # s2:dark_features_percentage
#   # s2:cloud_shadow_percentage
#   # s2:vegetation_percentage
#   # s2:water_percentage
#   # s2:not_vegetated_percentage
#   # s2:snow_ice_percentage, etc.
#   # 
#   # For many other collections, the Microsoft Planetary Computer has a STAC server at 
#   # https://planetarycomputer-staging.microsoft.com/api/stac/v1 (this info comes from 
#   # https://www.matecdev.com/posts/landsat-sentinel-aws-s3-python.html)
#   #==================================================================================================  
#   if ssr_code > eoIM.MAX_LS_CODE and ssr_code < eoIM.MOD_sensor:
#     query_conds['catalog']    = "https://earth-search.aws.element84.com/v1"
#     query_conds['collection'] = "sentinel-2-l2a"
#     query_conds['timeframe']  = str(StartStr) + '/' + str(EndStr)
#     query_conds['bands']      = SsrData['ALL_BANDS'] + ['scl']
#     query_conds['filters']    = {"eo:cloud_cover": {"lt": ClCover} }    
#     query_conds['filters']    = {"eo:cloud_cover": {"lt": ClCover} }    

#     query_conds['filters']    = {"eo:cloud_cover": {"lt": ClCover} }

#   elif ssr_code < eoIM.MAX_LS_CODE and ssr_code > 0:
#     query_conds['catalog']    = "https://earth-search.aws.element84.com/v1"
#     query_conds['collection'] = "landsat-c2-l2"
#     query_conds['timeframe']  = str(StartStr) + '/' + str(EndStr)
#     #query_conds['bands']      = ['OLI_B2', 'OLI_B3', 'OLI_B4', 'OLI_B5', 'OLI_B6', 'OLI_B7', 'qa_pixel']
#     query_conds['bands']      = ['blue', 'green', 'red', 'nir08', 'swir16', 'swir22', 'qa_pixel']
#     query_conds['filters']    = {"eo:cloud_cover": {"lt": ClCover}}  
#   elif ssr_code == eoIM.HLS_sensor:
#     query_conds['catalog']    = "https://cmr.earthdata.nasa.gov/stac/LPCLOUD"
#     query_conds['collection'] = "HLSL30.v2.0"
#     query_conds['timeframe']  = str(StartStr) + '/' + str(EndStr)
#     query_conds['bands']      = ['blue', 'green', 'red', 'nir08', 'swir16', 'swir22', 'qa_pixel']
#     query_conds['filters']    = {"eo:cloud_cover": {"lt": ClCover}}  

#   return query_conds



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

  view_angles = {}
  try:
    
    response = requests.get(granule_meta.href)
    response.raise_for_status()  # Check that the request was successful

    # Parse the XML content
    root = ET.fromstring(response.content)  
    
    elem = root.find(".//Mean_Viewing_Incidence_Angle[@bandId='8']")
    view_angles['vza'] = float(elem.find('ZENITH_ANGLE').text)
    view_angles['vaa'] = float(elem.find('AZIMUTH_ANGLE').text)
  except Exception as e:
    view_angles['vza'] = 0.0
    view_angles['vaa'] = 0.0
  
  return view_angles


def display_meta_assets(stac_items, First):
  if First == True:    
    first_item = stac_items[0]

    print('<<<<<<< The assets associated with the first item >>>>>>>\n' )
    for asset_key, asset in first_item.assets.items():
      print(f"Asset key: {asset_key}, title: {asset.title}, href: {asset.href}")    

    print('<<<<<<< The meta data associated with an item >>>>>>>\n' )
    print("ID:", first_item.id)
    print("Geometry:", first_item.geometry)
    print("Bounding Box:", first_item.bbox)
    print("Datetime:", first_item.datetime)
    properties = first_item.properties
    print(f"cloud cover: {properties['eo:cloud_cover']}")
    print("Properties:")

    for key, value in properties.items():
      print(f"  <{key}>: {value}")
  else:
    for item in stac_items:
      properties = item.properties
      print("ID: {}; vza: {}; vaa: {}; cloud_cover:{}".format(item.id, properties['vza'], properties['vaa'], properties['eo:cloud_cover']))





#############################################################################################################
# Description: This function returns the results of searching a STAC catalog
#
# Revision history:  2024-May-24  Lixin Sun  Initial creation
#                    2024-Jul-12  Lixin Sun  Added a filter to retain only one image from the items with
#                                            identical timestamps.
#
#############################################################################################################
def search_STAC_Catalog(inParams, MaxImgs):
  '''
    Args:
      inParams(dictionary): A dictionary containing all parameters for generating a composite image;
      MaxImgs(int): A specified maximum number of images in a queried list.
  '''
  
  #==========================================================================================================
  # Use publicly available STAC 
  #==========================================================================================================
  Criteria = inParams['Criteria']
  catalog  = psc.Client.open(str(Criteria['catalog']))

  #==========================================================================================================
  # Search and filter a image collection
  #==========================================================================================================
  Region = Criteria['region']
  print('<search_STAC_Images> The given region = ', Region)

  nCollections = len(Criteria['collection'])
  if nCollections > 1:   #For the case of "HLS_SR", which includes two collections: "HLSS30_SR" and "HLSL30_SR"  
    stac_items = []
    for coll in Criteria['collection']:
      stac_catalog = catalog.search(collections = [coll], 
                                    intersects  = Region,
                                    datetime    = str(Criteria['timeframe']),
                                    query       = Criteria['filters'],
                                    limit       = MaxImgs)
      # Get the items from the catalog
      items = stac_catalog.items()
      try:
        stac_items += items
      except: 
        raise ValueError("There is no list of STAC items found for your query. If you are using a custom region or time, please adjust and expand them accordingly.")
  else:
    stac_catalog = catalog.search(collections = Criteria['collection'], 
                                  intersects  = Region,                           
                                  datetime    = str(Criteria['timeframe']),
                                  query       = Criteria['filters'],
                                  limit       = MaxImgs)
    
    stac_items = list(stac_catalog.items())

  #==========================================================================================================
  # Ingest imaging geometry angles into each STAC item
  #==========================================================================================================
  ssr_str = str(inParams['sensor']).lower()
  if 'hls' not in ssr_str:   # For AWS data catalog, where imaging angles are not directly available
    stac_items, angle_time = ingest_Geo_Angles(stac_items)
    print('\n<search_STAC_Catalog> The total elapsed time for ingesting angles = %6.2f minutes'%(angle_time))

  return stac_items



#############################################################################################################
# Description: This function returns the grid/granule code of a given STAC item.
#
# Revision history:  2024-Oct-25  Lixin Sun  Initial creation
#                                            
#############################################################################################################
def get_grid_code(oneItem):
  
  item_ID = str(oneItem.id).upper()
  return item_ID.split('.')[2] if 'HLS' in item_ID else item_ID.split('_')[1]


#############################################################################################################
# Description: This function returns a list of unique tile names contained in a given "StatcItems" list.
#
# Revision history:  2024-Jul-17  Lixin Sun  Initial creation
#                    2024-Oct-20  Lixin Sun  Changed "unique_names" from a list to a dictionary, so that the
#                     number of stac items with the same 'grid:code' can be recorded.
#                    2024-Oct-20  Lixin Sun  Changed "unique_names" from a list to a dictionary, so that the
#                     number of stac items with the same 'grid:code' can be recorded.
#                                            
#############################################################################################################
def get_unique_tile_names(StacItems):
  
  '''
    Args:
      StacItems(List): A list of stac items. 
  
      StacItems(List): A list of stac items. 
  '''
  stac_items = list(StacItems)
  unique_names = {}

  if len(stac_items) < 2:
    return unique_names  
  
  unique_names[get_grid_code(StacItems[0])] = 1

  for item in stac_items:
    grid_code = get_grid_code(item)
    if grid_code in unique_names:
      unique_names[grid_code] += 1
    else:
      unique_names[grid_code] = 1   
  
  sorted_keys = sorted(unique_names, key = lambda x: unique_names[x], reverse=True)
  
  return sorted_keys 


#############################################################################################################
# Description: This function returns a list of unique STAC items by remaining only one item from those that 
#              share the same timestamp.
#
# Revision history:  2024-Jul-17  Lixin Sun  Initial creation
# 
#############################################################################################################
def get_unique_STAC_items(inSTACItems, MosaicParams):
  '''
     Args:
        inSTACItems(): A given list of STAC items to be filtered based on timestamps;
        MosaicParams(Dictionary): A dictionary containing all required parameters. '''
 
  #==========================================================================================================
  # Retain only one image from the items with identical timestamps
  #==========================================================================================================
  # Create a dictionary to store items by their timestamp
  items_by_id = defaultdict(list)
  ssr_str = str(MosaicParams['sensor']).lower()
  # Create a new dictionary with the core image ID as keys
  for item in inSTACItems:
    if 'hls' in ssr_str:
      tokens = str(item.id).split('.')   #core image ID
      id = tokens[1] + '_' + tokens[2] + '_' + tokens[3]
    else:
      tokens = str(item.id).split('_')   #core image ID
      id = tokens[0] + '_' + tokens[1] + '_' + tokens[2]

    items_by_id[id].append(item)
  #==========================================================================================================
  # Iterate through the items and retain only one item per timestamp
  #==========================================================================================================
  S2_items = []
  LS_items = []
  for id, item_group in items_by_id.items():
    # Assuming we keep the first item in each group
    item_ID = str(item_group[0]).upper()
    if 'S30' in item_ID or 'S2' in item_ID:  # For Sentinel-2 data from either AWS or LP DAAC of NASA
      S2_items.append(item_group[0])
    else:  # For Landsat data from LP DAAC of NASA
      LS_items.append(item_group[0])

  return {'S2': S2_items, 'LS': LS_items}


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
    item_ID = get_grid_code(item)
    if GranuleName == item_ID:
      tile_items.append(item)

  return tile_items 


#############################################################################################################
#############################################################################################################

#############################################################################################################
def ingest_Geo_Angles(StacItems):
  
  startT = time.time()
  #==========================================================================================================
  # Confirm the given item list is not empty
  #==========================================================================================================
  nItems = len(StacItems)
  if nItems < 1:
    return None
  
  def process_item(item):
    item.properties['sza'] = 90.0 - item.properties['view:sun_elevation']
    item.properties['saa'] = item.properties['view:sun_azimuth']
 
    view_angles = get_View_angles(item)      
    item.properties['vza'] = view_angles['vza']
    item.properties['vaa'] = view_angles['vaa']
    
    return item
  
  #==========================================================================================================
  # Attach imaging geometry angles as properties to each STAC item 
  #==========================================================================================================  
  out_items = []
  with concurrent.futures.ThreadPoolExecutor() as executor:
    futures = [executor.submit(process_item, item) for item in StacItems]
    for future in concurrent.futures.as_completed(futures):
      out_items.append(future.result())
  
  endT   = time.time()
  totalT = (endT - startT)/60

  return out_items, totalT


#############################################################################################################
# Description: This function returns a base image that covers the entire spatial region od an interested area.
#
# Revision history:  2024-May-24  Lixin Sun  Initial creation
#                    2024-Jul-17  Lixin Sun  Modified so that only unique and filtered STAC items will be
#                                            returned 
#                    2024-Dec-02  Marjan Asgari  Modified so that we have a base image backed by a dask array.
#                    2024-Dec-02  Marjan Asgari  Modified so that we have a base image backed by a dask array.
#############################################################################################################
def get_base_Image(StacItems, MosaicParams, ChunkDict):
  '''
    Args: 
      StacItems(List): A list of STAC items searched for a study area and a time window;
      MosaicParams(dictionary): A dictionary containing all the parameters required for generating a composite.
  '''
  
  #==========================================================================================================
  # Extract required parameters
  #==========================================================================================================
  Region     = MosaicParams['Criteria']['region'] 
  ProjStr    = MosaicParams['projection']
  Scale      = MosaicParams['resolution']
  Bands      = MosaicParams['Criteria']['bands'] 
  InclAngles = MosaicParams['IncludeAngles']

  #==========================================================================================================
  # Prepare some required variables
  #==========================================================================================================
  xy_bbox    = eoUs.get_region_bbox(Region, ProjStr)

  base_xrDS  = None  # To store the dataset once it is loaded
  items = len(StacItems)
  nb_tries = 20 if 20 < items else items
  #==========================================================================================================
  # Ensure the base image is a DASK backed xarray dataset
  #==========================================================================================================
  base_image_is_read = False    
  i = 0 
  while not base_image_is_read and i < nb_tries:
    try:
      # If out_xrDS is not None, skip loading data again
      if base_xrDS is not None:
        break  # Data already loaded, break the loop

      # Attempt to load the STAC item and process it
      load_bands = get_load_bands(StacItems[i], Bands, InclAngles)
      with odc.stac.load([StacItems[i]],
                        bands  = load_bands,
                        chunks = ChunkDict,
                        crs    = ProjStr, 
                        resolution = Scale, 
                        resampling = "nearest",
                        x = (xy_bbox[0], xy_bbox[2]),
                        y = (xy_bbox[3], xy_bbox[1])) as ds_xr:
          
        # Process the data once loaded successfully
        base_xrDS = ds_xr.isel(time=0).astype(np.float32)
        if base_xrDS is not None:
          base_image_is_read = True  # Mark as successfully read
        else:
          i += 1
          if i >= nb_tries:
            break  # Exit the loop after reaching max retries

    except Exception as e:
      i += 1
      if i >= nb_tries:
        break  # Exit the loop after reaching max retries
  
  if base_xrDS is None:
    raise ValueError("There is no data returned for the base image. If you are using a custom region or time, please adjust and expand them accordingly.")

  ssr_str = str(MosaicParams['sensor']).lower()
  if ssr_str == "HLS_SR": 
    base_xrDS['sensor'] = xr.DataArray(
            data = dask.array.zeros((base_xrDS.sizes['y'], base_xrDS.sizes['x']), chunks=(ChunkDict['x'], ChunkDict['y']), dtype = np.float32),
            dims=['y', 'x'],
            coords={
                'y': base_xrDS['y'],
                'x': base_xrDS['x'],
            }
    )

  #==========================================================================================================
  # Rename spectral bands as necessary
  #==========================================================================================================
  base_xrDS = rename_spec_bands(base_xrDS)

  #==========================================================================================================
  # Attach necessary extra bands
  #==========================================================================================================
  base_xrDS[eoIM.pix_date]  = base_xrDS['blue']
  base_xrDS[eoIM.pix_score] = base_xrDS['blue']

  scene_id = str(StacItems[0].id).lower()
  if InclAngles and ('s2a' in scene_id or 's2b' in scene_id):
    for var in ['SZA', 'SAA', 'VZA', 'VAA']:
      base_xrDS[var] = base_xrDS['blue']

  #==========================================================================================================
  # Mask out all the pixels in each variable of "base_img", so they will treated as gap/missing pixels
  # This step is very import if "combine_first" function is used to merge granule mosaic into based image. 
  #==========================================================================================================

  return base_xrDS.fillna(0)*0 + -10000.0




#############################################################################################################
# Description: This function returns reference bands for the blue and NIR bands.
#
# Revision history:  2024-May-28  Lixin Sun  Initial creation
#                    2024-Nov-26  Marjan Asgari Limiting the calculation of median to only the bands we want
#                                 skiping NA in median calculation.
#                    2024-Nov-26  Marjan Asgari Limiting the calculation of median to only the bands we want
#                                 skiping NA in median calculation.
#############################################################################################################
def get_score_refers(ready_IC):
  
  #==========================================================================================================
  # Create median images only for selected data variables
  #==========================================================================================================
  blu = ready_IC['blue'].median(dim='time',   skipna=True)
  red = ready_IC['red'].median(dim='time',    skipna=True)
  nir = ready_IC['nir08'].median(dim='time',  skipna=True)
  #sw1 = ready_IC['swir22'].median(dim='time', skipna=True)
  sw2 = ready_IC['swir22'].median(dim='time', skipna=True)

  #==========================================================================================================
  # Calculate NDVI and estimate blue band reference from SWIR2 reflectance
  #==========================================================================================================
  NDVI      = (nir - red)/(nir + red + 0.0001)  
  model_blu = sw2*0.25
  
  #==========================================================================================================
  # Correct the blue band values of median mosaic for the pixels with NDVI values larger than 0.3
  #========================================================================================================== 
  condition = (nir < 2*blu) | (NDVI < 0.3)    # | (sw2 < blu)
  blu       = blu.where(condition, other = model_blu)
  #blu       = blu.where(condition, other = model_blu)
  
  del red, sw2, NDVI, model_blu, condition
  
  return blu, nir




#############################################################################################################
# Description: This function attaches a score band to each image in a xarray Dataset object.
#
# Revision history:  2024-May-24  Lixin Sun  Initial creation
#                    2024-Jul-25  Lixin Sun  Parallelized the code using 'concurrent.futures' module.
#                    2024-Nov-26  Marjan Asgari  Removed the "Parallelized the code using 'concurrent.futures' 
#                                                module."" With dask distributed we cannot use other 
#                                                parallelization techniques.
#############################################################################################################
#==========================================================================================================
# Define an internal function that can calculate time and spectral scores for each image in 'ready_IC'
#==========================================================================================================
def image_score(i, T, ready_IC, midDate, SsrData, median_blu, median_nir, WinSize):
  """ 
  Returns a score band for a specified image in a xarray Dataset object.

    Parameters:
        i (int): Index of the time slice.
        T (str): Timestamp string for the image.
        ready_IC (xarray.Dataset): Dataset containing image data.
        midDate (datetime): Middle date of the compositing period.
        SsrData (dict): Sensor data containing the SSR_CODE.
        median_blu (float): Median blue reflectance.
        median_nir (float): Median NIR reflectance.
        WinSize(float): A float number represents the size/days of a compositing window.

    Returns:
        int, xarray.DataArray: Image index and calculated score. """
  
  #==========================================================================================================
  # Calculatr time score according to sensor type
  #==========================================================================================================  
  ssr_code   = int(SsrData['SSR_CODE'])
  is_S2_data = ssr_code in [eoIM.S2A_sensor, eoIM.S2B_sensor, eoIM.HLSS30_sensor]
  
  STD = float(WinSize/6.0 + 1 if is_S2_data == True or ssr_code == eoIM.HLS_sensor else WinSize/6.0 + 3)

  ImgDate   = pd.Timestamp(T).to_pydatetime()
  date_diff = float((ImgDate - midDate).days)
  #print(f'image date: {ImgDate}')
  
  time_score = 1.0/math.exp(0.5 * pow(date_diff/STD, 2))

  #==========================================================================================================
  # Extract some band images required for calculating spectral and cloud coverage scores
  #==========================================================================================================
  img = ready_IC.isel(time=i)
  
  max_SV  = xr.apply_ufunc(np.maximum, img['blue'], img['green'], dask='allowed')
  max_SW  = xr.apply_ufunc(np.maximum, img['swir16'], img['swir22'], dask='allowed')
  max_IR  = xr.apply_ufunc(np.maximum, img['nir08'], max_SW, dask='allowed')
  STD_blu = img['blue'].where(img['blue'] > 0.01, 0.01)
  
  #==================================================================================================
  # Calculate cloud coverage score
  #==================================================================================================
  CC_key = ImgDate.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
  CC_score = 1.0 - float(ready_IC.attrs["item_CC"][CC_key])/100.0

  #print(f'cloud cover score: {CC_score}')
  #print(ready_IC.attrs["item_CC"])

  # max_spec = xr.apply_ufunc(np.maximum, max_SV, max_IR, dask='allowed')

  # valid_mask  = xr.where(max_spec > 0.0, 1, 0)
  # valid_pixes = float(valid_mask.sum(dim=["x", "y"]).item())
  # total_pixes = float(img.sizes["x"] * img.sizes["y"])

  # CC_score = valid_pixes/total_pixes 
  
  #==================================================================================================
  # Calculate scores assuming all the pixels are land
  #==================================================================================================
  abs_blu_pen = xr.apply_ufunc(np.abs, img['blue'] - median_blu, dask='allowed')
  blu_pen     = xr.apply_ufunc(np.exp, abs_blu_pen, dask='allowed')

  nir_pen     = xr.apply_ufunc(np.abs, img['nir08']- median_nir, dask='allowed') 
  #nir_pen     = median_nir - img['nir08']
  #nir_pen     = nir_pen.where(nir_pen > 0, 0)
  
  #==================================================================================================
  # Calculate scores for non-vegetated surfaces
  #==================================================================================================
  nonveg_score = (median_blu+median_nir)/(blu_pen + nir_pen)

  spec_score = xr.where(median_nir > 2*median_blu, img['nir08']/(STD_blu + blu_pen + nir_pen), nonveg_score)
  #spec_score = land_score.where((max_SV < max_IR) | (max_SW > 3.0), water_score)
  spec_score = spec_score / (spec_score + 1) 
  
  del nir_pen, blu_pen, abs_blu_pen, max_IR, max_SW, max_SV, STD_blu, img
  
  #==================================================================================================
  # Determine the scoring weights based on the length of compositing window
  #==================================================================================================
  time_w = 0.4
  CC_w   = 0.9
  if WinSize > 31:
    time_w = 0.5

  return i, spec_score + time_w*time_score + CC_w*CC_score



#############################################################################################################
# Description: This function attaches a score band to each image in a XArray Dataset object, which is 
#              equivalent to an image collection in GEE.
# 
# Revision history:  2024-Oct-31  Lixin Sun  Initial creation
#
#############################################################################################################
def attach_score(ready_IC, SsrData, StartStr, EndStr):
  '''Attaches a score band to each image in a xarray Dataset object, an image collection equivalent in GEE.
     Args:
       ready_IC(xarray.dataset): A xarray dataset object containing a set of STAC images/items;
       SsrData(dictionary): A dictionary containing some metadata about a sensor;       
       StartStr(string): A string representing the start date of a compositing period;
       EndStr(string): A string representing the end date of a compositing period.'''  

  #==========================================================================================================
  # Create reference images for blue and NIR bands
  #==========================================================================================================
  median_blu, median_nir = get_score_refers(ready_IC)
  midDate = datetime.strptime(eoUs.period_centre(StartStr, EndStr), "%Y-%m-%d")
  start   = datetime.strptime(StartStr, "%Y-%m-%d")
  stop    = datetime.strptime(EndStr, "%Y-%m-%d")
  WinSize = (stop - start).days

  #==========================================================================================================
  # Parallelize the process of score calculations for every image in 'ready_IC'
  #==========================================================================================================
  time_vals = list(ready_IC.time.values)
  
  for i, T in enumerate(time_vals):    
    i, score = image_score(i, T, ready_IC, midDate, SsrData, median_blu, median_nir, WinSize)
    ready_IC[eoIM.pix_score][i, :,:] = score

  del median_blu, median_nir

  return ready_IC




#############################################################################################################
# Description: This function returns a list of band names to be loaded from a STAC catalog.  
# 
# Revision history:  2024-Oct-31  Lixin Sun  Initial creation
#                    2025-Jan-20  Lixin Sun  Modified to determine load bands mainly based on a given STAC
#                                            item and "IncludeAngles". 
#############################################################################################################
def get_load_bands(StacItem, Bands, IncludeAngles):
  '''
     Args:       
       STACItem(Dictionary): An optional input STACItem;
       Bands(List or dictionary): Bands item in "MosaicParams['Criteria'];"
       IncludeAngles(Boolean): Indicate if to include imaging angle bands.'''
  
  scene_id   = str(StacItem.id).lower()
  load_bands = None
  S2_names = ['s2a', 's2b', 's2c', 's2d']
  if any(name in scene_id for name in S2_names):
    load_bands = Bands
  elif 'hls.s30' in scene_id:
    load_bands = Bands['S2'] + Bands['angle'] if IncludeAngles == True else Bands['S2']
  elif 'hls.l30' in scene_id:
    load_bands = Bands['LS'] + Bands['angle'] if IncludeAngles == True else Bands['LS']

  else:
    print('f<get_load_bands> A wrong STAC item {scene_id} was provided!')
  
  return load_bands


#############################################################################################################
# Description: This function renames spectral band data variables in an xarray.dataset object from their
#              sensor-specific names to standardized spectral names such as blue, green and red.
#
# Revision history:  2024-Oct-31  Lixin Sun  Initial creation
#
#############################################################################################################
def rename_spec_bands(xrDS):
  ''' This function renames the spectral band images (data variables) in an xarray.dataset object.
    Args:
       xrDS(Xarray.dataset): A given Xarray.dataset object.
  '''
  
  #==========================================================================================================
  # Get the name list of the data variables in the given xrDS
  #==========================================================================================================
  data_vars = list(xrDS.data_vars) 
  if 'blue' in data_vars:   # Data variables are not renamed if they already use standardized names
    return xrDS
  
  #==========================================================================================================
  # Rename the data variables depending on different situations
  #==========================================================================================================
  if 'B08' in data_vars:   #For a HLS Sentinel-2A/B image from NASA's LP DAAC STAC catalog 
    if 'B05' not in data_vars:  
      out_DS = xrDS.rename({'B02': 'blue', 
                            'B03': "green", 
                            'B04': "red", 
                            'B08': "nir08", 
                            'B11': "swir16", 
                            'B12': "swir22"})
    else:
      out_DS = xrDS.rename({'B02': 'blue', 
                            'B03': "green", 
                            'B04': "red", 
                            'B05': "rededge1", 
                            'B06': "rededge2", 
                            'B07': "rededge3", 
                            'B08': "nir08", 
                            'B11': "swir16", 
                            'B12': "swir22"})  
  else:   #For a HLS Landsat-8/9 images from NASA's LP DAAC STAC catalog 
    out_DS = xrDS.rename({'B02': 'blue', 
                          'B03': "green", 
                          'B04': "red", 
                          'B05': "nir08", 
                          'B06': "swir16", 
                          'B07': "swir22"})  
  
  return out_DS  




#############################################################################################################
# Description: This function loads a list of STAC items at a specified resolution.
#
# Revision history:  2025-Dec-03  Lixin Sun  Initial creation
#
#############################################################################################################
def load_STAC_with_retry(STAC_items, bands, resolution, chunk_size, crs, max_attempts=15, sleep_sec=10):
  """
    Inputs:
      STAC_items: List of STAC items to be loaded;
      bands: List of band names to be loaded;
      resolution: Desired spatial resolution (e.g., 10, 20 or 30 meters);
      chunk_size: Dictionary defining the chunk sizes in X and Y dimensions;
      crs: String specifying projection;
      max_attempts: Maximum number of retry attempts for loading;
      sleep_sec: Number of seconds to wait between retry attempts.

    Returns:
        xarray.Dataset or None if failed.
  """
  attempt = 0
  while attempt < max_attempts:
    try:
      xrDS = odc.stac.load(
                STAC_items,
                bands=bands,
                chunks=chunk_size,
                crs=crs,
                fail_on_error=False,
                resolution=resolution,
                resampling="nearest",
                preserve_original_order=True)
      
      return xrDS  # success, return immediately
    
    except (TimeoutError, ConnectionError) as e:
      attempt += 1
      print(f"<load_stac_with_retry> Proxy/connection error: {e}. Retrying {attempt}/{max_attempts}...")
      if attempt < max_attempts:
        time.sleep(sleep_sec)

    except Exception as e:
      print(f"<load_stac_with_retry> An error occurred: {e}")
      return None
        
  return None  # all attempts failed




#############################################################################################################
# Description: This function loads a list of STAC items that were acquired by the same sensor (e.g., 
#              Sentinel-2 or Landsat series)
#
# Revision history:  2025-Jan-17  Lixin Sun  Initial creation
#
#############################################################################################################
def load_STAC_items(STAC_items, Bands, chunk_size, ProjStr, Scale):
  """
    Args:
      STAC_items(List): A list of STAC items to be loaded;
      Bands(List): A list of band names to be loaded;
      chunk_size(Dictionary): A dictionary defining the chunk sizes in X and Y dimensions;
      ProjStr(String): A string specifying projection;
      Scale(Float): A float number defining spatial resolution of the loaded images.
  """

  if len(STAC_items) < 1 or len(Bands) < 1:
    print('<load_STAC_items> Invalid parameter was provided!')  
    return None
  
  print('<load_STAC_items> Bands to be loaded are:', Bands)
  xrDS = load_STAC_with_retry(STAC_items, Bands, Scale, chunk_size, ProjStr)  

  print('<load_STAC_items> Complete calling load_STAC_with_retry function')
  if xrDS is None:
    return xrDS  

  #==========================================================================================================
  # Attach a dictionary that contains time tags and their corresponding cloud covers
  #==========================================================================================================
  item_CC = {}
  for item in STAC_items:
    properties = item.properties
    #print("<load_STAC_items> item time tag: datetime: {}; CC: {}".format(properties['datetime'], properties['eo:cloud_cover']))
    time_str = properties['datetime']
    #dt = datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S.%fZ")
    dt_obj  = parser.isoparse(time_str)
    iso_str = dt_obj.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    item_CC[iso_str] = properties['eo:cloud_cover']
  
  xrDS.attrs["item_CC"] = item_CC

  #print(f'item CCs {xrDS.attrs["item_CC"]}')

  return rename_spec_bands(xrDS)




#############################################################################################################
# Description: This function loads a list of STAC Sentinel-2 items at 10-m resolution ONLY.
#
# Note:        For 20-m bands, they will be loaded at 20-m resolution first and then duplicate to 10-m 
#              resolution.
#
# Revision history:  2025-Dec-03  Lixin Sun  Initial creation
#
#############################################################################################################
def load_STAC_10m_items(STAC_items, Bands_20m, chunk_size, ProjStr):
  """
    Args:
      STAC_items(List): A list of STAC items to be loaded;
      Bands_20m(List): A list of 20-m resolution band names to be loaded, 10m bands are always loaded;
      chunk_size(Dictionary): A dictionary defining the chunk sizes in X and Y dimensions;
      ProjStr(String): A string specifying projection.
  """
  nItems = len(STAC_items)

  if nItems < 1:
    print('<load_STAC_10m_items> Invalid parameters are provided!')  
    return None
  
  print('<load_STAC_10m_items> 20-m resolution bands to be loaded are:', Bands_20m)
  bands_10m = ['blue', 'green', 'red', 'nir08']  # 10-m bands to be loaded

  #==========================================================================================================
  # Load 10-m and 20-m resolution band images catalog separately
  #==========================================================================================================    
  xrDS_10m = load_STAC_with_retry(STAC_items, bands_10m, 10, chunk_size, ProjStr)
  
  if xrDS_10m is None:
    print('<load_STAC_10m_items> Failed to load 10-m resolution band images!')  
    return None
  
  xrDS_10m_allbands = xrDS_10m

  if len(Bands_20m) > 0:
    xrDS_20m = load_STAC_with_retry(STAC_items, Bands_20m, 20, chunk_size, ProjStr)

    if xrDS_20m is None:
      print('<load_STAC_10m_items> Failed to load 20-m resolution band images!')  
      return None
  
    #==========================================================================================================
    # Convert 20-m to 10-m resolution images as necessary
    #==========================================================================================================    
    def repeat_2x_to_10m(arr20, xrDS_10m):
      ax_y = arr20.get_axis_num("y")
      ax_x = arr20.get_axis_num("x")

      data10 = np.repeat(np.repeat(arr20.data, 2, axis=ax_y), 2, axis=ax_x)      
      
      temp_coords = {}
      dims = arr20.dims

      for d in dims:
        if d == "y":
          temp_coords["y"] = np.linspace(arr20.y[0], arr20.y[-1], data10.shape[ax_y])
        elif d == "x":
          temp_coords["x"] = np.linspace(arr20.x[0], arr20.x[-1], data10.shape[ax_x])
        else:
          temp_coords[d] = arr20.coords[d]
      
      arr10_tmp = xr.DataArray(data10,
                               dims  = dims,
                               coords=temp_coords,
                               attrs=arr20.attrs,
                               name=arr20.name)
      
      out = arr10_tmp.reindex(y=xrDS_10m.y, x=xrDS_10m.x, method=None)    # No interpolation: drop extra edge pixel
    
      out = out.rio.write_crs(xrDS_10m.rio.crs)
      out = out.rio.write_transform(xrDS_10m.rio.transform())

      return out

    xrDS20_to10 = xr.Dataset()

    for band in xrDS_20m.data_vars:
      arr20 = xrDS_20m[band]        # <-- DataArray
      arr10 = repeat_2x_to_10m(arr20, xrDS_10m)
      xrDS20_to10[band] = arr10

    xrDS20_to10["spatial_ref"] = xrDS_10m["spatial_ref"]
    xrDS_10m_allbands = xr.merge([xrDS_10m, xrDS20_to10])

  #==========================================================================================================
  # Attach a dictionary that contains time tags and their corresponding cloud covers
  #==========================================================================================================
  item_CC = {}
  for item in STAC_items:
    properties = item.properties
    #print("<load_STAC_items> item time tag: datetime: {}; CC: {}".format(properties['datetime'], properties['eo:cloud_cover']))
    time_str = properties['datetime']
    #dt = datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S.%fZ")
    dt_obj  = parser.isoparse(time_str)
    iso_str = dt_obj.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    item_CC[iso_str] = properties['eo:cloud_cover']
  
  xrDS_10m_allbands.attrs["item_CC"] = item_CC

  #print(f'item CCs {xrDS.attrs["item_CC"]}')

  return rename_spec_bands(xrDS_10m_allbands)




#############################################################################################################
# Description: This function preprocesses the given xrDS objects by adding sensor type, empty pixel score,
#              and pixel date layers, and applying default pixel masks and scaling factors (gain and offset). 
# 
# Revision history:  2025-Jan-17  Lixin Sun  Initial creation
#
############################################################################################################# 
def preprocess_xrDS(xrDS_S2, xrDS_LS, MosaicParams):
  """
    Args:
      xrDS_S2(XArray): A Xarray object Storing HLS S2 images;
      xrDS_S2(XArray): A Xarray object Storing HLS LS images;
      MosaicParams(Dictionary): A dictionary containing all the parameters required for generating a composite.
  """
  if xrDS_S2 is None and xrDS_LS is None:
    return None, None 
  
  if xrDS_S2 is not None and xrDS_LS is not None:    
    # When both HLS_S30 and HLS_L30 are available
    xrDS_S2[eoIM.pix_sensor] = xr.DataArray(data = dask.array.full((xrDS_S2.sizes['time'], xrDS_S2.sizes['y'], xrDS_S2.sizes['x']),
                                                                    eoIM.S2A_sensor,  # Value to fill the array with
                                                                    dtype=np.float32, 
                                                                    chunks=(1, 2000, 2000)),
                          dims   = ['time', 'y', 'x'],  # Dimensions should match those of existing variables in xrDS
                          coords = {'y': xrDS_S2['y'], 'x': xrDS_S2['x'], 'time' : xrDS_S2['time']}
                          )
    
    xrDS_LS[eoIM.pix_sensor] = xr.DataArray(data = dask.array.full((xrDS_LS.sizes['time'], xrDS_LS.sizes['y'], xrDS_LS.sizes['x']),
                                                                    eoIM.LS8_sensor,  # Value to fill the array with
                                                                    dtype=np.float32, 
                                                                    chunks=(1, 2000, 2000)),
                          dims   = ['time', 'y', 'x'],  # Dimensions should match those of existing variables in xrDS
                          coords = {'y': xrDS_LS['y'], 'x': xrDS_LS['x'], 'time' : xrDS_LS['time']}
                          )
    
    #Concatenate S2 and LS data into the same XAarray object
    xrDS = xr.concat([xrDS_LS, xrDS_S2], dim="time").sortby("time")

    # Merge two 'item_CC' dictionaries (cloud cover for each item) into one dictionary
    S2_item_CC = xrDS_S2.attrs["item_CC"]
    LS_item_CC = xrDS_LS.attrs["item_CC"]
    
    xrDS.attrs["item_CC"] = S2_item_CC | LS_item_CC
   
  elif xrDS_S2 is not None:
    # When only HLS_S30 data is available
    xrDS = xrDS_S2
  elif xrDS_LS is not None:
    # When only HLS_L30 data is available
    xrDS = xrDS_LS

  #==========================================================================================================
  # Attach three layers, an empty 'score', acquisition DOY and 'time_index', to eath item/image in "xrDS" 
  #==========================================================================================================  
  time_values = xrDS.coords['time'].values  

  time_datetime = pd.to_datetime(time_values)
  doys = [date.timetuple().tm_yday for date in time_datetime]  #Calculate DOYs for every temporal point  
  
  xrDS[eoIM.pix_score] = xrDS['blue']*0
  xrDS[eoIM.pix_date]  = xr.DataArray(np.array(doys, dtype='uint16'), dims=['time'])
  
  #==========================================================================================================
  # Apply default pixel mask, rescaling gain and offset to each image in 'xrDS'
  #==========================================================================================================
  SsrData = MosaicParams['SsrData']
  xrDS    = eoIM.apply_default_mask(xrDS, SsrData)
  xrDS    = eoIM.apply_gain_offset(xrDS, SsrData, 100, False)

  return xrDS, time_values






############################################################################################################# 
# This function should be here not in eoImage; Otherwise 1- dask workers get killed sometime 2- The execution time increases
############################################################################################################# 
def attach_AngleBands(xrDS, StacItems):  
  '''
  Attaches four imaging angle bands to a Sentinel-2 SURFACE REFLECTANCE mosaic image
  Args:    
    xrDS(xr Dateset): A xarray dataset object (a single image);
    StacItems(List): A list of STAC items corresponding to the given "xrDS".    
  '''  
  #==========================================================================================================
  # Sort the provided STAC items to match the image sequence in "xrDS"
  #==========================================================================================================  
  def get_sort_key(item):
    return item.datetime
  
  sorted_items = sorted(StacItems, key=get_sort_key)
  
  #==========================================================================================================
  # Create four np.arrays for storing the imaging geometry angles. 
  # Note: To be abble to apply "xr.apply_ufunc" function, these lists must be np.arrays
  #==========================================================================================================  
  SZAs = np.array([item.properties['sza'] for item in sorted_items])
  VZAs = np.array([item.properties['vza'] for item in sorted_items])
  SAAs = np.array([item.properties['saa'] for item in sorted_items])
  VAAs = np.array([item.properties['vaa'] for item in sorted_items])

  #==========================================================================================================
  # Define a function to map indices to the angle cosine values
  #==========================================================================================================
  def map_indices_to_values(IndxBand, values):
    indx_band = IndxBand.astype(np.int8)
    return values[indx_band]
  
  #==========================================================================================================
  # Apply the function using xarray.apply_ufunc
  #==========================================================================================================
  xrDS["SZA"] = xr.apply_ufunc(map_indices_to_values, xrDS["time_index"], SZAs).astype(np.float32)
  xrDS["VZA"] = xr.apply_ufunc(map_indices_to_values, xrDS["time_index"], VZAs).astype(np.float32)
  xrDS["SAA"] = xr.apply_ufunc(map_indices_to_values, xrDS["time_index"], SAAs).astype(np.float32)
  xrDS["VAA"] = xr.apply_ufunc(map_indices_to_values, xrDS["time_index"], VAAs).astype(np.float32)

  del SZAs, VZAs, SAAs, VAAs  
  
  return xrDS









def _extract_base_grid(BaseImg):
    """Create a lightweight description of BaseImg that workers can use to reindex.

    The returned dict contains small numpy arrays for coordinates and minimal
    spatial reference information if present.
    """
    base_grid = {}

    # Coordinates (as numpy arrays)
    try:
        base_grid["x"] = np.asarray(BaseImg.coords["x"].values)
        base_grid["y"] = np.asarray(BaseImg.coords["y"].values)
    except Exception:
        # Fallback for odd datasets: try index lookup
        base_grid["x"] = np.asarray(BaseImg["x"].values) if "x" in BaseImg.coords else np.asarray(BaseImg.coords[list(BaseImg.coords)[0]].values)
        base_grid["y"] = np.asarray(BaseImg["y"].values) if "y" in BaseImg.coords else np.asarray(BaseImg.coords[list(BaseImg.coords)[1]].values)

    # Attempt to capture spatial ref if stored as coordinate variable or attribute
    if "spatial_ref" in BaseImg.data_vars:
        try:
            # If it's a rasterio style object, keep as small array or attribute
            sr = BaseImg["spatial_ref"]
            # Many datasets include a single-value spatial_ref; store its attrs if any
            base_grid["spatial_ref"] = sr.attrs if hasattr(sr, "attrs") else None
        except Exception:
            base_grid["spatial_ref"] = None
    else:
        base_grid["spatial_ref"] = BaseImg.attrs.get("spatial_ref", None)

    # Keep a tiny template Dataset for reindexing (only coords)
    base_grid["template_coords"] = {"x": base_grid["x"], "y": base_grid["y"]}

    return base_grid










#############################################################################################################
# Description: 
# 
# Revision history:  2024-Nov-25  Marjan Asgari   This function is not a dask delayed anymore, because we are using dask arrays inside it
#                                                 Not loading one_DS in memory and instead we keep it as a dask array 
#                                                 max indices now should be computed before using it for slicing the mosaic array  
#
############################################################################################################# 
def get_granule_mosaic(Input_tuple):
  '''
     Args:
       Input_tuple(Tuple): A Tuple containing required parameters.
  '''
  
  #==========================================================================================================
  # Unpack the given tuple to obtain separate parameters
  #==========================================================================================================  
  BaseImg, granule, StacItems, MosaicParams = Input_tuple
  #BaseImgRefGrid, granule, StacItems, MosaicParams = Input_tuple

  #==========================================================================================================
  # Extract parameters from "MosaicParams"
  #==========================================================================================================  
  SsrData    = MosaicParams['SsrData']
  ProjStr    = MosaicParams['projection']
  Scale      = MosaicParams['resolution']
  Bands      = MosaicParams['Criteria']['bands']  
  InclAngles = MosaicParams['IncludeAngles']
  ChunkDict  = MosaicParams['chunk_size']

  StartStr, EndStr = eoPM.get_time_window(MosaicParams)  

  #==========================================================================================================
  # Load satellite images from a STAC catalog
  #==========================================================================================================
  one_granule_items = get_one_granule_items(StacItems, granule)                # Extract a list of STAC items based on an unique granule name
  filtered_items    = get_unique_STAC_items(one_granule_items, MosaicParams)   # Remain only one item from those that share the same timestamp
   
  if 'scl' in Bands:   # For Sentinel-2 images from AWS data catalog 
    #When in debugging mode, display metadata assets
    # if MosaicParams["debug"]:
    #   display_meta_assets(filtered_items['S2'], False)   
    if Scale > 10:
      xrDS_S2 = load_STAC_items(filtered_items['S2'], Bands, ChunkDict, ProjStr, Scale)  
    else:
      Bands_20m = [band for band in Bands if band not in ['blue', 'green', 'red', 'nir08']]
      xrDS_S2   = load_STAC_10m_items(filtered_items['S2'], Bands_20m, ChunkDict, ProjStr) 

    xrDS_LS = None  
  else:  #For both Snetinel-2 and Landsat data from LP DAAC of NASA
    S2_bands = Bands['S2'] + Bands['angle'] if InclAngles else Bands['S2']
    LS_bands = Bands['LS'] + Bands['angle'] if InclAngles else Bands['LS']
     
    xrDS_S2 = load_STAC_items(filtered_items['S2'], S2_bands, ChunkDict, ProjStr, Scale)
    xrDS_LS = load_STAC_items(filtered_items['LS'], LS_bands, ChunkDict, ProjStr, Scale)

  #==========================================================================================================     
  # Preprocess the loaded xarray Dataset objects by adding sensor type, empty pixel score, and pixel 
  # acquisition date layers, and applying default pixel masks and scaling factors (gain and offset). 
  #==========================================================================================================    
  xrDS, time_values = preprocess_xrDS(xrDS_S2, xrDS_LS, MosaicParams)
  
  if xrDS is None:
    return None
  
  #==========================================================================================================
  # Calculate compositing scores for every valid pixel in the xarray dataset (xrDS)
  #==========================================================================================================
  attach_score_args = functools.partial(attach_score, 
    SsrData  = SsrData, 
    StartStr = StartStr,
    EndStr   = EndStr
  )
  
  my_chunk = {'x': ChunkDict['x'], 'y':  ChunkDict['y'], 'time': xrDS.sizes['time']}             
  xrDS = xrDS.chunk(my_chunk).map_blocks(
    attach_score_args, 
    template = xrDS.chunk(my_chunk)
  )
  
  #==========================================================================================================
  # Create a composite image based on compositing scores
  # Note: calling "fillna" function before invoking "argmax" function is very important!!!
  #==========================================================================================================
  xrDS = xrDS.fillna(-10000.0).chunk(my_chunk)

  def granule_mosaic_template(xrDS, inBands, IncAngles):
    "Every variable used in this function must be input as a parameter. Global variables cannot be used!"
    mosaic_template = {}
    
    xrDA = xr.DataArray(
      data  = dask.array.zeros((xrDS.sizes['y'], xrDS.sizes['x']), chunks=(ChunkDict['x'], ChunkDict['y']), dtype=np.float32),
      dims  = ['y', 'x'],  # Include y and x only (no time here)
      coords= {'y': xrDS['y'], 'x': xrDS['x']},
    )

    for var_name in xrDS.data_vars:
      mosaic_template[var_name] = xrDA
    
    if IncAngles and 'scl' in inBands:
      for angle in ['SZA', 'VZA', 'SAA', 'VAA']:
        mosaic_template[angle]=  xrDA      

    return xr.Dataset(mosaic_template)
  
    
  def granule_mosaic(xrDS, Items, pix_score, time_values_len, inBands, IncAngles):
    "Every variable used in this function must be input as a parameter. Global variables cannot be used!"
    
    xrDS['time_index'] = xr.DataArray(np.array(range(0, time_values_len), dtype='uint8'), dims=['time'])
    max_indices        = xrDS[pix_score].argmax(dim="time")
    mosaic             = xrDS.isel(time=max_indices)
    
    #==========================================================================================================
    # Attach an additional bands as necessary 
    #==========================================================================================================
    nS2_imgs = len(Items['S2'])
    if IncAngles and 'scl' in inBands and nS2_imgs > 0:      
      mosaic = attach_AngleBands(mosaic, Items['S2'])

    #==========================================================================================================
    # Remove 'time_index', 'time', and 'spatial_ref' variables from submosaic 
    #==========================================================================================================
    return mosaic.drop_vars(["time", "spatial_ref", "time_index"])
    

  granule_mosaic_args = functools.partial(granule_mosaic, 
                                          Items = filtered_items,
                                          pix_score = eoIM.pix_score, 
                                          time_values_len = len(time_values),
                                          inBands = Bands,
                                          IncAngles = InclAngles)

  mosaic = xrDS.map_blocks(
    granule_mosaic_args, 
    template = granule_mosaic_template(xrDS, Bands, InclAngles)  # Pass the template (same structure as xrDS)
  )
  
  mosaic         = mosaic.where(mosaic[eoIM.pix_date] > 0)
  mosaic_aligned = mosaic.reindex_like(BaseImg).chunk(ChunkDict).fillna(-10000.0)
  #mosaic_aligned = mosaic.reindex_like(BaseImgRefGrid).chunk(ChunkDict).fillna(-10000.0)  

  # ==========================================================================================
  # NEW PART — Align mosaic to the BaseImg reference grid (subregion only)
  # ==========================================================================================
  # Compute the mosaic spatial window
  # xmin = float(mosaic.x.min())
  # xmax = float(mosaic.x.max())
  # ymin = float(mosaic.y.min())
  # ymax = float(mosaic.y.max())

  # # Extract only overlapping region of BaseImg reference grid
  # BaseSubset = BaseImgRefGrid.sel(
  #     x=slice(xmin, xmax),
  #     y=slice(ymax, ymin)  # S2 y axis decreases
  # )

  # # Align perfectly — identical x/y coordinates
  # mosaic_aligned, _ = xr.align(mosaic, BaseSubset, join="right")
  # mosaic_aligned = mosaic_aligned.chunk(ChunkDict).fillna(-10000.0)

  del xrDS, granule_mosaic_args
  gc.collect()
  
  return mosaic_aligned
  


#############################################################################################################
# Description: This function create a composite image by gradually merge the granule mosaics from 
#               "get_granule_mosaic" function.
# 
# Revision history:  2024-Oct-18  Lixin Sun  Initial creation
#
#############################################################################################################
def create_mosaic_at_once_distributed(base_img, unique_granules, stac_items, MosaicParams):
  """
  """
  unique_name = str(uuid.uuid4())
  tmp_directory = os.path.join(Path(MosaicParams["out_folder"]), f"dask_spill_{unique_name}")
  if not os.path.exists(tmp_directory):
    os.makedirs(tmp_directory)

  logging.getLogger('tornado').setLevel(logging.WARNING)
  logging.getLogger('tornado.application').setLevel(logging.CRITICAL)
  logging.getLogger('tornado.access').setLevel(logging.CRITICAL)
  logging.getLogger('tornado.general').setLevel(logging.CRITICAL)
  logging.getLogger('bokeh.server.protocol_handler').setLevel(logging.CRITICAL)

  count = 0 
  for granule in unique_granules:
    one_granule_items = get_one_granule_items(stac_items, granule)
    filtered_items    = get_unique_STAC_items(one_granule_items, MosaicParams)
    if 'S2' in filtered_items:
      count = count + len(filtered_items['S2'])
    if 'LS' in filtered_items:
      count = count + len(filtered_items['LS'])
  
  print(f'\n\n<<<<<<<<<< The count of all unique stack items is {count} >>>>>>>>>')
  
  with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    # these should be imported here due to "ValueError: signal only works in main thread of the main interpreter"
    from dask.distributed import Client
    from dask_jobqueue import SLURMCluster
    from dask import config
    
    def disable_spill():
      dask.config.set({
        'distributed.comm.retry.count': 5,
        'distributed.comm.timeouts.tcp' : 18000,
        'distributed.comm.timeouts.connect': 18000,
        'distributed.worker.memory.target': 1, 
        'distributed.worker.memory.spill': 0.95,
        'distributed.worker.memory.terminate': 0.95 
        })
    
    if MosaicParams["number_workers"] != 1 and  MosaicParams["nodes"] != 1:
      num_nodes  = MosaicParams["nodes"]
      memory     = MosaicParams["node_memory"]
      processes  = MosaicParams["number_workers"]
      cores      = processes * 4
    else: 
      num_nodes = 5
      processes = 4
      cores     = processes * 5
      if count >= 1000 and MosaicParams["resolution"] < 30:
        num_nodes = min(int(len(unique_granules) / processes), 10)
      memory    = "480G"
    
    os.environ['http_proxy'] = "http://webproxy.science.gc.ca:8888/"
    os.environ['https_proxy'] = "http://webproxy.science.gc.ca:8888/"
    out_file = Path(MosaicParams["out_folder"]) / f"log_{unique_name}.out"

    job_script_prologue_cr = ["export http_proxy=http://webproxy.science.gc.ca:8888/", "export https_proxy=http://webproxy.science.gc.ca:8888/"]
    if MosaicParams["sensor"] in ['HLSS30_SR', 'HLSL30_SR', 'HLS_SR']:
      eoPM.earth_data_authentication()
      job_script_prologue_cr.append("export CPL_VSIL_CURL_USE_HEAD=FALSE")
      job_script_prologue_cr.append("export GDAL_DISABLE_READDIR_ON_OPEN=YES")
      job_script_prologue_cr.append("export GDAL_HTTP_COOKIEJAR=/tmp/cookies.txt")
      job_script_prologue_cr.append("export GDAL_HTTP_COOKIEFILE=/tmp/cookies.txt")
    
    cluster = SLURMCluster(
      account = MosaicParams["account"],     # SLURM account
      queue = 'standard',        # SLURM partition (queue)
      walltime = '06:00:00',     
      cores = cores,
      processes = processes,      
      memory =memory,
      local_directory = tmp_directory,
      shared_temp_directory = os.path.expanduser("~"),
      worker_extra_args =[f"--memory-limit='{memory}'"],
      job_script_prologue = job_script_prologue_cr,
      job_extra_directives = [f" --output={out_file}"]
    )
    cluster.scale_up(n=num_nodes, memory=memory, cores=cores)
    client = Client(cluster, timeout=3000)
    client.register_worker_callbacks(setup=disable_spill)
    
    print(f'\n\n<<<<<<<<<< Dask dashboard is available {client.dashboard_link} >>>>>>>>>')
    while True:
      workers_info = client.scheduler_info()['workers']
      if len(workers_info) >= num_nodes * processes:
        print(f"Cluster has {len(workers_info)} workers. Proceeding...")
        break 
      else:
        print(f"Waiting for workers. Currently have {len(workers_info)} workers.")
        time.sleep(5)
    worker_names = [info['name'] for worker, info in workers_info.items()]
    
    # we submit the jobs to the cluster to process them in a distributed manner 
    granule_mosaics_data = [(base_img, granule, stac_items, MosaicParams) for granule in unique_granules]
    granule_mosaics = []

    for i in range(len(granule_mosaics_data)):
      worker_index = i % len(worker_names) 
      granule_mosaics.append(client.submit(get_granule_mosaic, granule_mosaics_data[i], workers=worker_names[worker_index], allow_other_workers=True))

    return granule_mosaics, client, cluster, unique_name



#############################################################################################################
# Description: This function create a composite image by gradually merging the submosaics for each granule 
#               "get_granule_mosaic" function.
# 
# Revision history:  2024-Oct-18  Lixin Sun  Initial creation
#
#############################################################################################################
def create_mosaic_at_once_one_machine(BaseImg, unique_granules, stac_items, MosaicParams):
  """
    Args:
      BaseImg(XArray): A XArray object to store the final composite image covering the entire ROI;
      unique_granules(List): A list of unique granule names;
      stac_items():
      MosaicParams(Dictionary): A dictionary containing all the parameters required for creating a final composite image. 
  """
  #==========================================================================================================
  # Create temporary folders within output directory for saving logging files
  #==========================================================================================================
  unique_name = str(uuid.uuid4())

  tmp_directory = os.path.join(Path(MosaicParams["out_folder"]), f"dask_spill_{unique_name}")
  if not os.path.exists(tmp_directory):
    os.makedirs(tmp_directory)
  
  logging.getLogger('tornado').setLevel(logging.WARNING)
  logging.getLogger('tornado.application').setLevel(logging.CRITICAL)
  logging.getLogger('tornado.access').setLevel(logging.CRITICAL)
  logging.getLogger('tornado.general').setLevel(logging.CRITICAL)
  logging.getLogger('bokeh.server.protocol_handler').setLevel(logging.CRITICAL)

  with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    # these should be imported here due to "ValueError: signal only works in main thread of the main interpreter"
    from dask.distributed import Client
    from dask.distributed import LocalCluster
    from dask import config
    
    #========================================================================================================
    # Special settings for linux platform
    #========================================================================================================
    if platform.system() == "Linux":    
      os.environ['http_proxy'] = "http://webproxy.science.gc.ca:8888/"
      os.environ['https_proxy'] = "http://webproxy.science.gc.ca:8888/"
    
    #========================================================================================================
    # Special settings for HLS data
    #========================================================================================================
    if MosaicParams["sensor"] in ['HLSS30_SR', 'HLSL30_SR', 'HLS_SR']:
      os.environ['CPL_VSIL_CURL_USE_HEAD'] = "FALSE"
      os.environ['GDAL_DISABLE_READDIR_ON_OPEN'] = "YES"
      os.environ['GDAL_HTTP_COOKIEJAR'] = "/tmp/cookies.txt"
      os.environ['GDAL_HTTP_COOKIEFILE'] = "/tmp/cookies.txt"
    
    #========================================================================================================
    # Create DASK local clusters for computation
    # Note: (1) always use ONR|E thread for each worker;
    #       (2) always set a memory limit for each Dask owrker 
    #========================================================================================================
    dask.config.set({
        'distributed.comm.retry.count': 5,
        'distributed.comm.timeouts.tcp' : 18000,
        'distributed.comm.timeouts.connect': 18000,

        # Memory thresholds for workers
        'distributed.worker.memory.target': 1.0,     # start spilling only at full memory
        'distributed.worker.memory.spill': 1.0,      # spill aggressively at full memory
        'distributed.worker.memory.pause': 1.0,      # do not pause task submission 
        'distributed.worker.memory.terminate': 2.0   # effectively prevent worker termination
        })    
      
    cluster = LocalCluster(
      n_workers          = MosaicParams["number_workers"],
      threads_per_worker = 1,  # always use ONE, more threads does not make computation faster
      memory_limit       = MosaicParams["node_memory"],   # the memory limit for each worker
      local_directory    = tmp_directory,
    )

    client = Client(cluster)
    #client.register_worker_callbacks(setup=disable_spill)
    print(f'\n\n<<<<<<<<<< Dask dashboard is available {client.dashboard_link} >>>>>>>>>')  
    
    #data = (BaseImg, unique_granules[0], stac_items, MosaicParams)
    #get_granule_mosaic(data)

    #========================================================================================================
    # Submit the jobs to the clusters to process them in a parallel mode 
    #========================================================================================================
    # def build_reference_grid(BaseImg):
    #   ref_grid = xr.Dataset(
    #     coords={
    #         'x': BaseImg['x'],
    #         'y': BaseImg['y'],
    #         'spatial_ref': BaseImg['spatial_ref']
    #     }
    #   )
      
    #   return ref_grid
    
    # BaseReferGrid = build_reference_grid(BaseImg)

    granule_mosaics_data = [(BaseImg, granule, stac_items, MosaicParams) for granule in unique_granules]
    #granule_mosaics_data = [(BaseReferGrid, granule, stac_items, MosaicParams) for granule in unique_granules]
    granule_mosaics      = [client.submit(get_granule_mosaic, data) for data in granule_mosaics_data]
    
    return granule_mosaics, client, cluster, unique_name








#############################################################################################################
# Description: This function creates/returns ONE composite image from images acquired within a given time 
#              period over a specific region.
# 
# Revision history:  2024-May-24  Lixin Sun  Initial creation
#                    2024-Jul-20  Lixin Sun  Modified to generate the final composite image tile by tile.
#############################################################################################################
def one_mosaic(AllParams, Output=True):
  '''
    Args:
      ProdParams(Dictionary): A dictionary containing all parameters related to composite image production;
      CompParams(Dictionary): A dictionary containing all parameters related to used computing environment.
      Output(Boolean): An integer indicating wheter to export resulting composite image.'''
  
  mosaic_start = time.time()   #Record the start time of the whole process
  ChunkDict = AllParams['chunk_size']

  #==========================================================================================================
  # Search all the STAC items based on a spatial region and a time window
  # Note: (1) The 2nd parameter (MaxImgs) for "search_STAC_Catalog" function cannot be too large. Otherwise,
  #           a server internal error will be triggered.
  #       (2) The imaging angles will be attached to each STAC item by "search_STAC_Catalog" function if S2
  #           data from AWS is used.
  #==========================================================================================================  
  stac_items = search_STAC_Catalog(AllParams, 100)  
  print(f"\n<period_mosaic> A total of {len(stac_items):d} items were found.\n")

  #When in debugging mode, display metadata assets
  if AllParams["debug"]:
    display_meta_assets(stac_items, True)   
  
  #==========================================================================================================
  # Create a base image that fully spans ROI
  #==========================================================================================================
  base_img = get_base_Image(stac_items, AllParams, ChunkDict)
  print('\n<period_mosaic> based mosaic image = ', base_img)
  
  #==========================================================================================================
  # Extract unique granule names and iterate over them to generate submosaic separately 
  #==========================================================================================================  
  unique_granules = get_unique_tile_names(stac_items)  #Get all unique tile names 
  print('\n<period_mosaic> The number of unique granule tiles = %d'%(len(unique_granules)))  

  if AllParams["debug"]:
    print('\n<<<<<< The unique granule tiles = ', unique_granules)  

  #==========================================================================================================
  # Create submosaic separately for each granule in parallel and on distributed workers
  #==========================================================================================================
  if AllParams["debug"]:
    submited_granules_mosaics, client, cluster, unique_name = create_mosaic_at_once_one_machine(base_img, unique_granules, stac_items, AllParams)
  else:
    submited_granules_mosaics, client, cluster, unique_name = create_mosaic_at_once_distributed(base_img, unique_granules, stac_items, AllParams)
  
  persisted_granules_mosaics = dask.persist(*submited_granules_mosaics, optimize_graph=True)
  for future, granules_mosaic in as_completed(persisted_granules_mosaics, with_results=True):    
    base_img = merge_granule_mosaics(granules_mosaic, base_img, eoIM.pix_score)
    client.cancel(future)
  
  # We do the compute to get a dask array instead of a future
  base_img = base_img.chunk(ChunkDict).compute()

  #==========================================================================================================
  # Mask out the pixels with negative date value
  #========================================================================================================== 
  mosaic = base_img.where(base_img[eoIM.pix_date] > 0) 

  #==========================================================================================================
  # Output resultant mosaic as required
  #========================================================================================================== 
  if Output:
    export_mosaic(AllParams, mosaic)
    print('\n<<<<<<<<<< Complete saving composite images >>>>>>>>>')

  #==========================================================================================================
  # Close the cluster and client
  #========================================================================================================== 
  try:
    client.close(timeout=600)
    cluster.close(timeout=600)

  except asyncio.CancelledError:
    print("Cluster is closed!")

  mosaic_stop = time.time()
  mosaic_time = (mosaic_stop - mosaic_start)/60 
  print('\n\n<<<<<< The total elapsed time for mosaic generation = %6.2f minutes >>>>>>'%(mosaic_time))

  #==========================================================================================================
  # Create logging files
  #========================================================================================================== 
  dask_out_file  = Path(AllParams["out_folder"]) / f"log_{unique_name}.out"
  dask_directory = os.path.join(Path(AllParams["out_folder"]), f"dask_spill_{unique_name}")

  if os.path.exists(dask_out_file):
      os.remove(dask_out_file)
      print(f"File '{dask_out_file}' has been deleted.")
  else:
      print(f"File '{dask_out_file}' does not exist.")

  if os.path.exists(dask_directory):
      shutil.rmtree(dask_directory)
      print(f"Directory '{dask_directory}' and its contents have been deleted.")
  else:
      print(f"Directory '{dask_directory}' does not exist.")
  
  return mosaic







#############################################################################################################
# Description: This function merges a submosaic into the base image based on pixel scores
#
# Note: This is the unique delayed function and will be executed on Dask workers.
#############################################################################################################
@delayed
def merge_granule_mosaics(mosaic, base_img, pix_score):
  """
  Process a single mosaic by applying the mask and updating base_img. 
  Args:
    mosaic(XArray): A given submosaic image;
    base_img(XArray): A base image to hold the resulting composite for entire ROI;
    pix_score(String): The string name of pixel score layer. """
  
  if mosaic is not None:
    mask = mosaic[pix_score] > base_img[pix_score]
    for var in base_img.data_vars:
      base_img[var] = xr.where(mask, mosaic[var], base_img[var])
    return base_img  # Return the updated base_img
  
  return None  # If mosaic is None, return None





#############################################################################################################
# Description: This function exports the band images of a mosaic into separate GeoTiff files
#
# Revision history:  2024-May-24  Lixin Sun  Initial creation
#                    2024-Dec-04  Marjan Asgari Add .compute() on the mosaic before saving it to tiff files
#############################################################################################################
def export_mosaic(inParams, inMosaic):
  '''
    This function exports the band images of a mosaic into separate GeoTiff files.

    Args:
      inParams(dictionary): A dictionary containing all required execution parameters;
      inMosaic(xrDS): A xarray dataset object containing mosaic images to be exported.'''
  
  #==========================================================================================================
  # Convert float pixel values to integers and then reproject the mosaic image
  #==========================================================================================================
  if '16' in inParams['out_datatype']:
    mosaic_int = (inMosaic * 100.0).astype(np.int16)
  else:  
    mosaic_int = (inMosaic).astype(np.int8)   # For testing integer value band images

  rio_mosaic = mosaic_int.rio.write_crs(inParams['projection'], inplace=True)  # Assuming WGS84 for this example

  #==========================================================================================================
  # Create a directory to store the output files
  #==========================================================================================================
  dir_path = inParams['out_folder']
  os.makedirs(dir_path, exist_ok=True)

  #==========================================================================================================
  # Create prefix filename
  #==========================================================================================================
  SsrData    = eoIM.SSR_META_DICT[str(inParams['sensor'])]   
  region_str = str(inParams['current_region'])
  period_str = str(inParams['time_str'])
 
  filePrefix = f"{SsrData['NAME']}_{region_str}_{period_str}"

  #==========================================================================================================
  # Create individual sub-mosaic and combine it into base image based on score
  #==========================================================================================================
  spa_scale    = inParams['resolution']
  export_style = str(inParams['export_style']).lower()
  #rio_mosaic   = rio_mosaic.compute()  
  ext_saved    = []
  if 'sepa' in export_style:
    #out_bands = rio_mosaic.data_vars if spa_scale > 15 else ['blue', 'green', 'red', 'nir08', 'score', 'date']
    out_bands = rio_mosaic.data_vars
    if 'bands' in inParams:
      for band in inParams["bands"]:
        if band.lower() not in [b.lower() for b in out_bands]: 
          out_bands.append(band)
    
    out_bands = list(set(band for band in out_bands))
    for band in out_bands:
      out_img     = rio_mosaic[band]
      if band == eoIM.pix_sensor:
        out_img = out_img.where(out_img >= 0, 0) / 100
      filename    = f"{filePrefix}_{band}_{spa_scale}m.tif"
      output_path = os.path.join(dir_path, filename)
      out_img.rio.to_raster(output_path)

  else:
    filename = f"{filePrefix}_mosaic_{spa_scale}m.tif"
    output_path = os.path.join(dir_path, filename)
    rio_mosaic.to_netcdf(output_path)
    ext_saved.append("mosaic")
  
  #return ext_saved, period_str


def get_slurm_node_cpu_cores():
  result = subprocess.check_output(f"scontrol show job {os.getenv('SLURM_JOB_ID')}", shell=True).decode()
  for line in result.splitlines():
      if 'TresPerTask' in line:
          tres_per_task = line.split("=")[1]
          if 'cpu:' in tres_per_task:
              cpu_count = tres_per_task.split('cpu:')[1]
              try:
                  cpu_count = int(cpu_count)
                  return cpu_count
              except ValueError:
                  return 1
          else:
              return 1
          
