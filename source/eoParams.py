import os
import re
import json
import pyproj
import sys
import netrc
import psutil
import argparse
import subprocess
import eoImage as eoIM
import eoUtils as eoUs
from pathlib import Path
import eoTileGrids as eoTG
from datetime import datetime

#############################################################################################################
# Description: Define a default execution parameter dictionary. 
# 
# Revision history:  2022-Mar-29  Lixin Sun  Initial creation
#
#############################################################################################################
# DefaultParams = {
#     'sensor': 'S2_SR',           # A sensor type and data unit string (e.g., 'S2_Sr' or 'L8_SR')    
#     'unit': 2,                   # Data unite (1=> TOA reflectance; 2=> surface reflectance)
#     'year': 2019,                # An integer representing image acquisition year
#     'nbYears': 1,                # Positive int for annual product, or negative int for monthly product
#     'months': [5,6,7,8,9,10],    # A list of integers represening one or multiple monthes     
#     'tile_names': ['tile55'],    # A list of (sub-)tile names (defined using CCRS' tile griding system) 
#     'prod_names': ['mosaic'],    # ['mosaic', 'LAI', 'fCOVER', ]
#     'resolution': 30,            # Exporting spatial resolution
#     'out_folder': '',            # The folder name for exporting
#     'export_style': 'separate',  # Two possible values are supported: 'separate' (default) or 'stack'
#     'out_datatype': 'int16',     # Two possible values are supported: 'int16' (default) or 'int8'
#     'start_dates': [''],         # A list of strings representing starting dates, e.g., ['2024-06-15','2024-07-15']  
#     'end_dates':  [''],          # A list of strings representing ending dates, e.g., ['2024-07-15','2024-08-15']
#     'scene_ID': '',              # A single image ID
#     'projection': 'EPSG:3979',   # Commonly used in CCRS
#     'CloudScore': False,         # Default value is False

#     'current_month': -1,         # Used internally in the code
#     'current_tile': '',          # Used internally in the code
#     'time_str': '',              # Mainly for creating output filename
#     'region_str': ''             # Mainly for creating output filename
# }



all_param_keys = ['sensor', 'ID', 'unit', 'bands', 'year', 'nbYears', 'months', 'tile_names', 'prod_names', 
                  'out_location', 'resolution', 'GCS_bucket', 'out_folder', 'export_style', 'out_datatype', 'projection', 'CloudScore',
                  'monthly', 'start_dates', 'end_dates', 'regions', 'scene_ID', 'current_time', 'current_region', 
                  'time_str','cloud_cover', 'SsrData', 'Criteria', 'IncludeAngles', 'debug', 'entire_tile',
                  'nodes', 'node_memory', 'number_workers', 'worker_threads', 'chunk_size', 'account', 'standardized']




#############################################################################################################
# Description: This function determines which product is required: mosaic or vegetation parameters
#
# Revision history:  2025-Sept-21  Lixin Sun  Initial creation
#
#############################################################################################################
def which_product(ProdParams):
  '''
    Args:  
      ProdParams(dictionary): A given parameter dictionary. 
  '''  
  #Ensure 'prod_names' is a key of "ProdParams" dictionary  
  if 'prod_names' in ProdParams:    
    # Covert all product name strings to lower cases
    prod_names = [s.lower() for s in ProdParams['prod_names']]  

    if 'lai' in prod_names or 'fcov' in prod_names or 'fap' in prod_names or 'alb' in prod_names:
      return 'veg_parama'
    elif 'mosaic' in prod_names:
      return 'mosaic' 
    else:
      return 'nothing' 
  else:
    return 'nothing' 




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
def get_query_conditions(inParams, StartStr, EndStr, Region):  
  #==================================================================================================
  # Create a filter for the search based on metadata. The filtering params will depend upon the 
  # image collection we are using. e.g. in case of Sentine-2 L2A, we can use params such as: 
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
  print(f'\n\n <get_query_conditions> inParams = {inParams}')

  SsrData     = inParams['SsrData']
  ssr_code    = SsrData['SSR_CODE']
  resolution  = inParams['resolution']
  CloudThresh = inParams['cloud_cover']
  
  HLS_angle_bands = ['SZA', 'SAA', 'VZA', 'VAA']

  query_conds = {}  
  query_conds['timeframe'] = str(StartStr) + '/' + str(EndStr)
  query_conds['filters']   = {"eo:cloud_cover": {"lt": CloudThresh} }
  query_conds['region']    = Region

  if ssr_code > eoIM.MAX_LS_CODE and ssr_code < eoIM.MOD_sensor:
    # For Sentinel-2 data from AWS data catalog
    query_conds['catalog']    = "https://earth-search.aws.element84.com/v1"
    query_conds['collection'] = ["sentinel-2-l2a"]
    
    if 'bands' not in inParams:
      query_conds['bands'] = SsrData['ALL_BANDS'] + ['scl'] if resolution > 15 else SsrData['SIX_BANDS'] + ['scl'] 

    else : 
      required_bands = SsrData['ALL_BANDS'] + ['scl'] if resolution > 15 else SsrData['SIX_BANDS'] + ['scl'] 
      for band in inParams['bands']:
          if band.lower() not in [b.lower() for b in required_bands]: 
              required_bands.append(band)
      # Remove duplicates
      required_bands = list(set(band.lower() for band in required_bands))
      query_conds['bands']  = required_bands

  elif ssr_code < eoIM.MAX_LS_CODE and ssr_code > 0:
    # For Landsat data from AWS data catalog (This is not working right now!!!!) 
    query_conds['catalog']    = "https://earth-search.aws.element84.com/v1"
    query_conds['collection'] = ["landsat-c2-l2"]
    query_conds['bands']      = ['blue', 'green', 'red', 'nir08', 'swir16', 'swir22', 'qa_pixel']
  
  elif ssr_code > eoIM.MOD_sensor and resolution >= 10:
    # For HLS data from NASA data centre
    if ssr_code == eoIM.HLSS30_sensor:
      query_conds['collection'] = ["HLSS30_2.0"]
      query_conds['bands']      = {'S2': ['B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B08', 'B11', 'B12', 'Fmask'],
                                   'LS': ['B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'Fmask'],
                                   'angle': HLS_angle_bands}
    elif ssr_code == eoIM.HLSL30_sensor:
      query_conds['collection'] = ["HLSL30_2.0"]
      query_conds['bands']      = {'S2': ['B02', 'B03', 'B04', 'B08', 'B11', 'B12', 'Fmask'],
                                   'LS': ['B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'Fmask'],
                                   'angle': HLS_angle_bands}
        
    elif ssr_code == eoIM.HLS_sensor:
      query_conds['collection'] = ["HLSS30_2.0", "HLSL30_2.0"]
      query_conds['bands']      = {'S2': ['B02', 'B03', 'B04', 'B08', 'B11', 'B12', 'Fmask'],
                                   'LS': ['B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'Fmask'],
                                   'angle': HLS_angle_bands}
      
    query_conds['catalog']   = "https://cmr.earthdata.nasa.gov/stac/LPCLOUD"

  else:
    print('<get_query_conditions> Wrong sensor code or spatial resolution was specified!')
    return None
  
  return query_conds




#############################################################################################################
# Description: This function tells if there is a customized region defined in parameter dictionary.
# 
# Revision history:  2024-Feb-27  Lixin Sun  Initial creation
#
#############################################################################################################
def has_custom_region(inParams):  
  
  nRegions = len(inParams['regions']) if 'regions' in inParams else 0
  
  if 'scene_ID' not in inParams:
    inParams['scene_ID'] = ''

  return True if nRegions > 0 or len(inParams['scene_ID']) > 5 else False 




#############################################################################################################
# Description: This function tells if customized time windows are defined in a given parameter dictionary.
# 
# Revision history:  2024-Feb-27  Lixin Sun  Initial creation
#
#############################################################################################################
def has_custom_window(inParams):
  start_len = len(inParams['start_dates']) if 'start_dates' in inParams else 0
  end_len   = len(inParams['end_dates']) if 'end_dates' in inParams else 0

  custom_time = False
  if start_len >= 1 and end_len >= 1 and start_len == end_len:
    custom_time = True
  
  elif start_len >= 1 and end_len >= 1 and start_len != end_len:  
    print('\n<has_custom_window> Inconsistent customized time list!')
  
  return custom_time
  




#############################################################################################################
# Description: This function sets values for 'current_time' and 'time_str' keys.
# 
# Note: If a customized time windows has been specified, then the given 'current_month' will be ignosed
#
# Revision history:  2024-Apr-08  Lixin Sun  Initial creation
#                    2025-Oct-06  Lixin Sun  Update query conditions accordingly the change in current time
#  
#############################################################################################################
def set_current_time(inParams, current_time):
  '''Sets values for 'curent_time' and 'time_str' keys based on 'current_time' input
     Args:
       inParams(Dictionary): A dictionary storing required input parameters;
       current_time(Integer): An integer representing the index in the list corresponding to 'start_dates'/'end_dates' keys.'''
  
  if 'start_dates' not in inParams or 'end_dates' not in inParams:
    print('\n<set_current_time> There is no \'start_dates\' or \'end_dates\' key!')
    return None
  
  #==========================================================================================================
  # Ensure the given 'current_time' is valid.
  #==========================================================================================================
  nStarts = len(inParams['start_dates'])
  nStops  = len(inParams['end_dates'])

  if nStarts != nStops or current_time < 0 or current_time >= nStarts:
    print('\n<set_current_time> Invalid \'current_time\' was provided!')
    return None
  
  #==========================================================================================================
  # Set values for 'current_time' and 'time_str' keys
  #==========================================================================================================
  inParams['current_time'] = current_time

  if inParams['monthly']:
    inParams['time_str'] = eoIM.get_MonthName(int(inParams['months'][current_time]))
  else:  
    inParams['time_str'] = str(inParams['start_dates'][current_time]) + '_' + str(inParams['end_dates'][current_time])

  #==========================================================================================================
  # Update STAC query timeframe accordingly 
  #==========================================================================================================
  startT, stopT = get_time_window(inParams)

  if 'Criteria' in inParams:
    inParams['Criteria']['timeframe'] = str(startT) + '/' + str(stopT)
  else:
    print('<set_current_time> Criteria key does not exist in Parameter dictionary!!')

  return inParams





#############################################################################################################
# Description: This function sets values for 'current_tile' and 'region_str' keys
# 
# Note: If a customized spatial region has been specified, then the given 'current_tile' will be ignosed
#
# Revision history:  2024-Apr-08  Lixin Sun  Initial creation
#
#############################################################################################################
def set_spatial_region(inParams, region_name):
  '''
    Args:
      inParams(dictionary): A given parameter dictionary for production;
      region_name(string): A specified region name.
  '''
  # Ensure 'inParams' dictionary contains a ket named 'regions'
  if 'regions' not in inParams:
    print('\n<set_spatial_region> There is no \'regions\' key!')
    return None
  
  # Get all names of all spatial region from 'inParams'
  region_names = inParams['regions'].keys()

  # Ensure the specified region name is included in region names
  if region_name not in region_names:
    print('<set_spatial_region> {} is an invalid tile name!'.format(region_name))
    return None
  
  # Change value for 'current_region' key 
  inParams['current_region'] = region_name
  
  #==========================================================================================================
  # Update STAC query region accordingly 
  #==========================================================================================================
  Region = inParams['regions'][region_name]

  if 'Criteria' in inParams:
    inParams['Criteria']['region'] = Region
  else:
    print('<set_current_time> Criteria key does not exist in Parameter dictionary!!')

  # Return the modified parameter dictionary
  return inParams





#############################################################################################################
# Description: This function fills default values for some critical parameters
# 
# Revision history:  2024-Oct-08  Lixin Sun  Initial creation
#                    2025-Jan-22  Lixin Sun  Added 'IncludeAngles' key and its default value (False)
#############################################################################################################
# def fill_critical_params(inParams):  
#   if 'sensor' not in inParams:
#     inParams['sensor'] = 'S2_SR'

#   if 'year' not in inParams:
#     inParams['year'] = datetime.now().year

#   if 'nbYears' not in inParams:
#     inParams['nbYears'] = 1
  
#   if 'prod_names' not in inParams:
#     inParams['prod_names'] = []
  
#   if 'out_location' not in inParams:
#     inParams['out_location'] = 'drive'
 
#   if 'resolution' not in inParams:
#     inParams['resolution'] = 30
  
#   if 'out_folder' not in inParams:
#     inParams['out_folder'] = inParams['sensor'] + '_' + inParams['year'] + '_results' 

#   if 'months' not in inParams:
#     inParams['months'] = []

#   if 'tile_names' not in inParams:
#     inParams['tile_names'] = []

#   if 'export_style' not in inParams:
#     inParams['export_style'] = 'separate'

#   if 'projection' not in inParams:
#     inParams['projection'] = 'EPSG:3979'
  
#   if 'IncludeAngles' not in inParams:
#     inParams['IncludeAngles'] = False

#   sensor_type = inParams['sensor'].lower()
#   if sensor_type.find('s2') < 0:
#     inParams['CloudScore'] = False 

#   return inParams




#############################################################################################################
# Description: This function validate a given user parameter dictionary.
#
# Revision history:  2024-Jun-07  Lixin Sun  Initial creation
#       
#############################################################################################################
def valid_ProdParams(UserParams):
  #==========================================================================================================
  # Ensure all the keys in user's parameter dictionary are valid
  #==========================================================================================================
  all_valid    = True
  user_keys    = list(UserParams.keys())
  default_keys = all_param_keys
  n_user_keys  = len(user_keys)

  key_presence = [element in default_keys for element in user_keys]
  for index, pres in enumerate(key_presence):
    if pres == False and index < n_user_keys:
      all_valid = False
      print('<valid_user_params> \'{}\' key in given parameter dictionary is invalid!'.format(user_keys[index]))
  
  if not all_valid:
    return all_valid, None  

  #==========================================================================================================
  # Validate values of critical parameters
  #==========================================================================================================  
  outParams = UserParams

  #----------------------------------------------------------------------------------------------------------
  # Fill or valid 'sensor' parameter
  #----------------------------------------------------------------------------------------------------------
  if 'sensor' not in outParams:
    outParams['sensor'] = 'S2_SR'
  else:  
    sensor_name = str(outParams['sensor']).upper()
    all_SSRs = ['S2_SR', 'HLSS30_SR', 'HLSL30_SR', 'HLS_SR', 'L5_SR', 'L7_SR', 'L8_SR', 'L9_SR']
    if sensor_name not in all_SSRs:
      all_valid = False
      print('<valid_user_params> Invalid sensor or unit was specified!')
  
  outParams['SsrData'] = eoIM.SSR_META_DICT[str(outParams['sensor']).upper()]

  #----------------------------------------------------------------------------------------------------------
  # Fill or valid 'year' parameter
  #----------------------------------------------------------------------------------------------------------
  if 'year' not in outParams:
    outParams['year'] = datetime.now().year
  else:  
    year = int(outParams['year'])
    if year < 1970 or year > datetime.now().year:
      all_valid = False
      print('<valid_user_params> Invalid year was specified!')

  #----------------------------------------------------------------------------------------------------------
  # Fill or valid 'bands' parameter
  #----------------------------------------------------------------------------------------------------------
  if 'bands' in outParams:
    bands = outParams['bands']
    valid_bands = eoIM.SSR_META_DICT[str(outParams['sensor']).upper()]["ALL_BANDS"]
    for band in bands:
      if band.lower() not in valid_bands:
        all_valid = False
        print(f'<valid_user_params> Invalid band name, {band}, was specified!')
    outParams['bands'] = [band.lower() for band in bands]
  
  #----------------------------------------------------------------------------------------------------------
  # Fill or valid 'nbYears' parameter
  #----------------------------------------------------------------------------------------------------------
  if 'nbYears' not in outParams:
    outParams['nbYears'] = 1
  else:  
    if int(outParams['nbYears']) > 3:
      all_valid = False
      print('<valid_user_params> Invalid number of years was specified!')

  #----------------------------------------------------------------------------------------------------------
  # Fill or valid 'prod_names' parameter
  #----------------------------------------------------------------------------------------------------------
  if 'prod_names' not in outParams:
    outParams['prod_names'] = []
  else:  
    prod_names = outParams['prod_names']
    nProds = len(prod_names)
    if nProds < 1:
      all_valid = False
      print('<valid_user_params> No product name was specified for prod_names key!')
  
    valid_prod_names = ['LAI', 'fAPAR', 'fCOVER', 'Albedo', 'mosaic', 'QC', 'date', 'partition']
    presence = [element in valid_prod_names for element in prod_names]
    if False in presence:
      all_valid = False
      print('<valid_user_params> At least one of the specified products is invalid!')
  
  #----------------------------------------------------------------------------------------------------------
  # Fill or valid 'out_location' parameter
  #----------------------------------------------------------------------------------------------------------
  if 'out_location' not in outParams:
    outParams['out_location'] = 'drive'
  else:  
    out_location = str(outParams['out_location']).upper()  
    if out_location not in ['DRIVE', 'STORAGE', 'ASSET']:
      all_valid = False
      print('<valid_user_params> Invalid out location was specified!')

  #----------------------------------------------------------------------------------------------------------
  # Fill or valid 'resolution' parameter
  #----------------------------------------------------------------------------------------------------------
  if 'resolution' not in outParams:
    outParams['resolution'] = 30
  else:  
    resolution = int(outParams['resolution'])
    if resolution < 1:
      all_valid = False
      print('<valid_user_params> Invalid spatial resolution was specified!')
    elif resolution < 15:  #For high resolution, angle bands should not be included
      outParams['IncludeAngles'] = False
      if 's2' not in str(sensor_name).lower():
        print(f'<valid_user_params> {resolution}m resolution images are only available in Sentinel-2 catalog of AWS!')
        all_valid = False
  
  if outParams['SsrData']['SSR_CODE'] >= eoIM.HLSS30_sensor and int(outParams['resolution']) < 20:
    # For HLS data, the minimal resolution is 20m 
    outParams['resolution'] = 20

  #----------------------------------------------------------------------------------------------------------
  # Fill or valid 'out_folder' parameter
  #----------------------------------------------------------------------------------------------------------
  if 'out_folder' not in outParams:
    outParams['out_folder'] = outParams['sensor'] + '_' + outParams['year'] + '_results' 
  else:
    out_folder = str(outParams['out_folder'])
    if Path(out_folder) == False or len(out_folder) < 2:
      all_valid = False
      print('<valid_user_params> The specified output path is invalid!')
  
  #----------------------------------------------------------------------------------------------------------
  # Fill or valid 'months' parameter
  #----------------------------------------------------------------------------------------------------------
  if 'months' not in outParams:
    outParams['months'] = []
  else:      
    max_month = max(outParams['months'])
    if max_month > 12:   #month number < 1 means an entire peak season 
      all_valid = False
      print('<valid_user_params> Invalid month number was specified!')

  #----------------------------------------------------------------------------------------------------------
  # Fill or valid 'tile_names' parameter
  #----------------------------------------------------------------------------------------------------------
  if 'tile_names' not in outParams:
    outParams['tile_names'] = []
  else:
    if outParams['tile_names'] is not None:
      tile_names = outParams['tile_names']
      nTiles = len(tile_names)
      if nTiles < 1:
        all_valid = False
        print('<valid_user_params> No tile name was specified for tile_names key!')

      for tile in tile_names:
        if eoTG.valid_tile_name(tile) == False:
          all_valid = False
          print('<valid_user_params> {} is an invalid tile name!'.format(tile))

    else:
      region_coords = outParams['regions']['coordinates'][0]
      for point in region_coords:
          lon, lat = point
          if not (-180 <= lon <= 180):
            print('<valid_user_params> Longitude {} is out of bounds. Must be between -180 and 180.'.format(lon))
          if not (-90 <= lat <= 90):
            print('<valid_user_params> Latitude {} is out of bounds. Must be between -90 and 90.'.format(lon))
      
      if len(region_coords) != 5 or region_coords[0] != region_coords[-1]:
          raise ValueError("Coordinates must be a closed polygon with 5 points (first equals last).")
  
  #----------------------------------------------------------------------------------------------------------
  # Fill or valid 'cloud_cover' parameter
  #----------------------------------------------------------------------------------------------------------
  if 'cloud_cover' not in outParams:
    outParams['cloud_cover'] = 80
  else:
    cloud_cover = outParams['cloud_cover']
    if cloud_cover > 100 or cloud_cover < 0:
      outParams['cloud_cover'] = 80

  #----------------------------------------------------------------------------------------------------------
  # Fill or valid 'export_style' parameter
  #----------------------------------------------------------------------------------------------------------
  if 'export_style' not in outParams:
    outParams['export_style'] = 'separate'
  elif 'separate' not in outParams['export_style'] and 'stack' not in outParams['export_style']:
    outParams['export_style'] = 'separate'

  #----------------------------------------------------------------------------------------------------------
  # Fill or valid 'out_datetype' parameter
  #----------------------------------------------------------------------------------------------------------
  if 'out_datatype' not in outParams:
    outParams['out_datatype'] = 'int16'
  elif '16' not in outParams['out_datatype'] and '8' not in outParams['out_datatype']:
    outParams['out_datatype'] = 'int16'
   
  #----------------------------------------------------------------------------------------------------------
  # Fill or valid 'projection' parameter
  #---------------------------------------------------------------------------------------------------------- 
  if 'projection' not in outParams:
    outParams['projection'] = 'EPSG:3979'
  
  #----------------------------------------------------------------------------------------------------------
  # Fill or valid 'IncludeAngles' parameter
  #----------------------------------------------------------------------------------------------------------
  if 'IncludeAngles' not in outParams:
    outParams['IncludeAngles'] = False  

  #----------------------------------------------------------------------------------------------------------
  # Fill or valid 'CloudScore' parameter
  #----------------------------------------------------------------------------------------------------------
  sensor_type = outParams['sensor'].lower()
  if sensor_type.find('s2') < 0:
    outParams['CloudScore'] = False 
  
  return all_valid, outParams






def valid_CompParams(CompParams):
  #==========================================================================================================
  # Ensure all the keys in user's parameter dictionary are valid
  #==========================================================================================================
  all_valid    = True
  user_keys    = list(CompParams.keys())
  default_keys = all_param_keys
  n_user_keys  = len(user_keys)

  key_presence = [element in default_keys for element in user_keys]
  for index, pres in enumerate(key_presence):
    if pres == False and index < n_user_keys:
      all_valid = False
      print('<valid_CompParams> \'{}\' key in given parameter dictionary is invalid!'.format(user_keys[index]))
  
  if not all_valid:
    return all_valid, None  

  outParams = CompParams  
  
  #==========================================================================================================
  # Fill or valid 'number_workers' parameter
  #==========================================================================================================
  nCPUs = os.cpu_count()

  if 'number_workers' not in outParams:
    outParams['number_workers'] = int((nCPUs-2)/2)  # Assume 2 threads per worker
  else:  
    nWorkers = int(outParams['number_workers'])
    if nWorkers < 1 or nWorkers > nCPUs-2:
      outParams['number_workers'] = int((nCPUs-2)/2)  # Assume 2 threads per worker

  #==========================================================================================================
  # Fill 'debug' and 'entire_tile' parameters
  #==========================================================================================================
  if 'debug' not in outParams:
    outParams['debug'] = True
  
  if 'entire_tile' not in outParams:
    outParams['entire_tile'] = True

  #==========================================================================================================
  # Fill 'nodes' parameter
  #==========================================================================================================
  if 'nodes' not in outParams:
    outParams['nodes'] = 1
  else:
    nNodes = int(outParams['nodes'])
    if nNodes < 1 or nNodes > 10:
      outParams['nodes'] = 1

  #==========================================================================================================
  # Fill 'node_memory' parameter
  #==========================================================================================================
  RAM_size = int(psutil.virtual_memory().total / (1024 ** 3))  # in GB
  used_RAM = RAM_size-4 if RAM_size < 32 else RAM_size-8 if RAM_size < 128 else RAM_size-16
  if 'node_memory' not in outParams:  
    outParams['node_memory'] = str(used_RAM/outParams['nodes']) +'G'
  else:
    node_memory = int("".join(re.findall(r"\d", str(outParams['node_memory'])))) 
    if node_memory < 0 or node_memory > used_RAM:
      outParams['node_memory'] = str(used_RAM/outParams['nodes']) +'G'

  #==========================================================================================================
  # Fill 'chunk_size' parameter
  #========================================================================================================== 
  if 'chunk_size' not in outParams:
    outParams['chunk_size'] = {'x': 2048, 'y': 2048}
  else:
    x_size = int(outParams['chunk_size']['x'])
    y_size = int(outParams['chunk_size']['y'])
    if x_size < 500 or y_size < 500:
      outParams['chunk_size'] = {'x': 2048, 'y': 2048}
    elif x_size > 5000 or y_size > 5000:
      outParams['chunk_size'] = {'x': 4096, 'y': 4096}

  return all_valid, outParams




#############################################################################################################
# Description: This function creates the start and end dates for a list of user-specified months and save 
#              them into two lists with 'start_dates' and 'end_dates' keys.
#
# Revision history  2024-Sep-03  Lixin Sun  Initial creation
#
#############################################################################################################
def form_time_windows(inParams):

  if not has_custom_window(inParams):
    inParams['monthly'] = True
    nMonths = len(inParams['months'])  # get the number of specified months

    year = inParams['year']
    for index in range(nMonths):
      month = inParams['months'][index]
      start, end = eoUs.month_range(year, month)

      if index == 0:
        inParams['start_dates'] = [start]
        inParams['end_dates']   = [end]
      else:  
        inParams['start_dates'].append(start)
        inParams['end_dates'].append(end) 

  elif 'standardized' not in inParams:
    inParams['monthly'] = False
  
  inParams['current_time'] = 0

  return inParams
  




#############################################################################################################
# Description: This function performs one of two actions, depending on whether custom regions are defined.
#              
#              (1) When there is no customized regions defined: it converts the sptail regions defined in 
#                  inParams['tile_names'] to customized regions and set the first tile as 'current_region';
#              (2) When there is at least one customized regions already defined in inParams['regions'],
#                  then just set the first customized region as 'current_region'.  
#
# Revision history  2024-Sep-03  Lixin Sun  Initial creation
#
#############################################################################################################
def form_spatial_regions(inParams):
  if not has_custom_region(inParams):   # There is no custom region, so use regular tile regions
    inParams['regions'] = {}
    for tile_name in inParams['tile_names']:
      if eoTG.valid_tile_name(tile_name):
        
        inParams['regions'][tile_name] = eoTG.get_tile_polygon(tile_name)
    
    #return set_spatial_region(inParams, inParams['tile_names'][0])  
  
  #==========================================================================================================
  # Set current region
  #==========================================================================================================
  if 'regions' in inParams:
    inParams['current_region'] = list(inParams['regions'].keys())[0]
  else:
    print('<form_spatial_regions> There is no customized regions in parameter dictionary!!')  

  return inParams




#############################################################################################################
# Description: This function modifies default parameter dictionary based on a given parameter dictionary.
# 
# Note:        The given parameetr dictionary does not have to include all "key:value" pairs, only the pairs
#              as needed.
#
# Revision history:  2022-Mar-29  Lixin Sun  Initial creation
#                    2024-Apr-08  Lixin Sun  Incorporated modifications according to customized time window
#                                            and spatial region.
#                    2024-Sep-03  Lixin Sun  Adjusted to ensure that regular months/season will also be 
#                                            handled as customized time windows.  
#                    2025-Sep-23  Lixin Sun  added checking and setting for 'standardized' key, which serves
#                                            as a lock to prevent a parameter dictionary from being 
#                                            standardized twice.  
#############################################################################################################
def standardize_params(inParams):
  if inParams is None:
    print('<update_default_params> The given parameter dictionary is None!!')
    return None
  
  #==========================================================================================================
  # Ensure the given parameter dictionary is not standardized twice
  #==========================================================================================================
  if 'standardized' in inParams:
    if inParams['standardized'] == True:
      print('The given parameter dictionary has been standardized, cannot be standardized again!')
      return inParams
    
  #==========================================================================================================
  # Validate the given parameters 
  #==========================================================================================================
  all_valid, out_Params = valid_ProdParams(inParams)

  if all_valid is False or out_Params is None:
    return None

  #==========================================================================================================
  # If regular months (e.g., 5,6,7) or season (e.g., -1) are specified, then convert them to date strings and
  # save in the lists corresponding to 'start_dates' and 'end_dates' keys. In this way, regular months/season
  # will be dealed with as customized time windows.    
  #==========================================================================================================
  out_Params = form_time_windows(out_Params)

  #==========================================================================================================
  # If only regular tile names are specified, then create a dictionary with tile names and their 
  # corresponding 'ee.Geometry.Polygon' objects as keys and values, respectively.   
  #==========================================================================================================
  out_Params = form_spatial_regions(out_Params) 

  #==========================================================================================================
  # Get ONE time window and ONE spatial region, respectively, and then generate a condition dictionary for
  # querying a STAC data catalog. 
  #==========================================================================================================
  StartStr, EndStr = get_time_window(out_Params)  
  Region           = get_spatial_region(out_Params)
  
  out_Params['Criteria'] = get_query_conditions(out_Params, StartStr, EndStr, Region)

  # Lock the returned parameter dictionary to prevent it from being standardized twice.
  out_Params['standardized'] = True

  return out_Params





############################################################################################################# 
# Description: Obtain a parameter dictionary for LEAF tool
#############################################################################################################
def get_LEAF_params(ProdParams, CompParams):
  out_Params = standardize_params(ProdParams)   # Standardize input parameters

  out_Params['unit'] = 2                        # Always surface reflectance for LEAF production
  out_Params['IncludeAngles'] = True            # Imaging geometry angles must be always included   

  #==========================================================================================================
  # Merge CompParams into "out_Params"
  #==========================================================================================================
  for key, value in CompParams.items():
    out_Params.update({key: value})

  return out_Params  




#############################################################################################################
# Description: This function returns a parameter dictionary for generating a single composite image 
#              (ONE combination of time windows and spatial regions).
#
# Revision history:  2024-Oct-24  Lixin Sun  Initial creation
#                                 Lixin Sun  Adjusted for spatial resolution of 10m  
#############################################################################################################
def get_mosaic_params(ProdParams, CompParams):
  '''
     Args:
        ProdParams(Dictionary): A dictionary containing all parameters related to composite image production;
        CompParams(Dictionary): A dictionary containing all parameters related to used computing environment.'''
  
  if ProdParams is None or CompParams is None:
    print('<get_mosaic_params> Either "ProdParams" or "CompParams" is missing!')
    return None
  
  prod_params = standardize_params(ProdParams)  # Modify default parameter dictionary with a given one
  if prod_params is None:
    print('<get_mosaic_params> Standardization of ProdParams failed!')
    return None
  
  prod_params['prod_names'] = ['mosaic']      # Of course, product name should be always 'mosaic'  
  
  #==========================================================================================================
  # Merge CompParams into "outParams"
  #==========================================================================================================
  all_vali, comp_params = valid_CompParams(CompParams)
  if not all_vali or comp_params is None:
    print('<get_mosaic_params> Validation of CompParams failed!')
    return None
  
  for key, value in comp_params.items():
    prod_params.update({key: value})

  return prod_params





#############################################################################################################
# Description: Obtain a parameter dictionary for land cover classification tool
#############################################################################################################
def get_LC_params(inParams):
  out_Params = standardize_params(inParams) # Modify default parameter dictionary with a given one
  out_Params['prod_names'] = ['mosaic']     # Of course, product name should be always 'mosaic'

  return out_Params 


#############################################################################################################
# Description: Obtain the cloud cover from input dictionary
#############################################################################################################
def get_cloud_coverage(inParams):
  return inParams['cloud_cover']


#############################################################################################################
# Description: This function returns ONE valid spatial region defined in parameter dictionary.
# 
# Revision history:  2024-Feb-27  Lixin Sun  Initial creation
#
#############################################################################################################
def get_spatial_region(inParams):
  '''Returns ONE valid spatial region defined in parameter dictionary.
     Args:
        inParams(Dictionary): A dictionary containing all required input parameters.
  '''

  #==========================================================================================================
  # Confirm required key:value pairs are defined
  #==========================================================================================================  
  if 'current_region' not in inParams or 'regions' not in inParams:
    print('\n<get_spatial_region> one of required keys is not exist!!')
    return None

  #==========================================================================================================
  # Extract 'current_region'
  #==========================================================================================================
  reg_name        = inParams['current_region']
  valid_reg_names = inParams['regions'].keys()
  if reg_name in valid_reg_names:
    return inParams['regions'][reg_name]
  else:
    print('\n<get_spatial_region> Invalid spatial region name provided!')
    return None
      





#############################################################################################################
# Description: This function returns ONE valid time window defined in parameter dictionary.
# 
# Revision history:  2024-Feb-27  Lixin Sun  Initial creation
#                    2024-Sep-03  Lixin Sun  Added 'current_time' input parameter so that a specific time
#                                            window can be identified. 
#############################################################################################################
def get_time_window(inParams):
  '''Returns ONE valid time window defined in parameter dictionary.
     Args:
        inParams(dictionary): A dictionary containing all required input parameters.
  '''
  
  #==========================================================================================================
  # Confirm required key:value pairs are defined
  #==========================================================================================================
  if 'current_time' not in inParams or 'start_dates' not in inParams or 'end_dates' not in inParams:
    print('\n<get_time_window> one of required keys is not exist!!')
    return None, None

  #==========================================================================================================
  # Extract 'current_time'
  #==========================================================================================================
  current_time = inParams['current_time']
  nDates       = len(inParams['start_dates'])
    
  if current_time >= nDates:
    print('\n<get_time_window> Invalidate \'current_time\' value!')
    return None, None

  start = inParams['start_dates'][current_time]
  end   = inParams['end_dates'][current_time]

  return start, end



#############################################################################################################
# Description: This function returns two required parameter dictionaries: ProdParams, CompParams
#              These two parameter dictionaries can be provided in two ways: 
#              1) user provide them directly
#              2) user provide through command line
#  
# Revision history:  2024-Feb-27  Lixin Sun  Initial creation
#            
#############################################################################################################
def form_inputs(ProdParams = None, CompParams = None):
  if ProdParams is not None and CompParams is not None:
    # user provide the parameter dictionaries directly
    return ProdParams, CompParams
  else:
    # user provide the parameter dictionaries through a command line
    return cmd_arguments()




def cmd_arguments(argv=None):    
  """
  Parse command line arguments.
  """
  
  parser = argparse.ArgumentParser(
      usage="%(prog)s [-h HELP] use -h to get supported arguments.",
      description="Mosaic High-resolution imagery using STAC and Xarray",
  )
  parser.add_argument(
      '-s', '--sensor', 
      type=str, 
      default='S2_SR', 
      choices=['S2_SR','HLSS30_SR', 'HLSL30_SR', 'HLS_SR'], 
      help="Sensor type (e.g., 'S2_SR', 'HLSS30_SR', 'HLSL30_SR', 'HLS_SR')"
  )
  parser.add_argument(
    '-b', '--bands', 
    type=str, 
    nargs='+', 
    default=[], 
    help="The name of extra requested bands. "
  )
  parser.add_argument(
      '-cl', '--cloud_cover', 
      type=int, 
      default=85.0, 
      help="Cloud cover "
  )
  parser.add_argument(
      '-u', '--unit', 
      type=int, 
      default=2,  
      choices=[1, 2], 
      help="Data unit code (1 for TOA, 2 for surface reflectance)"
  )
  parser.add_argument(
      '-y', '--year', 
      type=int, 
      help="Image acquisition year"
  )
  parser.add_argument(
      '-nby', '--nbyears', 
      type=int, 
      default=-1, 
      help="Positive integer for annual product, or negative for monthly product"
  )
  parser.add_argument(
      '-m', '--months', 
      type=int, 
      nargs='+', 
      choices=range(1, 13), 
      help="List of months included in the product (e.g., 5 6 7 8 9 10)"
  )
  parser.add_argument(
      '-sd', '--start_dates', 
      type=str, 
      default="", 
      nargs='+', 
      help="List of start dates (e.g., '2023-05-01')"
  )
  parser.add_argument(
      '-ed', '--end_dates', 
      type=str, 
      default="", 
      nargs='+', 
      help="List of end dates (e.g., '2023-10-30')"
  )
  parser.add_argument(
      '-t', '--tile_names', 
      type=str, 
      nargs='+', 
      help="List of (sub-)tile names"
  )
  parser.add_argument(
      '-pn', '--prod_names', 
      type=str, 
      nargs='+', 
      default=['LAI', 'fCOVER', 'fAPAR', 'Albedo'], 
      help="List of product names (e.g., 'mosaic', 'LAI', 'fCOVER')"
  )
  parser.add_argument(
      '-r', '--resolution', 
      type=int, 
      default=20, 
      help="Spatial resolution (default: 20)"
  )
  parser.add_argument(
      '-o', '--out_folder', 
      type=str, 
      help="Folder name for exporting results"
  )
  parser.add_argument(
      '-proj', '--projection', 
      type=str, 
      default='EPSG:3979', 
      help="Projection (e.g., 'EPSG:3979')"
  )
  parser.add_argument(
      '-d', '--debug', 
      action='store_true', 
      help="Run the program in debug mode. Creates a single-node Dask cluster."
  )
  parser.add_argument(
      '-et', '--entire_tile', 
      action='store_true', 
      help="Mosaic the entire tile. Program will run for all 9 subtile sections."
  )
  parser.add_argument(
      '-ang', '--include_angles', 
      action='store_true', 
      help="Whether to include angle bands in the Mosicking process or not."
  )
  parser.add_argument(
      '-nw', '--number_workers', 
      type=int, 
      default=1, 
      help="Number of Dask workers. Set based on available cores and nodes."
  )
  parser.add_argument(
      '-nm', '--node_memory', 
      type=str, 
      default=-1, 
      help="Memory allocated for each Dask worker."
  )
  parser.add_argument(
      '-n', '--nodes', 
      type=int, 
      default=1, 
      help="Number of physical nodes for distributed Dask mode"
  )
  parser.add_argument(
      '-rlat', '--region_lat', 
      type=float, 
      nargs='+', 
      help="The latitude of the region of interest in the following order: Top-left, Top-right, Bottom-right, and Bottom-left. "
  )
  parser.add_argument(
    '-rlon', '--region_lon', 
    type=float, 
    nargs='+', 
    help="The longitude of the region of interest in the following order: Top-left, Top-right, Bottom-right, and Bottom-left. "
  )
  parser.add_argument(
    '-rc', '--region_catalog', 
    type = str, 
    default=None,
    help="The STAC Catalog Json file for the desired Region of interest. "
  )

  args = parser.parse_args()
  
  sensor     = args.sensor
  region_lat =  args.region_lat
  region_lon =  args.region_lon
  region_catalog     = args.region_catalog 
  bands      = args.bands
  nbyears    = args.nbyears
  cloud_cover= args.cloud_cover
  year       = args.year
  unit       = args.unit
  months     = args.months
  tile_names = args.tile_names
  prod_names = args.prod_names 
  resolution = args.resolution 
  out_folder = args.out_folder
  projection = args.projection
  start_dates  = args.start_dates
  end_dates    = args.end_dates
  debug        = args.debug
  IncludeAngles = args.include_angles
  entire_tile  = args.entire_tile
  number_workers = args.number_workers
  nodes          = args.nodes if not debug else 1
  node_memory    = args.node_memory
  
  region_cr = {}
  id_custom = None

  if region_catalog is not None:
    
    # Open the file and load the JSON content
    with Path(region_catalog).open('r') as region_catalog:
      
      region_data = json.load(region_catalog)
      region_cr = region_data["geometry"]
    
  
  if region_lon is not None and len(region_cr) == 0:
    if len(region_lon) == 4 and len(region_lat) == 4:
      region_cr = {
        'type': 'Polygon',
        'coordinates': [[
            [float(region_lon[0]), float(region_lat[0])],
            [float(region_lon[1]), float(region_lat[1])],
            [float(region_lon[2]), float(region_lat[2])],
            [float(region_lon[3]), float(region_lat[3])],
            [float(region_lon[0]), float(region_lat[0])]  
        ]]
      }
  

  prod_params = {
      "sensor" : sensor,
      "ID" : id_custom,
      "bands" : bands,
      "regions" : region_cr,
      "unit" : unit, 
      "year" : year,
      "cloud_cover" : cloud_cover,
      "nbYears" : nbyears,
      "months" : months,
      "tile_names" : tile_names,
      "prod_names" : prod_names,
      "resolution" : resolution,
      "out_folder" : out_folder,
      "projection" : projection,
      "IncludeAngles" : IncludeAngles,
  }
  if 'end_dates' != "":
      prod_params["end_dates"] = end_dates
  if 'start_dates' != "": 
      prod_params["start_dates"] = start_dates
  

  # Get the SLURM_JOB_ID from the environment
  
  slurm_job_id = subprocess.getoutput("echo $SLURM_JOB_ID")
  account = ""
  if slurm_job_id:
      command = f"scontrol show job {slurm_job_id}"
      try:
          result = subprocess.check_output(command, shell=True, text=True)
          account_match = re.search(r'Account=([^\s]+)', result)
          if account_match:
              account = account_match.group(1)
          else:
              print("Account not found.")
      
      except subprocess.CalledProcessError as e:
          print(f"Error executing command: {e}")
  else:
      print("No SLURM job ID found. Are you inside a running SLURM job?")

  comp_params = {
      "debug"       : debug,
      "entire_tile" : entire_tile,
      "nodes"       : nodes,
      "node_memory" : node_memory,
      "number_workers" : number_workers,
      "account" : account
  }
  
  return prod_params, comp_params

  
def earth_data_authentication():
    
  netrc_path = os.path.expanduser('~/.netrc')
  try:
    netrc_data = netrc.netrc(netrc_path)
    # Get the login credentials for the specified machine (e.g., 'urs.earthdata.nasa.gov')
    machine = 'urs.earthdata.nasa.gov'  # Replace with the machine name you want to extract credentials for
    login, _, password = netrc_data.authenticators(machine)
    if login and password:
      pass
    else:
      print("<<<<<<<<<<The .netrc file does not exist. Please run the following command first:")
      print("<<<<<<<<<< ")
      print(' echo "machine urs.earthdata.nasa.gov login **your username** password **your password**" > ~/.netrc ')
      print(">>>>>>>>>> ")
      sys.exit(1)
  except Exception as e:
      print("<<<<<<<<<<The .netrc file does not exist. Please run the following command first:")
      print("<<<<<<<<<< ")
      print(' echo "machine urs.earthdata.nasa.gov login **your username** password **your password**" > ~/.netrc ')
      print(">>>>>>>>>> ")
      sys.exit(1)
