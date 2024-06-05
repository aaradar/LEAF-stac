#import eoImgSet as eoIS
import eoImage as eoIM
import eoUtils as eoUs
import eoTileGrids as eoTG
import re

from datetime import datetime, timedelta

#############################################################################################################
# Description: Define a default execution parameter dictionary. 
# 
# Revision history:  2022-Mar-29  Lixin Sun  Initial creation
#
#############################################################################################################
DefaultParams = {
    'sensor': 'S2_SR',           # A sensor type and data unit string (e.g., 'S2_Sr' or 'L8_SR')    
    'unit': 2,                   # data unite (1=> TOA reflectance; 2=> surface reflectance)
    'year': 2019,                # An integer representing image acquisition year
    'nbYears': 1,                # positive int for annual product, or negative int for monthly product
    'months': [5,6,7,8,9,10],    # A list of integers represening one or multiple monthes     
    'tile_names': ['tile55'],    # A list of (sub-)tile names (defined using CCRS' tile griding system) 
    'prod_names': ['mosaic'],    # ['mosaic', 'LAI', 'fCOVER', ]
    'resolution': 30,            # Exporting spatial resolution
    'out_folder': '',            # the folder name for exporting
    'export_style': 'separate',
    'start_date': '',
    'end_date':  '',
    'scene_ID': '',
    'projection': 'EPSG:3979',
    'CloudScore': False,

    'current_month': -1,
    'current_tile': '',
    'time_str': '',              # Mainly for creating output filename
    'region_str': ''             # Mainly for creating output filename
}





#############################################################################################################
# Description: This function tells if there is a customized region defined in parameter dictionary.
# 
# Revision history:  2024-Feb-27  Lixin Sun  Initial creation
#
#############################################################################################################
def is_custom_region(inParams):
  # get all keys in the given parameetr dictionary
  all_keys = inParams.keys()

  if 'custom_region' in all_keys:
    return True
  elif 'scene_ID' in all_keys: 
    return True if len(inParams['scene_ID']) > 5 else False
  else:
    return False 




#############################################################################################################
# Description: This function tells if there is a customized time window defined in parameter dictionary.
# 
# Revision history:  2024-Feb-27  Lixin Sun  Initial creation
#
#############################################################################################################
def is_custom_window(inParams):
  start_len = len(inParams['start_date'])
  end_len   = len(inParams['end_date'])
  #print('<is_custom_window> start and end date lengthes are:', start_len, end_len)
  
  return True if start_len > 7 and end_len > 7 else False
  



#############################################################################################################
# Description: This function makes the year values corresponding to 'start_date', 'end_date' and 'year' keys
#              in a execution parameter dictionary are consistent.
# 
# Revision history:  2024-Apr-08  Lixin Sun  Initial creation
#
#############################################################################################################
def year_consist(inParams):
  keys = inParams.keys()
  
  if 'start_date' in keys and 'end_date' in keys:
    start_date_str = str(inParams['start_date'])
    end_date_str   = str(inParams['end_date'])  
  
    if len(start_date_str) > 7 and len(end_date_str) > 7:
      # Modify the year of 'end_date' string using the year of 'start_date'  
      start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
      end_date   = datetime.strptime(end_date_str,   "%Y-%m-%d")
    
      end_date   = end_date.replace(year = start_date.year)

      inParams['end_date'] = end_date.strftime("%Y-%m-%d")
  
      # Modify the value corresponding 'year' key in parameter dictionary
      inParams['year'] = int(start_date.year)

  return inParams
  
    





#############################################################################################################
# Description: This function sets value for 'time_str' key based on if a customized time window has been 
#              specified.
# 
# Revision history:  2024-Apr-08  Lixin Sun  Initial creation
#
#############################################################################################################
def set_time_str(inParams):
  custon_window = is_custom_window(inParams)
  current_month = inParams['current_month']

  if custon_window == True:
    inParams = year_consist(inParams)
    inParams['time_str'] = str(inParams['start_date']) + '_' + str(inParams['end_date'])

  elif current_month > 0 and current_month < 13:
    inParams['time_str'] = eoIM.get_MonthName(current_month)

  else:
    inParams['time_str'] = 'season'

  return inParams





#############################################################################################################
# Description: This function sets value for 'region_str' key based on if a customized spatial region has been 
#              specified.
# 
# Revision history:  2024-Apr-08  Lixin Sun  Initial creation
#
#############################################################################################################
def set_region_str(inParams):
  custon_region = is_custom_region(inParams)

  if custon_region == True:
    inParams['region_str'] = 'custom_region'
    
  else:
    inParams['region_str'] = inParams['current_tile']

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
#############################################################################################################
def update_default_params(inParams):  
  out_Params = DefaultParams

  # get all the keys in the given dictionary
  inKeys = inParams.keys()  
  
  # For each key in the given dictionary, modify corresponding "key:value" pair
  for key in inKeys:
    out_Params[key] = inParams.get(key)
  
  # Ensure "CloudScore" is False if sensor type is not Sentinel-2 data
  sensor_type = out_Params['sensor'].lower()
  if sensor_type.find('s2') < 0:
    out_Params['CloudScore'] = False 
  
  #==========================================================================================================
  # If a customized time window has been provided
  #==========================================================================================================
  if is_custom_window(out_Params) == True:
    #Set value associated with 'time_str' key
    out_Params = set_time_str(out_Params)
 
  #==========================================================================================================
  # If a customized spatial region has been provided
  #==========================================================================================================
  if is_custom_region(out_Params) == True: 
    #Set value associated with 'region_str' key
    out_Params = set_region_str(out_Params)
  
  # return modified parameter dictionary 
  return out_Params





############################################################################################################# 
# Description: Obtain a parameter dictionary for LEAF tool
#############################################################################################################
def get_LEAF_params(inParams):
  out_Params = update_default_params(inParams)  # Modify default parameters with given ones
  out_Params['nbYears'] = -1                    # Produce monthly products in most cases
  out_Params['unit']    = 2                     # Always surface reflectance for LEAF production

  return out_Params  





#############################################################################################################
# Description: Obtain a parameter dictionary for Mosaic tool
#############################################################################################################
def get_mosaic_params(inParams):
  out_Params = update_default_params(inParams)  # Modify default parameter dictionary with a given one
  out_Params['prod_names'] = ['mosaic']         # Of course, product name should be always 'mosaic'

  return out_Params  





#############################################################################################################
# Description: Obtain a parameter dictionary for land cover classification tool
#############################################################################################################
def get_LC_params(inParams):
  out_Params = update_default_params(inParams) # Modify default parameter dictionary with a given one
  out_Params['prod_names'] = ['mosaic']        # Of course, product name should be always 'mosaic'

  return out_Params 





#############################################################################################################
# Description: This function returns a valid spatial region defined in parameter dictionary.
# 
# Revision history:  2024-Feb-27  Lixin Sun  Initial creation
#
#############################################################################################################
def get_spatial_region(inParams):
  all_keys = inParams.keys()

  if 'custom_region' in all_keys:
    return inParams['custom_region']
  
  elif len(inParams['current_tile']) > 2:
    tile_name = inParams['current_tile']
    if eoTG.valid_tile_name(tile_name):
      return eoTG.CAN_TILES[inParams['current_tile']]
    else:
      print('<get_spatial_region> Invalid spatial region defined!!!!')
      return None
    
  elif len(inParams['tile_names'][0]) > 2:
    tile_name = inParams['tile_names'][0]
    if eoTG.valid_tile_name(tile_name):
      return eoTG.CAN_TILES[inParams['tile_names'][0]]
    else:
      print('<get_spatial_region> Invalid spatial region defined!!!!')
      return None
    
  else:
    print('<get_spatial_region> No spatial region defined!!!!')
    return None




#############################################################################################################
# Description: This function returns a valid time window defined in parameter dictionary.
# 
# Revision history:  2024-Feb-27  Lixin Sun  Initial creation
#
#############################################################################################################
def get_time_window(inParams):  
  if is_custom_window(inParams) == True:    
    start_date = datetime.strptime(inParams['start_date'], "%Y-%m-%d")
    end_date   = datetime.strptime(inParams['end_date'],   "%Y-%m-%d")

    end_date   = end_date.replace(year=start_date.year)

    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
  
  else:
    current_month = inParams['current_month']
    if current_month > 12:
      current_month = 12
    elif current_month < 1:
      current_month = 1

      # Extract veg parameters on a monthly basis
      return eoUs.month_range(inParams['year'], current_month)
    else:  
      nYears = inParams['nbYears']
      year   = inParams['year']
   
      if nYears < 0 or current_month < 0:
        return eoUs.summer_range(year) 
      else:
        month = max(inParams['months'])
        if month > 12:
          month = 12
        elif month < 1:
          month = 1

        return eoUs.month_range(year, month)



'''
params = {
    'sensor': 'S2_SR',           # A sensor type string (e.g., 'S2_SR' or 'L8_SR' or 'MOD_SR')
    'unit': 2,                   # A data unit code (1 or 2 for TOA or surface reflectance)    
    'year': 2023,                # An integer representing image acquisition year
    'nbYears': -1,               # positive int for annual product, or negative int for monthly product
    'months': [8],               # A list of integers represening one or multiple monthes     
    'tile_name': 'tile42',       # A list of (sub-)tile names (defined using CCRS' tile griding system) 
    'prod_names': ['mosaic'],    #['mosaic', 'LAI', 'fCOVER', ]    
    'resolution': 20,            # Exporting spatial resolution    
    'folder': '',                # the folder name for exporting
    'buff_radius': 10, 
    'tile_scale': 4,
    'CloudScore': True,

    'start_date': '2022-06-15',
    'end_date': '2023-09-15'
}

year_consist(params)
'''