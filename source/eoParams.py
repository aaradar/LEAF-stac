#import eoImgSet as eoIS
import eoImage as eoIM
import eoUtils as eoUs
import eoTileGrids as eoTG
from pathlib import Path

from datetime import datetime

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
# Description: This function sets values for 'current_month' and 'time_str' keys.
# 
# Note: If a customized time windows has been specified, then the given 'current_month' will be ignosed
#
# Revision history:  2024-Apr-08  Lixin Sun  Initial creation
#
#############################################################################################################
def set_current_time(inParams, current_month):
  custon_window = is_custom_window(inParams)

  if custon_window == True:
    inParams = year_consist(inParams)
    inParams['time_str'] = str(inParams['start_date']) + '_' + str(inParams['end_date'])
  
  elif current_month > 0 and current_month < 13:
    inParams['time_str']      = eoIM.get_MonthName(current_month)
    inParams['current_month'] = current_month
    
  else:
    inParams['time_str'] = 'season'

  return inParams





#############################################################################################################
# Description: This function sets values for 'current_tile' and 'region_str' keys
# 
# Note: If a customized spatial region has been specified, then the given 'current_tile' will be ignosed
#
# Revision history:  2024-Apr-08  Lixin Sun  Initial creation
#
#############################################################################################################
def set_current_region(inParams, current_tile):
  custon_region = is_custom_region(inParams)

  if custon_region == True:
    inParams['region_str'] = 'custom_region'    

  elif len(current_tile) > 5:
    inParams['region_str']   = current_tile
    inParams['current_tile'] = current_tile
    
  else:
    print('<set_region_str> Invalid tile name provided!')
    inParams['region_str'] = 'invalid_tile'

  return inParams





#############################################################################################################
# Description: This function validate a given user parameter dictionary.
#
# Revision history:  2024-Jun-07  Lixin Sun  Initial creation
#       
#############################################################################################################
def valid_user_params(UserParams):
  #==========================================================================================================
  # Check if the keys in user parameter dictionary are valid
  #==========================================================================================================
  all_valid    = True
  user_keys    = list(UserParams.keys())
  default_keys = list(DefaultParams.keys())
  n_user_keys  = len(user_keys)

  key_presence = [element in default_keys for element in user_keys]
  for index, pres in enumerate(key_presence):
    if pres == False and index < n_user_keys:
      all_valid = False
      print('<valid_user_params> \'{}\' key in given parameter dictionary is invalid!'.format(user_keys[index]))
  
  if all_valid == False:
    return all_valid
  
  #==========================================================================================================
  # Validate some critical individual 'key:value' pairs
  #==========================================================================================================
  sensor = str(UserParams['sensor']).upper()
  all_SSRs = ['S2_SR', 'L5_SR', 'L7_SR', 'L8_SR', 'L9_SR']
  if sensor not in all_SSRs:
    all_valid = False
    print('<valid_user_params> Invalid sensor or unit was specified!')

  year = int(UserParams['year'])
  if year < 1970 or year > 2200:
    all_valid = False
    print('<valid_user_params> Invalid year was specified!')

  if int(UserParams['nbYears']) > 3:
    all_valid = False
    print('<valid_user_params> Invalid number of years was specified!')

  max_month = max(UserParams['months'])
  if max_month > 12:
    all_valid = False
    print('<valid_user_params> Invalid month number was specified!')

  tile_names = UserParams['tile_names']
  nTiles = len(tile_names)
  if nTiles < 1:
    all_valid = False
    print('<valid_user_params> No tile name was specified for tile_names key!')
  
  for tile in tile_names:
    if eoTG.valid_tile_name(tile) == False:
      all_valid = False
      print('<valid_user_params> {} is an invalid tile name!'.format(tile))

  prod_names = UserParams['prod_names']
  nProds = len(prod_names)
  if nProds < 1:
    all_valid = False
    print('<valid_user_params> No product name was specified for prod_names key!')
  
  user_prods = list(prod_names)
  prod_names = ['LAI', 'fAPAR', 'fCOVER', 'Albedo', 'mosaic', 'QC', 'date', 'partition']
  presence = [element in prod_names for element in user_prods]
  if False in presence:
    all_valid = False
    print('<valid_user_params> At least one of the specified products is invalid!')

  if int(UserParams['resolution']) < 1:
    all_valid = False
    print('<valid_user_params> Invalid spatial resolution was specified!')

  out_folder = str(UserParams['out_folder'])
  if Path(out_folder) == False or len(out_folder) < 2:
    all_valid = False
    print('<valid_user_params> The specified output path is invalid!')
  
  return all_valid





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
  
  if valid_user_params(inParams) == False:
    return None  
  
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
  # Set values for 'current_month' and 'time_str' keys as necessary
  #==========================================================================================================
  out_Params = set_current_time(out_Params, int(out_Params['months'][0]))
 
  #==========================================================================================================
  # Set values for 'current_tile' and 'region_str' keys as necessary
  #==========================================================================================================
  out_Params = set_current_region(out_Params, str(out_Params['tile_names'][0]))
  
  # return modified parameter dictionary 
  return out_Params





############################################################################################################# 
# Description: Obtain a parameter dictionary for LEAF tool
#############################################################################################################
def get_LEAF_params(inParams):
  out_Params = update_default_params(inParams)  # Modify default parameters with given ones

  if out_Params is not None:
    out_Params['nbYears'] = -1                    # Produce monthly products in most cases
    out_Params['unit']    = 2                     # Always surface reflectance for LEAF production

  return out_Params  





#############################################################################################################
# Description: Obtain a parameter dictionary for Mosaic tool
#############################################################################################################
def get_mosaic_params(inParams):
  out_Params = update_default_params(inParams)  # Modify default parameter dictionary with a given one

  if out_Params is not None:
    out_Params['prod_names'] = ['mosaic']       # Of course, product name should be always 'mosaic'

  return out_Params  





#############################################################################################################
# Description: Obtain a parameter dictionary for land cover classification tool
#############################################################################################################
def get_LC_params(inParams):
  out_Params = update_default_params(inParams) # Modify default parameter dictionary with a given one

  if out_Params is not None:
    out_Params['prod_names'] = ['mosaic']      # Of course, product name should be always 'mosaic'    

  return out_Params 





#############################################################################################################
# Description: This function returns a valid spatial region defined in a given parameter dictionary.
# 
# Revision history:  2024-Feb-27  Lixin Sun  Initial creation
#
#############################################################################################################
def get_spatial_region(inParams):
  if inParams == None:
    print('<get_spatial_region> No inparameter dictionary is provided!!!')
    return None
  
  all_keys = inParams.keys()

  if 'custom_region' in all_keys:
    return inParams['custom_region']
  
  elif len(inParams['current_tile']) > 2:
    tile_name = inParams['current_tile']
    if eoTG.valid_tile_name(tile_name):
      return eoTG.get_tile_polygon(tile_name)
    else:
      print('<get_spatial_region> Invalid spatial region defined!!!!')
      return None
    
  elif len(inParams['tile_names'][0]) > 2:
    tile_name = inParams['tile_names'][0]
    if eoTG.valid_tile_name(tile_name):
      return eoTG.get_tile_polygon(tile_name)
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
  if inParams == None:
    print('<get_time_window> No inparameter dictionary is provided!!!')
    return None, None
  
  all_keys = inParams.keys()

  if is_custom_window(inParams) == True:
    start_date = datetime.strptime(inParams['start_date'], "%Y-%m-%d")
    end_date   = datetime.strptime(inParams['end_date'],   "%Y-%m-%d")

    end_date   = end_date.replace(year=start_date.year)

    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
  
  else:    
    if 'current_month' in all_keys:
      current_month = inParams['current_month']
      if current_month > 0  and current_month < 13:
        # Extract veg parameters on a monthly basis
        return eoUs.month_range(inParams['year'], current_month)
      else:  
        nYears = inParams['nbYears']
        year   = inParams['year']
    
        if nYears < 0:
          return eoUs.summer_range(year) 
        else:
          month = inParams['months'][0]
          if month > 12:
            month = 12
          elif month < 1:
            month = 1

          return eoUs.month_range(year, month)
    else:
      print('<get_time_window> \'current_month\' is not a key in the given input parameter dictionary!!!')
      return None, None



# params = {
#     'sensor': 'S2_Sr',           # A sensor type string (e.g., 'S2_SR' or 'L8_SR' or 'MOD_SR')
#     'unit': 2,                   # A data unit code (1 or 2 for TOA or surface reflectance)    
#     'year': 1023,                # An integer representing image acquisition year
#     'nbYears': -1,               # positive int for annual product, or negative int for monthly product
#     'months': [8,9],               # A list of integers represening one or multiple monthes     
#     'tile_names': ['tile42'],       # A list of (sub-)tile names (defined using CCRS' tile griding system) 
#     'prod_names': ['mosaic'],    #['mosaic', 'LAI', 'fCOVER', ]    
#     'resolution': 20,            # Exporting spatial resolution    
#     'out_folder': 'c:/test',                # the folder name for exporting    
#     'start_date': '2022-06-15',
#     'end_date': '2023-09-15'
# }

# valid_user_params(params)