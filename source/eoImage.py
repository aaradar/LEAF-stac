
import math
import os
import numpy as np
import time
import datetime
#import rasterio
import rioxarray
import xarray as xr

import eoUtils as eoUs
import eoImage as eoIM


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


TOA_ref        = 1
sur_ref        = 2

DPB_band       = 0
BLU_band       = 1
GRN_band       = 2
RED_band       = 3
NIR_band       = 4
SW1_band       = 5
SW2_band       = 6
RED1_band      = 7
RED1_band      = 8
RED1_band      = 9
WV_band        = 10


pix_QA          = 'pix_qa'
pix_score       = 'score'
score_target    = 'score_target'
pix_date        = 'date'
neg_blu_score   = 'neg_blu_score'
Texture_name    = 'texture'
mosaic_ssr_code = 'ssr_code'
PARAM_NDVI      = 'ndvi'



# The integer code for the band types to be attached to images
EXTRA_NONE  = 0
EXTRA_ANGLE = 1
EXTRA_NDVI  = 2
EXTRA_CODE  = 3     # sensor code



SSR_META_DICT = {
  'S2_SR': { 'NAME': 'S2_SR',
             'SSR_CODE': S2A_sensor,
             'DATA_UNIT': sur_ref,
             'GAIN': 0.0001,
             'OFFSET': 0,
             'ALL_BANDS': ['blue', 'green', 'red', 'rededge1', 'rededge2', 'rededge3', 'nir08', 'swir16', 'swir22'],
             'OUT_BANDS': ['B2', 'B3', 'B4', 'B5', 'B6', 'B7', 'B8', 'B8A', 'B11', 'B12'], 
             '10M_BANDS': ['blue', 'green', 'red', 'nir08'],
             'SIX_BANDS': ['blue', 'green', 'red', 'nir08', 'swir16', 'swir22'],
             'NoA_BANDS': ['red', 'rededge1', 'rededge2', 'rededge3', 'nir08', 'swir16', 'swir22'],
             'GEE_NAME': 'COPERNICUS/S2_SR_HARMONIZED',
             'CLOUD': 'CLOUDY_PIXEL_PERCENTAGE',
             'SZA': 'view:sun_elevation',
             'SAA': 'view:sun_azimuth', 
             'VZA': 'view:sun_elevation',            
             'VAA': 'view:sun_azimuth',
             'BLU': 'blue',
             'GRN': 'green',
             'RED': 'red',
             'NIR': 'nir08',
             'SW1': 'swir16',
             'SW2': 'swir22'},

  'S2_TOA': {'NAME': 'S2_TOA',
             'SSR_CODE': S2A_sensor,
             'DATA_UNIT': TOA_ref,
             'GAIN': 0.0001,
             'OFFSET': 0,
             'ALL_BANDS': ['B1', 'B2', 'B3', 'B4', 'B5', 'B6', 'B7', 'B8', 'B8A', 'B9', 'B11', 'B12'],
             'OUT_BANDS': ['B2', 'B3', 'B4', 'B5', 'B6', 'B7', 'B8', 'B8A', 'B11', 'B12'], 
             '10M_BANDS': ['B2', 'B3', 'B4', 'B8'],
             'SIX_BANDS': ['B2', 'B3', 'B4', 'B8A', 'B11', 'B12'],
             'NoA_BANDS': ['B4', 'B5', 'B6', 'B7', 'B8', 'B8A', 'B11', 'B12'],
             'GEE_NAME': 'COPERNICUS/S2_HARMONIZED',
             "CLOUD": 'CLOUDY_PIXEL_PERCENTAGE',
             "SZA": 'MEAN_SOLAR_ZENITH_ANGLE',
             "VZA": 'MEAN_INCIDENCE_ZENITH_ANGLE_B8A',
             "SAA": 'MEAN_SOLAR_AZIMUTH_ANGLE', 
             "VAA": 'MEAN_INCIDENCE_AZIMUTH_ANGLE_B8A',
             'BLU': 'B2',
             'GRN': 'B3',
             'RED': 'B4',
             'NIR': 'B8A',
             'SW1': 'B11',
             'SW2': 'B12'},
  'HLS_SR': {'NAME': 'HLS_SR',
            'SSR_CODE': HLS_sensor,
            'DATA_UNIT': sur_ref,
            'GAIN': 1,
            'OFFSET': 0,
            'ALL_BANDS': ['B1', 'B2', 'B3', 'B4', 'B5', 'B6', 'B7'],
            'OUT_BANDS': ['B1', 'B2', 'B3', 'B4', 'B5', 'B6', 'B7'], 
            'SIX_BANDS': ['B2', 'B3', 'B4', 'B5', 'B6', 'B7'],
            'NoA_BANDS': ['B4', 'B5', 'B6', 'B7'],
            'GEE_NAME': 'NASA/HLS/HLSL30/v002',
            "CLOUD": 'CLOUD_COVERAGE',
            "SZA": 'MEAN_SUN_ZENITH_ANGLE',
            "SAA": 'MEAN_SUN_AZIMUTH_ANGLE', 
            "VZA": 'MEAN_VIEW_ZENITH_ANGLE',            
            "VAA": 'MEAN_VIEW_AZIMUTH_ANGLE',
            'BLU': 'B2',
            'GRN': 'B3',
            'RED': 'B4',
            'NIR': 'B5',
            'SW1': 'B6',
            'SW2': 'B7'},

  'L8_SR': {'NAME': 'L8_SR',
            'SSR_CODE': LS8_sensor,
            'DATA_UNIT': sur_ref,
            'GAIN': 0.0000275,
            'OFFSET': -0.2,
            'ALL_BANDS': ['SR_B1', 'SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7'],
            'OUT_BANDS': ['SR_B1', 'SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7'], 
            'SIX_BANDS': ['SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7'],
            'NoA_BANDS': ['SR_B4', 'SR_B5', 'SR_B6', 'SR_B7'],
            'GEE_NAME': 'LANDSAT/LC08/C02/T1_L2',
            "CLOUD": 'CLOUD_COVER',
            "SZA": 'SUN_ELEVATION',
            "SAA": 'SUN_AZIMUTH', 
            "VZA": 'SUN_ELEVATION',            
            "VAA": 'SUN_AZIMUTH',
            'BLU': 'SR_B2',
            'GRN': 'SR_B3',
            'RED': 'SR_B4',
            'NIR': 'SR_B5',
            'SW1': 'SR_B6',
            'SW2': 'SR_B7'},

  'L9_SR': {'NAME': 'L9_SR',
            'SSR_CODE': LS9_sensor,
            'DATA_UNIT': sur_ref,
            'GAIN': 0.0000275,
            'OFFSET': -0.2,
            'ALL_BANDS': ['SR_B1', 'SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7'],
            'OUT_BANDS': ['SR_B1', 'SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7'], 
            'SIX_BANDS': ['SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7'],
            'NoA_BANDS': ['SR_B4', 'SR_B5', 'SR_B6', 'SR_B7'],
            'GEE_NAME': 'LANDSAT/LC09/C02/T1_L2',
            "CLOUD": 'CLOUD_COVER',
            "SZA": 'SUN_ELEVATION',
            "SAA": 'SUN_AZIMUTH', 
            "VZA": 'SUN_ELEVATION',            
            "VAA": 'SUN_AZIMUTH',
            'BLU': 'SR_B2',
            'GRN': 'SR_B3',
            'RED': 'SR_B4',
            'NIR': 'SR_B5',
            'SW1': 'SR_B6',
            'SW2': 'SR_B7'},

  'L7_SR': {'NAME': 'L7_SR',
            'SSR_CODE': LS7_sensor,
            'DATA_UNIT': sur_ref,
            'GAIN': 0.0000275,
            'OFFSET': -0.2,
            'ALL_BANDS': ['SR_B1', 'SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B7'],
            'OUT_BANDS': ['SR_B1', 'SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B7'], 
            'SIX_BANDS': ['SR_B1', 'SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B7'],
            'NoA_BANDS': ['SR_B3', 'SR_B4', 'SR_B5', 'SR_B7'],
            'GEE_NAME': 'LANDSAT/LE07/C02/T1_L2',
            "CLOUD": 'CLOUD_COVER',
            "SZA": 'SUN_ELEVATION',
            "SAA": 'SUN_AZIMUTH', 
            "VZA": 'SUN_ELEVATION',            
            "VAA": 'SUN_AZIMUTH',
            'BLU': 'SR_B1',
            'GRN': 'SR_B2',
            'RED': 'SR_B3',
            'NIR': 'SR_B4',
            'SW1': 'SR_B5',
            'SW2': 'SR_B7'},

  'L5_SR': {'NAME': 'L5_SR',
            'SSR_CODE': LS5_sensor,
            'DATA_UNIT': sur_ref,
            'GAIN': 0.0000275,
            'OFFSET': -0.2,
            'ALL_BANDS': ['SR_B1', 'SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B7'],
            'OUT_BANDS': ['SR_B1', 'SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B7'], 
            'SIX_BANDS': ['SR_B1', 'SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B7'],
            'NoA_BANDS': ['SR_B3', 'SR_B4', 'SR_B5', 'SR_B7'],
            'GEE_NAME': 'LANDSAT/LT05/C02/T1_L2', 
            "CLOUD": 'CLOUD_COVER',
            "SZA": 'SUN_ELEVATION',
            "SAA": 'SUN_AZIMUTH', 
            "VZA": 'SUN_ELEVATION',            
            "VAA": 'SUN_AZIMUTH',
            'BLU': 'SR_B1',
            'GRN': 'SR_B2',
            'RED': 'SR_B3',
            'NIR': 'SR_B4',
            'SW1': 'SR_B5',
            'SW2': 'SR_B7'},

  'L8_TOA': {'NAME': 'L8_TOA',
             'SSR_CODE': LS8_sensor,
             'DATA_UNIT': TOA_ref,
             'GAIN': 1,
             'OFFSET': 0,
             'ALL_BANDS': ['B1', 'B2', 'B3', 'B4', 'B5', 'B6', 'B7'],
             'OUT_BANDS': ['B1', 'B2', 'B3', 'B4', 'B5', 'B6', 'B7'], 
             'SIX_BANDS': ['B2', 'B3', 'B4', 'B5', 'B6', 'B7'],
             'NoA_BANDS': ['B4', 'B5', 'B6', 'B7'],
             'GEE_NAME': 'LANDSAT/LC08/C02/T1_TOA',
             "CLOUD": 'CLOUD_COVER',
             "SZA": 'SUN_ELEVATION',
             "VZA": 'SUN_ELEVATION',
             "SAA": 'SUN_AZIMUTH', 
             "VAA": 'SUN_AZIMUTH',
             'BLU': 'B2',
             'GRN': 'B3',
             'RED': 'B4',
             'NIR': 'B5',
             'SW1': 'B6',
             'SW2': 'B7'},

  'L9_TOA': {'NAME': 'L9_TOA',
             'SSR_CODE': LS9_sensor,
             'DATA_UNIT': TOA_ref,
             'GAIN': 1,
             'OFFSET': 0,
             'ALL_BANDS': ['B1', 'B2', 'B3', 'B4', 'B5', 'B6', 'B7'],
             'OUT_BANDS': ['B1', 'B2', 'B3', 'B4', 'B5', 'B6', 'B7'], 
             'SIX_BANDS': ['B2', 'B3', 'B4', 'B5', 'B6', 'B7'],
             'NoA_BANDS': ['B4', 'B5', 'B6', 'B7'],
             'GEE_NAME': 'LANDSAT/LC09/C02/T1_TOA',
             "CLOUD": 'CLOUD_COVER',
             "SZA": 'SUN_ELEVATION',
             "VZA": 'SUN_ELEVATION',
             "SAA": 'SUN_AZIMUTH', 
             "VAA": 'SUN_AZIMUTH',
             'BLU': 'B2',
             'GRN': 'B3',
             'RED': 'B4',
             'NIR': 'B5',
             'SW1': 'B6',
             'SW2': 'B7'},
  'L7_TOA': {'NAME': 'L7_TOA',
            'SSR_CODE': LS7_sensor,
            'DATA_UNIT': TOA_ref,
            'GAIN': 1,
            'OFFSET': 0,
            'ALL_BANDS': ['B1', 'B2', 'B3', 'B4', 'B5', 'B7'],
            'OUT_BANDS': ['B1', 'B2', 'B3', 'B4', 'B5', 'B7'], 
            'SIX_BANDS': ['B1', 'B2', 'B3', 'B4', 'B5', 'B7'],
            'NoA_BANDS': ['B3', 'B4', 'B5', 'B7'],
            'GEE_NAME': 'LANDSAT/LE07/C02/T1_TOA',
            "CLOUD": 'CLOUD_COVER',
            "SZA": 'SUN_ELEVATION',
            "SAA": 'SUN_AZIMUTH', 
            "VZA": 'SUN_ELEVATION',            
            "VAA": 'SUN_AZIMUTH',
            'BLU': 'B1',
            'GRN': 'B2',
            'RED': 'B3',
            'NIR': 'B4',
            'SW1': 'B5',
            'SW2': 'B7'},
           
  'MOD_SR': {'NAME': 'MOD09_SR',
             'SSR_CODE': MOD_sensor,
             'DATA_UNIT': sur_ref,
             'GAIN': 0.0001,
             'OFFSET': 0,
             'ALL_BANDS': ['sur_refl_b03', 'sur_refl_b04', 'sur_refl_b01', 'sur_refl_b02', 'sur_refl_b05', 'sur_refl_b06', 'sur_refl_b07'],
             'OUT_BANDS': ['sur_refl_b03', 'sur_refl_b04', 'sur_refl_b01', 'sur_refl_b02', 'sur_refl_b05', 'sur_refl_b06', 'sur_refl_b07'], 
             'SIX_BANDS': ['sur_refl_b03', 'sur_refl_b04', 'sur_refl_b01', 'sur_refl_b02', 'sur_refl_b06', 'sur_refl_b07'],
             'NoA_BANDS': ['sur_refl_b01', 'sur_refl_b02', 'sur_refl_b06', 'sur_refl_b07'],
             'GEE_NAME': 'MODIS/061/MOD09A1', #Terra Surface Refklectance 8-day Global 500m
             "CLOUD": 'CLOUD_COVER',
             "SZA": 'SolarZenith',
             "SAA": 'SolarAzimuth', 
             "VZA": 'SensorZenith',             
             "VAA": 'SensorAzimuth',
             'BLU': 'sur_refl_b03',
             'GRN': 'sur_refl_b04',
             'RED': 'sur_refl_b01',
             'NIR': 'sur_refl_b02',
             'SW1': 'sur_refl_b06',
             'SW2': 'sur_refl_b07'}
}


DATA_TYPE   = ['S2_SR', 'LS8_SR', 'LS9_SR', 'LS7_SR', 'LS5_SR', 'S2_TOA', 'LS8_TOA', 'LS9_TOA', 'LS7_TOA', 'LS5_TOA']
STD_6_BANDS = ['blue', 'green', 'red', 'nir', 'swir1', 'swir2']
MONTH_NAMES = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']





#############################################################################################################
# Description: This function returns sensor_code, tile_name and acquisition date according to a given 
#              Image ID string.   
# 
# Samples: (1) Landsat image ID string:    LC08_034010_20230727    
#          (2) Sentinel-2 image ID string: 20220806T173909_20220806T173907_T17WMU
#
# Revision history:  2023-Nov-20  Lixin Sun  Initial creation
#
#############################################################################################################
def parse_ImgID(ImgID_str):
  tokens    = ImgID_str.split('_')
  ssr_code  = UNKNOWN_sensor
  tile_name = ''
  acq_date  = ''
  valid_ID  = True

  if len(tokens) > 2:
    # Determine the sensor type based on the first token
    if tokens[0].find('LC') > -1:  # is a Landsat scene
      if tokens[0].find('8'):
        ssr_code = LS8_sensor
      elif tokens[0].find('9'):
        ssr_code = LS9_sensor
      elif tokens[0].find('7'):
        ssr_code = LS7_sensor  
      elif tokens[0].find('5'):
        ssr_code = LS5_sensor
      else:
        valid_ID = False

      # Determine tile name and acquisition date
      tile_name = tokens[1] 
      acq_date  = tokens[2]
    else: # is a Sentinel-2 scene
      ssr_code  = S2A_sensor
      tile_name = tokens[2]
      acq_date  = tokens[0][0:8]
  
  return ssr_code, tile_name, acq_date, valid_ID




#############################################################################################################
# Description: This function returns a key string for retrieving a sensor data dictionary from 
#              "SSR_META_DICT" based on given sensor code and data unit.
#             
# Revision history:  2022-Nov-20  Lixin Sun  Initial creation
#
#############################################################################################################
def get_SsrData_key(SsrCode, DataUnit):
  if DataUnit == sur_ref:
    if SsrCode == LS8_sensor:
      return 'L8_SR'
    elif SsrCode == LS9_sensor:
      return 'L9_SR'
    elif SsrCode == S2A_sensor or SsrCode == S2B_sensor:
      return 'S2_SR'
    elif SsrCode == LS7_sensor:
      return 'L7_SR'
  elif DataUnit == TOA_ref:
    if SsrCode == LS8_sensor:
      return 'L8_TOA'
    elif SsrCode == LS9_sensor:
      return 'L9_TOA'
    elif SsrCode == S2A_sensor or SsrCode == S2B_sensor:
      return 'S2_TOA'
  else:
    print('<get_SsrData> Wrong sensor code or data unit provided!')
    return ''





#############################################################################################################
# Description: This function returns a cloud coverage percentage based on a given region and sensor data.
#
# Revision history:  2021-June-09  Lixin Sun  Initial creation
#                    2024-May-28   Lixin Sun  Modified for ODC-STAC and Xarray code.
#############################################################################################################
def get_cloud_rate(SsrData, Region):
  '''Returns a cloud coverage percentage based on the given location and sensor type. 
     Args:
        SsrData(Dictionary): A Dictionary containing metadata associated with a sensor and data unit;
        Region(Dictionary): A geospatial region of ROI.'''  

  # Determine the centre point of the given geographical region
  cLon, cLat = eoUs.get_region_centre(Region)

  # Determine cloud coverage percentage based on sensor type and latitude
  ST2_rate = 85 if cLat < 55 else 70
  LS_rate  = 90
  
  ssr_code = SsrData['SSR_CODE']
  return ST2_rate if ssr_code > MAX_LS_CODE else LS_rate






###################################################################################################
# Description: This function returns rescaling factors for converting the pixel values of an image
#              (either TOA or surface rflectance) to a range either between 0 and 100 or between 
#              0 and 1.
#
# Note:        The gain and offset for diffrent sensors and different data units are gathered from
#              GEE Data Catalog and summarized as follows:
#
#    Sensor  |  TOA reflectance  |  surface reflectance | TOA reflectance  |  surface reflectance |
#            | out range [0,100] | out range [1,100]    | out range [0,1]  | out range [1,1]      | 
#  ------------------------------------------------------------------------------------------------
#   coeffs   |    gain  offset   |    gain     offset   |  gain  offset   |  gain      offset     |             
#   S2       |    0.01   +0      |    0.01       +0     | 0.0001   0.0    | 0.0001       0.0      | 
#   L9 coll2 |    100    +0      |    0.00275    -20    | 1.0      0.0    | 0.0000275   -0.2      |
#   L8 coll2 |    100    +0      |    0.00275    -20    | 1.0      0.0    | 0.0000275   -0.2      |
#   L7 coll2 |    100    +0      |    0.00275    -20    | 1.0      0.0    | 0.0000275   -0.2      |
#   L5 coll2 |    100    +0      |    0.00275    -20    | 1.0      0.0    | 0.0000275   -0.2      |
#  ------------------------------------------------------------------------------------------------
#
# Revision history:  2021-May-10  Lixin Sun  Converted from Lixin's JavaScript code
#                    2022-Mar-24  Lixin Sun  Renamed the function from "get_rescale" to 
#                                            "get_gain_offset" since Landsat Collection-2 data uses
#                                            gain/scale and offset, instead of just scale only. 
#                    2022-Mar-29  Lixin Sun  Add 'MaxRef' parameter so that proper scaling factors
#                                            for different reflectance value ranges (either [0 to 1]
#                                            or [0 to 100]) are returned.  
#                    2024-May-28  Lixin Sun  Converted for odc-stac and xarray application
#
###################################################################################################
def get_gain_offset(SsrData, MaxRef):
  '''Returns a rescaling factor based on given sensor code and data unit.

     Args:        
        SsrData(Dictionary): A Dictionary containing metadata associated with a sensor and data unit;
        MaxRef: The maximum output reflectance value (1 or 100)''' 
  
  return MaxRef*SsrData['GAIN'], MaxRef*SsrData['OFFSET']




###################################################################################################
# Description: This function applys gain and offset to the optical bands of a given image.
#
# Revision history:  2022-Mar-24  Lixin Sun  Initial creation
#                    2022-Mar-28  Lixin Sun  Add 'MaxRef' parameter so that different reflectance
#                                            ranges ([0 to 1] or [0 to 100]) can be handled.  
#                    2024-May-28  Lixin Sun  Converted for odc-stac and xarray application
###################################################################################################
def apply_gain_offset(xrDS, SsrData, MaxRef, all_bands):
  '''Returns a rescaling factor based on given sensor code and data unit.

     Args:        
       xrDS(xrDataset): A given xarray dataset object to which gain and offset will be applied  
       SsrData(Dictionary): A Dictionary containing metadata associated with a sensor and data unit;
       MaxREF: The maximum reflectance value (1 or 100);
       all_bands(Boolean): A flag indicating if apply gain and offset to all bands or not.''' 
  
  #================================================================================================
  # Apply gain and offset
  #================================================================================================
  gain, offset = get_gain_offset(SsrData, MaxRef)
  #print('<apply_gain_offset> Rescaling gain and offset = \n',gain_offset[0], gain_offset[1])
  band_names = SsrData['ALL_BANDS'] # Get the names of all optical bands

  if all_bands == True:
    xrDS = xrDS*gain + offset
  else:    
    apply_coeffs = lambda x: x*gain + offset

    xrDS = xrDS.assign(**{var: apply_coeffs(xrDS[var]) for var in band_names})
  
  return xrDS






###################################################################################################
# Description: This function applys gain and offset to the optical bands of a given image.
#
# Revision history:  2024-May-28  Lixin Sun  Initial creation
#
###################################################################################################
def apply_default_mask(xrDS, SsrData):
  '''Returns a rescaling factor based on given sensor code and data unit.

     Args:        
       xrDS(xrDataset): A given xarray dataset object to which default mask will be applied  
       SsrData(Dictionary): A Dictionary containing metadata associated with a sensor and data unit.''' 
  ssr_code = SsrData['SSR_CODE']

  if ssr_code > MAX_LS_CODE and ssr_code < 25:  # For Sentinel-2 
    scl = xrDS['scl']
    # The pixels with SCL = 0 must be masked out

    return xrDS.where((scl == 2) | ((scl > 3) & (scl < 8)) | (scl == 10))
  
  else:    
    return xrDS
  





#############################################################################################################
# Description: This function attaches a date band to the given xarray dataset item (an image)
#
# Revision history:  2020-Juy-10  Lixin Sun  Initial creation
#                    2021-May-10  Lixin Sun  Converted from Lixin's JavaScript code
#                    2024-May-a8  Lixin Sun  Converted for odc-stac and xarray application
#############################################################################################################
def attach_Date(xrItem):
  '''Attaches an image acquisition date band to a given image
  Args:
    Img(xrDataset): A given xarray item.'''
  
  #==========================================================================================================
  # Obtain the unix epoch seconds of acquisition time
  # Note: the last three characters of the string returned from "xrItem.coords['time'].values" must be removed
  #==========================================================================================================
  acqu_date = str(xrItem.coords['time'].values)[:-3]
  print('<attach_Date> acquisition date = ', acqu_date)
    
  # Parse the timestamp string to a datetime object
  img_dt = datetime.datetime.strptime(acqu_date, '%Y-%m-%dT%H:%M:%S.%f')

  # Convert the datetime object to a Unix timestamp (seconds since epoch)
  img_epoch_secs = img_dt.timestamp()

  #==========================================================================================================
  # Create a datetime object with date and time
  #==========================================================================================================
  DOY_1st_epoch_secs = datetime.datetime(img_dt.year, 1, 1).timestamp()
  DOY = (img_epoch_secs - DOY_1st_epoch_secs)/86400    # 86,400 is the seconds per day
  xrItem[pix_date] = np.int16(DOY)                     # casted to 16bit integer
  #print('<attach_Date> DOY = ', DOY)

  return xrItem
  
  



#############################################################################################################
# Description: This function appends three imaging geometry angle bands to a specified mosaic image stored as 
#              an xarray dataset object. 
#
# Note:        This function assumes "xrDS" was created using the STAC items in "StacItems".
#  
# Revision history:  2024-Jul-19  Lixin Sun  Initial creation
#
#############################################################################################################
def attach_AngleBands(xrDS, StacItems):
  '''Attaches three angle bands to a satallite SURFACE REFLECTANCE image
  Args:    
    xrDS(xr Dateset): A xarray dataset object (a single image);
    StacItems(List): A list of STAC items corresponding to the "xrDS".'''  
  #==========================================================================================================
  # Sort the provided STAC items to match the image sequence in "xrDS"
  #==========================================================================================================  
  def get_sort_key(item):
    return item.datetime
  
  sorted_items = sorted(StacItems, key=get_sort_key)
 
  #for item in sorted_items:
  #  eoIM.get_Img_Angles(item)
  #  print(item)

  #eoUs.get_average_VAs('S2A', TimeStamp, CentreLat, CentreLon, CentreAlt)
  #==========================================================================================================
  # Create three lists to store the cosine values of the imaging geometry angles. 
  #==========================================================================================================  
  cosSZAs = np.cos(np.radians([item.properties['sza'] for item in sorted_items]))
  cosVZAs = np.cos(np.radians([item.properties['vza'] for item in sorted_items]))
  cosRAAs = np.cos(np.radians([item.properties['saa'] - item.properties['vaa'] for item in sorted_items]))

  #==========================================================================================================
  # Define a function to map indices to the angle cosine values
  #==========================================================================================================
  def map_indices_to_values(IndxBand, values):
    indx_band = IndxBand.astype(np.int8)
    return values[indx_band]
  
  #==========================================================================================================
  # Apply the function using xarray.apply_ufunc
  #==========================================================================================================
  xrDS["cosSZA"] = xr.apply_ufunc(map_indices_to_values, xrDS["time_index"], cosSZAs).astype(np.float32)
  xrDS["cosVZA"] = xr.apply_ufunc(map_indices_to_values, xrDS["time_index"], cosVZAs).astype(np.float32)
  xrDS["cosRAA"] = xr.apply_ufunc(map_indices_to_values, xrDS["time_index"], cosRAAs).astype(np.float32)

  return xrDS




#############################################################################################################
# Description: This function attach a NDVI band to a given image.
#  
# Revision history:  2022-Aug-10  Lixin Sun  Initial creation
#                    2024-May-28  Lixin Sun  Converted for odc-stac and xarray application
#
#############################################################################################################
def attach_NDVIBand(xrDS, SsrData):
  '''
  Args:
    xrDS(xrDataset): A given xarray dataset object;
    SsrData(Dictionary): A Dictionary containing metadata associated with a sensor and data unit.'''  

  gain, offset = get_gain_offset(SsrData, 100)  
  
  red = xrDS.sel(SsrData['RED'])*gain + offset
  nir = xrDS.sel(SsrData['NIR'])*gain + offset
    
  xrDS['ndvi'] = (nir - red)/ (nir + red)

  return xrDS




#############################################################################################################
# Description: This function returns a month name string according to a month number integer.
#  
# Revision history:  2022-Aug-10  Lixin Sun  Initial creation
#                    2024-May-28  Lixin Sun  Converted for odc-stac and xarray application
#
#############################################################################################################
def get_MonthName(month_numb):
  month = int(month_numb)

  if month > 0 and month < 13:
    return MONTH_NAMES[month-1]
  else:
    return 'season'
  



#############################################################################################################
# Description: This function reads a GeoTIF image from a local drive and saves it as a xarray.dataset object.   
#  
# Revision history:  2024-Aug-01  Lixin Sun  Initial creation
#
#############################################################################################################
def read_geotiff(ImgPath, OutName='band'):
  if not os.path.exists(ImgPath):
    print('<read_geotiff_img> The given image file <%s> does not exist!'%(ImgPath))
    return None
  
  return rioxarray.open_rasterio(ImgPath)
  
  '''
  # Read the GeoTIFF file into an xarray.DataArray
  with rasterio.open(ImgPath) as src:
    # Read the image data into a numpy array
    img_data = src.read(1)  # Read the first band

    # Create an xarray.DataArray
    data_array = xr.DataArray(img_data, 
                              dims = ["y", "x"],
                              coords = {"y": np.arange(img_data.shape[0]), "x": np.arange(img_data.shape[1])},
                              attrs = src.meta)

    # Convert the DataArray to a Dataset if needed
    return data_array.to_dataset(name=OutName)
  '''



#############################################################################################################
# Description: This function returns a subset of "Source_xrDS" so that it covers the same spatial area as
#              "Refer_xrDs" does and resample it as necessary.
#  
# Revision history:  2024-Aug-02  Lixin Sun  Initial creation
#
#############################################################################################################
def xrDS_spatial_match(Refer_xrDs, Source_xrDS, Flip_Y = True):
  #==========================================================================================================
  # Determine the spatial extent of the reference xarray.dataset
  #==========================================================================================================  
  min_x, max_x = Refer_xrDs.x.min().values, Refer_xrDs.x.max().values
  min_y, max_y = Refer_xrDs.y.min().values, Refer_xrDs.y.max().values
  
  if Flip_Y:
    min_y, max_y = max_y, min_y

  # Clip the larger dataset to the extent of the smaller dataset
  # The `sel` method will use the defined extent to select the region of interest
  clipped_source = Source_xrDS.sel(x=slice(min_x, max_x), y=slice(min_y, max_y))

  # Resample Source_xrDS to match that of Refer_xrDs
  return clipped_source.interp_like(Refer_xrDs)








#test_img = read_geotiff('C:\\Work_documents\\Canada_LC_2020_30m.tif', OutName='test_band')
#print(test_img)