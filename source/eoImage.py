

import os
import glob
import datetime
import rasterio
import rioxarray

import numpy as np
import xarray as xr

import eoUtils as eoUs



UNKNOWN_sensor = 0
LS5_sensor     = 5
LS7_sensor     = 7
LS8_sensor     = 8
LS9_sensor     = 9
LS_sensor      = 19
MAX_LS_CODE    = 20

S2A_sensor     = 21
S2B_sensor     = 22
S2C_sensor     = 23
MAX_S2_CODE    = 40

S1B_sensor     = 41
S1B_sensor     = 42
MAX_S1_CODE    = 50

MOD_sensor     = 80     # MODIS sensor
HLSS30_sensor  = 100    # Harmonized Sentinel-2A/B
HLSL30_sensor  = 101    # Harmonized Landsat-8/9
HLS_sensor     = 102    # Harmonized Landsat and Sentinel-2 data


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


pix_QA          = 'QC'
pix_score       = 'score'
score_target    = 'score_target'
pix_date        = 'date'
pix_sensor      = 'sensor' 
neg_blu_score   = 'neg_blu_score'
Texture_name    = 'texture'
mosaic_ssr_code = 'ssr_code'
PARAM_NDVI      = 'ndvi'



# The integer code for the band types to be attached to images
EXTRA_NONE  = 0
EXTRA_ANGLE = 1
EXTRA_NDVI  = 2
EXTRA_CODE  = 3     # sensor code

DATA_TYPE   = ['S2_SR', 'LS8_SR', 'LS9_SR', 'LS7_SR', 'LS5_SR', 'S2_TOA', 'LS8_TOA', 'LS9_TOA', 'LS7_TOA', 'LS5_TOA']
STD_6_BANDS = ['blue', 'green', 'red', 'nir08', 'swir16', 'swir22']
MONTH_NAMES = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

SSR_META_DICT = {
  'S2_SR': { 'NAME': 'S2_SR',
             'SSR_CODE': S2A_sensor,
             'DATA_UNIT': sur_ref,
             'GAIN': 0.0001,
             'OFFSET': 0,                          
             'ALL_BANDS': ['blue', 'green', 'red', 'rededge1', 'rededge2', 'rededge3', 'nir08', 'swir16', 'swir22'],             
             '10M_BANDS': ['blue', 'green', 'red', 'nir08', 'swir16', 'swir22'],
             'SIX_BANDS': ['blue', 'green', 'red', 'nir08', 'swir16', 'swir22'],
             'NoA_BANDS': ['red', 'rededge1', 'rededge2', 'rededge3', 'nir08', 'swir16', 'swir22'],
             'LEAF_BANDS':['green', 'red', 'rededge1', 'rededge2', 'rededge3', 'nir08', 'swir16', 'swir22'],
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

  'HLSS30_SR': {'NAME': 'HLSS30_SR',
            'SSR_CODE': HLSS30_sensor,
            'DATA_UNIT': sur_ref,
            'GAIN': 0.0001,
            'OFFSET': 0,
            'ALL_BANDS': ['B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B08', 'B11', 'B12'],
            'SIX_BANDS': ['B02', 'B03', 'B04', 'B08', 'B11', 'B12'],
            'NoA_BANDS': ['B04', 'B08', 'B11', 'B12'],
            'LEAF_BANDS':['B03', 'B04', 'B05', 'B06', 'B07', 'B08', 'B11', 'B12'],
            'ANGLE_BANDS': ['VZA', 'VAA', 'SZA', 'SAA'],
            "CLOUD": 'CLOUD_COVERAGE',            
            'BLU': 'B02',
            'GRN': 'B03',
            'RED': 'B04',
            'NIR': 'B08',
            'SW1': 'B11',
            'SW2': 'B12'},

  'HLSL30_SR': {'NAME': 'HLSL30_SR',
            'SSR_CODE': HLSL30_sensor,
            'DATA_UNIT': sur_ref,
            'GAIN': 0.0001,
            'OFFSET': 0,
            'ALL_BANDS': ['B02', 'B03', 'B04', 'B05', 'B06', 'B07'],
            'SIX_BANDS': ['B02', 'B03', 'B04', 'B05', 'B06', 'B07'],
            'NoA_BANDS': ['B04', 'B05', 'B06', 'B07'],
            'LEAF_BANDS':['B03', 'B04', 'B05', 'B06', 'B07'],
            'ANGLE_BANDS': ['VZA', 'VAA', 'SZA', 'SAA'],
            "CLOUD": 'CLOUD_COVERAGE',            
            'BLU': 'B02',
            'GRN': 'B03',
            'RED': 'B04',
            'NIR': 'B05',
            'SW1': 'B06',
            'SW2': 'B07'},

  'HLS_SR': {'NAME': 'HLS_SR',
            'SSR_CODE': HLS_sensor,
            'DATA_UNIT': sur_ref,
            'GAIN': 0.0001,
            'OFFSET': 0,
            'ALL_BANDS': ['B02', 'B03', 'B04', 'B05', 'B06', 'B07'],
            'SIX_BANDS': ['B02', 'B03', 'B04', 'B05', 'B06', 'B07'],
            'NoA_BANDS': ['B04', 'B05', 'B06', 'B07'],
            'LEAF_BANDS':['B03', 'B04', 'B05', 'B06', 'B07'],
            'ANGLE_BANDS': ['VZA', 'VAA', 'SZA', 'SAA'],
            "CLOUD": 'CLOUD_COVERAGE',            
            'BLU': 'B02',
            'GRN': 'B03',
            'RED': 'B04',
            'NIR': 'B05',
            'SW1': 'B06',
            'SW2': 'B07'},

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







###################################################################################################
# Description: This function identifies the type of the given image object.
#
# Revision history:  2025-Jun-10  Lixin Sun  Initial creation
###################################################################################################
def identify_image_type(ImgObj):
  if isinstance(ImgObj, xr.Dataset):
    return "xarray.dataset"
  elif isinstance(ImgObj, xr.DataArray):
    return "xarray.dataarray"
  elif isinstance(ImgObj, np.ndarray):
    return "numpy.ndarray"
  else:
    return type(ImgObj).__name__



###################################################################################################
# Description: This function extracts a window image (WH x WW x C) around a given pixel at (x, y)
#              from an image cube with shape (H, W, C).
# Note: If the given pixel is close to the image boundary, the extracted window
# Revision history:  2025-Nov-28  Lixin Sun  Initial creation
###################################################################################################
def extract_window(inImg, x, y, win=7):
  """
    Input:
      inImg(H, W, C): the given image cube in np.ndarray format,
      x, y: the pixel location of agiven pixel,
      win: the window size (default is 7)
      
    Returns:  
      winImg(WH, WW, C): the extracted window image. """
    
    #H, W, C = inImg.shape
  wr = win // 2  # window radius (3 for 7×7)
  ndim = inImg.ndim

  if ndim == 3:
    return inImg[x-wr:x+wr+1, y-wr:y+wr+1, :]
  
  elif ndim == 2:
    return inImg[x-wr:x+wr+1, y-wr:y+wr+1]
  
  else:
    raise ValueError("<extract_window> inImg must be 2D or 3D")

  # Compute valid boundaries
  x1 = max(0, x - wr)
  x2 = min(H, x + wr + 1)
  y1 = max(0, y - wr)
  y2 = min(W, y + wr + 1)

  return inImg[x1:x2, y1:y2, :]





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
    elif SsrCode == S2A_sensor or SsrCode == S2B_sensor or SsrCode == S2C_sensor:
      return 'S2_SR'
    elif SsrCode == LS7_sensor:
      return 'L7_SR'
  elif DataUnit == TOA_ref:
    if SsrCode == LS8_sensor:
      return 'L8_TOA'
    elif SsrCode == LS9_sensor:
      return 'L9_TOA'
    elif SsrCode == S2A_sensor or SsrCode == S2B_sensor or SsrCode == S2C_sensor:
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
       MaxRef: The maximum reflectance value (1 or 100);
       all_bands(Boolean): A flag indicating if apply gain and offset to all bands or not.''' 
  
  #================================================================================================
  # Obtain gain and offset, and the names of spectral bands
  #================================================================================================
  gain, offset = get_gain_offset(SsrData, MaxRef)
  all_spec_bands = ['blue', 'green', 'red', 'rededge1', 'rededge2', 'rededge3', 'nir08', 'swir16', 'swir22']
  xrDS_bands = list(xrDS.data_vars.keys())

  spec_bands = [band for band in xrDS_bands if band in all_spec_bands]
  
  #================================================================================================
  # Apply gain and offset to all or only spectral bands
  #================================================================================================
  if all_bands == True:
    xrDS = xrDS*gain + offset
  else:    
    apply_coeffs = lambda x: x*gain + offset

    xrDS = xrDS.assign(**{var: apply_coeffs(xrDS[var]) for var in spec_bands})
  
  return xrDS






###################################################################################################
# Description: This function masks out cloud and shadow pixels in a given XArray dataset object.
#
# Revision history:  2024-May-28  Lixin Sun  Initial creation
#
# SCL Class Table
# value     Description
#   1       Saturated or defective
#   2       Dark area pixels
#   3       Cloud Shadow
#   4       Vegetation
#   5       Bare soils
#   6       Water
#   7       Clouds low probability
#   8       Clouds medium probability
#   9       Clouds high probability
#   10      Cirrus
#   11      Snow/Ice
###################################################################################################
def apply_pixel_masks(xrDS, SsrData):
  '''Returns a rescaling factor based on given sensor code and data unit.
 
     Args:        
       xrDS(xrDataset): A given xarray dataset object to which default mask will be applied  
       SsrData(Dictionary): A Dictionary containing metadata associated with a sensor and data unit.
  '''

  ssr_code = SsrData['SSR_CODE']
  
  if ssr_code > MAX_LS_CODE and ssr_code < MAX_S2_CODE:  # For Sentinel-2 from AWS data catalog
    scl = xrDS['scl']
    # The pixels with SCL = 0 must be masked out
    #return xrDS.where(((scl > 3) & (scl < 8)) | (scl == 11))   # Original statement used before Oct.07, 2025
    masked_xrDS = xrDS.where(((scl > 1) & (scl < 8)) | (scl > 9))      # New statement used after Oct.07, 2025 as this provide better results

    # Mask out shadow pixels using NDVI and NIR thresholding
    # red = xrDS['red']
    # nir = xrDS['nir08']
    # ndvi = (nir - red) / (nir + red + 1e-6)
    # shadow_mask = (ndvi > 0.3) & (nir < 10.0)
    # masked_xrDS = masked_xrDS.where(~shadow_mask, drop=False)
    
    return masked_xrDS
 
  elif ssr_code in [HLSS30_sensor, HLSL30_sensor, HLS_sensor]:
    mask = xrDS['Fmask'].astype(np.uint8) & 0b00001110     #Seems there is no need to mask out aerosols
    return xrDS.where(mask == 0)
  
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

  return xrItem
  
  


def attach_AngleBands_to_coll(xrDS, StacItems, isHLSData = True):
  '''Attaches three angle bands to a satallite SURFACE REFLECTANCE image
  Args:    
    xrDS(xr Dateset): A xarray dataset object (a single image);
    StacItems(List): A list of STAC items corresponding to the "xrDS".'''    

  if not isHLSData:  #For the image data from the STAC catalog hosted by AWS
    time_to_angles = {}
    for item in StacItems:
      time_to_angles[item.properties['datetime']] = {'cosVZA': np.cos(np.radians(item.properties['vza'])), 
                                                     'cosSZA': np.cos(np.radians(item.properties['sza'])),
                                                     'cosRAA': np.cos(np.radians(item.properties['saa'] - item.properties['vaa']))}

    time_values = xrDS.coords['time'].values
    
    x_dim = xrDS.dims['x']
    y_dim = xrDS.dims['y']

    keys   = [str(t)[:-3] + 'Z' for t in time_values]    
    cosSZAs = [time_to_angles[key]['cosSZA'] for key in keys]
    cosVZAs = [time_to_angles[key]['cosVZA'] for key in keys]
    cosRAAs = [time_to_angles[key]['cosRAA'] for key in keys]

    cosSZA_array = np.array([np.full((y_dim, x_dim), val) for val in cosSZAs]) 
    cosVZA_array = np.array([np.full((y_dim, x_dim), val) for val in cosVZAs]) 
    cosRAA_array = np.array([np.full((y_dim, x_dim), val) for val in cosRAAs]) 

    xrDS["cosSZA"] = xr.DataArray(cosSZA_array, dims=['time', 'y', 'x'], coords=xrDS.coords, name="cosSZA").astype(np.float32)
    xrDS["cosVZA"] = xr.DataArray(cosVZA_array, dims=['time', 'y', 'x'], coords=xrDS.coords, name="cosVZA").astype(np.float32)
    xrDS["cosRAA"] = xr.DataArray(cosRAA_array, dims=['time', 'y', 'x'], coords=xrDS.coords, name="cosRAA").astype(np.float32)

  else:  #For the HLS data from the STAC catalog hosted by LP DAAC 
    angle_scale = 0.01
    xrDS["cosSZA"] = np.cos(np.radians(xrDS['SZA']*angle_scale)).astype(np.float32)
    xrDS["cosVZA"] = np.cos(np.radians(xrDS['VZA']*angle_scale)).astype(np.float32)
    xrDS["cosRAA"] = np.cos(np.radians((xrDS['VAA'] - xrDS['VAA'])*angle_scale)).astype(np.float32)
    xrDS = xrDS.drop_vars(['SZA', 'VZA', 'VAA', 'SAA'])

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



#############################################################################################################
# Description: This function reads multiple GeoTIF images from a given local directory into one 
#              xarray.dataset and then return it.   
#  
# Revision history:  2025-Oct-27  Lixin Sun  Initial creation
#
#############################################################################################################
def load_TIF_files_to_xr(ImgPath:str, KeyStrings:list[str], MonthStr:str = ''):
  '''
    Args:
      ImgPath(string): A string containing the path to a given directory;
      KeyStrings(List): A list of key strings for selecting a subset of GeoTiff files (files must contain
                        one of the key strings in their filenames);
      MonthStr(string): An optional string for filtering monthly data.'''  
  
  if not os.path.isdir(ImgPath):
    print("<read_multiple_geotiffs> The given directory does not exist!")
    return None
  
  #================================================================================================  
  # Get all .tif files under the given directory
  #================================================================================================  
  all_files = glob.glob(os.path.join(ImgPath, "*.tif"))

  #================================================================================================  
  # Filter: keep only files that contain any of the key strings
  #================================================================================================  
  if len(MonthStr) > 0:
    all_files = [f for f in all_files if MonthStr in os.path.basename(f)]
    
  selected_files = [f for f in all_files if any(key in os.path.basename(f) for key in KeyStrings)]
  
  if not selected_files or len(selected_files) < 2:
    print("<read_multiple_geotiffs> No matching GeoTIFF files found.")
    return None
  
  #================================================================================================  
  # Sort selected files based on their order in 'KeyStrings'
  #================================================================================================  
  #selected_files.sort()
  sorted_files = [file for key in KeyStrings for file in selected_files if key in os.path.basename(file)]
  
  #================================================================================================   
  # Read the selected files into an xarray DataArray
  #================================================================================================  
  bands = [rioxarray.open_rasterio(f) for f in sorted_files]
  cube = xr.concat(bands, dim="band")
 
  #================================================================================================  
  # Assign band names based on the key strings
  #================================================================================================    
  cleaned_keys = [s.replace("_", "") for s in KeyStrings]
  cube         = cube.assign_coords(band = cleaned_keys[:len(sorted_files)])
  
  band_names = cube.coords['band'].values.tolist()
  print(band_names)

  return cube




#############################################################################################################
# Description: This function reads multiple GeoTIF images from a given local directory into a 3D Numpy array 
#              and then return it.   
#  
# Revision history:  2025-Non-20  Lixin Sun  Initial creation
#
#############################################################################################################
def load_TIF_files_to_npa(ImgPath, KeyStrings, MonthStr = '', NoVal = 0, SaveMaskPath=None):
  '''
    Args:
      ImgPath(string): A string containing the path to a given directory;
      KeyStrings(List): A list of key strings for selecting a subset of GeoTiff files (files must contain
                        one of the key strings in their filenames);
      MonthStr(string): An optional string for filtering monthly data;
      NoVal(float): The no-data value used in the GeoTiff files.'''  
  
  if not os.path.isdir(ImgPath):
    print("<read_multiple_geotiffs> The given directory does not exist!")
    return None
  
  #================================================================================================  
  # Get all .tif files under the given directory
  #================================================================================================  
  all_files = glob.glob(os.path.join(ImgPath, "*.tif"))

  #================================================================================================  
  # Filter: keep only files that contain any of the key strings
  #================================================================================================  
  if len(MonthStr) > 0:
    all_files = [f for f in all_files if MonthStr in os.path.basename(f)]
  
  if len(KeyStrings) > 0:
    selected_files = [f for f in all_files if any(k in f for k in KeyStrings)]
    #selected_files = [f for f in all_files if any(key in os.path.basename(f) for key in KeyStrings)]
  
  if not selected_files or len(selected_files) < 1:
    print("<read_multiple_geotiffs> No matching GeoTIFF files found.")
    return None
  
  #================================================================================================  
  # Sort selected files based on their order in 'KeyStrings'
  #================================================================================================  
  #selected_files.sort()
  sorted_files = [file for key in KeyStrings for file in selected_files if key in os.path.basename(file)]
  
  #================================================================================================   
  # Read the selected files into a list of 2D Numpy arrays
  #================================================================================================  
  bands = []
  profile = None
  for fp in sorted_files:
    with rasterio.open(fp) as src:
      bands.append(src.read(1).astype(np.float32))
      if profile is None:
        profile = src.profile  # keep metadata if needed later
  
  #================================================================================================
  # Stack band images into a multi-band image
  #================================================================================================
  image = np.stack(bands, axis=-1)    # (H, W, C)
  
  mask  = np.all(image != NoVal, axis=-1)
  
  # Convert boolean → uint8 (True=1, False=0)
  mask_uint8 = mask.astype(np.uint8)

  #================================================================================================
  # Optionally save mask GeoTIFF
  #================================================================================================
  if SaveMaskPath is not None:
    # Update profile for single band, uint8 type
    mask_profile = profile.copy()
    mask_profile.update(
        dtype=rasterio.uint8,
        count=1,
        nodata=None
    )

    with rasterio.open(SaveMaskPath, 'w', **mask_profile) as dst:
      dst.write(mask_uint8, 1)

  return image, mask_uint8, profile
 
  


#############################################################################################################
# Description: This function saves Numpy Array to local hard drive as a GeoTIFF file.
#  
# Revision history:  2025-Nov-24  Lixin Sun  Initial creation
#
#############################################################################################################
def save_npa_as_geotiff(outPath, npa, profile, DataType='int16'):
  '''Saves a multi-band numpy array as a GeoTIFF file.
  Args:
    outPath(string): The output file path;
    npa(np.ndarray): A 3D numpy array representing the image data (H, W, C);
    profile(dict): A dictionary containing the metadata for the GeoTIFF file.'''  
  
  # Handle 2D and 3D arrays
  if npa.ndim == 2:
    count = 1
    npa_to_write = [npa]  # wrap in list to iterate uniformly

  elif npa.ndim == 3:
    count = npa.shape[2]
    npa_to_write = [npa[:, :, i] for i in range(count)]

  else:
    raise ValueError("npa must be a 2D or 3D NumPy array.")
  
  # Update the profile to reflect the number of layers
  profile.update(count = count, dtype=DataType)    

  with rasterio.open(outPath, 'w', **profile) as dst:
    # for i in range(npa.shape[2]):
    #   dst.write(npa[:, :, i], i + 1)  # Bands are 1-indexed in rasterio 
    for i, band in enumerate(npa_to_write, start=1):  # bands are 1-indexed
      dst.write(band, i)




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




#############################################################################################################
# Description: This function applys the given 'gain' and 'offset' to selected bands/variables in 'inImg' and 
#              then returns the modified xarray.dataset object.
#  
# Revision history:  2024-Aug-13  Lixin Sun  Initial creation
#
#############################################################################################################
def rescale_spec_bands(inImg, selected_vars, gain, offset):
  '''
    Args:
      inImg():
  '''
  
  img_vars = inImg.data_vars
  if len(selected_vars) < 1:
    selected_vars = img_vars

  if set(selected_vars) <= set(img_vars):
    for var in selected_vars:
      inImg[var] = inImg[var]*gain + offset

  return inImg


#############################################################################################################
# Description: This function creates a land mask based on given band images.
#  
# Revision history:  2025-Dec-31  Lixin Sun  Initial creation
#
#############################################################################################################
def land_mask(blu, red, nir, sw1):
  '''
    Args:
      blu, red, nir, sw1: 2D Numpy arrays representing the blue, red, near-infrared, and shortwave-infrared band images.
  '''
  
  land_cond1 = (sw1 > 3.0) & (sw1 > red)                       # Casted shadow and boundary condition
  land_cond2 = (sw1 > 15.0)                                    # Bright land condition
  land_cond3 = ((nir > 15.0) & ((nir >= 2*blu) | (sw1 > red))) # regular land condition

  return (land_cond1 | land_cond2 | land_cond3)




# ImgPath    = "C:\\Work_Data\\S2_mosaic_vancouver2020_20m_for_testing_gap_filling"
# KeyStrings = ['blue_', 'green_', 'red_', 'edge1_', 'edge2_', 'edge3_', 'nir08_', 'swir16_', 'swir22_']
# load_TIF_files_to_npa(ImgPath, KeyStrings, MonthStr = 'Jul_')
#load_TIF_files_to_npa(ImgPath, KeyStrings, MonthStr = 'Aug_')



