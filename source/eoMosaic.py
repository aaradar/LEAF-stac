
import os
import math
from datetime import datetime, timedelta
import numpy as np
import pandas as pd

import xarray as xr
from pystac_client.client import Client

#from pystac_client.stac_api_io import StacApiIO
#stac_api_io = StacApiIO()
#stac_api_io.session.verify = "C:\\Users\\lsun\\nrcan_azure_amazon.cer"


import odc.stac
from dask.diagnostics import ProgressBar

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
ottawa_region = {'type': 'Polygon', 'coordinates': [[[-76.120,45.184], [-75.383,45.171], [-75.390,45.564], [-76.105,45.568], [-76.120,45.184]]]}

tile55_922 = {'type': 'Polygon', 'coordinates': [[[-77.6221, 47.5314], [-73.8758, 46.7329], [-75.0742, 44.2113], [-78.6303, 44.9569], [-77.6221, 47.5314]]]}






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
  #==================================================================================================  
  if ssr_code > eoIM.MAX_LS_CODE & ssr_code < eoIM.MOD_sensor:
    query_conds['catalog']    = "https://earth-search.aws.element84.com/v1"
    query_conds['collection'] = "sentinel-2-l2a"
    query_conds['timeframe']  = str(StartStr) + '/' + str(EndStr)
    query_conds['bands']      = ['blue', 'green', 'red', 'nir08', 'swir16', 'swir22', 'scl']
    query_conds['filters']    = {"s2:cloud_shadow_percentage": {"lt": 0.9} }

  elif ssr_code < eoIM.MAX_LS_CODE & ssr_code > 0:
    query_conds['catalog']    = "https://landsatlook.usgs.gov/stac-server"
    query_conds['collection'] = "landsat-c2l2-sr"
    query_conds['timeframe']  = str(StartStr) + '/' + str(EndStr)
    query_conds['bands']      = ['blue', 'green', 'red', 'nir08', 'swir16', 'swir22', 'qa_pixel']
    query_conds['filters']    = ["eo:cloud_cover<10"]

  return query_conds





#############################################################################################################
# Description: This function returns a base image.
#
# Revision history:  2024-May-24  Lixin Sun  Initial creation
# 
#############################################################################################################
def get_base_Image(SsrData, Region, ProjStr, Scale, StartStr, EndStr):
  # get all query conditions 
  query_conds = get_query_conditions(SsrData, StartStr, EndStr)

  # use publically available stac link such as
  #catalog = Client.from_file(query_conds['catalog'], stac_io = stac_api_io)
  catalog = Client.open(str(query_conds['catalog'])) 

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
  
  #print('<<<<<<<get_base_Image>>>>>>> info on an item = ', items[0].to_dict())
  #==================================================================================================
  # define a geobox for my region
  #==================================================================================================
  # lazily combine items
  mybbox = eoUs.get_region_bbox(Region)
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
  #with warn.catch_warnings():
  #  warn.simplefilter('ignore') # Avoids RuntimeWarning about All-NaN slices
  #with ddiag.ProgressBar():
  #     #with rio.Env(GDAL_HTTP_UNSAFESSL = 'YES') as env: # Avoids NRCan network blocking access
  #     # compute (returns python object fit in mem), persist (returns dask object), load (like compute but inplace)
  #    cube = cube.load()
  
  return ds_xr.isel(time=0)





#############################################################################################################
# Description: This function returns a collection of images from a specified catalog and collection based on
#              given spatial region, timeframe and filtering criteria. The returned image collection will be 
#              stored in a xarray.Dataset structure.
#
# Revision history:  2024-May-24  Lixin Sun  Initial creation
# 
#############################################################################################################
def get_STAC_ImColl(SsrData, Region, ProjStr, Scale, StartStr, EndStr, GroupBy=True):
  # get all query conditions 
  query_conds = get_query_conditions(SsrData, StartStr, EndStr)

  # use publically available stac link such as
  #catalog = Client.from_file(query_conds['catalog'], stac_io = stac_api_io)
  catalog = Client.open(str(query_conds['catalog'])) 

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
  
  #==================================================================================================
  # define a geobox for my region
  #==================================================================================================
  # lazily combine items
  mybbox = eoUs.get_region_bbox(Region)
  print('<get_STAC_ImColl> The bbox of the given region = ', mybbox)

  ds_xr = odc.stac.load(search_IC.items(),
                        bands  = query_conds['bands'],
                        groupby='solar_day',  #For lower latitude scenses, save a lot memories
                        chunks = {'x': 1000, 'y': 1000},
                        crs    = ProjStr, 
                        bbox   = mybbox,
                        resolution = Scale)

  return ds_xr  





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
  median_img = ready_IC.median(dim='time').astype(np.float32)
  print('\n<get_score_refers> median image = ', median_img)
  #==========================================================================================================
  # Extract separate bands from the median image, then calculate NDVI and modeled blue median band
  #==========================================================================================================
  blu = median_img.blue
  red = median_img.red
  nir = median_img.nir08
  sw2 = median_img.swir22
  
  NDVI      = (nir - red)/(nir + red + 0.0001)  
  #print('\n\nNDVI = ', NDVI)
  model_blu = sw2*0.25
  
  #==========================================================================================================
  # Correct the blue band values of median mosaic for the pixels with NDVI values larger than 0.3
  #========================================================================================================== 
  condition = (model_blu > blu) | (NDVI < 0.3) | (sw2 < blu)
  median_img['blue'] = median_img['blue'].where(condition, other = model_blu)
  #print('\n\nnew median image = ', median_img)

  return median_img




#############################################################################################################
# Description: This function attaches a score band to each image within a xarray Dataset.
#
# Note:        The given "masked_IC" may be either an image collection with time dimension or a single image
#              without time dimension
#
# Revision history:  2024-May-24  Lixin Sun  Initial creation
# 
#############################################################################################################
def attach_score(SsrData, ready_IC, StartStr, EndStr):
  '''Attaches a score band to each image within a xarray Dataset (similar to an image collection in GEE)
  '''
  #print('<attach_score> ready IC = ', ready_IC)
  #==========================================================================================================
  # Attach an empty layer (with all pixels equal to ZERO) to eath temporal item (an image here) in "ready_IC" 
  #==========================================================================================================
  zero_img = ready_IC[SsrData['BLU']]*0.0 
  ready_IC[eoIM.pix_score] = zero_img.astype(np.float32)
  
  #print('\n\n<attach_score> ready IC after adding empty pixel score = ', ready_IC)
  #print('\n\n<attach_score> all pixel score layers in ready_IC = ', ready_IC[eoIM.pix_score])
  #==========================================================================================================
  # Determine central Date of a compositing period and a median image of all spectral bands
  #==========================================================================================================
  midDate = datetime.strptime(eoUs.period_centre(StartStr, EndStr), "%Y-%m-%d")

  median     = get_score_refers(ready_IC)
  median_blu = median[SsrData['BLU']]
  median_nir = median[SsrData['NIR']]

  #==========================================================================================================
  # Modify the empty layer with time and spectral scores  
  #==========================================================================================================
  for i, time_value in enumerate(ready_IC.time.values):
    #print(f"<get_sub_mosaic> Index: {i}, Timestamp: {time_value}")    
    #--------------------------------------------------------------------------------------------------------
    # Record time score for each temporal item
    #--------------------------------------------------------------------------------------------------------
    timestamp = pd.Timestamp(time_value).to_pydatetime()
    time_sc   = time_score(timestamp, midDate, SsrData['SSR_CODE'])   
    
    #--------------------------------------------------------------------------------------------------------
    # Multiply with spectral score
    #--------------------------------------------------------------------------------------------------------
    img = ready_IC.isel(time=i) 
    
    spec_sc = spec_score(SsrData, img, median_blu, median_nir) 
    
    ready_IC[eoIM.pix_score][i, :,:] = spec_sc * time_sc 
    #ready_IC[eoIM.pix_score][i] = spec_sc * time_sc

  return ready_IC





######################################################################################################
# Description: This function creates a map with all the pixels having an identical time score for a
#              given image. Time score is calculated based on the date gap between the acquisition
#              date of the given image and a reference date (midDate parameter), which normally is
#              the middle date of a time period (e.g., a peak growing season).
#
# Revision history:  2024-May-31  Lixin Sun  Initial creation
#
######################################################################################################
def time_score(ImgDate, MidDate, SsrCode):
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
def spec_score(SsrData, inImg, median_blu, median_nir):
  '''Attaches a score band to each image within a xarray Dataset
     Args:
       inImg(xarray dataset): a given single image;
       medianImg(xarray dataset): a given median image.'''
  
  blu = inImg[SsrData['BLU']]
  grn = inImg[SsrData['GRN']]
  red = inImg[SsrData['RED']]
  nir = inImg[SsrData['NIR']]
  sw1 = inImg[SsrData['SW1']]
  sw2 = inImg[SsrData['SW2']]

  min_val = 0.01
  max_SV = xr.apply_ufunc(np.maximum, blu, grn)
  max_SV = max_SV.where(max_SV > min_val, min_val)

  max_SW = xr.apply_ufunc(np.maximum, sw1, sw2)
  max_SW = max_SW.where(max_SW > min_val, min_val)

  max_IR = xr.apply_ufunc(np.maximum, nir, max_SW)
  max_IR = max_IR.where(max_IR > min_val, min_val)
    
  #print('\n\n<attach_score> max_SV = ', max_SV)
  #print('\n\n<attach_score> max_SW = ', max_SW)
  #print('\n<attach_score> max_IR = ', max_IR)
  #==================================================================================================
  # Calculate scores assuming all the pixels are water
  #==================================================================================================
  water_score = max_SV/max_IR
  water_score = water_score.where(median_blu > blu, -1*water_score)
  #print('\n<attach_score> water_score = ', water_score)

  #==================================================================================================
  # Calculate scores assuming all the pixels are land
  #==================================================================================================
  blu_pen = xr.apply_ufunc(np.abs, blu - median_blu)
  nir_pen = xr.apply_ufunc(np.abs, nir - median_nir)
  STD_blu = xr.apply_ufunc(np.maximum, sw2*0.25, red*0.5+0.8) 
  STD_blu = xr.apply_ufunc(np.maximum, STD_blu, blu) 
    
  #print('\n\n\n<attach_score> blu_pen = ', blu_pen)
  #print('\n\n\n<attach_score> nir_pen = ', nir_pen)
  #print('\n\n\n<attach_score> STD_blu = ', STD_blu)

  land_score = nir/(STD_blu + blu_pen + nir_pen)  

  return land_score.where((max_SV < max_IR) | (max_SW > 3.0), water_score)
  




#############################################################################################################
# Description: This function returns a mosaic image for a sub-region
#
# Revision history:  2024-May-24  Lixin Sun  Initial creation
# 
#############################################################################################################
def get_sub_mosaic(SsrData, SubRegion, ProjStr, Scale, StartStr, EndStr):
  # get all query conditions 
  query_conds = get_query_conditions(SsrData, StartStr, EndStr)

  # use publically available stac link such as
  #catalog = Client.from_file(query_conds['catalog'], stac_io = stac_api_io)  
  catalog = Client.open(str(query_conds['catalog'])) 

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
  mybbox = eoUs.get_region_bbox(SubRegion)
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
  condition = (raw_IC > 0) & (scl != 3) & (scl != 8) & (scl != 9) & (scl != 1)
  masked_IC = raw_IC.where(condition)

  #==================================================================================================
  # Apply gain and offset to each band in a xarray dataset
  #==================================================================================================
  ready_IC = eoIM.apply_gain_offset(masked_IC, SsrData, 100, False)
  print('\nFinished applying gain and offset\n')
  #==================================================================================================
  # Note: calling "fillna" function before invaking "argmax" function is very important!!!
  #==================================================================================================
  scored_IC   = attach_score(SsrData, ready_IC, StartStr, EndStr).fillna(-0.0001)
  max_indices = scored_IC[eoIM.pix_score].argmax(dim='time')
  sub_mosaic  = scored_IC.isel(time=max_indices)

  '''
  nImgs     = scored_IC.sizes['time']

  sub_mosaic = scored_IC.isel(time=0)
  #sub_img2   = scored_IC.isel(time=1)

  #return sub_mosaic, sub_img2
  for i in range(1, nImgs):
    new_img    = scored_IC.isel(time=i)
    sub_mosaic, new_img = xr.align(sub_mosaic, new_img, join='outer')

    merged = sub_mosaic.where(sub_mosaic.score > new_img.score, sub_mosaic, new_img)
    sub_mosaic = merged
  '''

  return sub_mosaic



#############################################################################################################
# Description: This function returns a mosaic image for a sub-region
#
# Revision history:  2024-May-24  Lixin Sun  Initial creation
# 
#############################################################################################################
def period_mosaic(inParams):
  #==========================================================================================================
  # Obtain required parameters
  #==========================================================================================================
  params  = eoPM.get_mosaic_params(inParams)

  SsrData = eoIM.SSR_META_DICT[str(params['sensor'])]
  ProjStr = str(params['projection'])  
  Scale   = int(params['resolution'])

  Region  = eoPM.get_spatial_region(params)
  StartStr, EndStr = eoPM.get_time_window(params)  

  #==========================================================================================================
  # Create a base image that has full spatial dimensions of the specified region
  #==========================================================================================================
  base_img = get_base_Image(SsrData, Region, ProjStr, Scale, StartStr, EndStr)*0.0
  
  base_img[eoIM.pix_score] = base_img[SsrData['BLU']]
  #base_img = attach_score(SsrData, base_img, StartStr, EndStr)*0.0
  base_img = base_img.where(base_img > 0)
  
  #==========================================================================================================
  # Create individual sub-mosaic and combine it into base image based on score
  #==========================================================================================================
  sub_regions = eoUs.divide_region(Region, 3)

  for sub_region in sub_regions:
    print('<period_mosaic> create a sub-mosaic for ', sub_region)
    sub_polygon = {'type': 'Polygon',  'coordinates': [sub_region] }

    sub_mosaic = get_sub_mosaic(SsrData, sub_polygon, ProjStr, Scale, StartStr, EndStr)
    
    #sub_mosaic = attach_score(sub_mosaic)
    sub_mosaic = sub_mosaic.where(sub_mosaic > 0)    

    #base_img = xr.merge([base_img, sub_mosaic], compat='override') # Didn't work
    base_img = base_img.combine_first(sub_mosaic)
  
  #==========================================================================================================
  # 
  #==========================================================================================================
  #base_img = base_img.rio.write_crs(ProjStr, inplace=True)

  return base_img
  



#############################################################################################################
# Description: This function exports the band images in a mosaic to separate GeoTiff files
#
# Revision history:  2024-May-24  Lixin Sun  Initial creation
# 
#############################################################################################################
def export_mosaic(inParams, inMosaic):
  #==========================================================================================================
  # Get all the parameters for exporting composite images
  #==========================================================================================================
  params = eoPM.get_mosaic_params(inParams)
  export_style = str(params['export_style']).lower()

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
  spa_scale  = params['spatial_scale']
  
  if 'sepa' in export_style:
    for band in rio_mosaic.data_vars:
      out_img  = rio_mosaic[band]
      filename = f"{filePrefix}_{band}_{spa_scale}m.tif"
      output_path = os.path.join(dir_path, filename)
      out_img.rio.to_raster(output_path)
  else:
    filename = f"{filePrefix}_mosaic_{spa_scale}m.tif"
    output_path = os.path.join(dir_path, filename)
    rio_mosaic.to_netcdf(output_path)




'''
params = {
    'sensor': 'S2_SR',           # A sensor type string (e.g., 'S2_SR' or 'L8_SR' or 'MOD_SR')
    'unit': 2,                   # A data unit code (1 or 2 for TOA or surface reflectance)    
    'year': 2022,                # An integer representing image acquisition year
    'nbYears': -1,               # positive int for annual product, or negative int for monthly product
    'months': [8],               # A list of integers represening one or multiple monthes     
    'tile_name': 'tile42_411',   # A list of (sub-)tile names (defined using CCRS' tile griding system) 
    'prod_names': ['mosaic'],    #['mosaic', 'LAI', 'fCOVER', ]    
    'resolution': 200,            # Exporting spatial resolution    
    'folder': '',                # the folder name for exporting
    'buff_radius': 10, 
    'tile_scale': 4,
    'CloudScore': True,

    #'start_date': '2022-06-15',
    #'end_date': '2023-09-15'
}

period_mosaic(params)
'''



'''
#!pip install geemap
import os
import sys

#Get the absolute path to the parent of current working directory 
cwd    = os.getcwd()
source_path = os.path.join(cwd, 'source')
sys.path.append(source_path)
sys.path


import eoImage as eoIM

sub_region = {
    'type': 'Polygon',
    'coordinates': [
       [[-78.6303, 44.2113],         
        [-77.0455, 44.2113], 
        [-77.0455, 45.318], 
        [-78.6303, 45.318], 
        [-78.6303, 44.2113]]
    ]
}


start_str    = '2019-07-01'
end_str      = '2019-07-31'
scale        = 100
ssr_data     = eoIM.SSR_META_DICT['S2_SR']

    
#mosaic = period_mosaic(ssr_data, sub_region, 'EPSG:3979', scale, start_str, end_str)
mosaic = get_sub_mosaic(ssr_data, sub_region, 'EPSG:3979', scale, start_str, end_str)
mosaic.to_netcdf('C:\\Work_documents\\stac_mosaic.nc')
#export_mosaic(mosaic, 'C:\\Work_documents\\stac_mosaic', scale, 'EPSG:3979')
'''


'''
import xarray as xr
import numpy as np

# Define the dimensions
time_dim = 3
spatial_dim_x = 2
spatial_dim_y = 2
bands = ["blue", "green", "red", "nir08", "swir16", "swir22"]

# Create the coordinates
times = np.arange(time_dim)
x = np.arange(spatial_dim_x)
y = np.arange(spatial_dim_y)

# Initialize data for each band with random values
data = {
    band: (("time", "x", "y"), np.random.randint(0, 100, size=(time_dim, spatial_dim_x, spatial_dim_y)))
    for band in bands
}

# Create the xarray dataset
dataset = xr.Dataset(
    data,
    coords={
        "time": times,
        "x": x,
        "y": y,
    }
)

start_str = '2019-07-01'
end_str   = '2019-07-31'
ssr_data  = eoIM.SSR_META_DICT['S2_SR']

print(dataset)
#get_score_refers(dataset)
score = attach_score(ssr_data, dataset, start_str, end_str)
print('\n\n\nscore = ', score)
print('\nend testing!!!')
'''