
import os
import math
import time
from datetime import datetime
import numpy as np
import pandas as pd

import xarray as xr
import pystac_client as psc
import odc.stac
import dask.diagnostics as ddiag
import dask
from joblib import Parallel
from joblib import delayed

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
# Description: This function returns the number of sub mosaics in all spatial dimension (X and Y)
#
# Revision history:  2024-Jul-02  Lixin Sun  Initial creation
# 
#############################################################################################################
def get_sub_numb(xrDS, sub_size):
  x_dim = xrDS.sizes['x']
  y_dim = xrDS.sizes['y']

  max_dim = max(x_dim, y_dim)

  nSub = int((max_dim/sub_size) + 0.5)

  return nSub if nSub > 1 else 2




#############################################################################################################
# Description: This function returns a list of unique tile names contained in a given "StatcItems" list.
#
# Revision history:  2024-Jul-17  Lixin Sun  Initial creation
# 
#############################################################################################################
def get_unique_tile_names(StacItems):
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
# Description: This function returns a list of item names corresponding to a specified MGRS/Snetinel-2 tile.
#
# Revision history:  2024-Jul-17  Lixin Sun  Initial creation
# 
#############################################################################################################
def get_one_tile_items(StacItems, TileName):
  stac_items = list(StacItems)
  tile_items = []

  if len(stac_items) < 2:
    return tile_items  
  
  for item in stac_items:
    if TileName == item.properties['grid:code']:
      tile_items.append(item)

  return tile_items 




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
    query_conds['bands']      = ['blue', 'green', 'red', 'rededge1', 'rededge2', 'rededge3', 'nir08', 'swir16', 'swir22', 'scl']
    query_conds['filters']    = {"eo:cloud_cover": {"lt": 80.0} }    

  elif ssr_code < eoIM.MAX_LS_CODE and ssr_code > 0:
    #query_conds['catalog']    = "https://landsatlook.usgs.gov/stac-server"
    #query_conds['collection'] = "landsat-c2l2-sr"
    query_conds['catalog']    = "https://earth-search.aws.element84.com/v1"
    query_conds['collection'] = "landsat-c2-l2"
    query_conds['timeframe']  = str(StartStr) + '/' + str(EndStr)
    #query_conds['bands']      = ['OLI_B2', 'OLI_B3', 'OLI_B4', 'OLI_B5', 'OLI_B6', 'OLI_B7', 'qa_pixel']
    query_conds['bands']      = ['blue', 'green', 'red', 'nir08', 'swir16', 'swir22', 'qa_pixel']
    query_conds['filters']    = {"eo:cloud_cover": {"lt": 80.0}}  
  elif ssr_code == eoIM.HLS_sensor:
    query_conds['catalog']    = "https://cmr.earthdata.nasa.gov/stac/LPCLOUD"
    query_conds['collection'] = "HLSL30.v2.0"
    query_conds['timeframe']  = str(StartStr) + '/' + str(EndStr)
    query_conds['bands']      = ['blue', 'green', 'red', 'nir08', 'swir16', 'swir22', 'qa_pixel']
    query_conds['filters']    = {"eo:cloud_cover": {"lt": 80.0}}  

  return query_conds




#############################################################################################################
# Description: This function reads and returns a geometry angle database from a local .CSV file, which was
#              created using GEE (in ImgSet.py)
#
# Revision history:  2024-Jul-15  Lixin Sun  Initial creation
# 
#############################################################################################################
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


#############################################################################################################
# Description: This function returns the results of searching a STAC catalog
#
# Revision history:  2024-May-24  Lixin Sun  Initial creation
#                    2024-Jul-12  Lixin Sun  Added a filter to retain only one image from the items with
#                                            identical timestamps.
#
#############################################################################################################
def search_STAC_Catalog(Region, Criteria, MaxImgs, based_on_region = True):
  '''
    Args:      
  '''
  # use publically available stac 
  catalog = psc.client.Client.open(str(Criteria['catalog'])) 

  #==================================================================================================
  # Search and filter a image collection
  #================================================================================================== 
  if based_on_region == True:
    print('<search_STAC_Images> The given region = ', Region)
    stac_catalog = catalog.search(collections = [str(Criteria['collection'])], 
                                  intersects  = Region,                           
                                  datetime    = str(Criteria['timeframe']), 
                                  query       = Criteria['filters'],
                                  limit       = MaxImgs)
        
  else:
    Bbox = eoUs.get_region_bbox(Region)
    print('<search_STAC_Images> The bbox of the given region = ', Bbox)

    stac_catalog = catalog.search(collections = [str(Criteria['collection'])], 
                                   bbox        = Bbox,                           
                                   datetime    = str(Criteria['timeframe']), 
                                   query       = Criteria['filters'],
                                   limit       = MaxImgs)
  
  stac_items = list(stac_catalog.items())
    
  return stac_items
    



#############################################################################################################
# Description: This function returns a list of IDs for the scenes to be obtained from a STAC catalog
#
# Revision history:  2024-Jul-09  Lixin Sun  Initial creation
# 
#############################################################################################################
def extract_scene_IDs(STACItems, SsrData):    
  scene_IDs   = {}
  series_code = 1
  for item in STACItems:
    scene_IDs[str(item.id)] = [series_code, item.datetime]
    print(f"{str(item.id)}: {series_code} {item.datetime}")
    series_code += 1    

  #print("<extract_scene_IDs> All scene IDs = ", scene_IDs)
  return scene_IDs




#############################################################################################################
# Description: This function returns a list of IDs for the scenes to be obtained from a STAC catalog
#
# Revision history:  2024-Jul-09  Lixin Sun  Initial creation
# 
#############################################################################################################
def get_sub_scene_IDs(AllSceneIDs, SubItems, SsrData):
  '''
    Args:
      AllSceneIDs(dictionary): A dictionary containing IDs and their series numbers for the scenes covering an entire ROI;
      SubItems(list): A list of scene items searched for a sub-ROI;
      SsrData(dictionary): A dictionary containing metadata for a specific sensor.'''

  allIDs = AllSceneIDs.keys()
  
  print('\n\n<<<<<<<<<<<<<<<<<<<<<<<< Meta info for sub mosaic >>>>>>>>>>>>>>>>>>>>>\n')
  sub_scene_IDs = {}
  for sub_i in SubItems:
    sub_ID = str(sub_i.id)
    for ID in allIDs:
      if sub_ID == ID:
        sub_scene_IDs[sub_ID] = AllSceneIDs[ID]
        print(f"{sub_ID}: {AllSceneIDs[ID][0]} and {AllSceneIDs[ID][1]}")

  return sub_scene_IDs




#############################################################################################################
# Description: This function returns a base image that covers the entire spatial region od an interested area.
#
# Revision history:  2024-May-24  Lixin Sun  Initial creation
#                    2024-Jul-17  Lixin Sun  Modified so that only unique and filtered STAC items will be
#                                            returned 
#############################################################################################################
def get_base_Image(SsrData, Region, ProjStr, Scale, StartStr, EndStr, AngleDB = None):
  '''
  '''
  start_time = time.time()

  #==========================================================================================================
  # Obtain query criteria, and then search a specified STAC catalog based on the criteria
  # Note: The third parameter (MaxImgs) for "search_STAC_Catalog" function cannot be too large. Otherwise,
  #       a server internal error will be triggered.
  #==========================================================================================================
  criteria   = get_query_conditions(SsrData, StartStr, EndStr)
  stac_items = search_STAC_Catalog(Region, criteria, 100, True)

  print(f"<get_base_Image> A total of {len(stac_items):d} items were found.")
  display_meta_assets(stac_items)
  
  #==========================================================================================================
  # Retain only one image from the items with identical timestamps
  #==========================================================================================================
  # Create a dictionary to store items by their timestamp
  items_by_id = defaultdict(list)

  # Create a new dictionary with the core image ID as keys
  for item in stac_items:    
    tokens = str(item.id).split('_')   #core image ID
    id = tokens[0] + '_' + tokens[1] + '_' + tokens[2]
    items_by_id[id].append(item)
  
  # Iterate through the items and retain only one item per timestamp
  unique_items = []
  for id, item_group in items_by_id.items():
    # Assuming we keep the first item in each group
    unique_items.append(item_group[0])

  #==========================================================================================================
  # Filter 'unique_items' based on 'AngleDB' if it is provided
  #==========================================================================================================
  AngleDB = pd.DataFrame(AngleDB)  
  if AngleDB.empty == False:
    filtered_items = []
    id_name = AngleDB.columns[0]
    DB_keys = AngleDB[id_name].values
    for item in unique_items:
      tokens   = str(item.id).split('_')   #core image ID
      scene_id = tokens[0] + '_' + tokens[1] + '_' + tokens[2] + '_' + tokens[4]

      if scene_id in DB_keys: #AngleDB[id_name].values:
        DB_index = AngleDB.loc[AngleDB[id_name] == scene_id].index
        row_dict = AngleDB.loc[DB_index].to_dict()
        sub_key = list(row_dict['sza'].keys())[0]
        #print(f'{sub_key}')

        item.properties['sza'] = row_dict['sza'][sub_key]
        item.properties['saa'] = row_dict['saa'][sub_key]
        item.properties['vza'] = row_dict['vza'][sub_key]
        item.properties['vaa'] = row_dict['vaa'][sub_key]
      
        filtered_items.append(item)
        #print(item.Datetime)

  else:
    filtered_items = unique_items

  #==========================================================================================================
  # Load the first image based on the boundary box of ROI
  #==========================================================================================================
  Bbox = eoUs.get_region_bbox(Region)
  print('<get_base_Image> The bbox of the given region = ', Bbox)

  ds_xr = odc.stac.load([filtered_items[0]],
                        bands  = criteria['bands'],
                        chunks = {'x': 1000, 'y': 1000},
                        crs    = ProjStr, 
                        bbox   = Bbox,
                        fail_on_error = False,
                        resolution = Scale)
  
  # actually load data into memory
  with ddiag.ProgressBar():
    ds_xr.load()

  out_xrDS = ds_xr.isel(time=0)
  stop_time = time.time() 
  
  return out_xrDS.astype(np.int16), filtered_items, (stop_time - start_time)/60





#############################################################################################################
# Description: This function returns a collection of images from a specified catalog and collection based on
#              given spatial region, timeframe and filtering criteria. The returned image collection will be 
#              stored in a xarray.Dataset structure.
#
# Revision history:  2024-May-24  Lixin Sun  Initial creation
# 
#############################################################################################################
def get_STAC_ImColl(SsrData, Region, ProjStr, Scale, StartStr, EndStr, based_on_region = True):
  # get all query conditions 
  query_conds = get_query_conditions(SsrData, StartStr, EndStr)

  # use publically available stac link such as
  #catalog = psc.client.Client.from_file(query_conds['catalog'], stac_io = stac_api_io)
  catalog = psc.client.Client.open(str(query_conds['catalog'])) 
  
  #==================================================================================================
  # Search and filter a image collection
  #================================================================================================== 
  Bbox = eoUs.get_region_bbox(Region)
  print('<search_STAC_Images> The bbox of the given region = ', Bbox)
  
  if based_on_region == True:
    print('<search_STAC_Images> The given region = ', Region)
    searched_IC = catalog.search(collections = [str(query_conds['collection'])], 
                               intersects  = Region,                           
                               datetime    = str(query_conds['timeframe']), 
                               query       = query_conds['filters'],
                               limit       = 5000)
        
  else:
    searched_IC = catalog.search(collections = [str(query_conds['collection'])], 
                               bbox        = Bbox,                           
                               datetime    = str(query_conds['timeframe']), 
                               limit       = 5000)
       
  
  items = list(searched_IC.items())
  print(f"Found: {len(items):d} datasets")
  
  #==================================================================================================
  # 
  #==================================================================================================
  ds_xr = odc.stac.load(searched_IC.items(),
                        bands  = query_conds['bands'],
                        groupby='solar_day',  #For lower latitude scenses, save a lot memories
                        chunks = {'x': 1000, 'y': 1000},
                        crs    = ProjStr, 
                        #bbox   = mybbox,
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
# Description: This function attaches a score band to each image in a xarray Dataset.
#
# Note:        The given "masked_IC" may be either an image collection with time dimension or a single image
#              without time dimension
#
# Revision history:  2024-May-24  Lixin Sun  Initial creation
# 
#############################################################################################################
def attach_score(SsrData, ready_IC, StartStr, EndStr, ExtraBandCode):
  '''Attaches a score band to each image in a xarray Dataset, which is equivalent to an image collection in GEE
  '''
  #print('<attach_score> ready IC = ', ready_IC)
  #print('\n\n<attach_score> ready IC after adding empty pixel score = ', ready_IC)
  #print('\n\n<attach_score> all pixel score layers in ready_IC = ', ready_IC[eoIM.pix_score])
  #==========================================================================================================
  # Determine central Date of a compositing period and a median image of all spectral bands
  #==========================================================================================================
  start = time.time()

  midDate = datetime.strptime(eoUs.period_centre(StartStr, EndStr), "%Y-%m-%d")

  median     = get_score_refers(ready_IC)
  median_blu = median[SsrData['BLU']]
  median_nir = median[SsrData['NIR']]
  
  def score_one_img(i, time):
    timestamp  = pd.Timestamp(time).to_pydatetime()
    time_score = get_time_score(timestamp, midDate, SsrData['SSR_CODE'])   
    
    img = ready_IC.isel(time=i)
    spec_score = get_spec_score(SsrData, img, median_blu, median_nir)     
    ready_IC[eoIM.pix_score][i, :,:] = spec_score * time_score 
   
  Parallel(n_jobs=-1, require='sharedmem')(delayed(score_one_img)(i, time) for i, time in enumerate(ready_IC.time.values))  
  #Parallel(n_jobs=-1)(delayed(score_one_img)(i, time) for i, time in enumerate(ready_IC.time.values))  

  stop = time.time() 

  return ready_IC, (stop - start)/60.0

  #==========================================================================================================
  # Modify the empty layer with time and spectral scores  
  #==========================================================================================================
  '''
  for i, time_value in enumerate(ready_IC.time.values):
    #print(f"<get_sub_mosaic> Index: {i}, Timestamp: {time_value}")    
    #--------------------------------------------------------------------------------------------------------
    # Record time score for each temporal item
    #--------------------------------------------------------------------------------------------------------
    timestamp  = pd.Timestamp(time_value).to_pydatetime()
    time_score = get_time_score(timestamp, midDate, SsrData['SSR_CODE'])   
    
    #--------------------------------------------------------------------------------------------------------
    # Multiply with spectral score
    #--------------------------------------------------------------------------------------------------------
    img = ready_IC.isel(time=i)
    #img = eoIM.apply_default_mask(img, SsrData)

    spec_score = get_spec_score(SsrData, img, median_blu, median_nir) 
    
    ready_IC[eoIM.pix_score][i, :,:] = spec_score * time_score 
    #ready_IC[eoIM.pix_score][i] = spec_sc * time_sc

    #if ExtraBandCode == eoIM.EXTRA_ANGLE:       
    #  ready_IC['cosSZA'][i, :,:] = 
  '''







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
# Description: This function returns a mosaic image for a sub-region
#
# Revision history:  2024-May-24  Lixin Sun  Initial creation
#                    2024-Jul-05  Lixin Sun  Added imaging angle bands to each item/image 
#
#############################################################################################################
def get_sub_mosaic(SsrData, SubRegion, ProjStr, Scale, StartStr, EndStr, ExtraBandCode):
  '''
     Args:
       SsrData(Dictionary): Some meta data on a used satellite sensor;
       SubRegion: The polygon for defining ROI;
       ProjStr(String): A string representing the projection of the resultant composite image;
       Scale(float): The spatial resolution of the resultant composite image;
       StartStr(string): A string representing the start date of compositing time window;
       EndStr(string): A string representing the end date of compositing time window; 
       AngleDB(pd.DataFrame): A dataframe containing imaging angle of all candidate image scenes;
       ExtraBandCode(Int): An integer indicating if to attach extra bands to mosaic image.'''
  
  start_time = time.time()

  #==========================================================================================================
  # Obtain query conditions, and then search a specified STAC catalog
  #========================================================================================================== 
  criteria   = get_query_conditions(SsrData, StartStr, EndStr)
  stac_items = search_STAC_Catalog(SubRegion, criteria, 500, False)
   
  nb_items = len(stac_items) 
  print(f"<get_sub_mosaic> Found: {nb_items} images for submosaic.")

  print("\n<get_sub_mosaic> Timestamps in stac_items:")  
  for item in stac_items:
    print(f'{item.datetime}, {item.properties['grid:code']}')

  if nb_items < 2:
    return None
  
  #==========================================================================================================
  # 
  #==========================================================================================================
  xrDS = odc.stac.load(stac_items,
                       bands  = criteria['bands'],
                       #groupby='solar_day',  #For lower latitude scenses, save a lot memories
                       chunks = {'x': 1000, 'y': 1000},
                       crs    = ProjStr, 
                       #bbox   = mybbox,
                       resolution = Scale)

  #==========================================================================================================
  # Actually load all data from a lazy-loaded dataset into in-memory Numpy arrays
  #==========================================================================================================
  with ddiag.ProgressBar():
    xrDS.load()
  
  print('\n<get_sub_mosaic> loaded xarray dataset:\n', xrDS) 

  time_values = xrDS.coords['time'].values
  print("\n<get_sub_mosaic> Time Dimension Values:")
  for t in time_values:
    print(t)

  #==========================================================================================================
  # Attach an empty layer (with all pixels equal to ZERO) to eath temporal item (an image here) in "xrDS" 
  #==========================================================================================================
  xrDS[eoIM.pix_score] = xrDS[SsrData['BLU']]*0
  
  xrDS['time'] = pd.to_datetime(xrDS['time'].values)
  xrDS[eoIM.pix_date] = xr.DataArray(xrDS['time'].dt.dayofyear, dims=['time'])
  xrDS['time_index']  = xr.DataArray(range(0, len(time_values)), dims=['time'])

  '''
  def attach_S2_angle_bands():
    vza = Image.getNumber(SsrData['VZA'])
    sza = Image.getNumber(SsrData['SZA'])    

    vza_rad = ee.Image.constant(vza).multiply(rad)
    sza_rad = ee.Image.constant(sza).multiply(rad)

    raa     = Image.getNumber(SsrData['SAA']).subtract(Image.getNumber(SsrData['VAA']))
    raa_rad = ee.Image.constant(raa.multiply(rad))
    
    return (Image.addBands(vza_rad.cos().rename(['cosVZA'])) \
                 .addBands(sza_rad.cos().rename(['cosSZA'])) \
                 .addBands(raa_rad.cos().rename(['cosRAA'])))  
  '''
  #==========================================================================================================
  # Apply default pixel mask to each of the images
  #==========================================================================================================
  xrDS, mask_time = eoIM.apply_default_mask(xrDS, SsrData)

  print('\n<get_sub_mosaic> Complete applying default mask, elapsed time = %6.2f minutes'%(mask_time))  

  #==========================================================================================================
  # Apply gain and offset to each band in a xarray dataset
  #==========================================================================================================  
  xrDS, rescale_time = eoIM.apply_gain_offset(xrDS, SsrData, 100, False)
  
  print('<get_sub_mosaic> Complete applying gain and offset, elapsed time = %6.2f minutes'%(rescale_time))

  #==========================================================================================================
  # Note: calling "fillna" function before invaking "argmax" function is very important!!!
  #==========================================================================================================
  xrDS, score_time = attach_score(SsrData, xrDS, StartStr, EndStr, eoIM.EXTRA_ANGLE)

  print('<get_sub_mosaic> Complete pixel scoring, elapsed time = %6.2f minutes'%(score_time)) 

  #================================================================================================
  # Attach an additional bands as necessary to each image in the image collection
  #================================================================================================
  '''
  extra_code = int(ExtraBandCode)
  if extra_code == eoIM.EXTRA_ANGLE:
    xrDS = eoIM.attach_AngleBands(xrDS, SsrData)
  elif extra_code == eoIM.EXTRA_NDVI:
    xrDS = eoIM.attach_NDVIBand(xrDS, SsrData)
  '''

  xrDS = xrDS.fillna(-0.0001)
  max_indices = xrDS[eoIM.pix_score].argmax(dim='time')
  sub_mosaic  = xrDS.isel(time=max_indices)

  print('\n\n<get_sub_mosaic> sub mosaic =', sub_mosaic)

  return sub_mosaic    #.compute()





#############################################################################################################
# Description: This function returns a mosaic image for a sub-region
#
# Revision history:  2024-May-24  Lixin Sun  Initial creation
#                    2024-Jul-05  Lixin Sun  Added imaging angle bands to each item/image 
#
#############################################################################################################
def get_tile_submosaic(SsrData, TileItems, StartStr, EndStr, Bands, ProjStr, Scale, ExtraBandCode):
  '''
     Args:
       SsrData(Dictionary): Some meta data on a used satellite sensor;
       TileItems(List): A list of STAC items associated with a specific tile;
       ExtraBandCode(Int): An integer indicating if to attach extra bands to mosaic image.'''
  
  start_time = time.time()
  #==========================================================================================================
  # 
  #==========================================================================================================
  xrDS = odc.stac.load(TileItems,
                       bands  = Bands,
                       chunks = {'x': 1000, 'y': 1000},
                       crs    = ProjStr, 
                       resolution = Scale)

  #==========================================================================================================
  # Actually load all data from a lazy-loaded dataset into in-memory Numpy arrays
  #==========================================================================================================
  with ddiag.ProgressBar():
    xrDS.load()
  
  print('\n<get_sub_mosaic> loaded xarray dataset:\n', xrDS) 

  time_values = xrDS.coords['time'].values
  print("\n<get_sub_mosaic> Time Dimension Values:")
  for t in time_values:
    print(t)

  #==========================================================================================================
  # Attach an empty layer (with all pixels equal to ZERO) to eath temporal item (an image here) in "xrDS" 
  #==========================================================================================================
  xrDS[eoIM.pix_score] = xrDS[SsrData['BLU']]*0
  
  #xrDS['time'] = pd.to_datetime(xrDS['time'].values)
  time_datetime = pd.to_datetime(time_values)
  doys = [date.timetuple().tm_yday for date in time_datetime]
  xrDS[eoIM.pix_date] = xr.DataArray(np.array(doys, dtype='uint16'), dims=['time'])
  xrDS['time_index']  = xr.DataArray(np.array(range(0, len(time_values)), dtype='uint8'), dims=['time'])
  
  #==========================================================================================================
  # Apply default pixel mask to each of the images
  #==========================================================================================================
  xrDS, mask_time = eoIM.apply_default_mask(xrDS, SsrData)

  print('\n<get_sub_mosaic> Complete applying default mask, elapsed time = %6.2f minutes'%(mask_time))  

  #==========================================================================================================
  # Apply gain and offset to each band in a xarray dataset
  #==========================================================================================================  
  xrDS, rescale_time = eoIM.apply_gain_offset(xrDS, SsrData, 100, False)
  
  print('<get_sub_mosaic> Complete applying gain and offset, elapsed time = %6.2f minutes'%(rescale_time))

  #==========================================================================================================
  # Calculate compositing scores for every valid pixel in xarray dataset object 
  #==========================================================================================================
  xrDS, score_time = attach_score(SsrData, xrDS, StartStr, EndStr, eoIM.EXTRA_ANGLE)

  print('<get_sub_mosaic> Complete pixel scoring, elapsed time = %6.2f minutes'%(score_time)) 

  #================================================================================================
  # Create a composite image based on compositing scores
  # Note: calling "fillna" function before invaking "argmax" function is very important!!!
  #================================================================================================
  xrDS = xrDS.fillna(-0.0001)
  max_indices = xrDS[eoIM.pix_score].argmax(dim='time')
  sub_mosaic  = xrDS.isel(time=max_indices)  
  
  #================================================================================================
  # Attach an additional bands as necessary 
  #================================================================================================  
  extra_code = int(ExtraBandCode)
  if extra_code == eoIM.EXTRA_ANGLE:
    sub_mosaic = eoIM.attach_AngleBands(sub_mosaic, TileItems)  
  #elif extra_code == eoIM.EXTRA_NDVI:
  #  xrDS = eoIM.attach_NDVIBand(xrDS, SsrData)
  
  #================================================================================================
  # Remove 'time_index' and 'score' variables from submosaic 
  #================================================================================================  
  sub_mosaic = sub_mosaic.drop_vars("time_index")
  sub_mosaic = sub_mosaic.drop_vars("score")

  return sub_mosaic






#############################################################################################################
# Description: This function returns a mosaic using the image acquired during 
# 
# Note: If "DB_fullpath" parameter is provided, then it indicates that imaging angles need to be included in
#       composite image.
#
# Revision history:  2024-May-24  Lixin Sun  Initial creation
#                    2024-Jul-15  Lixin Sun  Added the 2nd input parameter (DB_fullpath), which indicates
#                                            if imaging angles need to be included in composite image.
#############################################################################################################
def period_mosaic(inParams, DB_fullpath = ''):
  '''
    Args:
      inParams(dictionary): A dictionary containing all required parameters;
      DB_fullpath(String): A full path to a geometry angle database file in .CSV format.'''
  
  mosaic_start = time.time()
  #==========================================================================================================
  # Prepare required parameters
  #==========================================================================================================
  params = eoPM.get_mosaic_params(inParams)
  
  if params == None:
    print('<period_mosaic> Cannot create a mosaic image due to invalid input parameter!')
    return None
  
  SsrData = eoIM.SSR_META_DICT[str(params['sensor']).upper()]
  ProjStr = str(params['projection'])  
  Scale   = int(params['resolution'])

  Region  = eoPM.get_spatial_region(params)
  StartStr, EndStr = eoPM.get_time_window(params)  

  #==========================================================================================================
  # Create a base image that has full dimensions covering a specified spatial region
  # From this base image, the spatial dimensions can be obtained.
  #==========================================================================================================
  base_img, used_time = get_base_Image(SsrData, Region, ProjStr, Scale, StartStr, EndStr)
  
  print('\n<<<<<<<<<< Complete generating base image, elapsed time = %6.2f minutes>>>>>>>>>'%(used_time))  
  
  #==========================================================================================================
  # Determine if the final mosaic image is generated as a whole directly or by merging a number of submosaics
  #==========================================================================================================
  nSub = get_sub_numb(base_img, 1000)
  print('<period_mosaic> The number of sub mosaics = ', nSub)
  extra_bands = eoIM.EXTRA_NONE

  if nSub == 1:
    # The final mosaic is generated as a whole directly 
    # This option does not work!!! will get error message "APIError: {"message": "Internal server error"}"
    mosaic = get_sub_mosaic(SsrData, Region, ProjStr, Scale, StartStr, EndStr)
  else:
    #--------------------------------------------------------------------------------------------------------
    # Detect the existance of an imaging angle database
    #--------------------------------------------------------------------------------------------------------
    angle_DB = pd.DataFrame(read_angleDB(DB_fullpath))
    if angle_DB.empty == False:      
      extra_bands = eoIM.EXTRA_ANGLE

    #--------------------------------------------------------------------------------------------------------
    # Create the final mosaic by merging a number of submosaics
    #--------------------------------------------------------------------------------------------------------
    base_img = base_img*0  
    base_img[eoIM.pix_score] = base_img[SsrData['BLU']]

    # Mask out all the pixels in each variable of "base_img", so they will treated as gap/missing pixels
    base_img = base_img.where(base_img > 0)
    print('\n<period_mosaic> based mosaic image = ', base_img)

    sub_regions = eoUs.divide_region(Region, nSub)
    print('<period_mosaic> Divided sub-regions = ', sub_regions)
    count = 1

    for sub_region in sub_regions:
      sub_mosaic_start = time.time()
      print('\n\n<period_mosaic> Create a sub-mosaic for ', sub_region)
      sub_polygon = {'type': 'Polygon',  'coordinates': [sub_region] }

      sub_mosaic = get_sub_mosaic(SsrData, sub_polygon, ProjStr, Scale, StartStr, EndStr, angle_DB, extra_bands)

      if sub_mosaic != None:
        #sub_mosaic = attach_score(sub_mosaic)
        max_spec_val = xr.apply_ufunc(np.maximum, sub_mosaic[SsrData['BLU']], sub_mosaic[SsrData['NIR']])
        sub_mosaic = sub_mosaic.where(max_spec_val > 0)    

        # Fill the gaps/missing pixels in "base_img" with valid pixels in "sub_mosaic" 
        base_img = base_img.combine_first(sub_mosaic)

      sub_mosaic_stop = time.time()    
      sub_mosaic_time = (sub_mosaic_stop - sub_mosaic_start)/60
      print('\n<<<<<<<<<< Complete %2dth sub mosaic, elapsed time = %6.2f minutes>>>>>>>>>'%(count, sub_mosaic_time))
      count += 1
    
    mosaic = base_img

  mosaic_stop = time.time()
  mosaic_time = (mosaic_stop - mosaic_start)/60
  print('\n\n<<<<<<<<<< The total elapsed time for generating the mosaic = % 6.2f minutes>>>>>>>>>'%(mosaic_time))
  
  return mosaic
  



#############################################################################################################
# Description: This function returns a mosaic using the image acquired during 
# 
# Note: If "DB_fullpath" parameter is provided, then it indicates that imaging angles need to be included in
#       composite image.
#
# Revision history:  2024-May-24  Lixin Sun  Initial creation
#                    2024-Jul-15  Lixin Sun  Added the 2nd input parameter (DB_fullpath), which indicates
#                                            if imaging angles need to be included in composite image.
#############################################################################################################
def new_period_mosaic(inParams, DB_fullpath = ''):
  '''
    Args:
      inParams(dictionary): A dictionary containing all required parameters;
      DB_fullpath(String): A full path to a geometry angle database file in .CSV format.'''
  
  mosaic_start = time.time()
  #==========================================================================================================
  # Prepare required parameters and query criteria
  #==========================================================================================================
  params = eoPM.get_mosaic_params(inParams)
  
  if params == None:
    print('<period_mosaic> Cannot create a mosaic image due to invalid input parameter!')
    return None
  
  SsrData = eoIM.SSR_META_DICT[str(params['sensor']).upper()]
  ProjStr = str(params['projection'])  
  Scale   = int(params['resolution'])

  Region  = eoPM.get_spatial_region(params)
  StartStr, EndStr = eoPM.get_time_window(params)  

  criteria = get_query_conditions(SsrData, StartStr, EndStr)

  #==========================================================================================================
  # Detect the existance of an imaging angle database, which is supposed to be generated from GEE
  #==========================================================================================================
  angle_DB    = pd.DataFrame(read_angleDB(DB_fullpath))
  extra_bands = eoIM.EXTRA_ANGLE if angle_DB.empty == False else eoIM.EXTRA_NONE

  #==========================================================================================================
  # Create a base image that has full spatial dimensions covering ROI
  #==========================================================================================================
  base_img, stac_items, used_time = get_base_Image(SsrData, Region, ProjStr, Scale, StartStr, EndStr, angle_DB)
  
  #==========================================================================================================
  # Attach necessary extra bands and then mask out all the pixels
  #==========================================================================================================
  blue_band = base_img[SsrData['BLU']]
  
  base_img[eoIM.pix_date] = blue_band
  base_img["cosSZA"]      = blue_band
  base_img["cosVZA"]      = blue_band
  base_img["cosRAA"]      = blue_band
  #base_img[eoIM.pix_score] = blue_band

  # Mask out all the pixels in each variable of "base_img", so they will treated as gap/missing pixels
  base_img = base_img*0
  base_img = base_img.where(base_img > 0)
  print('\n<period_mosaic> based mosaic image = ', base_img)
  print('\n<<<<<<<<<< Complete generating base image, elapsed time = %6.2f minutes>>>>>>>>>'%(used_time))  

  #==========================================================================================================
  # Get a list of unique tile names and then loop through each unique tile to generate submosaic 
  #==========================================================================================================  
  unique_tiles = get_unique_tile_names(stac_items)  #Get all unique tile names  
  print('\n<<<<<< The number of unique tiles = %d >>>>>>>'%(len(unique_tiles)))  
  
  '''
  def ingest_one_tile(tile):
    one_tile_items  = get_one_tile_items(stac_items, tile) # Extract a list of items based on an unique tile name       
    one_tile_mosaic = get_tile_submosaic(SsrData, one_tile_items, StartStr, EndStr, criteria['bands'], ProjStr, Scale, extra_bands)

    if one_tile_mosaic != None:
      max_spec_val    = xr.apply_ufunc(np.maximum, one_tile_mosaic[SsrData['BLU']], one_tile_mosaic[SsrData['NIR']])
      one_tile_mosaic = one_tile_mosaic.where(max_spec_val > 0)

      # Fill the gaps/missing pixels in "base_img" with valid pixels in "sub_mosaic" 
      base_img = base_img.combine_first(one_tile_mosaic)
  
  #Parallel(n_jobs=1, require='sharedmem')(delayed(ingest_one_tile)(tile, base_img) for tile in unique_tiles) 
  '''

  count = 1
  for tile in unique_tiles:
    sub_mosaic_start = time.time()
    
    #ingest_one_tile(tile)
    one_tile_items  = get_one_tile_items(stac_items, tile) # Extract a list of items based on an unique tile name       
    one_tile_mosaic = get_tile_submosaic(SsrData, one_tile_items, StartStr, EndStr, criteria['bands'], ProjStr, Scale, extra_bands)

    if one_tile_mosaic != None:
      max_spec_val    = xr.apply_ufunc(np.maximum, one_tile_mosaic[SsrData['BLU']], one_tile_mosaic[SsrData['NIR']])
      one_tile_mosaic = one_tile_mosaic.where(max_spec_val > 0)

      # Fill the gaps/missing pixels in "base_img" with valid pixels in "sub_mosaic" 
      base_img = base_img.combine_first(one_tile_mosaic)

    print('\n\n<period_mosaic> base image after merging a submosaic =', base_img)

    sub_mosaic_stop = time.time()    
    sub_mosaic_time = (sub_mosaic_stop - sub_mosaic_start)/60
    print('\n<<<<<<<<<< Complete %2dth sub mosaic, elapsed time = %6.2f minutes>>>>>>>>>'%(count, sub_mosaic_time))
    count += 1
    
  mosaic_stop = time.time()
  mosaic_time = (mosaic_stop - mosaic_start)/60
  print('\n\n<<<<<<<<<< The total elapsed time for generating the mosaic = % 6.2f minutes>>>>>>>>>'%(mosaic_time))
  
  return base_img
  





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
  spa_scale  = params['resolution']
  
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
    'months': [6],               # A list of integers represening one or multiple monthes     
    'tile_names': ['tile42_411'], # A list of (sub-)tile names (defined using CCRS' tile griding system) 
    'prod_names': ['mosaic'],    #['mosaic', 'LAI', 'fCOVER', ]    
    'resolution': 200,            # Exporting spatial resolution    
    'out_folder': 'C:/Work_documents/test_xr_tile55_411_2021_200m',  # the folder name for exporting
    'projection': 'EPSG:3979'   
    
    #'start_date': '2022-06-15',
    #'end_date': '2022-09-15'
}

mosaic = new_period_mosaic(params, 'C:\\Work_documents\\scene_geo_angles\\tile42_geo_angles_2022_Summer.csv')

# export_mosaic(params, mosaic)
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

params = {
    'sensor': 'L8_SR',           # A sensor type string (e.g., 'S2_SR' or 'L8_SR' or 'MOD_SR')
    'unit': 2,                   # A data unit code (1 or 2 for TOA or surface reflectance)    
    'year': 2022,                # An integer representing image acquisition year
    'nbYears': -1,               # positive int for annual product, or negative int for monthly product
    'months': [8],               # A list of integers represening one or multiple monthes     
    'tile_names': ['tile42_922'], # A list of (sub-)tile names (defined using CCRS' tile griding system) 
    'prod_names': ['mosaic'],    #['mosaic', 'LAI', 'fCOVER', ]    
    'resolution': 400,            # Exporting spatial resolution    
    'out_folder': 'D:/WorkSpace/test_xr_output',        # the folder name for exporting
    'projection': 'EPSG:3979', 
    
    #'start_date': '2022-06-15',
    #'end_date': '2022-09-15'
}


    
mosaic = period_mosaic(params)
#mosaic = get_sub_mosaic(ssr_data, sub_region, 'EPSG:3979', scale, start_str, end_str)
#mosaic.to_netcdf('C:\\Work_documents\\stac_mosaic.nc')
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