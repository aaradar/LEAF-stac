
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



#############################################################################################################
# Description: This function returns the number of sub mosaics in all spatial dimension (X and Y)
#
# Revision history:  2024-Jul-02  Lixin Sun  Initial creation
# 
#############################################################################################################
def get_sub_numb(xrDS):
  x_dim = xrDS.sizes['x']
  y_dim = xrDS.sizes['y']

  max_dim = max(x_dim, y_dim)

  nSub = int((max_dim/5000.0) + 0.5)

  return nSub if nSub > 1 else 2





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
  # For many other collections, the Microsoft Planetary Computer has a STAC server at 
  # https://planetarycomputer-staging.microsoft.com/api/stac/v1 (this info comes from 
  # https://www.matecdev.com/posts/landsat-sentinel-aws-s3-python.html)
  #==================================================================================================  
  if ssr_code > eoIM.MAX_LS_CODE and ssr_code < eoIM.MOD_sensor:
    query_conds['catalog']    = "https://earth-search.aws.element84.com/v1"
    query_conds['collection'] = "sentinel-2-l2a"
    query_conds['timeframe']  = str(StartStr) + '/' + str(EndStr)
    query_conds['bands']      = ['blue', 'green', 'red', 'rededge1', 'rededge2', 'rededge3', 'nir08', 'swir16', 'swir22', 'scl']
    query_conds['filters']    = {"s2:cloud_shadow_percentage": {"lt": 0.8} }

  elif ssr_code < eoIM.MAX_LS_CODE and ssr_code > 0:
    #query_conds['catalog']    = "https://landsatlook.usgs.gov/stac-server"
    #query_conds['collection'] = "landsat-c2l2-sr"
    query_conds['catalog']    = "https://earth-search.aws.element84.com/v1"
    query_conds['collection'] = "landsat-c2-l2"
    query_conds['timeframe']  = str(StartStr) + '/' + str(EndStr)
    #query_conds['bands']      = ['OLI_B2', 'OLI_B3', 'OLI_B4', 'OLI_B5', 'OLI_B6', 'OLI_B7', 'qa_pixel']
    query_conds['bands']      = ['blue', 'green', 'red', 'nir08', 'swir16', 'swir22', 'qa_pixel']
    query_conds['filters']    = ["eo:cloud_cover<10"]
  
  elif ssr_code == eoIM.HLS_sensor:
    query_conds['catalog']    = "https://cmr.earthdata.nasa.gov/stac/LPCLOUD"
    query_conds['collection'] = "HLSL30.v2.0"
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
def search_STAC_Images(SsrData, Region, StartStr, EndStr, based_on_region):
  # get all query conditions 
  query_conds = get_query_conditions(SsrData, StartStr, EndStr)

  # use publically available stac link such as
  #catalog = psc.client.Client.from_file(query_conds['catalog'], stac_io = stac_api_io)
  catalog = psc.client.Client.open(str(query_conds['catalog'])) 

  #==================================================================================================
  # Search and filter a image collection
  #================================================================================================== 
  if based_on_region == True:
    print('<search_STAC_Images> The given region = ', Region)
    search_IC = catalog.search(collections = [str(query_conds['collection'])], 
                               intersects  = Region,                           
                               datetime    = str(query_conds['timeframe']), 
                               limit       = 500)
  else:
    Bbox = eoUs.get_region_bbox(Region)
    print('<search_STAC_Images> The bbox of the given region = ', Bbox)

    search_IC = catalog.search(collections = [str(query_conds['collection'])], 
                               bbox        = Bbox,                           
                               datetime    = str(query_conds['timeframe']), 
                               limit       = 500)

  return search_IC






#############################################################################################################
# Description: This function returns a base image.
#
# Revision history:  2024-May-24  Lixin Sun  Initial creation
# 
#############################################################################################################
def get_base_Image(SsrData, Region, ProjStr, Scale, StartStr, EndStr):
  # get all query conditions 
  query_conds = get_query_conditions(SsrData, StartStr, EndStr)

  searched_IC = search_STAC_Images(SsrData, Region, StartStr, EndStr, True)
  
  items = list(searched_IC.items())
  print(f"Found: {len(items):d} datasets")
  
  #print('<<<<<<<get_base_Image>>>>>>> info on an item = ', items[0].to_dict())
  for asset_key, asset in items[0].assets.items():
    print(f"Band: {asset_key}, Description: {asset.title or 'No title'}")

  #==================================================================================================
  # define a geobox for my region
  #==================================================================================================
  # lazily combine items
  Bbox = eoUs.get_region_bbox(Region)
  print('<get_base_Image> The bbox of the given region = ', Bbox)

  ds_xr = odc.stac.load([items[0], items[1]],
                        bands  = query_conds['bands'],                        
                        groupby='solar_day',  
                        chunks = {'x': 1000, 'y': 1000},
                        crs    = ProjStr, 
                        bbox   = Bbox,
                        fail_on_error = False,
                        resolution = Scale)
  
  # actually load it
  with ddiag.ProgressBar():
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
def attach_score(SsrData, ready_IC, StartStr, EndStr):
  '''Attaches a score band to each image in a xarray Dataset, which is equivalent to an image collection in GEE
  '''
  #print('<attach_score> ready IC = ', ready_IC)
  
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
    
  land_score = max_IR/(STD_blu + blu_pen + nir_pen)  

  return land_score.where((max_SV < max_IR) | (max_SW > 3.0), water_score)
  




#############################################################################################################
# Description: This function returns a mosaic image for a sub-region
#
# Revision history:  2024-May-24  Lixin Sun  Initial creation
# 
#############################################################################################################
def get_sub_mosaic(SsrData, SubRegion, ProjStr, Scale, StartStr, EndStr):
  
  #full_IC = get_STAC_ImColl(SsrData, SubRegion, ProjStr, Scale, StartStr, EndStr)

  #==========================================================================================================
  # get all query conditions 
  #==========================================================================================================
  query_conds = get_query_conditions(SsrData, StartStr, EndStr)

  #==========================================================================================================
  # Get a catalog object from STAC catalog available to public
  #==========================================================================================================
  #catalog = psc.client.Client.from_file(query_conds['catalog'], stac_io = stac_api_io)
  catalog = psc.client.Client.open(str(query_conds['catalog'])) 
  
  mybbox = eoUs.get_region_bbox(SubRegion)
  print('<get_sub_mosaic> The bbox of the given region = ', mybbox)

  #==========================================================================================================
  # Search and filter a image collection within the catalog
  #==========================================================================================================
  search_IC = catalog.search(collections = [str(query_conds['collection'])], 
                             #intersects  = SubRegion,
                             bbox        = mybbox,
                             datetime    = str(query_conds['timeframe']), 
                             query       = query_conds['filters'],
                             limit       = 500)
  
  items = list(search_IC.items())
  print(f"<get_sub_mosaic> Found: {len(items):d} images")
    
  #==========================================================================================================
  # Load all the STAC items as an xarray.Dataset object on a local machine
  #==========================================================================================================
  #chunk_size = get_chunk_size()

  full_IC = odc.stac.load(search_IC.items(),
                        bands  = query_conds['bands'],
                        groupby='solar_day',  #For lower latitude scenses, save a lot memories
                        #chunks = {'time': 30, 'x': 1000, 'y': 1000},
                        chunks = {'x': 1000, 'y': 1000},
                        fail_on_error = False,
                        crs    = ProjStr, 
                        #bbox   = mybbox,
                        resolution = Scale)

  #==========================================================================================================
  # Actually load all data from a lazy-loaded dataset into in-memory Numpy arrays
  #==========================================================================================================
  with ddiag.ProgressBar():
    full_IC.load()
  
  #==========================================================================================================
  # Attach an empty layer (with all pixels equal to ZERO) to eath temporal item (an image here) in "raw_IC" 
  #==========================================================================================================
  zero_img = full_IC[SsrData['BLU']]*0
  full_IC[eoIM.pix_score] = zero_img

  #==========================================================================================================
  # Apply default pixel mask to each of the images
  #==========================================================================================================
  mask_start = time.time()

  full_IC = eoIM.apply_default_mask(full_IC, SsrData)

  mask_end = time.time()
  mask_time = (mask_end - mask_start)/60
  print('\n<get_sub_mosaic> Complete applying default mask, elapsed time = %6.2f minutes'%(mask_time))  

  #==========================================================================================================
  # Apply gain and offset to each band in a xarray dataset
  #==========================================================================================================  
  GO_start = time.time()
  full_IC = eoIM.apply_gain_offset(full_IC, SsrData, 100, False)
  
  GO_end = time.time()  
  GO_time = (GO_end - GO_start)/60
  print('<get_sub_mosaic> Complete applying gain and offset, elapsed time = %6.2f minutes'%(GO_time))

  #==========================================================================================================
  # Note: calling "fillna" function before invaking "argmax" function is very important!!!
  #==========================================================================================================
  full_IC = attach_score(SsrData, full_IC, StartStr, EndStr).fillna(-0.0001)
  score_end = time.time()

  score_time = (score_end - GO_end)/60
  print('<get_sub_mosaic> Complete pixel scoring, elapsed time = %6.2f minutes'%(score_time))

  max_indices = full_IC[eoIM.pix_score].argmax(dim='time')
  max_end  = time.time()

  max_time = (max_end - score_end)/60
  print('<get_sub_mosaic> Complete searching max indices, elapsed time = %6.2f minutes'%(max_time))

  sub_mosaic  = full_IC.isel(time=max_indices)
  mosaic_end  = time.time()

  mosaic_time = (mosaic_end - max_end)/60
  print('<get_sub_mosaic> Complete creating a sub mosaic, elapsed time = %6.2f minutes'%(mosaic_time))
  print('\n\n<get_sub_mosaic> sub mosaic =', sub_mosaic)

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

  return sub_mosaic    #.compute()





# Mask out pixels according to the SCL band
# Assuming valid pixels are those where SCL == 4 (vegetation), SCL == 5 (non-vegetated), or SCL == 6 (water)
valid_scl = ds.SCL.isin([4, 5, 6])
ds_masked = ds.where(valid_scl, drop=True)

# Apply scaling factors to the pixels
# Example scaling factors for Sentinel-2 L2A data: reflectance values are scaled by 0.0001
scaling_factor = 0.0001
ds_scaled = ds_masked * scaling_factor

# Calculate NDVI
ndvi = (ds_scaled.B04 - ds_scaled.B03) / (ds_scaled.B04 + ds_scaled.B03)

# Select composite pixels based on maximum NDVI values
ndvi_max = ndvi.max(dim='time')
composite = ds_scaled.sel(time=ndvi.argmax(dim='time'))

# Add NDVI to the composite
composite['NDVI'] = ndvi_max

# Compute the result (this triggers the actual computation)
composite_result = composite.compute()






#############################################################################################################
# Description: This function returns a mosaic using the image acquired during 
#
# Revision history:  2024-May-24  Lixin Sun  Initial creation
# 
#############################################################################################################
def period_mosaic(inParams):
  mosaic_start = time.time()
  #==========================================================================================================
  # Obtain required parameters
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
  # Create a base image that has full spatial dimensions of the specified region
  #==========================================================================================================
  base_start = time.time()
  base_img = get_base_Image(SsrData, Region, ProjStr, Scale, StartStr, EndStr)*0
  
  base_img[eoIM.pix_score] = base_img[SsrData['BLU']]

  # Mask out all the pixels in each variable of "base_img", so they will treated as gap/missing pixels
  base_img = base_img.where(base_img > 0)
  base_stop = time.time()

  base_time = (base_stop - base_start)/60
  print('\n<<<<<<<<<< Complete generating base image, elapsed time = %6.2f minutes>>>>>>>>>'%(base_time))
  print('\n<period_mosaic> based mosaic image = ', base_img)
  
  #==========================================================================================================
  # Create individual sub-mosaic and combine it into base image based on score
  #==========================================================================================================
  nSub = get_sub_numb(base_img)
  print('<period_mosaic> The number of sub mosaics = ', nSub)

  sub_regions = eoUs.divide_region(Region, nSub)
  print('<period_mosaic> Divided sub-regions = ', sub_regions)
  count = 1

  for sub_region in sub_regions:
    sub_mosaic_start = time.time()
    print('\n\n<period_mosaic> Create a sub-mosaic for ', sub_region)
    sub_polygon = {'type': 'Polygon',  'coordinates': [sub_region] }

    sub_mosaic = get_sub_mosaic(SsrData, sub_polygon, ProjStr, Scale, StartStr, EndStr)
    
    #sub_mosaic = attach_score(sub_mosaic)
    max_spec_val = xr.apply_ufunc(np.maximum, sub_mosaic[SsrData['BLU']], sub_mosaic[SsrData['NIR']])
    sub_mosaic = sub_mosaic.where(max_spec_val > 0)    

    # Fill the gaps/missing pixels in "base_img" with valid pixels in "sub_mosaic" 
    base_img = base_img.combine_first(sub_mosaic)
    sub_mosaic_stop = time.time()
    
    sub_mosaic_time = (sub_mosaic_stop - sub_mosaic_start)/60
    print('\n<<<<<<<<<< Complete %2dth sub mosaic, elapsed time = %6.2f minutes>>>>>>>>>'%(count, sub_mosaic_time))
    count += 1
  
  mosaic_stop = time.time()
  mosaic_time = (mosaic_stop - mosaic_start)/60
  print('\n\n<<<<<<<<<< The total elapsed time for generating the mosaic = % 6.2f minutes>>>>>>>>>'%(mosaic_time))
  
  return base_img
  




#############################################################################################################
# Description: This function returns a mosaic using the image acquired during 
#
# Revision history:  2024-May-24  Lixin Sun  Initial creation
# 
#############################################################################################################
def new_period_mosaic(inParams):
  mosaic_start = time.time()
  #==========================================================================================================
  # Obtain required parameters
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
  # Create a base image that has full spatial dimensions of the specified region
  #==========================================================================================================
  base_start = time.time()
  base_img = get_base_Image(SsrData, Region, ProjStr, Scale, StartStr, EndStr)*0
  
  base_img[eoIM.pix_score] = base_img[SsrData['BLU']]

  # Mask out all the pixels in each variable of "base_img", so they will treated as gap/missing pixels
  base_img = base_img.where(base_img > 0)
  base_stop = time.time()

  base_time = (base_stop - base_start)/60
  print('\n<<<<<<<<<< Complete generating base image, elapsed time = %6.2f minutes>>>>>>>>>'%(base_time))
  print('\n<period_mosaic> based mosaic image = ', base_img)
  
  #==========================================================================================================
  # Create individual sub-mosaic and combine it into base image based on score
  #==========================================================================================================
  nSub = get_sub_numb(base_img)
  print('<period_mosaic> The number of sub mosaics = ', nSub)

  sub_regions = eoUs.divide_region(Region, nSub)
  print('<period_mosaic> Divided sub-regions = ', sub_regions)
  count = 1

  for sub_region in sub_regions:
    sub_mosaic_start = time.time()
    print('\n\n<period_mosaic> Create a sub-mosaic for ', sub_region)
    sub_polygon = {'type': 'Polygon',  'coordinates': [sub_region] }

    sub_mosaic = get_sub_mosaic(SsrData, sub_polygon, ProjStr, Scale, StartStr, EndStr)
    
    #sub_mosaic = attach_score(sub_mosaic)
    max_spec_val = xr.apply_ufunc(np.maximum, sub_mosaic[SsrData['BLU']], sub_mosaic[SsrData['NIR']])
    sub_mosaic = sub_mosaic.where(max_spec_val > 0)    

    # Fill the gaps/missing pixels in "base_img" with valid pixels in "sub_mosaic" 
    base_img = base_img.combine_first(sub_mosaic)
    sub_mosaic_stop = time.time()
    
    sub_mosaic_time = (sub_mosaic_stop - sub_mosaic_start)/60
    print('\n<<<<<<<<<< Complete %2dth sub mosaic, elapsed time = %6.2f minutes>>>>>>>>>'%(count, sub_mosaic_time))
    count += 1
  
  mosaic_stop = time.time()
  mosaic_time = (mosaic_stop - mosaic_start)/60
  print('\n\n<<<<<<<<<< The total elapsed time for generating the mosaic = % 6.2f minutes>>>>>>>>>'%(mosaic_time))
  
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





# params = {
#     'sensor': 'L8_SR',           # A sensor type string (e.g., 'S2_SR' or 'L8_SR' or 'MOD_SR')
#     'unit': 2,                   # A data unit code (1 or 2 for TOA or surface reflectance)    
#     'year': 2022,                # An integer representing image acquisition year
#     'nbYears': 1,                # positive int for annual product, or negative int for monthly product
#     'months': [8],               # A list of integers represening one or multiple monthes     
#     'tile_names': ['tile42_922'],   # A list of (sub-)tile names (defined using CCRS' tile griding system) 
#     'prod_names': ['mosaic'],    #['mosaic', 'LAI', 'fCOVER', ]    
#     'resolution': 1000,          # Exporting spatial resolution    
#     'out_folder': 'C:/Work_documents/test_xr_output',   # the folder name for exporting   
#     'CloudScore': True,
#     'current_month': 8,
#     'current_tile': 'tile42_922',

#     #'start_date': '2022-06-15',
#     #'end_date': '2023-09-15'
# }

# mosaic = period_mosaic(params)

# export_mosaic(params, mosaic)




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