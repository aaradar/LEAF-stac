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
from dask.distributed import as_completed
from urllib3.exceptions import TimeoutError, ConnectionError

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
import eoMosaic as eoMz

logging.basicConfig(level=logging.WARNING) 

# def get_query_conditions(SsrData, StartStr, EndStr, ClCover):
#   logging.basicConfig(level=logging.WARNING) 




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

  if 's2a' in scene_id or 's2b' in scene_id:
    load_bands = Bands
  elif 'hls.s30' in scene_id:
    load_bands = Bands['S2'] + Bands['angle'] if IncludeAngles == True else Bands['S2']
  elif 'hls.l30' in scene_id:
    load_bands = Bands['LS'] + Bands['angle'] if IncludeAngles == True else Bands['LS']

  else:
    print('<get_load_bands> A wrong STAC item was provided!')
  
  return load_bands





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
  xrDS_10m = eoMz.load_stac_with_retry(STAC_items, bands_10m, 10, chunk_size, ProjStr)
  
  if xrDS_10m is None:
    print('<load_STAC_10m_items> Failed to load 10-m resolution band images!')  
    return None
  
  xrDS_10m_allbands = xrDS_10m

  if len(Bands_20m) > 0:
    xrDS_20m = eoMz.load_stac_with_retry(STAC_items, Bands_20m, 20, chunk_size, ProjStr)

    if xrDS_20m is None:
      print('<load_STAC_10m_items> Failed to load 20-m resolution band images!')  
      return None
  
    #==========================================================================================================
    # Convert 20-m to 10-m resolution images as necessary
    #==========================================================================================================    
    xrDS20_to10 = xr.Dataset()

    for band in xrDS_20m.data_vars:
      arr20 = xrDS_20m[band]

      # duplicate to 10 m
      arr10 = arr20.repeat(2, dim="x").repeat(2, dim="y")

      # write correct geotransform + CRS
      #arr10 = arr10.rio.write_crs(ds10.rio.crs)
      #arr10 = arr10.rio.write_transform(ds10.rio.transform())

      xrDS20_to10[band] = arr10

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
  xrDS    = eoIM.apply_pixel_masks(xrDS, SsrData)
  xrDS    = eoIM.apply_gain_offset(xrDS, SsrData, 100, False)

  return xrDS, time_values





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

  #==========================================================================================================
  # Extract parameters from "MosaicParams"
  #==========================================================================================================  
  SsrData    = MosaicParams['SsrData']
  ProjStr    = MosaicParams['projection']
  Scale      = MosaicParams['resolution']
  Bands      = MosaicParams['Criteria']['bands']  
  InclAngles = MosaicParams['IncludeAngles']
  
  StartStr, EndStr = eoPM.get_time_window(MosaicParams)
  
  chunk_size = {'x': 2000, 'y': 2000}
  #==========================================================================================================
  # Load satellite images from a STAC catalog
  #==========================================================================================================
  one_granule_items = get_one_granule_items(StacItems, granule)                # Extract a list of STAC items based on an unique granule name
  filtered_items    = get_unique_STAC_items(one_granule_items, MosaicParams)   # Remain only one item from those that share the same timestamp
 
  if 'scl' in Bands:   # For Sentinel-2 images from AWS data catalog 
    #When in debugging mode, display metadata assets
    # if MosaicParams["debug"]:
    #   display_meta_assets(filtered_items['S2'], False)   

    xrDS_S2 = load_STAC_items(filtered_items['S2'], Bands, chunk_size, ProjStr, Scale)  
    xrDS_LS = None  
  else:  #For both Snetinel-2 and Landsat data from LP DAAC of NASA
    S2_bands = Bands['S2'] + Bands['angle'] if InclAngles else Bands['S2']
    LS_bands = Bands['LS'] + Bands['angle'] if InclAngles else Bands['LS']
     
    xrDS_S2 = load_STAC_items(filtered_items['S2'], S2_bands, chunk_size, ProjStr, Scale)
    xrDS_LS = load_STAC_items(filtered_items['LS'], LS_bands, chunk_size, ProjStr, Scale)

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
  
  xrDS = xrDS.chunk({'x': 2000, 'y': 2000, 'time': xrDS.sizes['time']}).map_blocks(
    attach_score_args, 
    template = xrDS.chunk({'x': 2000, 'y': 2000, 'time': xrDS.sizes['time']})
  )
  
  #==========================================================================================================
  # Create a composite image based on compositing scores
  # Note: calling "fillna" function before invoking "argmax" function is very important!!!
  #==========================================================================================================
  xrDS = xrDS.fillna(-10000.0).chunk({'x': 2000, 'y': 2000, 'time': xrDS.sizes['time']})

  def granule_mosaic_template(xrDS, inBands, IncAngles):
    "Every variable used in this function must be input as a parameter. Global variables cannot be used!"
    mosaic_template = {}
    
    xrDA = xr.DataArray(
      data  = dask.array.zeros((xrDS.sizes['y'], xrDS.sizes['x']), chunks=(2000, 2000), dtype=np.float32),
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
    template=granule_mosaic_template(xrDS, Bands, InclAngles)  # Pass the template (same structure as xrDS)
  )
  
  mosaic = mosaic.where(mosaic[eoIM.pix_date] > 0)
  mosaic = mosaic.reindex_like(BaseImg).chunk({'x': 2000, 'y': 2000}).fillna(-10000.0)
  
  del xrDS, granule_mosaic_args
  gc.collect()
  
  return mosaic
  


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
    #========================================================================================================
    def disable_spill():
      dask.config.set({
        'distributed.comm.retry.count': 5,
        'distributed.comm.timeouts.tcp' : 18000,
        'distributed.comm.timeouts.connect': 18000,
        'distributed.worker.memory.target': 1, 
        'distributed.worker.memory.spill': 0.95,
        'distributed.worker.memory.terminate': 0.95 
        })
      
    cluster = LocalCluster(
      n_workers = MosaicParams["number_workers"],
      threads_per_worker = 3,
      memory_limit = MosaicParams["node_memory"],
      local_directory = tmp_directory,
    )
    client = Client(cluster)
    client.register_worker_callbacks(setup=disable_spill)
    print(f'\n\n<<<<<<<<<< Dask dashboard is available {client.dashboard_link} >>>>>>>>>')
  
    #========================================================================================================
    # Submit the jobs to the clusters to process them in a parallel mode 
    #========================================================================================================
    granule_mosaics_data = [(BaseImg, granule, stac_items, MosaicParams) for granule in unique_granules]
    granule_mosaics      = [client.submit(get_granule_mosaic, data) for data in granule_mosaics_data]
    
    return granule_mosaics, client, cluster, unique_name





#############################################################################################################
# Description: This function creates/returns ONE composite image from images acquired within a given time 
#              period over a specific region.
# 
# Revision history:  2024-May-24  Lixin Sun  Initial creation
#                    2024-Jul-20  Lixin Sun  Modified to generate the final composite image tile by tile.
#############################################################################################################
def one_mosaic(ProdParams, CompParams, Output=True):
  '''
    Args:
      ProdParams(Dictionary): A dictionary containing all parameters related to composite image production;
      CompParams(Dictionary): A dictionary containing all parameters related to used computing environment.
      Output(Boolean): An integer indicating wheter to export resulting composite image.'''
  
  mosaic_start = time.time()   #Record the start time of the whole process

  #==========================================================================================================
  # Search all the STAC items based on a spatial region and a time window
  # Note: (1) The 2nd parameter (MaxImgs) for "search_STAC_Catalog" function cannot be too large. Otherwise,
  #           a server internal error will be triggered.
  #       (2) The imaging angles will be attached to each STAC item by "search_STAC_Catalog" function if S2
  #           data from AWS is used.
  #==========================================================================================================  
  stac_items = search_STAC_Catalog(ProdParams, 100)  
  print(f"\n<period_mosaic> A total of {len(stac_items):d} items were found.\n")

  #When in debugging mode, display metadata assets
  if ProdParams["debug"]:
    display_meta_assets(stac_items, True)   
  
  #==========================================================================================================
  # Create a base image that fully spans ROI
  #==========================================================================================================
  base_img = get_base_Image(stac_items, ProdParams)
  print('\n<period_mosaic> based mosaic image = ', base_img)
  
  #==========================================================================================================
  # Extract unique granule names and iterate over them to generate submosaic separately 
  #==========================================================================================================  
  unique_granules = get_unique_tile_names(stac_items)  #Get all unique tile names 
  print('\n<period_mosaic> The number of unique granule tiles = %d'%(len(unique_granules)))  

  if ProdParams["debug"]:
    print('\n<<<<<< The unique granule tiles = ', unique_granules)   

  #==========================================================================================================
  # Create submosaic separately for each granule in parallel and on distributed workers
  #==========================================================================================================
  if CompParams["debug"]:
    submited_granules_mosaics, client, cluster, unique_name = create_mosaic_at_once_one_machine(base_img, unique_granules, stac_items, ProdParams)
  else:
    submited_granules_mosaics, client, cluster, unique_name = create_mosaic_at_once_distributed(base_img, unique_granules, stac_items, ProdParams)
  
  persisted_granules_mosaics = dask.persist(*submited_granules_mosaics, optimize_graph=True)
  for future, granules_mosaic in as_completed(persisted_granules_mosaics, with_results=True):
    base_img = merge_granule_mosaics(granules_mosaic, base_img, eoIM.pix_score)
    client.cancel(future)
  
  # We do the compute to get a dask array instead of a future
  base_img = base_img.chunk({"x": 2000, "y": 2000}).compute()

  #==========================================================================================================
  # Mask out the pixels with negative date value
  #========================================================================================================== 
  mosaic = base_img.where(base_img[eoIM.pix_date] > 0)

  mosaic_stop = time.time()
  mosaic_time = (mosaic_stop - mosaic_start)/60

  try:
    client.close()
    cluster.close()
  except asyncio.CancelledError:
    print("Cluster is closed!")

  print('\n\n<<<<<<<<<< The total elapsed time for generating the mosaic = %6.2f minutes>>>>>>>>>'%(mosaic_time))

  #==========================================================================================================
  # Output resultant mosaic as required
  #========================================================================================================== 
  ext_tiffs_rec = ["test"]
  period_str = "test"
  if Output:
    ext_tiffs_rec, period_str = export_mosaic(ProdParams, mosaic)
  
  #==========================================================================================================
  # Create logging files
  #========================================================================================================== 
  dask_out_file  = Path(ProdParams["out_folder"]) / f"log_{unique_name}.out"
  dask_directory = os.path.join(Path(ProdParams["out_folder"]), f"dask_spill_{unique_name}")

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
  
  return ext_tiffs_rec, period_str, mosaic




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
# Description: This function can be used to perform mosaic production
#
#############################################################################################################
def Mosaic10MProduction(ProdParams, CompParams):
  '''Produces monthly biophysical parameter maps for a number of tiles and months.

     Args:
       ProdParams(Python Dictionary): A dictionary containing input parameters related to production;
       CompParams(Python Dictionary): A dictionary containing input parameters related to used computing environment.
  '''
  #==========================================================================================================
  # Standardize the parameter dictionaries so that they are applicable for mosaic generation
  #==========================================================================================================
  usedParams = eoPM.get_mosaic_params(ProdParams, CompParams)  
    
  if usedParams is None:
    print('<MosaicProduction> Inconsistent input parameters!')
    return None
  
  #==========================================================================================================
  # Generate composite images based on given input parameters
  #==========================================================================================================
  ext_tiffs_rec = []
  period_str = ""
  all_base_tiles = []  
  
  if CompParams["entire_tile"]:
    for tile_name in usedParams['tile_names']:
      subtiles = eoTG.get_subtile_names(tile_name)
      for subtile in subtiles:            
        tile_params = copy.deepcopy(usedParams)
        print(tile_params)
        tile_params['tile_names'] = [subtile]
        
        tile_params = eoPM.get_mosaic_params(tile_params, CompParams)
        if len(ext_tiffs_rec) == 0:
          ext_tiffs_rec, period_str, mosaic = one_mosaic(tile_params, CompParams)
        else: 
          _, _, mosaic = one_mosaic(tile_params, CompParams)

        all_base_tiles.append(tile_name)
  else:
    region_names = usedParams['regions'].keys()    # A list of region names
    nTimes       = len(usedParams['start_dates'])  # The number of time windows

    for reg_name in region_names:
      # Loop through each spatial region
      usedParams = eoPM.set_spatial_region(usedParams, reg_name)
      
      for TIndex in range(nTimes):
        # Produce vegetation parameter porducts for each time window
        usedParams = eoPM.set_current_time(usedParams, TIndex)

        # Produce and export products in a specified way (a compact image or separate images)      
        out_style = str(usedParams['export_style']).lower()
        if out_style.find('comp') > -1:
          print('\n<MosaicProduction> Generate and export mosaic images in one file .......')
          #out_params = compact_params(mosaic, SsrData, ClassImg)

          # Export the 64-bits image to either GD or GCS
          #export_compact_params(fun_Param_dict, region, out_params, task_list)

        else: 
          # Produce and export vegetation parameetr maps for a time period and a region
          print('\n<MosaicProduction> Generate and export separate mosaic images......')        
          one_mosaic(usedParams, CompParams)
 