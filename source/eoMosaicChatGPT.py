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
#import eoUtils as eoUs
import eoParams as eoPM
#import eoTileGrids as eoTG
import eoMosaic as eoMz

logging.basicConfig(level=logging.WARNING) 



from pathlib import Path
import os, uuid, warnings, logging, platform, shutil, time
import dask
from dask.distributed import Client, LocalCluster, as_completed
from dask import delayed
import xarray as xr
import numpy as np
import gc



    
    
    
import dask.array as da
import functools
import traceback


@delayed
def merge_granule_mosaicsA(mosaic, base_img, pix_score):
    """
    Merge a per-granule mosaic into the current base mosaic.
    
    Args:
      mosaic (xarray.Dataset): Sub-mosaic already reindexed to the base grid.
      base_img (xarray.Dataset): Current accumulation mosaic.
      pix_score (str): Score variable name used for picking better pixels.
      
    Returns:
      xarray.Dataset: New merged mosaic (base_img is NOT modified in-place).
    """

    # If nothing to merge, return current base mosaic
    if mosaic is None:
        return base_img

    # Compute mask: where new mosaic has higher score than existing mosaic
    mask = mosaic[pix_score] > base_img[pix_score]

    # Build a new merged output (no in-place mutation!)
    merged = {}
    for var in base_img.data_vars:
        merged[var] = xr.where(mask, mosaic[var], base_img[var])

    # Return a new xarray.Dataset
    return xr.Dataset(merged, coords=base_img.coords, attrs=base_img.attrs)



# ---------------------------------------------------------------------------
# Rewritten get_granule_mosaic to accept lightweight base_grid
# ---------------------------------------------------------------------------
def get_granule_mosaicA(base_grid, granule, StacItems, MosaicParams, ChunkDict):
    """Worker-side function that builds a mosaic for a single granule.

    Inputs:
      - base_grid: small dict containing 'x' and 'y' numpy arrays and optional spatial_ref
      - granule: granule id (string)
      - StacItems: full stac_items structure (scattered)
      - MosaicParams: parameters dict (scattered)
      - ChunkDict: chunk size dict (scattered)

    The function mirrors the behavior of the original get_granule_mosaic but:
      - avoids any use of a full BaseImg object
      - uses base_grid for reindexing
      - returns an xarray.Dataset mosaic for the granule (dask-backed if appropriate)

    NOTE: This function assumes that helper functions referenced (load_STAC_items,
    preprocess_xrDS, attach_score, attach_AngleBands, eoIM.pix_score, etc.) are
    importable and available on the worker.
    """

    try:
        # Unpack common params
        SsrData = MosaicParams['SsrData']
        ProjStr = MosaicParams['projection']
        Scale = MosaicParams['resolution']
        Bands = MosaicParams['Criteria']['bands']
        InclAngles = MosaicParams['IncludeAngles']

        StartStr, EndStr = __import__('eoPM').eoPM.get_time_window(MosaicParams) if 'eoPM' not in globals() else eoPM.get_time_window(MosaicParams)

        # Get items for just this granule
        one_granule_items = eoMz.get_one_granule_items(StacItems, granule)
        filtered_items = eoMz.get_unique_STAC_items(one_granule_items, MosaicParams)

        # Load STAC items (these helper functions are expected to exist)
        if 'scl' in Bands:
            if Scale > 10:
                xrDS_S2 = eoMz.load_STAC_items(filtered_items['S2'], Bands, ChunkDict, ProjStr, Scale)
            else:
                Bands_20m = [band for band in Bands if band not in ['blue', 'green', 'red', 'nir08']]
                xrDS_S2 = eoMz.load_STAC_10m_items(filtered_items['S2'], Bands_20m, ChunkDict, ProjStr)
            xrDS_LS = None
        else:
            S2_bands = Bands['S2'] + Bands['angle'] if InclAngles else Bands['S2']
            LS_bands = Bands['LS'] + Bands['angle'] if InclAngles else Bands['LS']
            xrDS_S2 = eoMz.load_STAC_items(filtered_items['S2'], S2_bands, ChunkDict, ProjStr, Scale)
            xrDS_LS = eoMz.load_STAC_items(filtered_items['LS'], LS_bands, ChunkDict, ProjStr, Scale)

        # Preprocess (assumes it returns xrDS and time values)
        xrDS, time_values = eoMz.preprocess_xrDS(xrDS_S2, xrDS_LS, MosaicParams)
        if xrDS is None:
            return None

        # Attach score and compute mosaic (dask-aware)
        attach_score_args = functools.partial(eoMz.attach_score,
                                              SsrData=SsrData,
                                              StartStr=StartStr,
                                              EndStr=EndStr)

        my_chunk = {'x': ChunkDict['x'], 'y': ChunkDict['y'], 'time': xrDS.sizes['time']}
        xrDS = xrDS.chunk(my_chunk).map_blocks(attach_score_args, template=xrDS.chunk(my_chunk))
        xrDS = xrDS.fillna(-10000.0).chunk(my_chunk)

        # Build mosaic template (small, dask arrays inside)
        def granule_mosaic_template(xrDS_local, inBands, IncAngles):
            mosaic_template = {}
            xrDA = xr.DataArray(
                data=dask.array.zeros((xrDS_local.sizes['y'], xrDS_local.sizes['x']),
                                      chunks=(ChunkDict['x'], ChunkDict['y']), dtype=np.float32),
                dims=['y', 'x'],
                coords={'y': xrDS_local['y'], 'x': xrDS_local['x']},
            )
            for var_name in xrDS_local.data_vars:
                mosaic_template[var_name] = xrDA
            if IncAngles and 'scl' in inBands:
                for angle in ['SZA', 'VZA', 'SAA', 'VAA']:
                    mosaic_template[angle] = xrDA
            return xr.Dataset(mosaic_template)

        def granule_mosaic(xrDS_local, Items, pix_score, time_values_len, inBands, IncAngles):
            xrDS_local['time_index'] = xr.DataArray(np.array(range(0, time_values_len), dtype='uint8'), dims=['time'])
            max_indices = xrDS_local[pix_score].argmax(dim='time')
            mosaic_ds = xrDS_local.isel(time=max_indices)

            nS2_imgs = len(Items['S2'])
            if IncAngles and 'scl' in inBands and nS2_imgs > 0:
                mosaic_ds = eoMz.attach_AngleBands(mosaic_ds, Items['S2'])

            return mosaic_ds.drop_vars([v for v in ['time', 'spatial_ref', 'time_index'] if v in mosaic_ds])

        granule_mosaic_args = functools.partial(granule_mosaic,
                                               Items=filtered_items,
                                               pix_score=eoIM.pix_score,
                                               time_values_len=len(time_values),
                                               inBands=Bands,
                                               IncAngles=InclAngles)

        mosaic = xrDS.map_blocks(granule_mosaic_args, template=granule_mosaic_template(xrDS, Bands, InclAngles))      
        mosaic = mosaic.where(mosaic[eoIM.pix_date] > 0)

        # Reindex to the light-weight base_grid coordinates (no full BaseImg needed)
        target_coords = {"x": base_grid["x"], "y": base_grid["y"]}
        # Ensure dims exist in mosaic (swap dims order if necessary)
        # Use reindex to align with the final grid
        mosaic = mosaic.reindex(indexers=target_coords, method=None).chunk(ChunkDict).fillna(-10000.0)

        # Cleanup large temporary objects
        del xrDS, granule_mosaic_args
        gc.collect()

        return mosaic

    except Exception as e:
        # Log minimal information and return None so caller can handle failures
        try:
            logging.exception(f"get_granule_mosaic failed for granule {granule}: {e}")
        except Exception:
            pass
        return None





def create_mosaic_at_once_one_machineA(BaseImg, unique_granules, stac_items, MosaicParams):
    """Create a LocalCluster and submit per-granule mosaic tasks using best practices.

    This implementation follows 'Option A' — workers only receive a lightweight
    BaseGrid (x/y coords and minimal spatial ref) instead of the full BaseImg.

    Returns:
      - list of futures (one per granule)
      - dask client
      - cluster
      - unique_name (temporary directory suffix)

    NOTE: helper functions used inside get_granule_mosaic (e.g. load_STAC_items,
    preprocess_xrDS, attach_score, etc.) are assumed to be importable in the
    worker environment.
    """

    unique_name = str(uuid.uuid4())
    tmp_directory = os.path.join(Path(MosaicParams["out_folder"]), f"dask_spill_{unique_name}")
    os.makedirs(tmp_directory, exist_ok=True)

    # Reduce verbose logs from networking/server libs
    logging.getLogger('tornado').setLevel(logging.WARNING)
    logging.getLogger('tornado.application').setLevel(logging.CRITICAL)
    logging.getLogger('tornado.access').setLevel(logging.CRITICAL)
    logging.getLogger('tornado.general').setLevel(logging.CRITICAL)
    logging.getLogger('bokeh.server.protocol_handler').setLevel(logging.CRITICAL)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")

        # platform-specific tweaks
        if platform.system() == "Linux":
            os.environ.setdefault('http_proxy', "http://webproxy.science.gc.ca:8888/")
            os.environ.setdefault('https_proxy', "http://webproxy.science.gc.ca:8888/")

        if MosaicParams.get("sensor") in ['HLSS30_SR', 'HLSL30_SR', 'HLS_SR']:
            os.environ.setdefault('CPL_VSIL_CURL_USE_HEAD', "FALSE")
            os.environ.setdefault('GDAL_DISABLE_READDIR_ON_OPEN', "YES")
            os.environ.setdefault('GDAL_HTTP_COOKIEJAR', "/tmp/cookies.txt")
            os.environ.setdefault('GDAL_HTTP_COOKIEFILE', "/tmp/cookies.txt")

        # Dask config for worker behavior (can be changed by caller)
        def _disable_spill():
            dask.config.set({
                'distributed.comm.retry.count': 5,
                'distributed.comm.timeouts.tcp': 18000,
                'distributed.comm.timeouts.connect': 18000,
                'distributed.worker.memory.target': 1,
                'distributed.worker.memory.spill': 0.95,
                'distributed.worker.memory.terminate': 0.95
            })

        # cluster = LocalCluster(
        #     n_workers         = MosaicParams.get("number_workers", 1),
        #     threads_per_worker= MosaicParams.get("threads_per_worker", 3),
        #     memory_limit      = MosaicParams.get("node_memory", "4GB"),
        #     local_directory   = tmp_directory,
        # )

        cluster = LocalCluster(
            n_workers = MosaicParams["number_workers"],
            threads_per_worker = 4,
            memory_limit = MosaicParams["node_memory"],
            local_directory = tmp_directory)

        client = Client(cluster)
        client.register_worker_callbacks(setup=_disable_spill)
        print(f'\n\n<<<<<<<<<< Dask dashboard is available {client.dashboard_link} >>>>>>>>>')  

        # Extract a tiny grid description from BaseImg and scatter it
        base_grid = _extract_base_grid(BaseImg)

        # Scatter large static objects once and broadcast them to workers
        base_grid_f    = client.scatter(base_grid, broadcast=True)
        stac_items_f   = client.scatter(stac_items, broadcast=True)
        MosaicParams_f = client.scatter(MosaicParams, broadcast=True)
        ChunkDict_f    = client.scatter(MosaicParams.get('chunk_size', {}), broadcast=True)

        # Submit tasks: only the granule id varies per-task
        granule_futures = []
        for granule in unique_granules:
            fut = client.submit(
                get_granule_mosaic,
                base_grid_f,
                granule,
                stac_items_f,
                MosaicParams_f,
                ChunkDict_f,
                pure=False,
            )
            granule_futures.append(fut)

        # Return the futures (not computed yet) so caller can gather/persist as desired
        return granule_futures, client, cluster, unique_name, base_grid





def one_mosaicA(AllParams, Output=True):

    mosaic_start = time.time()
    ChunkDict = AllParams['chunk_size']

    # --------------------------------------------------------------------------
    # Search STAC
    # --------------------------------------------------------------------------
    stac_items = eoMz.search_STAC_Catalog(AllParams, 100)
    print(f"\n<period_mosaic> A total of {len(stac_items):d} items were found.\n")

    if AllParams["debug"]:
        eoMz.display_meta_assets(stac_items, True)

    # --------------------------------------------------------------------------
    # Build base image locally (full domain grid)
    # --------------------------------------------------------------------------
    base_img = eoMz.get_base_Image(stac_items, AllParams, ChunkDict)
    print('\n<period_mosaic> based mosaic image = ', base_img)

    # --------------------------------------------------------------------------
    # Unique granules
    # --------------------------------------------------------------------------
    unique_granules = eoMz.get_unique_tile_names(stac_items)
    print('\n<period_mosaic> The number of unique granule tiles = %d' %
          (len(unique_granules)))

    if AllParams["debug"]:
        print('\n<<<<<< The unique granule tiles = ', unique_granules)

    # --------------------------------------------------------------------------
    # Submit parallel mosaics
    #
    # NEW: rewritten create_mosaic_at_once_one_machine returns 5 objects
    #      (futures, client, cluster, unique_name, base_grid)
    # --------------------------------------------------------------------------
    if AllParams["debug"]:
        (submitted_granules, client, cluster,
         unique_name, base_grid) = create_mosaic_at_once_one_machineA(
            base_img, unique_granules, stac_items, AllParams
        )
    else:
        (submitted_granules, client, cluster,
         unique_name, base_grid) = eoMz.create_mosaic_at_once_distributed(
            base_img, unique_granules, stac_items, AllParams
        )

    # --------------------------------------------------------------------------
    # Persist granule futures and merge results one by one
    # NOTE: each granule_mosaic is already aligned using "base_grid" on worker
    # --------------------------------------------------------------------------
    persisted = dask.persist(*submitted_granules, optimize_graph=True)
    for future, gran_mos in as_completed(persisted, with_results=True):
        # ---- NEW: merging no longer needs full base_img, because
        # ----      each granule mosaic already matches base_grid.shape
        base_img = merge_granule_mosaicsA(gran_mos, base_img, eoIM.pix_score)
        client.cancel(future)

    
    # Final compute to real xarray    
    base_img = base_img.chunk(ChunkDict).compute()

    # Negative date mask
    mosaic = base_img.where(base_img[eoIM.pix_date] > 0)

    mosaic_stop = time.time()
    print('\n\n<<<<<<<<<< Total mosaic time = %6.2f minutes>>>>>>>>>' % ((mosaic_stop - mosaic_start) / 60))
    

    # Output files
    ext_tiffs_rec = ["test"]
    period_str = "test"
    if Output:
        ext_tiffs_rec, period_str = eoMz.export_mosaic(AllParams, mosaic)

    # Remove dask logs / spill dirs
    dask_out_file = Path(AllParams["out_folder"]) / f"log_{unique_name}.out"
    dask_spill_dir = Path(AllParams["out_folder"]) / f"dask_spill_{unique_name}"

    if dask_out_file.exists():
        dask_out_file.unlink()
    if dask_spill_dir.exists():
        shutil.rmtree(dask_spill_dir)    

    # Clean up cluster
    try:
        client.close()
        cluster.close()
    except Exception:
        pass


    return ext_tiffs_rec, period_str, mosaic






# -------------------------------------------------------------------------------------------------
# Merge a single granule mosaic into the base image
# -------------------------------------------------------------------------------------------------
@delayed
def merge_granule_mosaicsB(mosaic, base_img, pix_score):
    """
    Update the base image with a submosaic based on pixel score.

    Args:
        mosaic (XArray): Submosaic image.
        base_img (XArray): Base image covering the full ROI.
        pix_score (str): Name of the pixel score variable.
    Returns:
        XArray: Updated base image.
    """
    if mosaic is not None:
        mask = mosaic[pix_score] > base_img[pix_score]
        for var in base_img.data_vars:
            base_img[var] = xr.where(mask, mosaic[var], base_img[var])
        return base_img
    
    return base_img  # Return base_img even if mosaic is None to avoid losing state



'''
# -------------------------------------------------------------------------------------------------
# Generate a mosaic for a single granule
# -------------------------------------------------------------------------------------------------
def get_granule_mosaicB(Input_tuple):
    """
    Load STAC items for a granule, preprocess, compute pixel scores, and generate mosaic.

    Args:
        Input_tuple (tuple): (BaseImg, granule, StacItems, MosaicParams, ChunkDict)
    Returns:
        XArray: Mosaic for the granule
    """
    BaseImg, granule, StacItems, MosaicParams, ChunkDict = Input_tuple

    # Extract parameters
    SsrData = MosaicParams['SsrData']
    ProjStr = MosaicParams['projection']
    Scale = MosaicParams['resolution']
    Bands = MosaicParams['Criteria']['bands']
    InclAngles = MosaicParams['IncludeAngles']
    StartStr, EndStr = eoPM.get_time_window(MosaicParams)

    # Load STAC items for the granule
    one_granule_items = eoMz.get_one_granule_items(StacItems, granule)
    filtered_items = eoMz.get_unique_STAC_items(one_granule_items, MosaicParams)

    # Load datasets based on sensor type
    if 'scl' in Bands:  # Sentinel-2 only
        if Scale > 10:
            xrDS_S2 = eoMz.load_STAC_items(filtered_items['S2'], Bands, ChunkDict, ProjStr, Scale)
        else:
            Bands_20m = [b for b in Bands if b not in ['blue', 'green', 'red', 'nir08']]
            xrDS_S2 = eoMz.load_STAC_10m_items(filtered_items['S2'], Bands_20m, ChunkDict, ProjStr)
        xrDS_LS = None
    else:  # Sentinel-2 + Landsat
        S2_bands = Bands['S2'] + Bands['angle'] if InclAngles else Bands['S2']
        LS_bands = Bands['LS'] + Bands['angle'] if InclAngles else Bands['LS']
        xrDS_S2 = eoMz.load_STAC_items(filtered_items['S2'], S2_bands, ChunkDict, ProjStr, Scale)
        xrDS_LS = eoMz.load_STAC_items(filtered_items['LS'], LS_bands, ChunkDict, ProjStr, Scale)

    # Preprocess datasets
    xrDS, time_values = eoMz.preprocess_xrDS(xrDS_S2, xrDS_LS, MosaicParams)
    if xrDS is None:
        return None

    # Compute pixel score
    attach_score_args = functools.partial(eoMz.attach_score, SsrData=SsrData, StartStr=StartStr, EndStr=EndStr)
    my_chunk = {'x': ChunkDict['x'], 'y': ChunkDict['y'], 'time': xrDS.sizes['time']}
    xrDS = xrDS.chunk(my_chunk).map_blocks(attach_score_args, template=xrDS.chunk(my_chunk))
    xrDS = xrDS.fillna(-10000.0).chunk(my_chunk)

    # Template for per-granule mosaic
    def granule_mosaic_template(xrDS, inBands, IncAngles):
        mosaic_template = {}
        xrDA = xr.DataArray(
            data=dask.array.zeros((xrDS.sizes['y'], xrDS.sizes['x']),
                                  chunks=(ChunkDict['x'], ChunkDict['y']),
                                  dtype=np.float32),
            dims=['y', 'x'],
            coords={'y': xrDS['y'], 'x': xrDS['x']}
        )
        for var_name in xrDS.data_vars:
            mosaic_template[var_name] = xrDA
        if IncAngles and 'scl' in inBands:
            for angle in ['SZA', 'VZA', 'SAA', 'VAA']:
                mosaic_template[angle] = xrDA
        return xr.Dataset(mosaic_template)

    # Function to compute mosaic for this granule
    def granule_mosaic(xrDS, Items, pix_score, time_values_len, inBands, IncAngles):
        xrDS['time_index'] = xr.DataArray(np.arange(time_values_len, dtype='uint8'), dims=['time'])
        max_indices = xrDS[pix_score].argmax(dim="time")
        mosaic = xrDS.isel(time=max_indices)

        nS2_imgs = len(Items['S2'])
        if IncAngles and 'scl' in inBands and nS2_imgs > 0:
            mosaic = eoMz.attach_AngleBands(mosaic, Items['S2'])

        return mosaic.drop_vars(["time", "spatial_ref", "time_index"])

    granule_mosaic_args = functools.partial(
        granule_mosaic,
        Items=filtered_items,
        pix_score=eoIM.pix_score,
        time_values_len=len(time_values),
        inBands=Bands,
        IncAngles=InclAngles
    )

    mosaic = xrDS.map_blocks(granule_mosaic_args, template=granule_mosaic_template(xrDS, Bands, InclAngles))
    mosaic = mosaic.where(mosaic[eoIM.pix_date] > 0)
    mosaic = mosaic.reindex_like(BaseImg).chunk(ChunkDict).fillna(-10000.0)

    del xrDS, granule_mosaic_args
    gc.collect()

    return mosaic

# -------------------------------------------------------------------------------------------------
# Create per-granule mosaics on a single machine using a Dask LocalCluster
# -------------------------------------------------------------------------------------------------
def create_mosaic_at_once_one_machineB(BaseImg, unique_granules, stac_items, MosaicParams):
    """
    Create Dask LocalCluster and submit per-granule mosaic tasks (Option B: broadcast BaseImg).

    Returns:
        granule_futures (list)
        client (Dask Client)
        cluster (LocalCluster)
        unique_name (str)
    """
    unique_name = str(uuid.uuid4())
    tmp_directory = os.path.join(Path(MosaicParams["out_folder"]), f"dask_spill_{unique_name}")
    os.makedirs(tmp_directory, exist_ok=True)

    # Reduce verbose logging
    logging.getLogger('tornado').setLevel(logging.WARNING)
    logging.getLogger('tornado.application').setLevel(logging.CRITICAL)
    logging.getLogger('tornado.access').setLevel(logging.CRITICAL)
    logging.getLogger('tornado.general').setLevel(logging.CRITICAL)
    logging.getLogger('bokeh.server.protocol_handler').setLevel(logging.CRITICAL)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")

        # Linux proxy
        if platform.system() == "Linux":
            os.environ.setdefault('http_proxy', "http://webproxy.science.gc.ca:8888/")
            os.environ.setdefault('https_proxy', "http://webproxy.science.gc.ca:8888/")

        # HLS specific GDAL settings
        if MosaicParams.get("sensor") in ['HLSS30_SR', 'HLSL30_SR', 'HLS_SR']:
            os.environ.setdefault('CPL_VSIL_CURL_USE_HEAD', "FALSE")
            os.environ.setdefault('GDAL_DISABLE_READDIR_ON_OPEN', "YES")
            os.environ.setdefault('GDAL_HTTP_COOKIEJAR', "/tmp/cookies.txt")
            os.environ.setdefault('GDAL_HTTP_COOKIEFILE', "/tmp/cookies.txt")

        # Worker memory config
        def _disable_spill():
            dask.config.set({
                'distributed.comm.retry.count': 5,
                'distributed.comm.timeouts.tcp': 18000,
                'distributed.comm.timeouts.connect': 18000,
                'distributed.worker.memory.target': 1.0,
                'distributed.worker.memory.spill': 0.95,
                'distributed.worker.memory.terminate': 0.95
            })

        cluster = LocalCluster(
            n_workers=MosaicParams.get("number_workers", 1),
            threads_per_worker=MosaicParams.get("threads_per_worker", 3),
            memory_limit=MosaicParams.get("node_memory", "4GB"),
            local_directory=tmp_directory
        )

        client = Client(cluster)
        client.register_worker_callbacks(setup=_disable_spill)
        print(f"\n\n<<<<<<<<<< Dask dashboard is available {client.dashboard_link} >>>>>>>>>")

        # Broadcast objects
        base_img_f     = client.scatter(BaseImg, broadcast=True)
        stac_items_f   = client.scatter(stac_items, broadcast=True)
        MosaicParams_f = client.scatter(MosaicParams, broadcast=True)
        ChunkDict_f    = client.scatter(MosaicParams.get('chunk_size', {}), broadcast=True)

        # Submit tasks
        granule_futures = [
            client.submit(get_granule_mosaicB, (base_img_f, g, stac_items_f, MosaicParams_f, ChunkDict_f), pure=False)
            for g in unique_granules
        ]

    return granule_futures, client, cluster, unique_name

# -------------------------------------------------------------------------------------------------
# Main function to create full-period mosaic
# -------------------------------------------------------------------------------------------------
def one_mosaicB(AllParams, Output=True):
    """
    Full-period mosaic creation using per-granule parallelization.

    Args:
        AllParams (dict): Parameters for production and computation.
        Output (bool): Whether to export the mosaic to disk.
    Returns:
        ext_tiffs_rec (list), period_str (str), mosaic (XArray)
    """
    mosaic_start = time.time()
    ChunkDict = AllParams['chunk_size']

    # Search STAC items
    stac_items = eoMz.search_STAC_Catalog(AllParams, 100)
    print(f"\n<period_mosaic> A total of {len(stac_items)} items were found.\n")

    if AllParams.get("debug"):
        eoMz.display_meta_assets(stac_items, True)

    # Base image
    base_img = eoMz.get_base_Image(stac_items, AllParams, ChunkDict)
    print(f'\n<period_mosaic> base mosaic image = {base_img}')

    # Unique granules
    unique_granules = eoMz.get_unique_tile_names(stac_items)
    print(f'\n<period_mosaic> The number of unique granule tiles = {len(unique_granules)}')
    if AllParams.get("debug"):
        print(f'\n<<<<<< The unique granule tiles = {unique_granules}')

    # Create per-granule mosaics (Option B)
    granule_futures, client, cluster, unique_name = create_mosaic_at_once_one_machineB(
        base_img, unique_granules, stac_items, AllParams
    )

    persisted_futures = dask.persist(*granule_futures, optimize_graph=True)
    for future, granule_mosaic in as_completed(persisted_futures, with_results=True):
        base_img = eoMz.merge_granule_mosaics(granule_mosaic, base_img, eoIM.pix_score).compute()
        client.cancel(future)

    # Mask negative date pixels
    mosaic = base_img.where(base_img[eoIM.pix_date] > 0)

    mosaic_stop = time.time()
    print(f'\n\n<<<<<<<<<< Total elapsed time = {(mosaic_stop - mosaic_start)/60:.2f} minutes >>>>>>>>')

    # Export if needed
    ext_tiffs_rec = ["test"]
    period_str = "test"
    if Output:
        ext_tiffs_rec, period_str = eoMz.export_mosaic(AllParams, mosaic)

    # Clean logs and tmp directories
    dask_out_file  = Path(AllParams["out_folder"]) / f"log_{unique_name}.out"
    dask_directory = os.path.join(Path(AllParams["out_folder"]), f"dask_spill_{unique_name}")

    if os.path.exists(dask_out_file):
        os.remove(dask_out_file)
    if os.path.exists(dask_directory):
        shutil.rmtree(dask_directory)

    # Close cluster
    try:
        client.close()
        cluster.close()
    except asyncio.CancelledError:
        print("Cluster is already closed.")

    return ext_tiffs_rec, period_str, mosaic
'''


# -------------------------------------------------------------------------------------------------
# Merge a single granule mosaic into the base image
# -------------------------------------------------------------------------------------------------
@delayed
def merge_granule_mosaicsB(mosaic, base_img, pix_score):
    """
    Update the base image with a submosaic based on pixel score.

    Args:
        mosaic (xarray.Dataset): Submosaic image.
        base_img (xarray.Dataset): Base image covering the full ROI.
        pix_score (str): Name of the pixel score variable.
    Returns:
        xarray.Dataset: Updated base image.
    """
    if mosaic is not None:
        mask = mosaic[pix_score] > base_img[pix_score]
        for var in base_img.data_vars:
            base_img[var] = xr.where(mask, mosaic[var], base_img[var])
    return base_img


# -------------------------------------------------------------------------------------------------
# Generate a mosaic for a single granule
# -------------------------------------------------------------------------------------------------
def get_granule_mosaicB(BaseImg_template, granule, StacItems, MosaicParams, ChunkDict):
    """
    Load STAC items for a granule, preprocess, compute pixel scores, and generate mosaic.

    Args:
        BaseImg_template (xarray.Dataset): Template with coords of BaseImg (no pixel data required)
        granule (str): Unique granule name
        StacItems (list): STAC items
        MosaicParams (dict): Mosaic parameters
        ChunkDict (dict): Chunk size
    Returns:
        xarray.Dataset: Mosaic for the granule
    """
    import gc
    import functools
    import dask.array as da

    # Extract parameters
    SsrData = MosaicParams['SsrData']
    ProjStr = MosaicParams['projection']
    Scale = MosaicParams['resolution']
    Bands = MosaicParams['Criteria']['bands']
    InclAngles = MosaicParams['IncludeAngles']
    StartStr, EndStr = eoPM.get_time_window(MosaicParams)

    # Load STAC items for the granule
    one_granule_items = eoMz.get_one_granule_items(StacItems, granule)
    filtered_items = eoMz.get_unique_STAC_items(one_granule_items, MosaicParams)

    # Load datasets
    if 'scl' in Bands:  # Sentinel-2 only
        if Scale > 10:
            xrDS_S2 = eoMz.load_STAC_items(filtered_items['S2'], Bands, ChunkDict, ProjStr, Scale)
        else:
            Bands_20m = [b for b in Bands if b not in ['blue', 'green', 'red', 'nir08']]
            xrDS_S2 = eoMz.load_STAC_10m_items(filtered_items['S2'], Bands_20m, ChunkDict, ProjStr)
        xrDS_LS = None
    else:  # Sentinel-2 + Landsat
        S2_bands = Bands['S2'] + Bands['angle'] if InclAngles else Bands['S2']
        LS_bands = Bands['LS'] + Bands['angle'] if InclAngles else Bands['LS']
        xrDS_S2 = eoMz.load_STAC_items(filtered_items['S2'], S2_bands, ChunkDict, ProjStr, Scale)
        xrDS_LS = eoMz.load_STAC_items(filtered_items['LS'], LS_bands, ChunkDict, ProjStr, Scale)

    # Preprocess datasets
    xrDS, time_values = eoMz.preprocess_xrDS(xrDS_S2, xrDS_LS, MosaicParams)
    if xrDS is None:
        return None

    # Compute pixel score
    attach_score_args = functools.partial(
        eoMz.attach_score,
        SsrData=SsrData,
        StartStr=StartStr,
        EndStr=EndStr
    )
    my_chunk = {'x': ChunkDict['x'], 'y': ChunkDict['y'], 'time': xrDS.sizes['time']}
    xrDS = xrDS.chunk(my_chunk).map_blocks(attach_score_args, template=xrDS.chunk(my_chunk))
    xrDS = xrDS.fillna(-10000.0).chunk(my_chunk)

    # Template for mosaic
    def granule_mosaic_template(xrDS, inBands, InclAngles):
        mosaic_template = {}
        xrDA = xr.DataArray(
            data=da.zeros((xrDS.sizes['y'], xrDS.sizes['x']), chunks=(ChunkDict['y'], ChunkDict['x']), dtype=np.float32),
            dims=['y', 'x'],
            coords={'y': xrDS['y'], 'x': xrDS['x']}
        )
        for var_name in xrDS.data_vars:
            mosaic_template[var_name] = xrDA
        if InclAngles and 'scl' in inBands:
            for angle in ['SZA', 'VZA', 'SAA', 'VAA']:
                mosaic_template[angle] = xrDA
        return xr.Dataset(mosaic_template)

    # Function to compute mosaic
    def granule_mosaic(xrDS, Items, pix_score, time_values_len, inBands, InclAngles):
        xrDS['time_index'] = xr.DataArray(np.arange(time_values_len, dtype='uint8'), dims=['time'])
        max_indices = xrDS[pix_score].argmax(dim="time")
        mosaic = xrDS.isel(time=max_indices)

        nS2_imgs = len(Items['S2'])
        if InclAngles and 'scl' in inBands and nS2_imgs > 0:
            mosaic = eoMz.attach_AngleBands(mosaic, Items['S2'])

        return mosaic.drop_vars(["time", "spatial_ref", "time_index"])

    granule_mosaic_args = functools.partial(
        granule_mosaic,
        Items=filtered_items,
        pix_score=eoIM.pix_score,
        time_values_len=len(time_values),
        inBands=Bands,
        InclAngles=InclAngles
    )

    mosaic = xrDS.map_blocks(granule_mosaic_args, template=granule_mosaic_template(xrDS, Bands, InclAngles))
    mosaic = mosaic.where(mosaic[eoIM.pix_date] > 0)

    # Reindex **after returning to driver**, not inside workers
    mosaic = mosaic.chunk(ChunkDict).fillna(-10000.0)

    del xrDS, granule_mosaic_args
    gc.collect()

    return mosaic


# -------------------------------------------------------------------------------------------------
# Create per-granule mosaics on a single machine using Dask LocalCluster
# -------------------------------------------------------------------------------------------------
def create_mosaic_at_once_one_machineB(BaseImg, unique_granules, stac_items, MosaicParams):
    """
    Submit per-granule mosaic tasks (Option B: broadcast BaseImg template).

    Returns:
        granule_futures (list)
        client (Dask Client)
        cluster (LocalCluster)
        unique_name (str)
    """
    import warnings
    import logging

    unique_name = str(uuid.uuid4())
    tmp_directory = os.path.join(Path(MosaicParams["out_folder"]), f"dask_spill_{unique_name}")
    os.makedirs(tmp_directory, exist_ok=True)

    logging.getLogger('tornado').setLevel(logging.WARNING)
    logging.getLogger('tornado.application').setLevel(logging.CRITICAL)
    logging.getLogger('tornado.access').setLevel(logging.CRITICAL)
    logging.getLogger('tornado.general').setLevel(logging.CRITICAL)
    logging.getLogger('bokeh.server.protocol_handler').setLevel(logging.CRITICAL)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")

        # Linux proxy
        if platform.system() == "Linux":
            os.environ.setdefault('http_proxy', "http://webproxy.science.gc.ca:8888/")
            os.environ.setdefault('https_proxy', "http://webproxy.science.gc.ca:8888/")

        # HLS GDAL settings
        if MosaicParams.get("sensor") in ['HLSS30_SR', 'HLSL30_SR', 'HLS_SR']:
            os.environ.setdefault('CPL_VSIL_CURL_USE_HEAD', "FALSE")
            os.environ.setdefault('GDAL_DISABLE_READDIR_ON_OPEN', "YES")
            os.environ.setdefault('GDAL_HTTP_COOKIEJAR', "/tmp/cookies.txt")
            os.environ.setdefault('GDAL_HTTP_COOKIEFILE', "/tmp/cookies.txt")

        # Worker memory config
        def _disable_spill():
            dask.config.set({
                'distributed.comm.retry.count': 5,
                'distributed.comm.timeouts.tcp': 18000,
                'distributed.comm.timeouts.connect': 18000,
                'distributed.worker.memory.target': 1.0,
                'distributed.worker.memory.spill': 0.95,
                'distributed.worker.memory.terminate': 0.95
            })

        cluster = LocalCluster(
            n_workers=MosaicParams.get("number_workers", 1),
            threads_per_worker=MosaicParams.get("threads_per_worker", 3),
            memory_limit=MosaicParams.get("node_memory", "4GB"),
            local_directory=tmp_directory
        )

        client = Client(cluster)
        client.register_worker_callbacks(setup=_disable_spill)
        print(f"\n\n<<<<<<<<<< Dask dashboard is available {client.dashboard_link} >>>>>>>>>")

        # Broadcast BaseImg template and other objects
        BaseImg_template = BaseImg.copy()
        BaseImg_template = BaseImg_template.isel(**{v: slice(0, 0) for v in ['y', 'x']})  # keep coords only
        base_img_f = client.scatter(BaseImg_template, broadcast=True)
        stac_items_f = client.scatter(stac_items, broadcast=True)
        MosaicParams_f = client.scatter(MosaicParams, broadcast=True)

        # Submit tasks
        granule_futures = [
            client.submit(get_granule_mosaicB, base_img_f, g, stac_items_f, MosaicParams_f, MosaicParams['chunk_size'], pure=False)
            for g in unique_granules
        ]

    return granule_futures, client, cluster, unique_name


# -------------------------------------------------------------------------------------------------
# Main function to create full-period mosaic
# -------------------------------------------------------------------------------------------------
def one_mosaicB(AllParams, Output=True):
    """
    Full-period mosaic creation using per-granule parallelization.

    Returns:
        ext_tiffs_rec (list), period_str (str), mosaic (XArray)
    """
    import dask
    from dask.distributed import as_completed
    import gc
    import shutil

    mosaic_start = time.time()
    ChunkDict = AllParams['chunk_size']

    # Search STAC items
    stac_items = eoMz.search_STAC_Catalog(AllParams, 100)
    print(f"\n<period_mosaic> A total of {len(stac_items)} items were found.\n")

    if AllParams.get("debug"):
        eoMz.display_meta_assets(stac_items, True)

    # Base image
    base_img = eoMz.get_base_Image(stac_items, AllParams, ChunkDict)
    print(f'\n<period_mosaic> base mosaic image = {base_img}')

    # Unique granules
    unique_granules = eoMz.get_unique_tile_names(stac_items)
    print(f'\n<period_mosaic> The number of unique granule tiles = {len(unique_granules)}')
    if AllParams.get("debug"):
        print(f'\n<<<<<< The unique granule tiles = {unique_granules}')

    # Submit per-granule mosaics
    granule_futures, client, cluster, unique_name = create_mosaic_at_once_one_machineB(
        base_img, unique_granules, stac_items, AllParams
    )

    # Merge mosaics using delayed
    merged = [merge_granule_mosaicsB(f, base_img, eoIM.pix_score) for f in granule_futures]
    base_img = dask.compute(*merged)[-1]  # compute once at the end

    # Mask negative date pixels
    mosaic = base_img.where(base_img[eoIM.pix_date] > 0)

    mosaic_stop = time.time()
    print(f'\n\n<<<<<<<<<< Total elapsed time = {(mosaic_stop - mosaic_start)/60:.2f} minutes >>>>>>>>')

    # Export if needed
    ext_tiffs_rec = ["test"]
    period_str = "test"
    if Output:
        ext_tiffs_rec, period_str = eoMz.export_mosaic(AllParams, mosaic)

    # Clean logs and tmp directories
    dask_out_file  = Path(AllParams["out_folder"]) / f"log_{unique_name}.out"
    dask_directory = os.path.join(Path(AllParams["out_folder"]), f"dask_spill_{unique_name}")

    if os.path.exists(dask_out_file):
        os.remove(dask_out_file)
    if os.path.exists(dask_directory):
        shutil.rmtree(dask_directory)

    # Close cluster
    try:
        client.close()
        cluster.close()
    except asyncio.CancelledError:
        print("Cluster is already closed.")

    return ext_tiffs_rec, period_str, mosaic




# Option C — Robust per-granule compute on workers, sequential merge on driver
# ---------------------------------------------------------------------------
from pathlib import Path
import os
import uuid
import time
import gc
import shutil
import warnings
import logging
import platform


import numpy as np
import xarray as xr
import dask
import dask.array as da
from dask.distributed import Client, LocalCluster, as_completed
from dask import delayed

# ---------------------------------------------------------------------------
# Helper: worker memory config callback
# ---------------------------------------------------------------------------
def _disable_spill():
    """Worker callback to adjust Dask config (registered via client.register_worker_callbacks)."""
    dask.config.set({
        'distributed.comm.retry.count': 5,
        'distributed.comm.timeouts.tcp': 18000,
        'distributed.comm.timeouts.connect': 18000,
        # conservative defaults; tweak if needed
        'distributed.worker.memory.target': 1,
        'distributed.worker.memory.spill': 0.95,
        'distributed.worker.memory.terminate': 0.95
    })

# ---------------------------------------------------------------------------
# Merge function executed on the DRIVER (synchronous, not delayed)
# ---------------------------------------------------------------------------
def merge_into_base(mosaic, base_img, pix_score):
    """
    Merge a fully-computed (in-memory) mosaic into base_img based on pix_score.
    Returns a new xarray.Dataset (does not mutate inputs in-place).
    """
    if mosaic is None:
        return base_img

    # Ensure dims align (simple guard)
    if not set(base_img.dims).issuperset(set(mosaic.dims)):
        raise ValueError("Dimension mismatch between base_img and mosaic during merge.")

    # Build mask where mosaic has higher score
    mask = mosaic[pix_score] > base_img[pix_score]

    merged_vars = {}
    for var in base_img.data_vars:
        if var in mosaic.data_vars:
            merged_vars[var] = xr.where(mask, mosaic[var], base_img[var])
        else:
            merged_vars[var] = base_img[var]

    merged = xr.Dataset(merged_vars, coords=base_img.coords, attrs=base_img.attrs)
    return merged


# ---------------------------------------------------------------------------
# Worker function: compute per-granule mosaic fully on the worker, return in-memory xarray.Dataset
# ---------------------------------------------------------------------------
def get_granule_mosaicC(granule, StacItems, MosaicParams, ChunkDict):
    """
    Worker-side: load STAC items for this granule, preprocess, compute scores,
                 generate the per-granule mosaic, compute it to in-memory, and return.

    Args:
      granule (str)
      StacItems (list/dict) - resolved by Dask before calling worker
      MosaicParams (dict) - resolved by Dask before calling worker
      ChunkDict (dict) - plain dict with 'x','y' chunk sizes
    Returns:
      xarray.Dataset (in-memory) or None on failure
    """

    try:
        # Extract parameters
        SsrData = MosaicParams['SsrData']
        ProjStr = MosaicParams['projection']
        Scale = MosaicParams['resolution']
        Bands = MosaicParams['Criteria']['bands']
        InclAngles = MosaicParams.get('IncludeAngles', False)
        StartStr, EndStr = eoPM.get_time_window(MosaicParams)

        # 1) find STAC items for this granule and pick unique timestamps
        one_granule_items = eoMz.get_one_granule_items(StacItems, granule)
        filtered_items = eoMz.get_unique_STAC_items(one_granule_items, MosaicParams)

        # 2) load data (xarray.Dataset, possibly dask-backed)
        if 'scl' in Bands:
            if Scale > 10:
                xrDS = eoMz.load_STAC_items(filtered_items['S2'], Bands, ChunkDict, ProjStr, Scale)
            else:
                Bands_20m = [b for b in Bands if b not in ['blue', 'green', 'red', 'nir08']]
                xrDS = eoMz.load_STAC_10m_items(filtered_items['S2'], Bands_20m, ChunkDict, ProjStr)
        else:
            S2_bands = Bands['S2'] + Bands.get('angle', []) if InclAngles else Bands['S2']
            LS_bands = Bands['LS'] + Bands.get('angle', []) if InclAngles else Bands['LS']
            xrDS_S2 = eoMz.load_STAC_items(filtered_items['S2'], S2_bands, ChunkDict, ProjStr, Scale)
            xrDS_LS = eoMz.load_STAC_items(filtered_items['LS'], LS_bands, ChunkDict, ProjStr, Scale)
            xrDS, _ = eoMz.preprocess_xrDS(xrDS_S2, xrDS_LS, MosaicParams)

        # 3) preprocess (add sensor type, score layers, masks)
        xrDS, time_values = eoMz.preprocess_xrDS(xrDS if 'xrDS' in locals() else None, None, MosaicParams)
        if xrDS is None:
            return None

        # 4) attach compositing score
        attach_score_args = functools.partial(eoMz.attach_score, SsrData=SsrData, StartStr=StartStr, EndStr=EndStr)
        my_chunk = {'x': ChunkDict['x'], 'y': ChunkDict['y'], 'time': xrDS.sizes['time']}
        xrDS = xrDS.chunk(my_chunk).map_blocks(attach_score_args, template=xrDS.chunk(my_chunk))
        xrDS = xrDS.fillna(-10000.0).chunk(my_chunk)

        # 5) compute the per-granule mosaic (argmax over time)
        pix_score = eoIM.pix_score

        '''
        # compute argmax indexer (may be dask-backed) then materialize to NumPy
        max_indices_da = xrDS[pix_score].argmax(dim='time')

        # Materialize the indexer: prefer compute() on dask array or DataArray
        # This computes a y,x integer array on the worker (should be much smaller than whole xrDS)
        if hasattr(max_indices_da.data, "compute"):
            max_indices_np = max_indices_da.data.compute()
        else:
            # if data is numpy-backed already
            max_indices_np = np.asarray(max_indices_da.values)
        '''

        # --------------------------------------------
        # SAFE ARGMAX IMPLEMENTATION (CHUNK-WISE)
        # --------------------------------------------
        # xrDS[pix_score] has dims (time, y, x)
        score = xrDS[pix_score]

        # Force loading the *score* only into memory
        # We expect it to be manageable because it is only 1 variable
        score_np = score.compute().values   # shape (T, Y, X)

        # Compute the index of maximum score along time axis
        max_indices_np = np.argmax(score_np, axis=0)   # shape (Y, X)

        # Free score
        del score_np
        gc.collect()


        # Ensure numpy array, squeeze if needed
        max_indices_np = np.asarray(max_indices_np)
        # Remove singleton dims if any (e.g., shape (y, x, 1) etc.)
        max_indices_np = np.squeeze(max_indices_np)

        # Ensure shape is (ny, nx)
        # Obtain target spatial sizes from xrDS coords (y first, x second)
        try:
            # Get coordinate arrays; compute if dask-backed
            y_coord = xrDS['y'].data.compute() if hasattr(xrDS['y'].data, "compute") else np.asarray(xrDS['y'].values)
        except Exception:
            y_coord = np.asarray(xrDS['y'].values)
        try:
            x_coord = xrDS['x'].data.compute() if hasattr(xrDS['x'].data, "compute") else np.asarray(xrDS['x'].values)
        except Exception:
            x_coord = np.asarray(xrDS['x'].values)

        ny = y_coord.shape[0]
        nx = x_coord.shape[0]

        # If max_indices_np is 1D of length ny*nx, reshape; if 2D but transposed, fix.
        if max_indices_np.size == ny * nx and max_indices_np.ndim == 1:
            max_indices_np = max_indices_np.reshape((ny, nx))
        elif max_indices_np.ndim == 2:
            if max_indices_np.shape != (ny, nx):
                # try transpose
                if max_indices_np.shape == (nx, ny):
                    max_indices_np = max_indices_np.T
                else:
                    # attempt to broadcast or reshape if trivial
                    raise IndexError(f"Computed argmax shape {max_indices_np.shape} doesn't match expected {(ny,nx)}")
        else:
            # unexpected shape
            raise IndexError(f"Computed argmax has unexpected shape {max_indices_np.shape}, expected (ny,nx)={(ny,nx)}")

        # Clip indices to valid range [0, time-1]
        tsize = xrDS.sizes['time']
        max_indices_np = np.clip(max_indices_np.astype('int64'), 0, max(0, tsize - 1))

        # Build a labeled DataArray indexer with dims ('y','x') and coords from xrDS
        indexer_da = xr.DataArray(max_indices_np, dims=('y', 'x'), coords={'y': y_coord, 'x': x_coord})

        # Now use isel with labeled indexer
        mosaic = xrDS.isel(time=indexer_da)

        # attach angle bands if needed
        nS2_imgs = len(filtered_items.get('S2', []))
        if InclAngles and 'scl' in Bands and nS2_imgs > 0:
            mosaic = eoMz.attach_AngleBands(mosaic, filtered_items['S2'])

        # drop variables not needed
        drop_vars = [v for v in ["time", "spatial_ref"] if v in mosaic]
        mosaic = mosaic.drop_vars(drop_vars, errors='ignore')

        # 6) finalize: rechunk to desired chunking and compute into memory on worker
        mosaic = mosaic.chunk(ChunkDict).fillna(-10000.0)

        # Force compute on worker to return concrete numpy-backed arrays to driver
        mosaic = mosaic.compute()

        # cleanup
        try:
            del xrDS
        except Exception:
            pass
        gc.collect()

        return mosaic

    except Exception as exc:
        # Log trace to help debugging and return None instead of crashing worker
        try:
            logging.exception(f"get_granule_mosaicC failed for granule {granule}: {exc}\n{traceback.format_exc()}")
        except Exception:
            pass
        # ensure we don't leave dask in undefined state
        try:
            gc.collect()
        except Exception:
            pass
        return None


# ---------------------------------------------------------------------------
# Create per-granule mosaic tasks on LocalCluster (Option C)
# ---------------------------------------------------------------------------
def create_mosaic_at_once_one_machineC(unique_granules, stac_items, MosaicParams):
    """
    Launch workers to compute per-granule mosaics. DOES NOT broadcast BaseImg.
    Returns:
      - list of futures
      - client
      - cluster
      - unique_name
    """
    unique_name = str(uuid.uuid4())
    tmp_directory = os.path.join(Path(MosaicParams["out_folder"]), f"dask_spill_{unique_name}")
    os.makedirs(tmp_directory, exist_ok=True)

    # quiet logs
    logging.getLogger('tornado').setLevel(logging.WARNING)
    logging.getLogger('tornado.application').setLevel(logging.CRITICAL)
    logging.getLogger('tornado.access').setLevel(logging.CRITICAL)
    logging.getLogger('tornado.general').setLevel(logging.CRITICAL)
    logging.getLogger('bokeh.server.protocol_handler').setLevel(logging.CRITICAL)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")

        # platform settings
        if platform.system() == "Linux":
            os.environ.setdefault('http_proxy', "http://webproxy.science.gc.ca:8888/")
            os.environ.setdefault('https_proxy', "http://webproxy.science.gc.ca:8888/")

        # HLS settings (if needed)
        if MosaicParams.get("sensor") in ['HLSS30_SR', 'HLSL30_SR', 'HLS_SR']:
            os.environ.setdefault('CPL_VSIL_CURL_USE_HEAD', "FALSE")
            os.environ.setdefault('GDAL_DISABLE_READDIR_ON_OPEN', "YES")
            os.environ.setdefault('GDAL_HTTP_COOKIEJAR', "/tmp/cookies.txt")
            os.environ.setdefault('GDAL_HTTP_COOKIEFILE', "/tmp/cookies.txt")

        # setup Dask LocalCluster
        cluster = LocalCluster(
            n_workers = MosaicParams["number_workers"],
            threads_per_worker = 4,
            memory_limit = MosaicParams["node_memory"],
            local_directory = tmp_directory)
        
        client = Client(cluster)
        client.register_worker_callbacks(setup=_disable_spill)
        print(f"\n\n<<<<<<<<<< Dask dashboard is available {client.dashboard_link} >>>>>>>>>")

        # scatter static large objects (STAC catalog and mosaic params)
        stac_items_future   = client.scatter(stac_items,   broadcast=True)
        MosaicParams_future = client.scatter(MosaicParams, broadcast=True)

        ChunkDict = MosaicParams['chunk_size']

        # submit tasks
        futures = []
        for granule in unique_granules:
            fut = client.submit(
                get_granule_mosaicC,
                granule,
                stac_items_future,
                MosaicParams_future,
                ChunkDict,
                pure=False
            )
            futures.append(fut)

        return futures, client, cluster, unique_name





# ---------------------------------------------------------------------------
# Main pipeline (Option C): compute per-granule mosaics on workers, reindex+merge on driver
# ---------------------------------------------------------------------------
def one_mosaicC(AllParams, Output=True):
    """
    Option C: robust workflow.
    - Workers compute per-granule mosaics and return in-memory xarray Datasets.
    - Driver reindexes each mosaic to BaseImg and merges sequentially.
    Returns:
      ext_tiffs_rec, period_str, mosaic (xarray.Dataset)
    """
    mosaic_start = time.time()
    ChunkDict = AllParams['chunk_size']

    # 1) search STAC items (user function)
    stac_items = eoMz.search_STAC_Catalog(AllParams, 100)
    print(f"\n<period_mosaic> A total of {len(stac_items)} items were found.\n")
    if AllParams.get('debug', False):
        eoMz.display_meta_assets(stac_items, True)

    # 2) build base image (driver holds full BaseImg)
    base_img = eoMz.get_base_Image(stac_items, AllParams, ChunkDict)
    print(f'\n<period_mosaic> base mosaic image = {base_img}')

    # 3) unique granules
    unique_granules = eoMz.get_unique_tile_names(stac_items)
    print(f'\n<period_mosaic> Number of unique granules = {len(unique_granules)}')
    if AllParams.get('debug', False):
        print(unique_granules)

    # 4) submit per-granule tasks (do NOT send BaseImg)
    print('<one_mosaicC> Step 4: Submitting per-granule mosaic tasks to Dask workers...')
    futures, client, cluster, unique_name = create_mosaic_at_once_one_machineC(
        unique_granules, stac_items, AllParams
    )

    # 5) As results finish, gather them and merge sequentially on driver
    print('<one_mosaicC> Step 5: Merging per-granule mosaics on driver as they complete...')
    pix_score = eoIM.pix_score
    merged_count = 0
    total = len(futures)

    def show_progress(done, total, bar_len=30):
        filled = int(bar_len * done / total)
        bar = "#" * filled + "-" * (bar_len - filled)
        print(f"\r[{bar}] {done}/{total} mosaics ready...", end="", flush=True)

    print("")  # blank line before progress bar
    
    '''
    try:
        for future in as_completed(futures, with_results=True):
            finished_future, mosaic_result = future  # tuple (future, result)
            # mosaic_result is an in-memory xarray.Dataset (or None)
            if mosaic_result is None:
                client.cancel(finished_future)
                continue

            # Reindex mosaic to driver base_img grid (do this on driver)
            try:
                mosaic_aligned = mosaic_result.reindex_like(base_img, method=None)
            except Exception:
                mosaic_aligned = mosaic_result
                mosaic_aligned = mosaic_aligned.assign_coords(x=base_img['x'], y=base_img['y'])
                mosaic_aligned = mosaic_aligned.reindex_like(base_img, method=None)

            mosaic_aligned = mosaic_aligned.fillna(-10000.0)

            # Merge synchronously on driver
            base_img = merge_into_base(mosaic_aligned, base_img, pix_score)

            merged_count += 1
            client.cancel(finished_future)  # free worker memory

    finally:
        try:
            client.close()
            cluster.close()
        except Exception:
            pass
    '''
    try:
        for finished_future, mosaic_result in as_completed(futures, with_results=True):
            # Print progress immediately when a future finishes
            merged_count += 1
            show_progress(merged_count, total)

            # If mosaic is None skip
            if mosaic_result is None:
                client.cancel(finished_future)
                continue

            # Align mosaic to base grid
            try:
                mosaic_aligned = mosaic_result.reindex_like(base_img, method=None)
            except Exception:
                mosaic_aligned = mosaic_result.assign_coords(
                    x=base_img["x"],
                    y=base_img["y"]
                ).reindex_like(base_img, method=None)

            mosaic_aligned = mosaic_aligned.fillna(-10000.0)

            # Merge into base synchronously
            base_img = merge_into_base(mosaic_aligned, base_img, pix_score)

            client.cancel(finished_future)  # Free worker memory

    finally:
        # Finish the progress bar
        print("\n")
        try:
            client.close()
            cluster.close()
        except Exception:
            pass

    # 6) Final mask and compute (base_img already in-memory)
    mosaic = base_img.where(base_img[eoIM.pix_date] > 0)

    mosaic_stop = time.time()
    print(f'\n\n<<<<<<<<<< Total elapsed time = {(mosaic_stop - mosaic_start)/60:.2f} minutes >>>>>>>>')
    print(f'Number of granules merged: {merged_count}')

    # 7) Export result if requested
    ext_tiffs_rec = []
    period_str = ""
    if Output:
        ext_tiffs_rec, period_str = eoMz.export_mosaic(AllParams, mosaic)

    # 8) Clean temporary files
    dask_out_file = Path(AllParams["out_folder"]) / f"log_{unique_name}.out"
    dask_directory = os.path.join(Path(AllParams["out_folder"]), f"dask_spill_{unique_name}")
    if os.path.exists(dask_out_file):
        os.remove(dask_out_file)
    if os.path.exists(dask_directory):
        shutil.rmtree(dask_directory)

    return ext_tiffs_rec, period_str, mosaic
