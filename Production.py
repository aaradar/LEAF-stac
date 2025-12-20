import os
#import re
import sys
import copy
#import subprocess
import argparse
from pathlib import Path


#Get the absolute path to the parent of current working directory 
cwd    = os.getcwd()
source_path = os.path.join(cwd, 'source')
sys.path.append(source_path)
if str(Path(__file__).parents[0]) not in sys.path:
    sys.path.insert(0, str(Path(__file__).parents[0]))
if str(Path(__file__).parents[0]) not in sys.path:
    sys.path.insert(0, str(Path(__file__).parents[0]))

import source.eoMosaic as eoMz
import source.eoParams as eoPM
import source.eoTileGrids as eoTG
import source.eoMosaicChatGPT as AIMz
import source.LEAFProduction as leaf


def gdal_mosaic_rasters(sorted_files_to_mosaic:list, merge_output_file:str):
        
    options = ['-of', 'GTiff', '-v']
    gdal_merge_command = ['gdal_merge.py', '-o', merge_output_file] + options + sorted_files_to_mosaic
    os.system(" ".join(gdal_merge_command))




#############################################################################################################
# Define some default input parameters for debugging purposes
#############################################################################################################
ottawa_region = {
    'type': 'Polygon',
    'coordinates': [
       [
         [-76.120,45.184], 
         [-75.383,45.171],
         [-75.390,45.564], 
         [-76.105,45.568], 
         [-76.120,45.184]
       ]
    ]
}



vancouver_region = {
    'type': 'Polygon',
    'coordinates': [
       [
         [-123.32, 49.37], 
         [-122.44, 49.35],
         [-122.96, 48.75], 
         [-123.27, 48.96], 
         [-123.32, 49.37]
       ]
    ]
}



CompParams = {
  "number_workers" : 10,   #For S2 data, only use 10 rather than 20
  "debug"       : True,
  "entire_tile" : False,     #
  "nodes"       : 1,
  "node_memory" : "50G",
  'chunk_size': {'x': 2048, 'y': 2048}
}

  
ProdParams = {
    'sensor': 'HLS_SR',       # A sensor type string (e.g., 'S2_SR', 'HLS_SR', 'HLSL30_SR', 'HLSS30_SR' or 'MOD_SR')
    'unit': 2,                   # A data unit code (1 or 2 for TOA or surface reflectance)    
    'year': 2025,                # An integer representing image acquisition year
    'nbYears': -1,               # positive int for annual product, or negative int for monthly product
    'months': [5],            # A list of integers represening one or multiple monthes     
    'tile_names': ['tile55_422'], # A list of (sub-)tile names (defined using CCRS' tile griding system) 
    'prod_names': ['mosaic'],    #['LAI', 'fCOVER', 'fAPAR', 'Albedo'], 
    'resolution': 30,            # Exporting spatial resolution    
    'out_folder': 'C:/Work_Data/HLS_mosaic_tile55_422_2025_temp',  # the folder name for exporting
    'out_datatype': 'int16',     # 'int8' or 'int16'
    'projection': 'EPSG:3979',
    'IncludeAngles': False,
    #'start_dates': ['2025-06-15'],
    #'end_dates': ['2025-09-15'],
    #'regions': {'ottawa': ottawa_region}  #, {'vancouver': vancouver_region}    
}




#############################################################################################################
# Description: This function can be used to perform mosaic production
#
#############################################################################################################
def MosaicProduction(ProdParams, CompParams):
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
  
  print('<MosaicProduction> User defined input parameters:')
  for key, value in usedParams.items():
    print(f'{key}: {value}')

  #==========================================================================================================
  # Generate composite images based on given input parameters
  #==========================================================================================================
  ext_tiffs_rec = []
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
          ext_tiffs_rec, period_str, mosaic = AIMz.one_mosaicB(tile_params, CompParams)
        else: 
          _, _, mosaic = AIMz.one_mosaicB(tile_params, CompParams)

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
          #AIMz.one_mosaicC(usedParams, CompParams)
          eoMz.one_mosaic(usedParams, CompParams)
 



#############################################################################################################
# Description: This is the main function for generating composite images or vegetation biophysical parameter
#              maps. The selection of these two products is controlled by 'prod_names' parameter in 
#              "ProdParams" input dictionary. 
#############################################################################################################
def main(inProdParams, inCompParams):
  '''
    Args:
      inProdParams(Dictionary): A parameter dictionary containing parameters related to data production;
      inCompParams(Dictionary): A parameter dictionary containing parameters related to the computing environment.
  '''
  #==========================================================================================================
  # The following two lines are for two ways to obtaining input parameters
  #==========================================================================================================
  prod_params, comp_params = eoPM.form_inputs(inProdParams, inCompParams)  # Using two dictionaries to input required parameters
  #prod_params, comp_params = eoPM.form_inputs()                           # Using command options to input required parameters   
  if prod_params is None or comp_params is None:
    print('<main> Incomplete input parameters (two dictionaries, ProdParams and CompParams, are required)!')
    return
     
  #==========================================================================================================
  # Determine which product, mosaic image or vegetation biophysical parameter maps, will be generated
  #==========================================================================================================
  prod_type = eoPM.which_product(prod_params)
  if 'veg' in prod_type:
    leaf.LEAF_production(prod_params, comp_params)

  else:
    MosaicProduction(prod_params, comp_params)
    



#############################################################################################################
# Description: The following code will be called only when executed within VSCode; it will be ignored when
#              executed from the command Prompt or a Jupyter notebook. 
#
#############################################################################################################
if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument("ProdParams", nargs='?', default=ProdParams, help="First input parameter")
  parser.add_argument("CompParams", nargs='?', default=CompParams, help="Second input parameter")
  args = parser.parse_args()

  main(args.ProdParams, args.CompParams)

