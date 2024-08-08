import pystac_client as psc
import odc.stac

import eoParams as eoPM
import eoUtils as eoUs

CCMEO_DC_URL =  'https://datacube.services.geo.ca/api'

#############################################################################################################
# Description: This function obtains the Canada landcover map from CCMEO's DataCube.
#
# Revision history:  2024-Aug-07  Lixin Sun  Initial creation.
#
#############################################################################################################
def get_CanLC(inYear, Region, Resolution, Projection):
  '''Obtains the Canada landcover map from CCMEO's DataCube.

     Args:      
       Year(int or string): A target year.'''
  #==========================================================================================================
  # Obtain landcover collection fromCCMEO DataCube
  #==========================================================================================================  
  catalog = psc.Client.open(CCMEO_DC_URL)  
  #collection = ccmeo_catalog.get_collection('landcover')

  stac_catalog = catalog.search(collections = ['landcover'],
                                intersects  = Region)
  
  stac_items = list(stac_catalog.items())
  
  LC_xrDS = odc.stac.load([stac_items[0]],
                        bands  = ['classification'],
                        chunks = {'x': 1000, 'y': 1000},
                        crs    = Projection, 
                        bbox   = eoUs.get_region_bbox(Region),
                        fail_on_error = False,
                        resolution = Resolution)
  
  print(LC_xrDS)

  #==========================================================================================================
  # Select a landcover item based on the given "inYear"
  #==========================================================================================================  
  '''
  LC_items = ['landcover-2010', 'landcover-2015', 'landcover-2020']
  year = int(inYear)  

  target_year = 2020

  if year < 2013:
    target_year = 2010
  elif year >= 2013 and year < 2018:
    target_year = 2015
  elif year >= 2018 and year < 2025:  
    target_year = 2020

  item = ccmeo_catalog.get_item('landcover-' + str(target_year))
  '''

  #==========================================================================================================
  # Create a CCRS land cover image
  #==========================================================================================================
  #return ee.Image(ccrs_LC)


params = {
    'sensor': 'S2_SR',           # A sensor type string (e.g., 'S2_SR' or 'L8_SR' or 'MOD_SR')
    'unit': 2,                   # A data unit code (1 or 2 for TOA or surface reflectance)    
    'year': 2022,                # An integer representing image acquisition year
    'nbYears': -1,               # positive int for annual product, or negative int for monthly product
    'months': [6],               # A list of integers represening one or multiple monthes     
    'tile_names': ['tile42_911'], # A list of (sub-)tile names (defined using CCRS' tile griding system) 
    'prod_names': ['LAI', 'fCOVER'],    #['mosaic', 'LAI', 'fCOVER', ]    
    'resolution': 20,            # Exporting spatial resolution    
    'out_folder': 'C:/Work_documents/test_xr_tile55_411_2021_200m',  # the folder name for exporting
    'projection': 'EPSG:3979'   
    
    #'start_date': '2022-06-15',
    #'end_date': '2022-09-15'
}

params = eoPM.update_default_params(params)

ProjStr = str(params['projection'])  
Scale   = int(params['resolution'])

Region = eoPM.get_spatial_region(params)  


get_CanLC(2022, Region, Scale, ProjStr)