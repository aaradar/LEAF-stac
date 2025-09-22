import os
import odc.stac
from osgeo import gdal
import pystac_client as psc
from pyproj import Transformer


import eoImage as eoIM

#============================================================================================================
# The following gdal settings are necessary for loading data from Geobase through NRCan's network
#============================================================================================================
gdal.SetConfigOption('GDAL_HTTP_COOKIEFILE','~/cookies.txt')
gdal.SetConfigOption('GDAL_HTTP_COOKIEJAR', '~/cookies.txt')
gdal.SetConfigOption('GDAL_DISABLE_READDIR_ON_OPEN','EMPTY_DIR')
gdal.SetConfigOption('CPL_VSIL_CURL_ALLOWED_EXTENSIONS','TIF')
gdal.SetConfigOption('GDAL_HTTP_UNSAFESSL', 'YES')
gdal.SetConfigOption('GDAL_HTTP_MAX_RETRY', '50')
gdal.SetConfigOption('GDAL_HTTP_RETRY_DELAY', '0.5')


CCMEO_DC_URL = 'https://datacube.services.geo.ca/api'




#############################################################################################################
# Description: This function returns two lists containing latitudes and longitudes, respectively, from a
#              given geographic region.
#
# Note:        For the coordinates of each vertex, longitude is always saved before latitude. 
#
# Revision history:  2024-May-28  Lixin Sun  Initial creation
# 
#############################################################################################################
def get_lats_lons(inRegion):
  '''Returns two lists containing latitudes and longitudes, respectively, from a given geographic region.
     Args:
        inRegion(dictionary): a given geographic region'''
  
  coords  = inRegion['coordinates'][0]
  nPoints = len(coords)
     
  lons = []
  lats = []
  
  if nPoints > 0:
    for i in range(nPoints):
      lons.append(coords[i][0])    #longitude is always before latitude
      lats.append(coords[i][1])

  return lats, lons



#############################################################################################################
# Description: 
#############################################################################################################
def proj_LatLon(Lat, Lon, Target_proj):
  
  transformer = Transformer.from_crs("EPSG:4326", Target_proj, always_xy=True)
  return transformer.transform(Lon, Lat)




#############################################################################################################
# Description: This function returns a boundary box of a given geographic region. If no projection or
#              "epsg:4326" is provided as 'out_epsg_code', then lat/long boundary box will be returned.
#              Otherwise, a boundary box in specified projection will be returned. 
#
# Revision history:  2024-May-28  Lixin Sun  Initial creation
# 
#############################################################################################################
def get_region_bbox(inRegion, out_epsg_code=''):
  lats, lons = get_lats_lons(inRegion)
  
  epsg_code = str(out_epsg_code).lower()
  if epsg_code == 'epsg:4326' or len(out_epsg_code) < 4:
    return [min(lons), min(lats), max(lons), max(lats)]
  elif epsg_code == 'epsg:3979':
    nPts = len(lats)
    xs = []
    ys = []
    for i in range(nPts):
      x, y = proj_LatLon(lats[i], lons[i], 'epsg:3979')
      xs.append(x)
      ys.append(y)

    return [min(xs), min(ys), max(xs), max(ys)]  
  else:
    print('<get_region_bbox> Unsuported EPSG code provided!')    
    return None


#############################################################################################################
# Description: This function returns the Canada landcover map downloaded from CCMEO's DataCube.
#
# Note:        (1) The collection name for vegetation parameter maps: monthly-vegetation-parameters-20m-v1
#              (2) The collection name for landcover maps: landcover  
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
  #collection = catalog.get_collection('landcover')

  stac_catalog = catalog.search(collections = ['landcover'], intersects  = Region)  

  stac_items = list(stac_catalog.items())  
  xy_bbox = get_region_bbox(Region, Projection)  
  
  LC_xrDS = odc.stac.load(stac_items,
                        bands  = ['classification'],
                        chunks = {'x': 1000, 'y': 1000},
                        crs    = Projection, 
                        #bbox   = eoUs.get_region_bbox(Region, ''),
                        fail_on_error = False,
                        dtype = 'uint8',
                        resolution = Resolution, 
                        x = (xy_bbox[0], xy_bbox[2]),
                        y = (xy_bbox[3], xy_bbox[1]))
  LC_xrDS.load()

  print(LC_xrDS)
  return LC_xrDS




#############################################################################################################
# Description: This function returns the Canada landcover map read from a local file.
#
# Revision history:  2024-Aug-07  Lixin Sun  Initial creation.
#
#############################################################################################################
def get_local_CanLC(FilePath, Refer_xrDs):
  if not os.path.exists(FilePath):
    print('<get_local_CanLC> The given file path <%s> is invalid!'%FilePath)
    return None
  
  LC_map = eoIM.read_geotiff(FilePath, OutName='classMap').squeeze('band')
  print('\n<get_local_CanLC> original LC map = ', LC_map) 
  
  sub_LC_map = eoIM.xrDS_spatial_match(Refer_xrDs, LC_map, True)    
  print('\n<get_local_CanLC> clipped LC map = ', sub_LC_map) 

  return sub_LC_map






#############################################################################################################
# Description: This function returns a xarray.dataset containing vegetation biophysical parameter maps for 
#              a number of years.
#
# Revision history:  2024-Oct-29  Lixin Sun  Initial creation.
#
#############################################################################################################
def get_VegBioPhyMaps(Region, StartStr, EndStr, Resolution, Projection):
  '''Obtains the Canada landcover map from CCMEO's DataCube.

     Args:      
       Region(): A target region.'''
  #==========================================================================================================
  # Obtain landcover collection fromCCMEO DataCube
  #==========================================================================================================
  catalog      = psc.Client.open(CCMEO_DC_URL)  
  stac_catalog = catalog.search(collections = ['monthly-vegetation-parameters-20m-v1'], 
                                intersects  = Region,
                                datetime    = str(StartStr) + '/' + str(EndStr))

  stac_items = list(stac_catalog.items())  

  xy_bbox = get_region_bbox(Region, Projection)  

  LC_xrDS = odc.stac.load(stac_items,
                        bands  = ['LAI'],
                        chunks = {'x': 1000, 'y': 1000},
                        crs    = Projection, 
                        #bbox   = eoUs.get_region_bbox(Region, ''),
                        fail_on_error = False,
                        dtype = 'uint8',
                        resolution = Resolution,
                        x = (xy_bbox[0], xy_bbox[2]),
                        y = (xy_bbox[3], xy_bbox[1]))
  LC_xrDS.load()

  print(LC_xrDS)
  return LC_xrDS




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



#############################################################################################################
# Description: This function can be used to export a vegetation biophysical parameter map to hard drive.
#
# Revision history:  2024-Oct-29  Lixin Sun  Initial creation.
#
#############################################################################################################
def export_bioimages(inParams, inBioImg):
  '''
    This function exports the band images of a mosaic into separate GeoTiff files.

    Args:
      inParams(dictionary): A dictionary containing all required execution parameters;
      inMosaic(xrDS): A xarray dataset object containing mosaic images to be exported.'''
  
  #==========================================================================================================
  # Get all the parameters for exporting composite images
  #==========================================================================================================
  params = eoPM.get_mosaic_params(inParams)  

  #==========================================================================================================
  # Convert float pixel values to integers
  #==========================================================================================================
  rio_img = inBioImg.rio.write_crs(params['projection'], inplace=True)  # Assuming WGS84 for this example

  #==========================================================================================================
  # Create a directory to store the output files
  #==========================================================================================================
  dir_path = params['out_folder']
  os.makedirs(dir_path, exist_ok=True)

  #==========================================================================================================
  # Create prefix filename
  #==========================================================================================================
  region_str = str(params['current_region'])  
  filePrefix = f"LAI_{region_str}"

  spa_scale  = params['resolution']
  
  out_img     = rio_img['LAI']
  filename    = f"{filePrefix}_{spa_scale}m.tif"
  output_path = os.path.join(dir_path, filename)
  out_img.rio.to_raster(output_path)





#############################################################################################################
# Description: This function can be used to export three landcover maps to hard drive.
#
# Revision history:  2024-Oct-29  Lixin Sun  Initial creation.
#
#############################################################################################################
def export_LCMap(inParams, inLCMap):
  '''
    This function exports the band images of a mosaic into separate GeoTiff files.

    Args:
      inParams(dictionary): A dictionary containing all required execution parameters;
      inMosaic(xrDS): A xarray dataset object containing mosaic images to be exported.'''
  
  #==========================================================================================================
  # Get all the parameters for exporting composite images
  #==========================================================================================================
  params = eoPM.get_mosaic_params(inParams)  

  #==========================================================================================================
  # Convert float pixel values to integers
  #==========================================================================================================
  rio_img = inLCMap.rio.write_crs(params['projection'], inplace=True)  # Assuming WGS84 for this example

  #==========================================================================================================
  # Create a directory to store the output files
  #==========================================================================================================
  dir_path = params['out_folder']
  os.makedirs(dir_path, exist_ok=True)

  #==========================================================================================================
  # Create prefix filename
  #==========================================================================================================
  region_str = str(params['current_region'])  
  filePrefix = f"LC_{region_str}"

  spa_scale  = params['resolution']
  
  for t in rio_img['time'].values:
    out_img     = rio_img['classification'].sel(time=t)
    LC_time     = str(t)[0:10] 
    filename    = f"{filePrefix}_{LC_time}_{spa_scale}m.tif"
    output_path = os.path.join(dir_path, filename)
    out_img.rio.to_raster(output_path)