#import pvlib
import calendar
import numpy as np
from pyproj import Transformer
from datetime import datetime, timedelta


import eoImage as Img
import eoTileGrids as eoTG



######################################################################################################
# Description: This function returns the last date of a specified month.
#
# Revision history:  2022-Aug-08  Lixin Sun  Initial creation
#                    2024-May-29  Lixin Sun  Modified for odc-stac and xarray applications
#
######################################################################################################
def month_end(Year, Month):
  '''Returns the last date of a specified month.
     Args:
       Year(int): A specified year integer;
       Month(int): A specified month integer. '''
  month = int(Month)
  year  = int(Year)

  if month < 1:
    return calendar.monthrange(year, 1)[1]
  elif month > 12: 
    return calendar.monthrange(year, 12)[1]
  else:
    return calendar.monthrange(year, month)[1]



######################################################################################################
# Description: Creates the start and end date strings of a specified year and month
#
# Revision history:  2021-May-20  Lixin Sun  Initial creation
#                    2021-Oct-15  Lixin Sun  Added a new case when "inMonth" is out of valid range
#                                            (1 to 12), then the start and end dates of the peak season
#                                            will be returned. 
#                    2024-May-29  Lixin Sun  Modified for odc-stac and xarray applications
#
######################################################################################################
def month_range(Year, Month):
  '''Creates the start and end date strings of a specified year and month
     Args:
       Year(int): A specified year;
       Month(int): A specified month.'''
  month = int(Month)
  year  = int(Year)
  
  if month < 1:
    month =1
  elif month > 12:
    month = 12

  start_date = datetime(year, month, 1).strftime("%Y-%m-%d")
  end_date   = datetime(year, month, month_end(year, month)).strftime("%Y-%m-%d")

  return start_date, end_date  




######################################################################################################
# Description: This function creates starting and stoping dates for a peak season
#
# Revision history:  2021-May-20  Lixin Sun  Initial creation
#                    2024-May-29  Lixin Sun  Modified for odc-stac and xarray applications
#
######################################################################################################
def summer_range(Year):
  '''Returns the starting and stoping dates of a peak season. 
  Arg: 
     Year(int): A regular pyhton integer'''
  
  return datetime(Year, 6, 15).strftime("%Y-%m-%d"), datetime(Year, 9, 15).strftime("%Y-%m-%d")





######################################################################################################
# Description: This function creates a summer centre date string 
#
# Revision history:  2021-May-20  Lixin Sun  Initial creation
#                    2024-May-29  Lixin Sun  Modified for odc-stac and xarray applications
#
######################################################################################################
def summer_centre(Year):
  '''Returns the middle date of growing peak season. 
  Arg: 
    Year(int): A regular pyhton integer, rather than a GEE object'''
  return datetime(Year, 7, 31).strftime("%Y-%m-%d")




######################################################################################################
# Description: This function returns the middle date of a given time period.
# 
# Revision history:  2021-May-20  Lixin Sun  Initial creation 
#                    2024-May-29  Lixin Sun  Modified for odc-stac and xarray applications
#  
######################################################################################################
def period_centre(StartD, StopD):
  '''Returns the middle date of a given time period. 
  Arg: 
    StartD(string): Start date string in "%Y-%m-%d" format;
    StopD(string): Stop date string in "%Y-%m-%d" format.'''  
  
  # Parse the input date strings to datetime objects
  start = datetime.strptime(StartD, "%Y-%m-%d")
  stop  = datetime.strptime(StopD, "%Y-%m-%d")
    
  # Calculate the central date
  return (start + (stop - start)/2).strftime("%Y-%m-%d")
    




######################################################################################################
# Description: This function returns a time range based on a given centre date and time window size.
# 
# Revision history:  2023-Nov-20  Lixin Sun  Initial creation 
#                    2024-May-29  Lixin Sun  Modified for odc-stac and xarray applications
#
######################################################################################################
def time_range(MidDate, WinSize):
  '''Returns a time range based on a given centre date and time window size.
  Arg: 
    MidDate(string): A given centre date string in "%Y-%m-%d" format;
    WinSize(int): Half of the time window.'''  
  win_size = int(WinSize)

  if win_size < 1:
    win_size = 1

  centre = datetime.strptime(MidDate, "%Y-%m-%d")
  delta  = timedelta(days = win_size)
  
  return (centre - delta).strftime("%Y-%m-%d"), (centre + delta).strftime("%Y-%m-%d")





######################################################################################################
# Description: This function returns the time window size based on given start and stop dates.
# 
# Revision history:  2023-Nov-20  Lixin Sun  Initial creation 
#                    2024-May-29  Lixin Sun  Modified for odc-stac and xarray applications
#
######################################################################################################
def time_window_size(StartD, StopD):
  '''Returns the middle date of a given time period. 
  Arg: 
    StartD(string): Start date string in "%Y-%m-%d" format;
    StopD(string): Stop date string in "%Y-%m-%d" format.'''  
    # Parse the input date strings to datetime objects
  start = datetime.strptime(StartD, "%Y-%m-%d")
  stop  = datetime.strptime(StopD, "%Y-%m-%d")
 
  diff = stop - start

  return int(diff.days)




#==================================================================================================
# define a spatial region around Ottawa
#==================================================================================================
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


tile55_922 = {
    'type': 'Polygon',
    'coordinates': [
       [
         [-77.6221, 47.5314], 
         [-73.8758, 46.7329],
         [-75.0742, 44.2113], 
         [-78.6303, 44.9569],
         [-77.6221, 47.5314]
       ]
    ]
}



#############################################################################################################
# Description: 
#############################################################################################################
def proj_LatLon(Lat, Lon, Target_proj):
  transformer = Transformer.from_crs("EPSG:4326", Target_proj, always_xy=True)

  return transformer.transform(Lon, Lat)




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
# Description: This function returns a boundary box [min(longs), min(lats), max(longs), max(lats)] of a given 
#              geographic region.
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
# Description: This function returns central coordinates (long and lat) of a given polygon.
#
# Revision history:  2024-May-28  Lixin Sun  Initial creation
# 
#############################################################################################################
def get_region_centre(inRegion):
  bbox = get_region_bbox(inRegion)
  
  return (bbox[0]+bbox[2])/2, (bbox[1]+bbox[3])/2




#############################################################################################################
# Description: This function divides the bbox associated with a given geographic region (defined by 
#              Lats/Longs) into a number of sub-bboxes
#
# Revision history:  2024-May-27  Lixin Sun  Initial creation
# 
#############################################################################################################
def divide_region(inRegion, nDivides):
  if nDivides <= 1:
    nDivides = 1
  
  sub_regions = []  
  if nDivides == 1:
    #sub_polygon = {'type': 'Polygon',  'coordinates': [sub_region] }
    sub_regions.append(inRegion['coordinates'][0])
    return sub_regions
  
  # Obtain the bbox of the given geographic region 
  bbox = get_region_bbox(inRegion)
  left_lon = bbox[0]
  btom_Lat = bbox[1]

  lon_delta = (bbox[2] - left_lon)/nDivides
  lat_delta = (bbox[3] - btom_Lat)/nDivides

  for i in range(nDivides):    
    for j in range(nDivides):
      sub_region = []
      BL_lon = left_lon+i*lon_delta
      BL_lat = btom_Lat+j*lat_delta

      sub_region.append([BL_lon, BL_lat])
      sub_region.append([BL_lon+lon_delta, BL_lat])
      sub_region.append([BL_lon+lon_delta, BL_lat+lat_delta])
      sub_region.append([BL_lon, BL_lat+lat_delta])
      sub_region.append([BL_lon, BL_lat])
  
      sub_regions.append(sub_region)
  
  return sub_regions




#############################################################################################################
# Description: This function divides a given bbox into a number of sub-bboxes
#
# Revision history:  2024-May-27  Lixin Sun  Initial creation
# 
#############################################################################################################
def divide_bbox(inBbox, nDivides):
  if nDivides <= 1:
    nDivides = 1
  
  sub_regions = []
  
  # Obtain the bbox of the given geographic region 
  left_lon = inBbox[0]
  btom_Lat = inBbox[1]

  lon_delta = (inBbox[2] - left_lon)/nDivides
  lat_delta = (inBbox[3] - btom_Lat)/nDivides

  for i in range(nDivides):    
    for j in range(nDivides):
      sub_region = []
      BL_lon = left_lon+i*lon_delta
      BL_lat = btom_Lat+j*lat_delta

      sub_region.append([BL_lon, BL_lat])
      sub_region.append([BL_lon+lon_delta, BL_lat])
      sub_region.append([BL_lon+lon_delta, BL_lat+lat_delta])
      sub_region.append([BL_lon, BL_lat+lat_delta])
      sub_region.append([BL_lon, BL_lat])
  
      sub_regions.append(sub_region)
  
  return sub_regions






# region = eoTG.get_tile_polygon('tile55_922')
# bbox = get_region_bbox(region, 'EPSG:3979')
# print(bbox)


#############################################################################################################
# cases testing code
#############################################################################################################
#get_average_VAs('S2A', TimeStamp, CentreLat, CentreLon, CentreAlt)

#result = time_window_size('2023-09-30', '2024-10-10')

#print('\nresult = ', result)
#print('testing ended!')