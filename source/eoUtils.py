#import pvlib
import calendar
import numpy as np
from datetime import datetime, timedelta
from sgp4.api import Satrec
from sgp4.api import jday


import eoImage as Img


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
def get_region_bbox(inRegion):
  lats, lons = get_lats_lons(inRegion)
  
  return [min(lons), min(lats), max(lons), max(lats)]




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







#############################################################################################################
# Description: This function returns sun zenith and azimuth angles corresponding to given cases testing code
# Note:        date = pd.Timestamp('2023-06-21 12:00:00', tz='UTC')  # Example date and time in UTC
#
# Revision history:  2024-Jul-09  Lixin Sun  Initial creation
#############################################################################################################
'''
def get_sun_angles(Date, Lat, Lon):
    # Define the location
    location = pvlib.location.Location(Lat, Lon)

    # Calculate solar position
    solar_position = location.get_solarposition(times=Date)

    # Extract sun zenith and azimuth angles
    sza = solar_position['zenith'].values
    saa = solar_position['azimuth'].values

    return sza, saa
'''



#############################################################################################################
# Description: This function returns the position and velocity of a satellite
#
# Note:        Two Line Element (TLE) sets were obtained from "https://www.n2yo.com/satellite/?s=42063#results"
#
# Revision history:  2024-Jul-22  Lixin Sun  Initial creation
#
#############################################################################################################
def get_satellite_pos(SsrKeyStr, DT_obj):
  '''
    Args:
      SsrKeyStr(String): A string representing a specific EO satellite. E.g., 'S2A', 'S2B', 'LS8' and 'LS9'
                         represent Sentinel-2A, Sentinel-2B, Landsat-8 and Landsat-9 satellite, respectively.'''
  #==========================================================================================================
  # Validate the given sensor key string
  #==========================================================================================================
  valid_ssr_str = ['s2a', 's2b', 'ls8', 'ls9']

  ssr_str = str(SsrKeyStr).lower()
  if ssr_str not in valid_ssr_str:
    return None
  
  #==========================================================================================================
  # Extract TLE lines according to the given sensor key string
  #==========================================================================================================
  TLEs = {'S2A': {'line1': '1 40697U 15028A   24204.20753257  .00000390  00000-0  16528-3 0  9992',
                  'line2': '2 40697  98.5677 277.9775 0001155  93.0244 267.1071 14.30813154474355'},
          'S2B': {'line1': '1 42063U 17013A   24204.17258506  .00000401  00000-0  16965-3 0  9998',
                  'line2': '2 42063  98.5682 277.9400 0001138  94.6249 265.5063 14.30819489385269'},
          'LS8': {'line1': '1 39084U 13008A   24204.15805741  .00001050  00000-0  24313-3 0  9991',
                  'line2': '2 39084  98.2276 273.3416 0001165 108.8946 251.2378 14.57101933608506'},
          'LS9': {'line1': '1 49260U 21088A   24204.19228775  .00001091  00000-0  25218-3 0  9995',
                  'line2': '2 49260  98.2303 273.3466 0001292  93.5162 266.6184 14.57118537149783'}
         }
  
  tle_line1 = TLEs[SsrKeyStr]['line1']
  tle_line2 = TLEs[SsrKeyStr]['line2']
  
  #==========================================================================================================
  # 
  #==========================================================================================================
  satellite = Satrec.twoline2rv(tle_line1, tle_line2)
  
  #==========================================================================================================
  # Date and time of image capture (replace with actual date and time)
  #==========================================================================================================
  jd, fr = jday(DT_obj.year, DT_obj.month, DT_obj.day, DT_obj.hour, DT_obj.minute, DT_obj.second)
  e, r, v = satellite.sgp4(jd, fr)

  return [e, r, v]



#############################################################################################################
# Description: This function returns the Earth-Centre, Earth-Fixed (ECEF) coordinates (x,y,z) of a point with
#              Latitude, Longitude and Altitude values.
#
# Revision history:  2024-Jul-22  Lixin Sun  Initial creation
#
#############################################################################################################
def latlon_to_ECEF(lat, lon, alt):
  # WGS84 ellipsoid constants
  a = 6378137.0  # semi-major axis
  e = 8.1819190842622e-2  # eccentricity

  lat = np.radians(lat)
  lon = np.radians(lon)

  N = a / np.sqrt(1 - e**2 * np.sin(lat)**2)
  x = (N + alt) * np.cos(lat) * np.cos(lon)
  y = (N + alt) * np.cos(lat) * np.sin(lon)
  z = ((1 - e**2) * N + alt) * np.sin(lat)

  return np.array([x, y, z])





#############################################################################################################
# Description: This function returns the position and velocity of a satellite
#
# Revision history:  2024-Jul-22  Lixin Sun  Initial creation
#
#############################################################################################################
def calculate_angles(sat_pos, pixel_pos):
  sat_to_pixel = pixel_pos - sat_pos
  pixel_to_center = -pixel_pos

  zenith_angle = np.arccos(
    np.dot(sat_to_pixel, pixel_to_center) /
    (np.linalg.norm(sat_to_pixel) * np.linalg.norm(pixel_to_center))
  )

  north = np.array([0, 0, 1])  # Assuming a simplified north vector
  proj_sat_to_pixel = sat_to_pixel - np.dot(sat_to_pixel, north) * north
  azimuth_angle = np.arctan2(proj_sat_to_pixel[1], proj_sat_to_pixel[0])

  return np.degrees(zenith_angle), np.degrees(azimuth_angle)



#############################################################################################################
# Description: This function returns average view zenith and azimuth angles
#
# Revision history:  2024-Jul-22  Lixin Sun  Initial creation
#
#############################################################################################################
def get_average_VAs(SsrKeyStr, TimeStamp, CentreLat, CentreLon, CentreAlt):
  sensor_pos = get_satellite_pos(SsrKeyStr, TimeStamp)
  pixel_pos  = latlon_to_ECEF(CentreLat, CentreLon, CentreAlt)
  
  if sensor_pos == None or sensor_pos[0] != 0:
    return None
  
  sat_pos = np.array(sensor_pos[1])*1000
  sat_to_pixel    = pixel_pos - sat_pos
  #sat_to_pixel = [x - y for x, y in zip(pixel_pos, sensor_pos[1])]
  pixel_to_center = -pixel_pos

  zenith_angle = np.arccos(
    np.dot(sat_to_pixel, pixel_to_center) /
    (np.linalg.norm(sat_to_pixel) * np.linalg.norm(pixel_to_center))
  )

  north = np.array([0, 0, 1])  # Assuming a simplified north vector
  proj_sat_to_pixel = sat_to_pixel - np.dot(sat_to_pixel, north) * north
  azimuth_angle = np.arctan2(proj_sat_to_pixel[1], proj_sat_to_pixel[0])

  #vza, vaa = calculate_angles(sat_pos, pixel_pos)

  return {'vza': zenith_angle, 'vaa': azimuth_angle}






#############################################################################################################
# cases testing code
#############################################################################################################
#get_average_VAs('S2A', TimeStamp, CentreLat, CentreLon, CentreAlt)

#result = time_window_size('2023-09-30', '2024-10-10')

#print('\nresult = ', result)
#print('testing ended!')