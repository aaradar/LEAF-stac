import pickle
import os


VALID_DS_NAMES = ['S2_SR', 'S2_SR_10M', 'L8_SR', 'L9_SR', 'HLS_30M']
VALID_VP_NAMES = ['LAI', 'FAPAR', 'FCOVER', 'ALBEDO', 'CCC', 'CWC', 'DASF']



#############################################################################################################
# Description: This function returns a GEE-style FeatureCollection saved in pickle format.
#
#############################################################################################################
def create_FeatureCollection(Filename):
  with open(Filename, "rb") as fp:   #Pickling
    net_FC = pickle.load(fp)
  fp.close()    
  return net_FC


#############################################################################################################
# Description: This function determines if a specified dataset name is valid.
#
#############################################################################################################
def is_valid_DS_name(DSName):
  ds_name = DSName.upper()

  return True if ds_name in VALID_DS_NAMES else False


#############################################################################################################
# Description: This function determines if a specified vegetation parameetr name is valid.
#
#############################################################################################################
def is_valid_VP_name(VPName):
  VPName = VPName.upper()

  return True if VPName in VALID_VP_NAMES else False



#############################################################################################################
# Description: This function returns the full name/description corresponding to a specified dataset name.
#
#############################################################################################################
def get_DS_description(DSName):
  DSName = DSName.upper()

  if DSName == VALID_DS_NAMES[0]:   #'S2_SR'
    return 'Sentinel-2 L2C'
  elif DSName == VALID_DS_NAMES[1]: #'S2_SR_10M'
    return 'Sentinel-2 L2C 10m'
  elif DSName == VALID_DS_NAMES[2]: #'L8_SR'
    return 'Landsat-8 L2C'
  elif DSName == VALID_DS_NAMES[3]: #'L9_SR'
    return 'Landsat-9 L2C'
  elif DSName == VALID_DS_NAMES[4]: #'HLS_30M'
    return 'HLS 30m'
  else:
    print ('<get_DS_description> The given dataset name <%s> is invalid!'%(DSName))
    return ''



#############################################################################################################
# Description: This function returns the full name to a specified vegetation parameter.
#
#############################################################################################################
def get_VP_description(VPName):
  VPName = VPName.upper()

  if VPName == VALID_VP_NAMES[0]:    #'LAI'
    return 'Leaf area index'
  
  elif VPName == VALID_VP_NAMES[1]:  #'FAPAR'
    return 'Fraction of absorbed photosynthetically active radiation'
  
  elif VPName == VALID_VP_NAMES[2]:  #'FCOVER'
    return 'Fraction of canopy cover'
  
  elif VPName == VALID_VP_NAMES[3]:  #'ALBEDO'
    return 'Black sky albedo'

  elif VPName == VALID_VP_NAMES[4]:  #'CCC'
    return 'Canopy chlorophyll content'

  elif VPName == VALID_VP_NAMES[5]:  #'CWC'
    return 'Canopy water content'
  
  elif VPName == VALID_VP_NAMES[6]:  #'DASF'
    return 'Directional area scattering factor'
  
  else:
    print ('<get_VP_description> The given dataset name <%s> is invalid!'%(VPName))
    return ''


#############################################################################################################
# Description: This dictionary returns a dictionary containing the filenames related to the SL2P models for
#              a specified dataset/sensor.
#
#############################################################################################################
def get_SL2P_filenames(DSName, NetPath):
  ds_name = DSName.upper()
  
  filenames = {}
  if ds_name == VALID_DS_NAMES[0]:  #'S2_SR'
    filenames['estimate'] = NetPath + '/s2_20m_sl2pccrs.pkl'
    filenames['error']    = NetPath + '/s2_20m_sl2pccrs_error.pkl'
    filenames['domain']   = NetPath + '/s2_20m_sl2pccrs_domain.pkl'
    filenames['netID']    = NetPath + '/s2_20m_sl2pccrs_parameter_file.pkl'
    filenames['legend']   = NetPath + '/s2_20m_sl2pccrs_legend.pkl'
    return filenames
  
  elif ds_name == VALID_DS_NAMES[1]:  #'S2_SR_10M'
    filenames['estimate'] = NetPath + '/s2_10m_sl2pccrs.pkl'
    filenames['error']    = NetPath + '/s2_10m_sl2pccrs_error.pkl'
    filenames['domain']   = NetPath + '/s2_10m_sl2pccrs_domain.pkl'
    filenames['netID']    = NetPath + '/s2_10m_sl2pccrs_parameter_file.pkl'
    filenames['legend']   = NetPath + '/s2_10m_sl2pccrs_legend.pkl'
    return filenames
  
  elif ds_name == VALID_DS_NAMES[2]:  #'L8_SR'
    filenames['estimate'] = NetPath + '/l8_sl2pccrs.pkl'
    filenames['error']    = NetPath + '/l8_sl2pccrs_error.pkl'
    filenames['domain']   = NetPath + '/l8_sl2pccrs_domain.pkl'
    filenames['netID']    = NetPath + '/l8_sl2pccrs_parameter_file.pkl'
    filenames['legend']   = NetPath + '/l8_sl2pccrs_legend.pkl'
    return filenames
  
  elif ds_name == VALID_DS_NAMES[3]:  #'L9_SR'
    filenames['estimate'] = NetPath + '/l9_sl2pccrs.pkl'
    filenames['error']    = NetPath + '/l9_sl2pccrs_error.pkl'
    filenames['domain']   = NetPath + '/l9_sl2pccrs_domain.pkl'
    filenames['netID']    = NetPath + '/l9_sl2pccrs_parameter_file.pkl'
    filenames['legend']   = NetPath + '/l9_sl2pccrs_legend.pkl'
    return filenames
  
  elif ds_name == VALID_DS_NAMES[4]:  #'HLS_30M'
    filenames['estimate'] = NetPath + '/l8_sl2pccrs.pkl'
    filenames['error']    = NetPath + '/l8_sl2pccrs_error.pkl'
    filenames['domain']   = NetPath + '/l8_sl2pccrs_domain.pkl'
    filenames['netID']    = NetPath + '/l8_sl2pccrs_parameter_file.pkl'
    filenames['legend']   = NetPath + '/l8_sl2pccrs_legend.pkl'
    return filenames
  
  else:
    print ('<get_DS_description> The given dataset name <%s> is invalid!'%(DSName))
    return filenames



#############################################################################################################
# Description: This function returns the band names to be applied as inputs to the SL2P models for a
#              specified dataset/sensor.
#
#############################################################################################################
def get_DS_bands(SsrData):
  return ['cosVZA','cosSZA','cosRAA'] + SsrData['LEAF_BANDS']
  



#############################################################################################################
# Description: This function creates a dictionary containing all the options associated with a dataset type.
#  
#############################################################################################################
def make_DS_options(NetPath, SsrData):
  if os.path.exists(NetPath) == False or os.path.isdir(NetPath) == False:
    print ('<make_DS_options> The given network path <%s> dose not exist!'%(NetPath))
    return None
  
  DSName = SsrData['NAME'].upper()
  if is_valid_DS_name(DSName) == False:
    print ('<make_DS_options> The given dataset name <%s> is invalid!'%(DSName))
    return None
    
  DS_OPTIONS = {}

  DS_OPTIONS['name']           = DSName
  DS_OPTIONS['description']    = get_DS_description(DSName)

  filenames = get_SL2P_filenames(DSName, NetPath)
  DS_OPTIONS['SL2P_estimates'] = create_FeatureCollection(filenames['estimate'])
  DS_OPTIONS['SL2P_errors']    = create_FeatureCollection(filenames['error'])
  DS_OPTIONS['SL2P_domain']    = create_FeatureCollection(filenames['domain'])
  DS_OPTIONS['Network_Ind']    = create_FeatureCollection(filenames['netID'])
  DS_OPTIONS['legend']         = create_FeatureCollection(filenames['legend'])
  DS_OPTIONS['inputBands']     = get_DS_bands(SsrData)
  DS_OPTIONS['numVariables']   = 7

  return DS_OPTIONS



#############################################################################################################
# Description: This function creates a dictionary containing all the options associated with a vegetation 
#              parameter.
#
#############################################################################################################
def make_VP_options(VPName): 
  VPName = VPName.upper()
  if is_valid_VP_name(VPName) == False:
    print (f'<make_VP_options> The given parameter name {VPName} is invalid!')
    return None
    
  PROD_OPTIONS = {}

  PROD_OPTIONS['Name']         = VPName
  PROD_OPTIONS['errorName']    = 'error'+VPName
  PROD_OPTIONS['maskName']     = 'mask'+VPName
  PROD_OPTIONS['description']  = get_VP_description(VPName)

  if VPName == VALID_VP_NAMES[0]:    #'LAI'
    PROD_OPTIONS['variable']       = 1  #The ID code for LAI parameter
    PROD_OPTIONS['outmin']         = 0  
    PROD_OPTIONS['outmax']         = 8  
    PROD_OPTIONS['scale_factor']   = 20
    PROD_OPTIONS['compact_factor'] = 256
  
  elif VPName == VALID_VP_NAMES[1]:  #'FAPAR'
    PROD_OPTIONS['variable']       = 2  #The ID code for fAPAR parameter
    PROD_OPTIONS['outmin']         = 0  
    PROD_OPTIONS['outmax']         = 1  
    PROD_OPTIONS['scale_factor']   = 200
    PROD_OPTIONS['compact_factor'] = 256
  
  elif VPName == VALID_VP_NAMES[2]:  #'FCOVER'
    PROD_OPTIONS['variable']       = 3  #The ID code for fCOVER parameter
    PROD_OPTIONS['outmin']         = 0  
    PROD_OPTIONS['outmax']         = 1  
    PROD_OPTIONS['scale_factor']   = 200
    PROD_OPTIONS['compact_factor'] = 256
  
  elif VPName == VALID_VP_NAMES[3]:  #'ALBEDO'
    PROD_OPTIONS['variable']       = 6  #The ID code for Albedo parameter
    PROD_OPTIONS['outmin']         = 0  
    PROD_OPTIONS['outmax']         = 0.2
    PROD_OPTIONS['scale_factor']   = 200
    PROD_OPTIONS['compact_factor'] = 256

  elif VPName == VALID_VP_NAMES[4]:  #'CCC'
    PROD_OPTIONS['variable']       = 4 #The ID code of for CCC parameter
    PROD_OPTIONS['outmin']         = 0  
    PROD_OPTIONS['outmax']         = 600
    PROD_OPTIONS['scale_factor']   = 1
    PROD_OPTIONS['compact_factor'] = 256

  elif VPName == VALID_VP_NAMES[5]:  #'CWC'
    PROD_OPTIONS['variable']       = 5  #The ID code for CWC parameter
    PROD_OPTIONS['outmin']         = 0   
    PROD_OPTIONS['outmax']         = 0.55
    PROD_OPTIONS['scale_factor']   = 1
    PROD_OPTIONS['compact_factor'] = 256
  
  elif VPName == VALID_VP_NAMES[6]:  #'DASF'
    PROD_OPTIONS['variable']       = 7  #The ID code for DASF parameter
    PROD_OPTIONS['outmin']         = 0  
    PROD_OPTIONS['outmax']         = 1  
    PROD_OPTIONS['scale_factor']   = 200
    PROD_OPTIONS['compact_factor'] = 256
  
  return PROD_OPTIONS


'''
test_VP_options = make_VP_options('fcover')
test_VP_options
'''