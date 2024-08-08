import numpy as np
import pandas as pd
import xarray as xr
from datetime import datetime


import SL2P_V1



# read coefficients of a network from csv EE asset
def getCoefs(netData, ind):
  return netData['properties']['tabledata%s'%(ind)]



#############################################################################################################
# Description: This function saves a network model into a dictionary. This function is equivalent to 
#              "FNet_to_DNet" in GEE-based LEAF Toolbox
#
#############################################################################################################
def makeNets(feature, netNum):
  '''
    Args:
      feature(ee.Feature): A GEE-style ee.Feature object containing all the coefficients of a network;
      netNum(int): An integer representing a biophydical parameter. '''
    
  # get the requested network and initialize the created network
  netData = feature[netNum]
  net = {}
    
  # input slope
  num   = 6
  start = num + 1
  end   = num + netData['properties']['tabledata%s'%(num)]
  net["inpSlope"] = [getCoefs(netData, ind) for ind in range(start, end+1)] 
    
  #input offset
  num   = end+1
  start = num+1
  end   = num+netData['properties']['tabledata%s'%(num)]
  net["inpOffset"] = [getCoefs(netData,ind) for ind in range(start, end+1)] 
    
  # hidden layer 1 weight
  num = end+1
  start = num+1
  end = num+netData['properties']['tabledata%s'%(num)]
  net["h1wt"] = [getCoefs(netData,ind) for ind in range(start,end+1)] 

  # hidden layer 1 bias
  num = end+1
  start = num+1
  end = num+netData['properties']['tabledata%s'%(num)]
  net["h1bi"] = [getCoefs(netData,ind) for ind in range(start,end+1)] 

  # hidden layer 2 weight
  num = end+1
  start = num+1
  end = num+netData['properties']['tabledata%s'%(num)]
  net["h2wt"] = [getCoefs(netData,ind) for ind in range(start,end+1)] 
  
  # hidden layer 2 bias
  num = end+1
  start = num+1
  end = num+netData['properties']['tabledata%s'%(num)]
  net["h2bi"] = [getCoefs(netData,ind) for ind in range(start,end+1)] 

  # output slope
  num = end+1
  start = num+1
  end = num+netData['properties']['tabledata%s'%(num)]
  net["outSlope"] = [getCoefs(netData,ind) for ind in range(start,end+1)] 
  
  # output offset
  num = end+1
  start = num+1
  end = num+netData['properties']['tabledata%s'%(num)]
  net["outBias"] = [getCoefs(netData,ind) for ind in range(start,end+1)] 

  return [net]



#############################################################################################################
# Description: This function creates network models for parameter estimations and error calculations
#
#############################################################################################################
def makeNetVars(Model_FC, numNets, ParamID):
  '''
    Args:
      Model_FC(ee.FeatureCollection): A GEE-style FeatureCollection containing network coefficints;
      numNets(Int): The number of networks;
      ParamID(Int): The ID of a biophysical parameter.'''
  
  # Extract features from given model featureCollection (Model_FC)  
  filtered_features =[ff for ff in Model_FC['features'] if ff['properties']['tabledata3'] == ParamID+1]

  #Put net variavles into a list
  netVars = [makeNets(filtered_features, netNum) for netNum in range(numNets)]

  return netVars




#############################################################################################################
# Description: This function creates network models for all vegetation parameters and all land cover types.
#              So returned networks are 2D networks with rows and columns corresponding to different veg
#              parameters and land cover types, respectively.
#
#############################################################################################################
def makeModels(DS_Options):
  #Determine the number of networks
  numNets = len({k: v for k, v in (DS_Options["Network_Ind"]['features'][0]['properties']).items() if k not in ['Feature Index','lon']}) 

  #Create two network 2D-matrics with rows and columns corresponding to different veg parameters and land cover types, respectively
  SL2P_estimate_2Dnets = [makeNetVars(DS_Options["SL2P_estimates"], numNets, ParamID) for ParamID in range(DS_Options['numVariables'])]
  SL2P_error_2Dnets    = [makeNetVars(DS_Options["SL2P_errors"],    numNets, ParamID) for ParamID in range(DS_Options['numVariables'])]  

  return SL2P_estimate_2Dnets, SL2P_error_2Dnets




#############################################################################################################
# Description: This function returns a network ID corresponding to a given classID
#############################################################################################################
def makeIndexLayer(LCMap, DSOptions):
  '''
     Args:
       LCMap(xaaray.dataset): A land cover map;
       nbClsNets(Int): The number of networks for different land cover types;
       DSOptions(Dictionary): A dictionary containing options for a satellite dataset (e.g., 'S2_SR' or 'L8_SR').'''
  
  classLegend = DSOptions["legend"]
  Network_Ind = DSOptions["Network_Ind"]

  # Get all class IDs and and their names 
  LC_IDs   = [ff['properties']['Value'] for ff in classLegend['features']]
  LC_names = [ff['properties']['SL2P Network'] for ff in classLegend['features']]

  # Get all network IDs according to class names
  netIDs = [Network_Ind['features'][0]['properties'][nn] for nn in LC_names]
  
  # Create a mapping dictionary
  mapping_dict = {LC_IDs[i]: netIDs[i] for i in range(len(LC_IDs))}

  # Apply the mapping to the land cover map
  netID_map_np = np.vectorize(mapping_dict.get)(LCMap)
  
  netID_map_xr = LCMap
  netID_map_xr.data = netID_map_np

  return netID_map_xr





'''
def applyNet(inImg, net, Network_Ind):
    [d0,d1,d2] = inImg.shape
    inImg      = inImg.reshape(d0, d1*d2)

    inpSlope   = np.array(net[Network_Ind][0]['inpSlope'])
    inpOffset  = np.array(net[Network_Ind][0]['inpOffset'])
    h1wt       = np.array(net[Network_Ind][0]['h1wt'])
    h2wt       = np.array(net[Network_Ind][0]['h2wt'])
    h1bi       = np.array(net[Network_Ind][0]['h1bi'])
    h2bi       = np.array(net[Network_Ind][0]['h2bi']) 
    outBias    = np.array(net[Network_Ind][0]['outBias'])
    outSlope   = np.array(net[Network_Ind][0]['outSlope']) 
    
    # input scaling
    l1inp2D=(inImg*inpSlope[:,None])+inpOffset[:,None]

    # hidden layers
    l12D=np.matmul(np.reshape(h1wt,[len(h1bi),len(inpOffset)]),l1inp2D)+h1bi[:,None]

    # apply tansig 2/(1+exp(-2*n))-1
    l2inp2D=2/(1+np.exp(-2*l12D))-1
     
    # purlin hidden layers
    l22D = np.sum(l2inp2D*h2wt[:,None],axis=0)+h2bi

    # output scaling 
    outputBand = (l22D-outBias[:,None])/outSlope[:,None]
    
    outputBand = outputBand.reshape(d1,d2)

    return outputBand.flatten()
'''


'''
def applyNet(xrDS, net, Network_Ind):
  # Assume the input xarray dataset has dimensions: [band, y, x]
  bands = xrDS.band.values
  inpSlope = np.array(net[Network_Ind][0]['inpSlope'])
  inpOffset = np.array(net[Network_Ind][0]['inpOffset'])
  h1wt = np.array(net[Network_Ind][0]['h1wt'])
  h2wt = np.array(net[Network_Ind][0]['h2wt'])
  h1bi = np.array(net[Network_Ind][0]['h1bi'])
  h2bi = np.array(net[Network_Ind][0]['h2bi'])
  outBias = np.array(net[Network_Ind][0]['outBias'])
  outSlope = np.array(net[Network_Ind][0]['outSlope'])

  # Input scaling
  scaled_xrDS = xrDS * inpSlope[:, None, None] + inpOffset[:, None, None]

  # Hidden layers
  l12D = np.tensordot(h1wt, scaled_xrDS, axes=(1, 0)) + h1bi[:, None, None]

  # Apply tansig 2/(1+exp(-2*n))-1
  l2inp3D = 2 / (1 + np.exp(-2 * l12D)) - 1

  # Purlin hidden layers
  l22D = np.tensordot(h2wt, l2inp3D, axes=(0, 0)) + h2bi[:, None, None]

  # Output scaling
  outputBand = (l22D - outBias[:, None, None]) / outSlope[:, None, None]

  # Create a new xarray DataArray for the output
  outputBand = xr.DataArray(outputBand, dims=['y', 'x'])

  return outputBand
'''



#############################################################################################################
# Description: This function applys the SL2P network of ONE veg parameter to the pixels marked with 'Net_ID'
#
#############################################################################################################
def applyNet(stacked_xrDS, one_VP_nets, netID_map, netID):
    '''
      Args:
        stacked_xrDS(Xarray.Dataset): A xarray.dataset with all the spectral bands stacked together;
        one_VP_nets(List):A list of SL2P networks for one vege parameter, but different landcover types;
        netID_map(Xarray.Dataset): A 2D map containing network IDs for different pixels;
        netID(int): A specific network ID to identify a network to be applied. '''

    # Extract band values
    #bands = stacked_xrDS.band.values    
    #========================================================================================================
    # Create an image by masking out the pixels with different network IDs from 'netID'  
    #========================================================================================================
    masked_stacked_xrDS = stacked_xrDS.where(netID_map['classID'] == netID, np.nan)

    #========================================================================================================
    # Select one network specific for ONE vegetation parameter and a given 'netID'
    #========================================================================================================
    selected_net = one_VP_nets[netID][0] 

    inpSlope  = np.array(selected_net['inpSlope'])
    inpOffset = np.array(selected_net['inpOffset'])
    h1wt      = np.array(selected_net['h1wt'])
    h1bi      = np.array(selected_net['h1bi'])
    h2wt      = np.array(selected_net['h2wt'])
    h2bi      = np.array(selected_net['h2bi'])
    outBias   = np.array(selected_net['outBias'])
    outSlope  = np.array(selected_net['outSlope'])
    
    # Convert stacked data to NumPy array
    data_array = masked_stacked_xrDS.values
    
    # Input scaling
    scaled_data = data_array * inpSlope[:, None, None] + inpOffset[:, None, None]
    
    # First hidden layer
    l12D = np.tensordot(h1wt, scaled_data, axes=(1, 0)) + h1bi[:, None, None]
    
    # Apply tansig activation function
    l2inp3D = 2 / (1 + np.exp(-2 * l12D)) - 1
    
    # Second hidden layer
    l22D = np.tensordot(h2wt, l2inp3D, axes=(0, 0)) + h2bi[:, None, None]
    
    # Output scaling
    output_band = (l22D - outBias[:, None, None]) / outSlope[:, None, None]
    
    # Create a new xarray.Dataset for the output
    output_ds = xr.Dataset({f'band_{i}': (('y', 'x'), output_band[i]) for i in range(output_band.shape[0])},
                           coords={'y': masked_stacked_xrDS.coords['y'], 'x': masked_stacked_xrDS.coords['x']})
    
    return output_ds




#############################################################################################################
# Description: This function 
#############################################################################################################
def wrapperNNets(SL2P_2DNets, VP_Options, DS_Options, xrDS, netID_map):
  '''Applies a set of shallow networks to an image based on a land cover map.

     Args: 
       SL2P_2DNets(ee.List): a 2D matrix of networks with rows and columns for different veg parameters and land cover types;     
       VP_Options(ee.Dictionary): a dictionary containing the options related to a selected vege parameter type;
       DS_Options(ee.Dictionary): a dictionary containing the options related to a selected satellite type;
       inImg(xarray.dataset): a mosaic image for vegetation parameter extraction;
       netID_map(xarray.dataset): a 2D map containing network IDs for different pixels. '''
  
  #==========================================================================================================
  # Get networks for one vegetation parameter (defined by "VPOptions['variable']-1") and all landcover types
  #==========================================================================================================
  one_param_nets = SL2P_2DNets[VP_Options['variable']-1]
  nbClsNets = len(one_param_nets)  
  
  #========================================================================================================
  # Stack the spectral band variables into a single DataArray
  #========================================================================================================
  stacked_data = xrDS.to_array(dim='band')
  
  for netID in range(nbClsNets):
    estimates = [applyNet(stacked_data, one_param_nets, netID_map, netID)]
  
  #return applyNet(xrDS, one_param_nets, netID_map)





def invalidInput(image,netOptions,colOptions):
    print('Generating sl2p input data flag')
    [d0,d1,d2]=image.shape
    sl2pDomain=np.sort(np.array([row['properties']['DomainCode'] for row in colOptions["sl2pDomain"]['features']]))
    bandList={b:netOptions["inputBands"].index(b) for b in netOptions["inputBands"] if not b.startswith('cos')}
    image=image.reshape(image.shape[0],image.shape[1]*image.shape[2])[list(bandList.values()),:]

    #Image formatting
    image_format=np.sum((np.uint8(np.ceil(image*10)%10))* np.array([10**value for value in range(len(bandList))])[:,None],axis=0)
    
    # Comparing image to sl2pDomain
    flag=np.isin(image_format, sl2pDomain,invert=True).astype(int)
    return flag.reshape(d1*d2)




def invalidOutput(estimate,netOptions):
    print('Generating sl2p output product flag')
    return np.where((estimate<netOptions['outmin']) | (estimate>netOptions['outmax']),1,0)




#############################################################################################################
# Description: 
#############################################################################################################
def estimate_VParams(inParams, DS_Options, xrDS, netID_map):
  '''
    Args:
      inParams(Dictionary): A dictionary containing all required input parameters;      
      DS_Options(Dictionary): A dictionary containing options for a specific satellite sensor/dataset;
      xrDS(xarray.dataset): A xarray.dataset containing all required bands;
      netID_map(xarray.dataset): A xarray.dataset containing network IDs for different pixels. '''
  #==========================================================================================================
  # Prepare SL2P network 2D-matrics with rows and columns corresponding to different veg parameters and 
  # landcover types, respectively
  #==========================================================================================================
  estimateSL2P_2DNets, errorsSL2P_2DNets = makeModels(DS_Options) 
    
  #==========================================================================================================
  # Clip 'netID_map' to match the spatial dimensions of 'xrDS' as necessary
  #==========================================================================================================
  xrDS_spatial_dims      = {dim: size for dim, size in xrDS.items() if dim in ['x', 'y']}
  netID_map_spatial_dims = {dim: size for dim, size in netID_map.items() if dim in ['x', 'y']}
  
  cliped_netID_map = netID_map
  if xrDS_spatial_dims != netID_map_spatial_dims:
    cliped_netID_map = netID_map.sel(x=xrDS['x'], y=xrDS['y'])

  #==========================================================================================================
  # run SL2P
  #==========================================================================================================
  for v_param in inParams['prod_names']:
    VP_Options = SL2P_V1.make_VP_options(v_param)
    if VP_Options != None:
      outDF['estimate'+v_param],   outDF['networkID'] = wrapperNNets(estimateSL2P_2DNets, VP_Options, DS_Options, xrDS, cliped_netID_map)
      outDF['error'+v_param], Network_Ind_uncertainty = wrapperNNets(errorsSL2P_2DNets,   VP_Options, DS_Options, xrDS, cliped_netID_map)

  print('SL2P end: %s' %(datetime.now()))
    
  # generate sl2p input data flag
  outDF['QC_input'] = invalidInput(xrDS, VP_Options, DS_Options)

  # generate sl2p output product flag
  outDF['QC_output'] = invalidOutput(outDF.loc[:,'estimate'+VPName], VP_Options)
  print('Done')

  return outDF





#############################################################################################################
# Description: 
#############################################################################################################
# def apply_net(DF, variableName, imageCollectionName, partition=1):
#   sl2p_inp=DF.values.T
#   sl2p_inp=np.reshape(sl2p_inp,(sl2p_inp.shape[0],sl2p_inp.shape[1],-1))

#   netOptions  = SL2P_V1.PROD_OPTIONS[variableName]
#   collOptions = SL2P_V1.COLL_OPTIONS[imageCollectionName]
    
#   # prepare SL2P network models for estimate and error,respectively
#   estimateSL2P, errorsSL2P = makeModel(collOptions) 
    
#   # run SL2P
#   print('Run SL2P...\nSL2P start: %s' %(datetime.now()))
#   outDF=pd.DataFrame()

#   outDF['estimate'+variableName],   outDF['networkID'] = wrapperNNets(estimateSL2P, netOptions, collOptions, sl2p_inp, partition=partition)
#   outDF['error'+variableName], Network_Ind_uncertainty = wrapperNNets(errorsSL2P,   netOptions, collOptions, sl2p_inp, partition=partition)

#   print('SL2P end: %s' %(datetime.now()))
    
#   # generate sl2p input data flag
#   outDF['QC_input'] = invalidInput(sl2p_inp, netOptions, collOptions)

#   # generate sl2p output product flag
#   outDF['QC_output'] = invalidOutput(outDF.loc[:,'estimate'+variableName], netOptions)
#   print('Done')

#   return outDF



#############################################################################################################
#
#############################################################################################################
def SL2PCCRS(xrDS, VParamName, DatasetName):
  #==========================================================================================================
  # Validate input parameters
  #==========================================================================================================
  if not SL2P_V1.is_valid_DS_name(DatasetName):
    raise ValueError(('Supported dataset names: %s'%(DatasetName)))
  
  if not SL2P_V1.is_valid_VP_name(VParamName):
    raise ValueError(('Supported biophysical parameetr names: %s'%(VParamName)))  

  DS_Options = SL2P_V1.make_DS_options('sl2p_nets', DatasetName)
  VP_Options = SL2P_V1.make_VP_options(VParamName)
  
  #==========================================================================================================
  # 
  #==========================================================================================================
  print('Estimating %s from %s data using SL2P-CCRS' %(VParamName, DatasetName))

  sl2p_inputs_bands = DS_Options['inputBands']
  print ('SL2P input data format for %s data: %s' %(DatasetName, sl2p_inputs_bands))

  outDF=pd.DataFrame()

  if 'partition' not in samplesDF.columns:
    raise ValueError("""You should provide partition column when using SL2P-CCRS!!   """
        """Partition dictionary:  %s"""%({ff['properties']['SL2P Network']:ff['properties']['Value'] for ff in DS_Options["legend"]['features']}))         
  else:
    for classID in np.unique(samplesDF['partition']):
      samplesDF0 = samplesDF[samplesDF['partition'] == classID]
      samplesDF0 = samplesDF0[sl2p_inputs_bands]

      outDF0     = apply_net(samplesDF0, ParamName, DatasetName, classID)

      outDF0     = outDF0.set_index(samplesDF0.index)
      outDF0['partition'] = classID
      outDF      = pd.concat([outDF,outDF0],axis=0)  
  
  return outDF.sort_index()





# VParamName  = 'LAI'
# DatasetName = 'L8_SR'
# fn='./testdata/Surface_refelctance_LC08_partition.csv'

# #======================================================================================================
# #Read/prepare data
# #======================================================================================================
# data=pd.read_csv(fn)
# data

# #======================================================================================================
# # Run SL2PCCRS
# #======================================================================================================
# DF = SL2PCCRS(data, VParamName, DatasetName)
# DF



'''
# Example usage
# Define a sample xarray.Dataset (replace this with your actual dataset)
data = {
    'band1': (('y', 'x'), np.random.rand(4, 5)),
    'band2': (('y', 'x'), np.random.rand(4, 5)),
    'band3': (('y', 'x'), np.random.rand(4, 5)),
}
xrDS = xr.Dataset(data)
print(xrDS)


# Define a sample neural network dictionary (replace this with your actual network)
net = [{
    'inpSlope': [1.0, 1.0, 1.0],
    'inpOffset': [0.0, 0.0, 0.0],
    'h1wt': np.random.rand(3, 3),
    'h2wt': np.random.rand(3, 3),
    'h1bi': np.random.rand(3),
    'h2bi': np.random.rand(3),
    'outBias': [0.0, 0.0, 0.0],
    'outSlope': [1.0, 1.0, 1.0],
}]

# Apply the network to the dataset
output_ds = applyNet(xrDS, net, 0)
print(output_ds)
'''