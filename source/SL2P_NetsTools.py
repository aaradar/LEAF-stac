import numpy as np
import pandas as pd
import xarray as xr
from datetime import datetime

import eoImage as eoIM
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
  
  netID_map = LCMap.copy(deep=True)
  netID_map.data = netID_map_np
  netID_map.name = 'netID_map'

  return netID_map.to_dataset()





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
    #masked_stacked_xrDS = stacked_xrDS.where(netID_map['netID_map'] == netID, np.nan)

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
    data_array   = stacked_xrDS.values
    [d0, d1, d2] = data_array.shape
    data_array   = data_array.reshape(d0, d1*d2) 

    # Input scaling
    scaled_data = data_array * inpSlope[:, None] + inpOffset[:, None]
    
    # First hidden layer    
    #reshaped_h1wt = np.reshape(h1wt,[len(h1bi),len(inpOffset)])
    #res_shape = reshaped_h1wt.shape[:1] + scaled_data.shape[1:]
    #l12D = (reshaped_h1wt @ scaled_data.reshape(scaled_data.shape[0], -1)).reshape(res_shape) + h1bi[:, None, None]
    #l12D = np.tensordot(h1wt, scaled_data, axes=(1, 0)) + h1bi[:, None, None]

    l12D = np.matmul(np.reshape(h1wt,[len(h1bi),len(inpOffset)]), scaled_data)+h1bi[:,None]
    
    # Apply tansig activation function
    l2inp2D = 2 / (1 + np.exp(-2 * l12D)) - 1
    
    # Second hidden layer
    l22D = np.sum(l2inp2D*h2wt[:, None], axis=0) + h2bi
    
    # Output scaling
    outputBand = (l22D - outBias[:, None]) / outSlope[:, None]
    
    # Create a new xarray.Dataset for the output
    outputBand = outputBand.reshape(d1,d2)
    #outputBand = outputBand.flatten()

    #output_ds = xr.Dataset({f'band_{i}': (('y', 'x'), output_band[i]) for i in range(output_band.shape[0])},
    #                       coords={'y': masked_stacked_xrDS.coords['y'], 'x': masked_stacked_xrDS.coords['x']})
    coords = {'y': stacked_xrDS.coords['y'].values, 'x': stacked_xrDS.coords['x'].values}

    return xr.DataArray(outputBand, coords, ['y', 'x'])




#############################################################################################################
# Description: This function returns one vegetation parameter map corresponding to the given image (inImg)
#
#############################################################################################################
def one_vege_param_map(SL2P_2DNets, VP_Options, DS_Options, inImg, netID_map):
  '''Applies a set of shallow networks to an image based on a land cover map.

     Args: 
       SL2P_2DNets(ee.List): a 2D matrix of networks with rows and columns for different veg parameters and land cover types;     
       VP_Options(ee.Dictionary): a dictionary containing the options associated with a specific vege parameter type;
       DS_Options(ee.Dictionary): a dictionary containing the options associated with a selected satellite type;
       inImg(xarray.dataset): a mosaic image for vegetation parameter extraction;
       netID_map(xarray.dataset): a 2D map containing network IDs for different pixels. '''
  
  #==========================================================================================================
  # Get networks for one vegetation parameter (defined by "VPOptions['variable']-1") and all landcover types
  #==========================================================================================================
  one_param_nets = SL2P_2DNets[VP_Options['variable']-1]
  nbClsNets      = len(one_param_nets)  
  
  #========================================================================================================
  # Stack the spectral band variables into a single DataArray
  #========================================================================================================
  print('<estimate_VParams> the bands in DS options = ', DS_Options['inputBands'])
  inImg = inImg[DS_Options['inputBands']]

  print('<estimate_VParams> the variables in the given image = ', inImg.data_vars)
  stacked_data = inImg.to_array(dim='band')
  
  estimates = []
  for netID in range(nbClsNets):
    estimates.append(applyNet(stacked_data, one_param_nets, netID_map, netID))
  
  combined = xr.concat(estimates, dim='class_param')

  vege_param_map = combined.max(dim = 'class_param')

  return vege_param_map





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
# Description: This function returns a sub netID_map clipped based on the spatial dimensions of 'inImg'
#############################################################################################################
def clip_netID_map(inImg, netID_map):
  Img_dims = (inImg.dims['x'], inImg.dims['y'])
  Map_dims = (netID_map.dims['x'], netID_map.dims['y'])

  sub_netID_map = netID_map
  if Img_dims != Map_dims:
    #sub_netID_map = netID_map.sel(y=inImg['y'], x=inImg['x'])
    sub_netID_map = netID_map.reindex_like(inImg, method='nearest')
    #sub_netID_map = sub_netID_map.fillna(0)

  #print('variables in sub_netID_map = ', sub_netID_map)

  return sub_netID_map



#############################################################################################################
# Description: 
#############################################################################################################
def estimate_VParams(inParams, DS_Options, inImg, netID_map):
  '''
    Args:
      inParams(Dictionary): A dictionary containing all required input parameters;      
      DS_Options(Dictionary): A dictionary containing options for a specific satellite sensor/dataset;
      inImg(xarray.dataset): A image in xarray.dataset format and containing all required bands;
      netID_map(xarray.dataset): A xarray.dataset containing network IDs for different pixels. '''
  #==========================================================================================================
  # Prepare SL2P network 2D-matrics with rows and columns corresponding to different veg parameters and 
  # landcover types, respectively
  #==========================================================================================================
  estimateSL2P_2DNets, errorsSL2P_2DNets = makeModels(DS_Options) 
    
  #==========================================================================================================
  # Clip 'netID_map' to match the spatial dimensions of the given image
  #==========================================================================================================
  sub_netID_map = clip_netID_map(inImg, netID_map)  
  print('variables in sub_netID_map = ', sub_netID_map)

  #==========================================================================================================
  # Loop through each vegetation parameter
  #==========================================================================================================  
  #coords       = {coord: inImg.coords[coord] for coord in ['x', 'y']}  
  date_img     = inImg[eoIM.pix_date]
  out_veg_maps = xr.Dataset({eoIM.pix_date: date_img})
  inImg        = inImg.drop_vars([eoIM.pix_date]) 

  for param_name in inParams['prod_names']:
    VP_Options = SL2P_V1.make_VP_options(param_name)  #VP => vegetation parameter
    if VP_Options != None:
      out_veg_maps[param_name] = one_vege_param_map(estimateSL2P_2DNets, VP_Options, DS_Options, inImg, sub_netID_map)
      #outDF['error'+v_param] = one_vege_param_map(errorsSL2P_2DNets,   VP_Options, DS_Options, inImg, cliped_netID_map)

  '''
  print('SL2P end: %s' %(datetime.now()))
    
  # generate sl2p input data flag
  outDF['QC_input'] = invalidInput(xrDS, VP_Options, DS_Options)

  # generate sl2p output product flag
  outDF['QC_output'] = invalidOutput(outDF.loc[:,'estimate'+VPName], VP_Options)
  print('Done')
  '''

  return out_veg_maps





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