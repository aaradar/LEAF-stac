import xarray as xr
import numpy as np
import time

import eoImage as Img
import eoClustering as Clust



def Cluster_water_mapping(Cluster_map, Centroids):
  water_clusters = []
  N, C = Centroids.shape
  
  if C < 5:
    for i in range(N):
      grn = Centroids[i, 1]
      red = Centroids[i, 2]
      nir = Centroids[i, 3]

      ndwi = (grn - nir) / (nir + grn + 1e-6)
      ndvi = (nir - red) / (nir + red + 1e-6)

      if ndwi > 0.01 and ndvi < 0.25:
        water_clusters.append(i)

  else:
    for i in range(N):
      grn = Centroids[i, 1]
      sw1 = Centroids[i, 7] if C > 6 else Centroids[i, 4]
      sw2 = Centroids[i, 8] if C > 6 else Centroids[i, 5]
      min_sw = min(sw1, sw2)
      max_sw = max(sw1, sw2)

      mndwi = (grn - min_sw) / (grn + min_sw + 1e-6)

      if mndwi > 0.1 and max_sw*0.01 < 7.5:
        water_clusters.append(i)

  if len(water_clusters) > 0:
    print('\n\nwater clusters: ', water_clusters)
    mask = np.isin(Cluster_map, water_clusters)   # True for allowed values, False otherwise
    return np.where(mask, Cluster_map, -10)

  else:    
    return np.zeros_like(Cluster_map)
  





# KeyStrings = ['blue_', 'green_', 'red_', 'edge1_', 'edge2_', 'edge3_', 'nir08_', 'swir16_', 'swir22_']
# KeyStrings_10m = ['blue_', 'green_', 'red_', 'nir08_']

# DataDir = 'C:\\Work_Data\\S2_mosaic_vancouver2020_20m_for_testing_gap_filling'
# taget_img, taget_mask, taget_profile = Img.load_TIF_files_to_npa(DataDir, KeyStrings, 'Aug')

# cluster_map, centroids = Clust.faiss_cluster(taget_img, 20, 200, 100000)
# outFile = DataDir + '\\' + 'cluster_map_20m.tif'

# Img.save_npa_as_geotiff(outFile, cluster_map, taget_profile)

# print("\n\nCentroids:")
# np.set_printoptions(precision=4, suppress=True)
# print(centroids)

# water_map = Cluster_water_mapping(cluster_map, centroids)
# outFile = DataDir + '\\' + 'water_map_20m.tif'
# Img.save_npa_as_geotiff(outFile, water_map, taget_profile)

