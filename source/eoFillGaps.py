# import numpy as np
# import glob
# from sklearn.cluster import KMeans
# from osgeo import gdal, gdal_array

# # Enable GDAL exceptions
# gdal.UseExceptions()

# # Step 1: Get list of all TIFF files (Adjust path as needed)
# tif_files = sorted(glob.glob("C:/Work_Data/test_clustering/*.tif"))  # Sort to maintain order

# if not tif_files:
#     raise FileNotFoundError("No TIFF files found in the specified directory!")

# # Step 2: Read all TIFF files and stack as a multi-band NumPy array
# bands = []
# for file in tif_files:
#     ds = gdal.Open(file, gdal.GA_ReadOnly)
#     if ds is None:
#         raise RuntimeError(f"Error opening {file}")
    
#     band = ds.GetRasterBand(1).ReadAsArray()  # Read single-band raster
#     bands.append(band)

# # Convert list of 2D arrays into a 3D NumPy array (height, width, bands)
# img_stack = np.dstack(bands)
# height, width, num_bands = img_stack.shape
# print(f"Image dimensions: {height}x{width} with {num_bands} bands")

# # Step 3: Reshape data for clustering (each pixel is a feature vector)
# X = img_stack.reshape((-1, num_bands))  # Shape: (num_pixels, num_bands)

# # Step 4: Perform K-Means Clustering
# n_clusters = 8  # Adjust as needed
# k_means = KMeans(n_clusters=n_clusters, init="k-means++", n_init=10, random_state=42)
# k_means.fit(X)

# # Step 5: Reshape cluster labels back to image dimensions
# X_cluster = k_means.labels_.reshape(height, width)

# # Step 6: Save the clustered image as a new raster
# output_file = "C:/Work_Data/test_clustering/clustered_image.tif"
# driver = gdal.GetDriverByName("GTiff")
# out_ds = driver.Create(output_file, width, height, 1, gdal.GDT_Byte)

# out_ds.GetRasterBand(1).WriteArray(X_cluster)
# out_ds.FlushCache()
# out_ds = None  # Close file

# print(f"Clustering completed! Saved result to {output_file}")

#############################################################################################################
# Description: This function returns a dictionary that contains statistics (mean, STD and number of pixels)
#              on each cluster.
#
# Revision history:  2025-Feb-26  Lixin Sun  
#############################################################################################################
def cluster_stats(k_means, Data, n_clusters):
  cluster_stats = {}

  for cluster_id in range(n_clusters):
    cluster_pixels = Data[k_means.labels_ == cluster_id]  # Extract pixels in this cluster
    
    if cluster_pixels.size > 0:  # Ensure there are pixels in the cluster
      mean_values = np.mean(cluster_pixels, axis=0)  # Mean per band
      std_values  = np.std(cluster_pixels, axis=0)  # Std deviation per band
    else:
      mean_values = std_values = np.zeros(num_bands)  # Placeholder if empty

    cluster_stats[cluster_id] = {
      "mean": mean_values,
      "std": std_values,
      "num_pixels": cluster_pixels.shape[0]
    }

  # Print results
  for cluster_id, stats in cluster_stats.items():
    print(f"Cluster {cluster_id}:")
    print(f"  Mean per band: {stats['mean']}")
    print(f"  Std deviation per band: {stats['std']}")
    print(f"  Number of pixels: {stats['num_pixels']}\n")



import numpy as np
import glob
from sklearn import cluster
from osgeo import gdal, osr

# Enable GDAL exceptions
gdal.UseExceptions()

# Step 1: Get list of all TIFF files (Adjust path as needed)
tif_files  = sorted(glob.glob("C:/Work_Data/S2_tile55_923_mosaic/*.tif"))  # Ensure proper ordering
spec_files = [f for f in tif_files if 'tile' in f]

if not spec_files:
  raise FileNotFoundError("No TIFF files found in the specified directory!")
else:
  print('\n All spectral file names:\n')
  print(spec_files)

# Step 2: Read all TIFF files and stack them into a multi-band NumPy array
bands = []
ds_ref = gdal.Open(spec_files[0], gdal.GA_ReadOnly)  # Use first file as reference

if ds_ref is None:
  raise RuntimeError(f"Error opening {spec_files[0]}")

geotransform = ds_ref.GetGeoTransform()  # Spatial reference
projection   = ds_ref.GetProjection()  # Projection info
width        = ds_ref.RasterXSize
height       = ds_ref.RasterYSize

for file in spec_files:
  ds = gdal.Open(file, gdal.GA_ReadOnly)
  if ds is None:
    raise RuntimeError(f"Error opening {file}")
    
  band = ds.GetRasterBand(1).ReadAsArray()  # Read single-band raster
  bands.append(band)

# Convert list of 2D arrays into a 3D NumPy array (height, width, bands)
img_stack = np.dstack(bands).astype(np.float32) / 100.0
_, _, num_bands = img_stack.shape
print(f"Image dimensions: {height}x{width} with {num_bands} bands")

# Step 3: Reshape data for clustering (each pixel is a feature vector)
X = img_stack.reshape((-1, num_bands))   # Shape: (num_pixels, num_bands)
sample_size = min(1000000, X.shape[0])  # Limit to 100k samples
X_sample = X[np.random.choice(X.shape[0], sample_size, replace=False)]

# Step 4: Perform K-Means Clustering
n_clusters = 500  # Adjust as needed
k_means = cluster.MiniBatchKMeans(n_clusters=n_clusters, init="k-means++", n_init=10, random_state=42)
k_means.fit(X_sample)

cluster_stats(k_means, X_sample, n_clusters)

# Step 5: Reshape cluster labels back to image dimensions
#X_cluster = k_means.labels_.reshape(height, width)
X_cluster = k_means.predict(X).reshape(height, width)

# Step 6: Save the clustered image as a new raster with original georeferencing
output_file = "C:/Work_Data/test_clustering/clustered_image_MBKM_500.tif"
driver = gdal.GetDriverByName("GTiff")
out_ds = driver.Create(output_file, width, height, 1, gdal.GDT_Int16)

out_ds.SetGeoTransform(geotransform)  # Preserve spatial reference
out_ds.SetProjection(projection)  # Preserve projection info
out_ds.GetRasterBand(1).WriteArray(X_cluster)
out_ds.FlushCache()
out_ds = None  # Close file

print(f"Clustering completed! Saved result to {output_file} with georeferencing.")
