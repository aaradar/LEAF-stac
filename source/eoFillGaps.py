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


import numpy as np
import rasterio
import faiss

import eoImage as eoImg




###################################################################################################
# Description: This function separate valid pixels in "img" into two groups: reference pixels and
#              gap pixels
#             
###################################################################################################
def separate_valid_pixels(ReferImg, ReferMask, GapMask):
  """
    Inputs:
      ReferImg(H, W, C): A given multispectral reference image
      ReferMask(H, W): A pixel mask (1 = valid pixel) for valid pixels in the reference image
      GapMask(H, W): A pixel mask (1 = gap pixel) for gap pixels in the target image

    Returns:
      ref_pixels: (N_ref, C) array of reference pixel spectra
      gap_pixels: (N_gap, C) array of gap pixel spectra
      ref_coords: (N_ref, 2) array of (row, col) coordinates of reference pixels
      gap_coords: (N_gap, 2) array of (row, col) coordinates of gap pixels
  """

  # make a copy of the pixel mask for valid pixels in reference image
  combined_mask = ReferMask.copy()

  # Create a pixel mask identifying the valid pixels in both the target and reference images
  ref_pixels_mask = combined_mask & (1 - GapMask)

  # Create a pixel mask identifying the gap pixels in target image, but valid in reference image
  gap_pixels_mask = combined_mask & GapMask

  # Extract shared valid pixels in both the target and reference images
  ref_rows, ref_cols = np.where(ref_pixels_mask == 1)
  shared_valid_pixs = ReferImg[ref_rows, ref_cols]  # shape (N_ref, C)

  # Extract valid pixels in the reference image corresponding to gap pixels in target image
  gap_rows, gap_cols = np.where(gap_pixels_mask == 1)
  valid_gap_pixs = ReferImg[gap_rows, gap_cols]  # shape (N_gap, C)

  # Optional: coordinates of each pixel in the original image
  shared_valid_coords = np.column_stack((ref_rows, ref_cols))  # (row, col)
  valid_gap_coords    = np.column_stack((gap_rows, gap_cols))

  return shared_valid_pixs, valid_gap_pixs, shared_valid_coords, valid_gap_coords




###################################################################################################
# Description: This function builds FAISS index and coordinate lookup table from "img" and "mask".
#
# Revision history:  2025-Nov-20  Lixin Sun
###################################################################################################
# def build_faiss_index(ImgArr, Coords, metric="cosine", use_ivf=False, nlist=1024):
#   """
#     Inputs:
#       ImgArr: (N x C) multispectral image array, N = number of pixels, C = number of bands
#       metric: "cosine", "euclidean"
#       use_ivf: bool, whether to use IVF index
#       nlist: number of IVF lists (initially created clusters if use_ivf=True)

#     Returns:
#       index: faiss index (searchable)
#       coords: (N_valid, 2) array mapping index -> (row, col)
#       valid_pixels: (N_valid, C) array used to build index (float32)
#   """

#   N, C = ImgArr.shape
#   #================================================================================================
#   # Extract and flaten all valid pixels into (N, C), where N and C are number of valid pixels and 
#   # number of bands, respectively.
#   #================================================================================================
#   #valid_pixels = img[mask == 1].reshape(-1, C).astype("float32")

#   # Store original pixel coordinates
#   #coords = np.column_stack(np.where(mask))  # shape (N, 2)

#   #================================================================================================
#   # Build index container depending on "metric" and "use_ivf"
#   #================================================================================================
#   if metric == "cosine":
#     # Normalize vectors -> inner product == cosine similarity
#     faiss.normalize_L2(ImgArr)

#     if use_ivf:
#       quantizer = faiss.IndexFlatIP(C)
#       index     = faiss.IndexIVFFlat(quantizer, C, nlist, faiss.METRIC_INNER_PRODUCT)
#       index.train(ImgArr)
#     else:
#       index = faiss.IndexFlatIP(C)

#   elif metric == "euclidean":
#     if use_ivf:
#       quantizer = faiss.IndexFlatL2(C)
#       index     = faiss.IndexIVFFlat(quantizer, C, nlist, faiss.METRIC_L2)
#       index.train(ImgArr)
#     else:
#       index = faiss.IndexFlatL2(C)

#   else:
#     raise ValueError("Unsupported metric: choose 'cosine' or 'euclidean'")
  
#   #================================================================================================
#   # Add valid pixels, which must be a 2-D NumPy array of float32, to the index container
#   #================================================================================================
#   index.add(ImgArr)

#   return index, Coords



def build_faiss_index(ImgArr, Coords, metric="cosine", use_ivf=False, nlist=1024):
    ImgArr = ImgArr.astype("float32").copy()  # avoid modifying original

    N, C = ImgArr.shape

    # ------------------------------------------------------------------
    # Cosine normalization
    # ------------------------------------------------------------------
    if metric == "cosine":
        faiss.normalize_L2(ImgArr)

    # ------------------------------------------------------------------
    # Build FAISS index
    # ------------------------------------------------------------------
    if metric == "cosine":
        quantizer = faiss.IndexFlatIP(C)
        if use_ivf:
            index = faiss.IndexIVFFlat(quantizer, C, nlist, faiss.METRIC_INNER_PRODUCT)
            index.train(ImgArr)
        else:
            index = quantizer

    elif metric == "euclidean":
        quantizer = faiss.IndexFlatL2(C)
        if use_ivf:
            index = faiss.IndexIVFFlat(quantizer, C, nlist, faiss.METRIC_L2)
            index.train(ImgArr)
        else:
            index = quantizer
    else:
        raise ValueError("Unsupported metric")

    # Add vectors
    index.add(ImgArr)

    return index, Coords






###################################################################################################
# Description: This function searchs index for efficiently searching valid pixels in the given image.
#
# Revision history:  2025-Nov-20  Lixin Sun
###################################################################################################
def query_targets(target_pixels, index, coords, k=5, metric="cosine"):
  """
  target_pixels: array (M, C)
  returns:
      nn_coords: (M, k, 2)
      scores: similarity or distance values
  """

  Xq = target_pixels.astype("float32")

  # Normalize if cosine similarity
  if metric == "cosine":
    faiss.normalize_L2(Xq)

  # FAISS batch search
  scores, I = index.search(Xq, k)

  # Convert FAISS indices → image coordinates
  nn_coords = coords[I]

  return nn_coords, scores




def estimate_gap_pixels(TargetImg, Coords, scores):
  """
    Inputs:
      TargetImg(H, W, C): A given multispectral target image with gaps;
      nn_coords(M, k, 2): An array of nearest neighbor coordinates for M gap pixels;
      scores(M, k): An array of similarity or distance scores for M gap pixels; """

  # coords shape: (G, K, 2)  -> (4520, 7, 2)
  G, K, _ = Coords.shape

  # Split row and column indices
  rows = Coords[:, :, 0]
  cols = Coords[:, :, 1]

  # Extract pixel spectra based on row and col coordinates
  # Result shape: (G, K, C)
  pixel_groups = TargetImg[rows, cols]  

  # Compute mean spectrum across the K pixels
  # Result shape: (G, C)
  mean_spectra = pixel_groups.mean(axis=1)
  
  return mean_spectra



def assign_estimated_pixels(TargetImg, GapCoords, EstimatedPixels):
  """
    Inputs:
      TargetImg(H, W, C): A given multispectral target image with gaps;
      GapCoords(N, 2): An array of coordinates for N gap pixels;
      EstimatedPixels(N, C): An array of estimated pixel spectra for N gap pixels;
  """
  # Split row and column indices
  rows = GapCoords[:, 0]
  cols = GapCoords[:, 1]

  # Assign estimated pixel spectra to the target image at the gap coordinates
  TargetImg[rows, cols] = EstimatedPixels

  return TargetImg



def save_filled_image(FilledImg, Profile, SavePath):
  """
    Inputs:
      FilledImg(H, W, C): A given multispectral gap-filled target image;
      Profile: rasterio profile for saving the image;
      SavePath(string): A string specifying the path to save the filled image.
  """
  out_type  = rasterio.int16
  out_image = (FilledImg * 100).astype(out_type)

  # Update profile for multi-band image
  Profile.update(dtype=out_type, count=out_image.shape[2])

  with rasterio.open(SavePath, 'w', **Profile) as dst:
    for i in range(out_image.shape[2]):
      dst.write(out_image[:, :, i].astype(out_type), i + 1)





def fill_gaps(DataPath, TargetMonth, ReferMonth, nBands, TileStr, SaveMask=False):
  """
    Inputs:
      DataPath(string): A string containing the path to a given data directory;
      TargetMonth(string): A string specifying the target month for gap filling;
      ReferMonth(string): A string specifying the reference month for gap filling;
      nBands(int): An integer specifying the number of bands in the image;
      SaveMask(Boolean): A boolean indicating whether to save the mask file.
  """
  #================================================================================================
  # Determine key band strings based on number of bands
  #================================================================================================
  if nBands == 6:
    KeyStrings = ['blue_', 'green_', 'red_', 'nir08_', 'swir16_', 'swir22_']
  elif nBands > 6 and nBands <= 9:
    KeyStrings = ['blue_', 'green_', 'red_', 'edge1_', 'edge2_', 'edge3_', 'nir08_', 'swir16_', 'swir22_']
  else:
    print("<fill_gaps> Unsupported number of bands!")
    return
  
  #================================================================================================
  # Load target month image and its valid mask
  #================================================================================================  
  taget_img, taget_mask, taget_profile = eoImg.load_TIF_files_to_npa(DataPath, KeyStrings, TargetMonth, TileStr)
  refer_img, refer_mask, refer_profile = eoImg.load_TIF_files_to_npa(DataPath, KeyStrings, ReferMonth, TileStr)

  if taget_img is None or taget_mask is None:
    print("<fill_gaps> Failed to load target month image or mask.")
    return  
  
  if SaveMask:
    target_mask_path = DataPath + '\\' + TargetMonth + '_mask.tif'
    refer_mask_path  = DataPath + '\\' + ReferMonth + '_mask.tif'
    mask_profile     = taget_profile.copy()
    mask_profile.update(dtype=rasterio.uint8, count=1, nodata=None)

    eoImg.save_mask_to_tif(taget_mask, mask_profile, target_mask_path)
    eoImg.save_mask_to_tif(refer_mask, mask_profile, refer_mask_path)

  #================================================================================================
  # Rescale the SR values from integer to float
  #================================================================================================
  taget_img = taget_img * 0.01
  refer_img = refer_img * 0.01
  
  gap_mask = 1 - taget_mask
  shared_valid_pixs, valid_gap_pixs, shared_valid_coords, valid_gap_coords = separate_valid_pixels(refer_img, refer_mask, gap_mask)

  #index, coords = build_faiss_index(refer_img, refer_mask, "euclidean", True)
  index, shared_valid_coords = build_faiss_index(shared_valid_pixs, shared_valid_coords, "euclidean", True)

  nn_coords, scores = query_targets(valid_gap_pixs, index, shared_valid_coords, 7, "euclidean")

  #estimated_pixels = estimate_gap_pixels(taget_img, nn_coords, scores)
  estimated_pixels = estimate_gap_pixels(taget_img, nn_coords, scores)

  
  filled_target = assign_estimated_pixels(taget_img, valid_gap_coords, estimated_pixels)
  
  out_path = DataPath + '\\' + TargetMonth + '_filled.tif'
  save_filled_image(filled_target, taget_profile, out_path)
  
  return




fill_gaps('E:\HLS_mosaics_2025\HLS_mosaic_tile42_2025_30m', 'Jun', 'Jul', 6, 'tile42_422',
          'E:\HLS_mosaics_2025\HLS_mosaic_tile42_2025_30m\\Jun_Jul_mask.tif')