
'''
ATPRK downscale example (20m -> 10m) for Sentinel-2 using 4 x 10m bands as predictors.
'''

import numpy as np
import rasterio
import matplotlib.pyplot as plt
import dask.array as da

import eoImage as Img


from dask import delayed, compute
from joblib import Parallel, delayed
from rasterio.enums import Resampling
#from pykrige.ok import OrdinaryKriging
from scipy.ndimage import uniform_filter
from scipy import stats
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed





###################################################################################################
# Description: This function returns a set of coefficients of a local mapping function between VNIR
#              band windows and a target band window.
#
# Revision history:  2024-Nov-28  Lixin Sun  initial creation
###################################################################################################
def local_mapping_function(VNIRImg, TargetImg, x, y, WinSize=7):
  """
    Inputs:
      VNIRImg: A given image cube containing VNIR band images [blue, green, red, and NIR].
      TargetImg: A target band image/array at coarse resolution (e.g., 20-m).
      x, y: Coordinates of the pixel to fit the local mapping function.

    Returns:
      coef: Coefficients of the local mapping function (including intercept).
  """

  if WinSize < 3:
    WinSize = 3

  VNIR_Win   = Img.extract_window(VNIRImg, x, y, WinSize)
  target_Win = Img.extract_window(TargetImg, x, y, WinSize)

  # Flatten spatial dimensions
  flatVNIR   = VNIR_Win.reshape(-1, VNIR_Win.shape[2])        # shape = (49, 4)
  flatTarget = target_Win.reshape(-1)         # shape = (49,)

  # Add bias column of 1's → for intercept d
  X_design = np.hstack([flatVNIR, np.ones((flatVNIR.shape[0], 1))])  # shape (49, 5)

  # Solve least squares: [a1, a2, a3, a4, d]
  params, _, _, _ = np.linalg.lstsq(X_design, flatTarget, rcond=None)
  
  coeffs = params[:-1]
  bias = params[-1]

  return coeffs, bias




###################################################################################################
# Description: This function parallelizes computation of local mapping function for all pixels.
#
# Revision history:  2024-Nov-28  Lixin Sun  initial creation
###################################################################################################
# def compute_all_mappings(VNIRImg, TargetImg, Linear = True, WinSize=7, n_jobs=-1):
#   """
#     Inputs:
#       VNIRImg   : (H, W, 4) - VNIR image cube [blue, green, red, NIR]
#       TargetImg : (H, W)    - Target band image at coarse resolution
#       WinSize   : window size for local mapping function
#       Linear    : whether to use linear mapping only (True) or include nonlinear terms (False)
#       n-jobs    : number of parallel jobs (-1 means all available cores)

#       Returns:
#       A_coeff   : (H, W, 4)   # coefficients a1..a4
#       D_bias    : (H, W)      # intercept d.
#   """

#   if WinSize < 3:
#     WinSize = 3

#   H, W, C = VNIRImg.shape
#   if TargetImg.ndim == 3:
#     TargetImg = np.squeeze(TargetImg, axis=2)

#   if TargetImg.shape[0] != H or TargetImg.shape[1] != W:
#     raise ValueError("TargetImg must match VNIRImg spatial dimensions.")

#   wr = WinSize // 2  # window radius

#   #================================================================================================
#   # Pad the given images so boundary pixels still produce full windows
#   # Pad with edge values (replicate padding)
#   #================================================================================================
#   padded_VNIR = np.pad(VNIRImg, ((wr, wr), (wr, wr), (0, 0)), mode='edge')
#   padded_Tgt  = np.pad(TargetImg, ((wr, wr), (wr, wr)), mode='edge')

#   #================================================================================================
#   # Add nolinear components to the image cube if needed
#   #================================================================================================
#   if not Linear:
#     red = padded_VNIR[:, :, 2]
#     nir = padded_VNIR[:, :, 3]

#     red_nir = (red * nir)[:, :, None]
#     red_red = (red * red)[:, :, None]
#     nir_nir = (nir * nir)[:, :, None]

#     UsedImg = np.concatenate([padded_VNIR, red_nir, red_red, nir_nir], axis=2)
#     nCoeffs = 7
#   else:
#     UsedImg = padded_VNIR
#     nCoeffs = 4

#   #================================================================================================
#   # run local mapping on all pixels in parallel
#   #================================================================================================
#   A_coeff = np.zeros((H, W, nCoeffs), dtype=np.float32)
#   D_bias  = np.zeros((H, W), dtype=np.float32)

#   # Function to process one row of pixels
#   def process_row(x, UsedImg, padded_Tgt, wr, WinSize, W, nCoeffs):
#     row_coeffs = np.zeros((W, nCoeffs), dtype=np.float32)
#     row_bias   = np.zeros(W, dtype=np.float32)

#     for y in range(W):
#       coeffs, bias = local_mapping_function(UsedImg, padded_Tgt, x+wr, y+wr, WinSize)
#       row_coeffs[y] = coeffs
#       row_bias[y]   = bias

#     return row_coeffs, row_bias

#   # Run in parallel per row
#   results = Parallel(n_jobs=n_jobs, backend="loky")(
#     delayed(process_row)(x, UsedImg, padded_Tgt, wr, WinSize, W, nCoeffs) for x in range(H))

# #   results = []
# #   for x in range(H):
# #     result = process_row(x, UsedImg, padded_Tgt, wr, WinSize, W, nCoeffs)
# #     results.append(result)

#   #================================================================================================
#   # unpack results
#   #================================================================================================
#   for x, (row_coeffs, row_bias) in enumerate(results):
#     A_coeff[x, :, :] = row_coeffs
#     D_bias[x, :]     = row_bias

#   return A_coeff, D_bias





def compute_all_mappings(VNIRImg, TargetImg, Linear=True, WinSize=7, n_jobs=-1, row_block=32):
    """
    Inputs:
      VNIRImg   : (H, W, 4) - VNIR image cube [blue, green, red, NIR]
      TargetImg : (H, W)    - Target band image at coarse resolution
      WinSize   : window size for local mapping function
      Linear    : whether to use linear mapping only (True) or include nonlinear terms (False)
      n_jobs    : number of parallel jobs (-1 means all available cores)
      row_block : number of rows processed per parallel job

    Returns:
      A_coeff   : (H, W, nCoeffs)
      D_bias    : (H, W)
    """

    if WinSize < 3:
        WinSize = 3

    H, W, C = VNIRImg.shape
    if TargetImg.ndim == 3:
        TargetImg = np.squeeze(TargetImg, axis=2)

    if TargetImg.shape[:2] != (H, W):
        raise ValueError("TargetImg must match VNIRImg spatial dimensions.")

    wr = WinSize // 2

    # Pad the images
    padded_VNIR = np.pad(VNIRImg,  ((wr, wr), (wr, wr), (0, 0)), mode='edge')
    padded_Tgt  = np.pad(TargetImg, ((wr, wr), (wr, wr)), mode='edge')

    # Add nonlinear components
    if not Linear:
        red = padded_VNIR[:, :, 2]
        nir = padded_VNIR[:, :, 3]

        red_nir = (red * nir)[:, :, None]
        red_red = (red * red)[:, :, None]
        nir_nir = (nir * nir)[:, :, None]

        UsedImg = np.concatenate([padded_VNIR, red_nir, red_red, nir_nir], axis=2)
        nCoeffs = 7
    else:
        UsedImg = padded_VNIR
        nCoeffs = 4

    # Output arrays
    A_coeff = np.zeros((H, W, nCoeffs), dtype=np.float32)
    D_bias  = np.zeros((H, W), dtype=np.float32)

    # -------------------------------------------------------------------------
    # Function to process a block of rows
    # -------------------------------------------------------------------------
    def process_row_block(start_row, end_row, UsedImg, padded_Tgt, wr, WinSize, W, nCoeffs):
        block_h = end_row - start_row
        block_coeff = np.zeros((block_h, W, nCoeffs), dtype=np.float32)
        block_bias  = np.zeros((block_h, W), dtype=np.float32)

        for idx, x in enumerate(range(start_row, end_row)):
            px = x + wr  # padded x location

            for y in range(W):
                coeffs, bias = local_mapping_function(
                    UsedImg, padded_Tgt, px, y + wr, WinSize
                )
                block_coeff[idx, y] = coeffs
                block_bias[idx, y]  = bias

        return start_row, end_row, block_coeff, block_bias

    # Build jobs as (start_row, end_row)
    row_jobs = []
    for start in range(0, H, row_block):
        end = min(start + row_block, H)
        row_jobs.append((start, end))

    # Run row-blocks in parallel
    results = Parallel(n_jobs=n_jobs, backend="loky")(
        delayed(process_row_block)(
            start, end, UsedImg, padded_Tgt, wr, WinSize, W, nCoeffs
        )
        for start, end in row_jobs
    )

    # Unpack results
    for start, end, blk_coeff, blk_bias in results:
        A_coeff[start:end, :, :] = blk_coeff
        D_bias[start:end, :]     = blk_bias

    return A_coeff, D_bias






###################################################################################################
# Description: This function computes local mapping coefficients for all pixels using DASK blocks.
#
# Revision history:  2024-Nov-28  Lixin Sun  initial creation
###################################################################################################
def compute_all_mappings_dask(VNIRImg, TargetImg, Linear=True, WinSize=7, BlockSize=256):
  """    
    Inputs:
      VNIRImg   : (H, W, 4) - VNIR image cube [blue, green, red, NIR]
      TargetImg : (H, W)    - Target band image at coarse resolution
      Linear    : True for linear mapping, False to include nonlinear terms
      WinSize   : int, window size (default 7)
      BlockSize: int, size of blocks for Dask processing
    
    Returns:
      A_coeff : (H, W, 4 or 7) - local coefficients for each pixel
      D_bias  : (H, W)         - intercept for each pixel
  """
    
  if WinSize < 3:
    WinSize = 3

  H, W, C = VNIRImg.shape
  if TargetImg.ndim == 3:
    TargetImg = np.squeeze(TargetImg, axis=2)
  
  if TargetImg.shape[0] != H or TargetImg.shape[1] != W:
    raise ValueError("TargetImg must match VNIRImg spatial dimensions.")

  wr = WinSize // 2

  # Pad images to handle borders
  padded_VNIR = np.pad(VNIRImg, ((wr, wr), (wr, wr), (0, 0)), mode='edge')
  padded_Tgt  = np.pad(TargetImg, ((wr, wr), (wr, wr)), mode='edge')

  # Add nonlinear terms if needed
  if not Linear:
    red = padded_VNIR[:, :, 2]
    nir = padded_VNIR[:, :, 3]

    red_nir = (red * nir)[..., None]
    red_red = (red * red)[..., None]
    nir_nir = (nir * nir)[..., None]

    UsedImg = np.concatenate([padded_VNIR, red_nir, red_red, nir_nir], axis=2)
    nCoeffs = 7
  else:
    UsedImg = padded_VNIR
    nCoeffs = 4

  # Convert to Dask arrays with chunks
  dask_VNIR = da.from_array(UsedImg, chunks=(BlockSize, BlockSize, UsedImg.shape[2]))
  dask_Tgt  = da.from_array(padded_Tgt, chunks=(BlockSize, BlockSize))

  # Define function to process a block
  def process_block(vnir_block, tgt_block):
    H_blk, W_blk, _ = vnir_block.shape
    A_coeff_blk = np.zeros((H_blk, W_blk, nCoeffs), dtype=np.float32)
    D_bias_blk  = np.zeros((H_blk, W_blk), dtype=np.float32)
    
    #if tgt_block.shape[0] == H_blk and tgt_block.shape[1] == W_blk:
    for i in range(H_blk):
      for j in range(W_blk):
        coeffs, bias = local_mapping_function(vnir_block, tgt_block, i, j, WinSize)
        A_coeff_blk[i, j, :] = coeffs
        D_bias_blk[i, j] = bias    
  
    return A_coeff_blk, D_bias_blk
   
  # Wrap with delayed for Dask
  #def delayed_block(vnir_block, tgt_block):
  #  return delayed(process_block)(vnir_block, tgt_block)
  delayed_blocks = []
  for i in range(0, H + 2*wr, BlockSize):
    for j in range(0, W + 2*wr, BlockSize):
      v_block = dask_VNIR[i:i+BlockSize, j:j+BlockSize, :].compute()
      t_block = dask_Tgt[i:i+BlockSize, j:j+BlockSize].compute()
      delayed_blocks.append(delayed(process_block)(v_block, t_block))

  # Compute all blocks in parallel
  results = compute(*delayed_blocks, scheduler='threads')  

#   delayed_blocks = []
#   for i in range(0, dask_VNIR.shape[0], BlockSize):
#     for j in range(0, dask_VNIR.shape[1], BlockSize):
#       v_block = dask_VNIR[i:i+BlockSize, j:j+BlockSize, :]  #.compute()
#       t_block = dask_Tgt[i:i+BlockSize, j:j+BlockSize]  #.compute()
#       delayed_blocks.append(delayed(process_block)(v_block, t_block))
#       #delayed_blocks.append(process_block(v_block, t_block))

#   # Compute all blocks in parallel lazily
#   results = compute(*delayed_blocks, scheduler='threads')
  #results = list(compute(*delayed_blocks, scheduler='single-threaded'))
  #results = delayed_blocks  # already computed above

  print("Type of results:", type(results))
  print("Length of results:", len(results))
  print("Type of first element:", type(results[0]))
  print("length of first element:", len(results[0]))
  print("content of first element:", results[0])

  # Combine results into full arrays
  A_coeff = np.zeros((H + 2*wr, W + 2*wr, nCoeffs), dtype=np.float32)
  D_bias  = np.zeros((H + 2*wr, W + 2*wr), dtype=np.float32)

  blk_idx = 0
  bh, bw = BlockSize, BlockSize
  for i in range(0, H + 2*wr, bh):
    for j in range(0, W + 2*wr, bw):
      blk = results[blk_idx]    

      _, a_blk, d_blk = results[blk_idx]

      print("[DEBUG] a_blk={}, type={}, length={}".format(a_blk, type(a_blk), len(a_blk)))
    #   blk = results[blk_idx]
    #   a_blk = blk["A"]
    #   d_blk = blk["D"]
      H_blk, W_blk, _ = a_blk.shape

      A_coeff[i:i+H_blk, j:j+W_blk, :] = a_blk
      D_bias[i:i+H_blk, j:j+W_blk] = d_blk
      blk_idx += 1

  # Crop padded borders
  A_coeff = A_coeff[wr:wr+H, wr:wr+W, :]
  D_bias  = D_bias[wr:wr+H, wr:wr+W]

  return A_coeff, D_bias






###################################################################################################
# Description: This function applys the regression coefficients derived from low-resolution (LR)
#              images to a high-resolution (HR) VNIR image.
#
# Revision history:  2024-Nov-28  Lixin Sun  initial creation
###################################################################################################
def apply_mapping_coeffs(HR_VNIRImg, LR_Coeffs_Map, LR_bias_Map, H_to_L_ratio=2):
  """
    Inputs:
      HR_VNIRImg   : (HR_H, HR_W, C) - High-resolution VNIR image (blue, green, red, NIR)
      LR_Coeffs_Map: (LR_H, LR_W, C) - Coefficients map (a1..a4) at high resolution
      LR_bias_Map  : (LR_H, LR_W)    - Bias map (d) at low resolution
      H_to_L_ratio : int             - Ratio of high to low resolution (e.g., 2 for 20m->10m)

    Output:
      HR_TargetImg : (HR_H, HR_W)    - Predicted target band image at high resolution.
  """

  HR_H, HR_W, nBands  = HR_VNIRImg.shape
  LR_H, LR_W, nCoeffs = LR_Coeffs_Map.shape

  #================================================================================================
  # Upsample the low-resolution coefficients to high-resolution by repeating in 2×2 blocks
  #================================================================================================
  if H_to_L_ratio > 1:
    HR_Coeffs_map = np.repeat(np.repeat(LR_Coeffs_Map, H_to_L_ratio, axis=0), H_to_L_ratio, axis=1)   # shape → (HR_H, HR_W, 4)
    HR_bias_map   = np.repeat(np.repeat(LR_bias_Map,   H_to_L_ratio, axis=0), H_to_L_ratio, axis=1)   # shape → (LR_H, LR_W)
  else:
    HR_Coeffs_map = LR_Coeffs_Map
    HR_bias_map   = LR_bias_Map
    
  #================================================================================================
  # Ensure dimensions match exactly (crop if necessary)
  #================================================================================================
  HR_Coeffs_map = HR_Coeffs_map[:HR_H, :HR_W, :]
  HR_bias_map   = HR_bias_map[:HR_H, :HR_W]

  #================================================================================================
  # Apply the mapping: a1*B + a2*G + a3*R + a4*NIR + d
  #================================================================================================
  if nCoeffs == 4:
    HR_TargetImg = (
      HR_Coeffs_map[:, :, 0] * HR_VNIRImg[:, :, 0] +
      HR_Coeffs_map[:, :, 1] * HR_VNIRImg[:, :, 1] +
      HR_Coeffs_map[:, :, 2] * HR_VNIRImg[:, :, 2] +
      HR_Coeffs_map[:, :, 3] * HR_VNIRImg[:, :, 3] +
      HR_bias_map)
  elif nCoeffs == 7:
    red = HR_VNIRImg[:, :, 2]
    nir = HR_VNIRImg[:, :, 3]

    red_nir = red * nir
    red_red = red * red
    nir_nir = nir * nir

    HR_TargetImg = (
        HR_Coeffs_map[:, :, 0] * HR_VNIRImg[:, :, 0] +
        HR_Coeffs_map[:, :, 1] * HR_VNIRImg[:, :, 1] +
        HR_Coeffs_map[:, :, 2] * HR_VNIRImg[:, :, 2] +
        HR_Coeffs_map[:, :, 3] * HR_VNIRImg[:, :, 3] +
        HR_Coeffs_map[:, :, 4] * red_nir +
        HR_Coeffs_map[:, :, 5] * red_red +
        HR_Coeffs_map[:, :, 6] * nir_nir +
        HR_bias_map)

  return HR_TargetImg





def Piecewise_SuperResolution(DataDir, Month, TargetName='swir16_'):
  '''
  '''

  #KeyStrings = ['blue_', 'green_', 'red_', 'edge1_', 'edge2_', 'edge3_', 'nir08_', 'swir16_', 'swir22_']
  HR_bands = ['blue_', 'green_', 'red_', 'nir08_']

  VNIR_Img, VNIR_mask, VNIR_profile       = Img.load_TIF_files_to_npa(DataDir, HR_bands, Month)
  target_Img, target_mask, target_profile = Img.load_TIF_files_to_npa(DataDir, [TargetName], Month)

  coeff_map, offset_map = compute_all_mappings(VNIR_Img, target_Img, False, 7, -1, 50)
  #coeff_map, offset_map = compute_all_mappings(VNIR_Img, target_Img, True, 7, 256)

  HR_Target = apply_mapping_coeffs(VNIR_Img, coeff_map, offset_map, 1)

  outFile = DataDir + '\\' + 'super_resed_map_20m.tif'

  Img.save_npa_as_geotiff(outFile, HR_Target, target_profile)


DataDir = 'C:\\Work_Data\\S2_mosaic_vancouver2020_20m_for_testing_gap_filling'
Piecewise_SuperResolution(DataDir, 'Aug', 'swir16_')
print("Done ATPRK super-resolution.")





###################################################################################################
# Purpose: Load a Sentinel-2 band from disk.
###################################################################################################
def load_band_rasterio(path):
    """Load a single-band raster with rasterio. Returns array (float) and meta.
       Args:
           path (string): file path to raster file"""
    with rasterio.open(path) as src:
        arr = src.read(1).astype(float)
        meta = src.meta.copy()

    return arr, meta




###################################################################################################
# Description: Aggregate 10m array to 20m by averaging each 2x2 block. Assumes b10 shape is
#              divisible by 2. Returns aggregated array of shape (h/2, w/2).
###################################################################################################
def aggregate_10_to_20_mean(b10):
    """
    Aggregate 10m array to 20m by averaging each 2x2 block. Assumes b10 shape is divisible by 2.
    Returns aggregated array of shape (h/2, w/2).
    """
    # if shapes are odd, trim last row/col
    h, w = b10.shape
    h2 = (h // 2) * 2
    w2 = (w // 2) * 2
    b10_trim = b10[:h2, :w2]

    # reshape trick to block-average 2x2
    b10_blocks = b10_trim.reshape(h2//2, 2, w2//2, 2)
    agg = b10_blocks.mean(axis=(1, 3))

    return agg



###################################################################################################
# Description: Fit multiple linear regression at coarse scale. This function returns coefficients
#              (including intercept) and predicted coarse.
###################################################################################################
def fit_regression_on_coarse_slow(CoarseImg, AgregatedImgs):
    """
      Args:
        CoarseImg: A band image (flattened array) in coarse resolution, e.g., 20-m SWIR1 or 2
        AgregatedImgs: A list of aggregated images (e.g., B2,B3,B4,B8 for S2) with the same resolution as CoarseImg.
    """
    n = CoarseImg.size

    X = np.column_stack([p.ravel() for p in AgregatedImgs] + [np.ones(n)])
    y = CoarseImg.ravel()

    coef, *_ = np.linalg.lstsq(X, y, rcond=None)
    y_hat = (X @ coef).reshape(CoarseImg.shape)

    return coef, y_hat




###################################################################################################
# Description: Fit multiple linear regression at coarse scale via normal equations (no full X).
#              This function returns coefficients (including intercept) and predicted coarse.
###################################################################################################
def fit_regression_on_coarse_fast(CoarseImg, AgregatedImgs):
    """
      Args:
        CoarseImg: A band image (flattened array) in coarse resolution, e.g., 20-m SWIR1 or 2
        AgregatedImgs: A list of aggregated images (e.g., B2,B3,B4,B8 for S2) with the same resolution as CoarseImg.
    """
    y = CoarseImg.astype(np.float64).ravel()
    P = [p.astype(np.float64).ravel() for p in AgregatedImgs]
    P.append(np.ones_like(y))  # intercept
    k = len(P)

    XT_X = np.zeros((k, k), dtype=np.float64)
    XT_y = np.zeros((k,), dtype=np.float64)

    # compute upper triangle and mirror (dot products use BLAS)
    for i in range(k):
        Xi = P[i]
        XT_y[i] = Xi.dot(y)
        for j in range(i, k):
            XT_X[i, j] = Xi.dot(P[j])
            XT_X[j, i] = XT_X[i, j]

    coef = np.linalg.solve(XT_X, XT_y)
    # predicted
    y_hat = np.zeros_like(y, dtype=np.float64)
    for c, p in zip(coef, P):
        y_hat += c * p
    y_hat = y_hat.reshape(CoarseImg.shape)
    return coef, y_hat





###################################################################################################
# Description: Apply regression coefficients at fine scale 10-m resolution images [B2,B3,B4,B8].
#              This prediction is the first step in ATPRK, before kriging residuals are added.
#
###################################################################################################
def predict_fine_from_regression_slow(coef, predictors10):
    """
      Args:
        coef: regression coefficients (including intercept);
        predictors10: A list of 10-m resolution images/arrays [B2,B3,B4,B8].
    """
    n = predictors10[0].size   # number of pixels in predictors10
    Xf = np.column_stack([p.ravel() for p in predictors10] + [np.ones(n)])

    # Produce the 10 m regression prediction of the coarse band.
    y_fine = (Xf @ coef).reshape(predictors10[0].shape)

    return y_fine




###################################################################################################
# Description: Apply regression coefficients at fine scale 10-m resolution images [B2,B3,B4,B8].
#              This prediction is the first step in ATPRK, before kriging residuals are added.
#
###################################################################################################
def predict_fine_from_regression_fast(coef, predictors10):
    """
      Args:
        coef: regression coefficients (including intercept);
        predictors10: A list of 10-m resolution images/arrays [B2,B3,B4,B8].
    """
    # predictors10: list of 2D arrays (float)
    pred = np.zeros_like(predictors10[0], dtype=np.float64)
    for i in range(len(predictors10)):
        pred += coef[i] * predictors10[i].astype(np.float64)
    pred += coef[-1]
    return pred




###################################################################################################
# Compute centroid coordinates for raster (meters assumed)
# meta must include 'transform' (affine tuple (a,b,c,d,e,f)) and width/height
# transform[2] = top-left x, transform[5] = top-left y; transform[0] pixel width, transform[4] pixel height (often negative)
###################################################################################################
def compute_centroid_coords(meta):
    """
    Compute centroid coordinates (x,y) for each pixel in a raster given rasterio meta.
    Returns arrays of shape (h,w) for x and y.
    """
    transform = meta['transform']
    width  = meta['width']
    height = meta['height']
    cols   = np.arange(width)
    rows   = np.arange(height)

    xx = transform[2] + cols * transform[0] + transform[0] / 2.0
    yy = transform[5] + rows * transform[4] + transform[4] / 2.0

    xgrid, ygrid = np.meshgrid(xx, yy)
    return xgrid, ygrid




###################################################################################################
# Helper: get coarse points within a radius of a window bounding box
###################################################################################################
def _select_coarse_points_in_radius(x20v, y20v, resv, win_x_min, win_x_max, win_y_min, win_y_max, radius, max_points):
    """
    Select coarse points whose coordinates are within (window bbox expanded by radius).
    Returns arrays xk, yk, rk (residuals), optionally subsampled to max_points.
    """
    # Expand bbox by radius (meters)
    minx = win_x_min - radius
    maxx = win_x_max + radius
    miny = win_y_min - radius
    maxy = win_y_max + radius

    mask = (x20v >= minx) & (x20v <= maxx) & (y20v >= miny) & (y20v <= maxy)
    idx = np.nonzero(mask)[0]
    if idx.size == 0:
        return np.array([]), np.array([]), np.array([])

    if idx.size > max_points:
        # uniform random sample to limit points
        sel = np.random.choice(idx, size=max_points, replace=False)
    else:
        sel = idx

    return x20v[sel], y20v[sel], resv[sel]



###################################################################################################
# Worker: krige a single window (fine window indices)
# - window is defined in fine-pixel coordinates (row_start,row_stop, col_start,col_stop)
# - predicts residuals for fine pixels in this window only
###################################################################################################
def _worker_krige_window(args):
    """
    args: tuple containing all necessary data (to keep pickling simple)
    returns: (row_start, row_stop, col_start, col_stop, z_window) where z_window matches window shape
    """
    (row_start, row_stop, col_start, col_stop,
     x20v, y20v, resv, x10, y10, fine_shape,
     radius, max_coarse_points, min_coarse_points,
     variogram_model) = args

    # bounding box of window in coordinates
    # compute coordinates for fine pixels in this window and their flat indices
    rows = np.arange(row_start, row_stop)
    cols = np.arange(col_start, col_stop)
    # x10 and y10 are full grids (2D). Extract subgrids
    x_window = x10[np.ix_(rows, cols)]
    y_window = y10[np.ix_(rows, cols)]
    flat_xw = x_window.ravel()
    flat_yw = y_window.ravel()

    win_x_min, win_x_max = flat_xw.min(), flat_xw.max()
    win_y_min, win_y_max = flat_yw.min(), flat_yw.max()

    # select coarse points within radius expanded bbox
    xk, yk, rk = _select_coarse_points_in_radius(x20v, y20v, resv,
                                                 win_x_min, win_x_max, win_y_min, win_y_max,
                                                 radius, max_coarse_points)

    # If not enough points, return zeros (no kriging correction)
    if xk.size < min_coarse_points:
        z_window = np.zeros_like(flat_xw, dtype=np.float64)
        return (row_start, row_stop, col_start, col_stop, z_window.reshape(x_window.shape))

    # Optionally, we could also further restrict xk,yk to nearest N by centroid distance
    # Build OrdinaryKriging and predict
    try:
        OK = OrdinaryKriging(xk, yk, rk, variogram_model=variogram_model, verbose=False, enable_plotting=False)
        z_pred, ss = OK.execute('points', flat_xw, flat_yw)
        z_window = z_pred.reshape(x_window.shape)
    except Exception as e:
        # If kriging fails for numerical reasons, return zeros
        print(f"Local kriging failed for window {(row_start, col_start)}: {e}")
        z_window = np.zeros_like(flat_xw, dtype=np.float64)
        z_window = z_window.reshape(x_window.shape)

    return (row_start, row_stop, col_start, col_stop, z_window)



###################################################################################################
# Main optimized ATPRK function (windowed kriging, parallel)
###################################################################################################
def atprk_downscale_optimized(coarse20, coarse_meta, predictors10, predictors10_meta,
                              window_size_fine=2048, overlap=128, radius_m=3000,
                              max_coarse_points=2000, min_coarse_points=30,
                              n_workers=4, variogram_model='spherical',
                              sample_for_kriging=False, sample_size=2000):
    """
    Optimized ATPRK pipeline for very large tiles.
    Returns: (atprk_10, reg_pred_fine, kriged_residuals)
    - coarse20: 2D array (20m)
    - coarse_meta: rasterio-style meta (transform, height, width)
    - predictors10: list of 2D arrays (10m) e.g., [B2,B3,B4,B8], same shape as predictors10_meta
    - predictors10_meta: rasterio-style meta for 10m
    - window_size_fine: window size in fine pixels (10m)
    - overlap: overlap in pixels between windows
    - radius_m: search radius (meters) to collect coarse points for local kriging
    """
    # 0) types and basic checks
    predictors10 = [p.astype(np.float32) for p in predictors10]
    coarse20     = coarse20.astype(np.float32)

    # 1) Aggregate predictors to 20m
    predictors20 = [aggregate_10_to_20_mean(p) for p in predictors10]

    # 2) Ensure shapes align (resample (not implemented here) if necessary)
    if predictors20[0].shape != coarse20.shape:
        raise ValueError("Aggregated predictors shape does not match coarse shape — resampling required.")

    # 3) Fit regression (fast)
    coef, reg_pred_coarse = fit_regression_on_coarse_fast(coarse20, predictors20)
    print("Regression coefficients:", coef)

    # 4) Predict at fine scale
    reg_pred_fine = predict_fine_from_regression_fast(coef, predictors10)

    # 5) residuals on coarse grid -> flatten coords for kriging sources
    residuals_coarse = coarse20.astype(np.float64) - reg_pred_coarse.astype(np.float64)
    x20, y20 = compute_centroid_coords(coarse_meta)
    x20v = x20.ravel()
    y20v = y20.ravel()
    resv = residuals_coarse.ravel()

    # optionally subsample coarse residuals for kriging (global sample)
    if sample_for_kriging:
        if resv.size > sample_size:
            idx = np.random.choice(resv.size, size=sample_size, replace=False)
            x20v = x20v[idx]; y20v = y20v[idx]; resv = resv[idx]

    # 6) Prepare fine coords grid
    x10, y10 = compute_centroid_coords(predictors10_meta)
    fine_h, fine_w = predictors10[0].shape

    # 7) Build window list (row_start, row_stop, col_start, col_stop)
    rows = list(range(0, fine_h, window_size_fine - overlap))
    cols = list(range(0, fine_w, window_size_fine - overlap))

    windows = []
    for r0 in rows:
        r1 = min(r0 + window_size_fine, fine_h)
        for c0 in cols:
            c1 = min(c0 + window_size_fine, fine_w)
            windows.append((r0, r1, c0, c1))

    print(f"Total windows: {len(windows)} (window_size={window_size_fine}, overlap={overlap})")

    # 8) Launch parallel kriging for each window
    tasks = []
    for (r0,r1,c0,c1) in windows:
        args = (r0, r1, c0, c1,
                x20v, y20v, resv,
                x10, y10, (fine_h, fine_w),
                radius_m, max_coarse_points, min_coarse_points, variogram_model)
        tasks.append(args)

    kriged_residuals = np.zeros((fine_h, fine_w), dtype=np.float64)

    # Use process pool
    with ProcessPoolExecutor(max_workers=n_workers) as ex:
        futures = {ex.submit(_worker_krige_window, task): task for task in tasks}
        for fut in tqdm(as_completed(futures), total=len(futures), desc="Local kriging windows"):
            try:
                r0, r1, c0, c1, z_win = fut.result()
                kriged_residuals[r0:r1, c0:c1] += z_win  # windows overlap -> sum contributions
            except Exception as e:
                task = futures[fut]
                print(f"Window {task[:4]} failed: {e}")

    # Optional: average overlapping areas
    # Build overlap-count grid to divide summed overlaps
    count_grid = np.zeros_like(kriged_residuals)
    for (r0,r1,c0,c1) in windows:
        count_grid[r0:r1, c0:c1] += 1.0
    # Avoid division by zero
    mask = count_grid > 0
    kriged_residuals[mask] /= count_grid[mask]

    # 9) Final ATPRK result
    atprk_10 = reg_pred_fine + kriged_residuals

    return atprk_10.astype(np.float32), reg_pred_fine.astype(np.float32), kriged_residuals.astype(np.float32), coef



def wald_validation(original10, downscale_func, coarse_band20, coarse_meta, predictors10, predictors10_meta):
    """
    Wald-style validation: simulate by downsampling the 10m 'original10' to 20m,
    apply the downscaling pipeline to the simulated coarse, and compare result with original10.
    Here, original10 is a true 10m band (e.g., B04) used to test algorithm.)
    """
    # create simulated coarse by block-averaging original10
    sim_coarse = aggregate_10_to_20_mean(original10)
    # create meta for simulated coarse by copying coarse_meta? For simplicity we will re-use coarse_meta
    # call atprk on sim_coarse with provided predictors (use provided predictors10)
    atprk_sim, reg_sim, res_sim, coef = downscale_func(sim_coarse, coarse_meta, predictors10, predictors10_meta)
    # compare atprk_sim with original10 (trim to matched shapes)
    # NOTE: our aggregation trimmed odd dims; ensure shapes align
    h, w = atprk_sim.shape
    original10_crop = original10[:h, :w]
    # metrics
    rmse = np.sqrt(np.mean((original10_crop - atprk_sim)**2))
    r, _ = stats.pearsonr(original10_crop.ravel(), atprk_sim.ravel())
    print(f"Wald test RMSE: {rmse:.4f}, Pearson r: {r:.4f}")
    return rmse, r



###################################################################################################
# USER: set your file paths
###################################################################################################
B02_10m_fp = "B02_10m.tif"
B03_10m_fp = "B03_10m.tif"
B04_10m_fp = "B04_10m.tif"
B08_10m_fp = "B08_10m.tif"
COARSE_20m_fp = "B11_20m.tif"      # the band to downscale (20m)
OUT_10m_fp = "B11_downscaled_10m.tif"



# -------------------------------------------------------
# MAIN runnable block
# -------------------------------------------------------
if __name__ == "__main__":
    # 1. Load inputs
    b02_10, meta10   = load_band_rasterio(B02_10m_fp)
    b03_10, _        = load_band_rasterio(B03_10m_fp)
    b04_10, _        = load_band_rasterio(B04_10m_fp)
    b08_10, _        = load_band_rasterio(B08_10m_fp)
    coarse20, meta20 = load_band_rasterio(COARSE_20m_fp)

    predictors10 = [b02_10, b03_10, b04_10, b08_10]

    # 2. Run OPTIMIZED ATPRK
    atprk_10, reg10, resid10, coef = atprk_downscale_optimized(
        coarse20,                 # coarse_res_image
        meta20,                   # coarse_meta
        predictors10,             # list of predictors at 10m
        meta10,                   # predictors metadata
        window_size_fine=2048,
        overlap=128,
        radius_m=3000,
        max_coarse_points=2000,
        min_coarse_points=30,
        n_workers=4,
        variogram_model='spherical',
        sample_for_kriging=False,
        sample_size=2000
    )

    print("Optimized ATPRK done.")

    # 3. Save result as GeoTIFF
    out_meta = meta10.copy()
    out_meta.update({"dtype": "float32", "count": 1})

    with rasterio.open(OUT_10m_fp, "w", **out_meta) as dst:
        dst.write(atprk_10.astype(np.float32), 1)

    print("Saved downscaled band to:", OUT_10m_fp)

    # 4. Optional Wald validation
    try:
        rmse, r = wald_validation(b04_10, atprk_downscale_optimized,
                                  coarse20, meta20, predictors10, meta10)
        print("Wald validation RMSE:", rmse, "r:", r)
    except Exception as e:
        print("Wald validation failed:", e)

    # 5. Quick visualization
    plt.figure(figsize=(12, 6))
    vmin, vmax = np.percentile(atprk_10, (2, 98))

    plt.subplot(1, 3, 1)
    plt.title("Regression pred (10m)")
    plt.imshow(reg10, vmin=vmin, vmax=vmax)
    plt.axis("off")

    plt.subplot(1, 3, 2)
    plt.title("Kriged residuals (10m)")
    plt.imshow(resid10,
               vmin=np.percentile(resid10, 2),
               vmax=np.percentile(resid10, 98))
    plt.axis("off")

    plt.subplot(1, 3, 3)
    plt.title("Final Optimized ATPRK 10m")
    plt.imshow(atprk_10, vmin=vmin, vmax=vmax)
    plt.axis("off")

    plt.tight_layout()
    plt.show()





if __name__ == "__main__":
    # 1. Load inputs
    b02_10, meta10   = load_band_rasterio(B02_10m_fp)
    b03_10, _        = load_band_rasterio(B03_10m_fp)
    b04_10, _        = load_band_rasterio(B04_10m_fp)
    b08_10, _        = load_band_rasterio(B08_10m_fp)
    coarse20, meta20 = load_band_rasterio(COARSE_20m_fp)

    predictors10 = [b02_10, b03_10, b04_10, b08_10]

    # 2. Run ATPRK (note: may take minutes depending on tile size)
    atprk_10, reg10, resid10, coef = atprk_downscale(coarse20, meta20, predictors10, meta10,
                                                    sample_for_kriging=True, max_krige_points=2500)
    print("ATPRK done.")

    # 3. Save result as GeoTIFF (10m grid metadata)
    out_meta = meta10.copy()
    out_meta.update({"dtype": "float32", "count": 1})
    with rasterio.open(OUT_10m_fp, "w", **out_meta) as dst:
        dst.write(atprk_10.astype(np.float32), 1)
    print("Saved downscaled band to:", OUT_10m_fp)

    # 4. (Optional) Wald validation if you have a reference 10m band to test
    # Example: use B04 as 'original' and try to downscale its 20m version to 10m
    # Here we use b04_10 as truth
    try:
        rmse, r = wald_validation(b04_10, atprk_downscale, coarse20, meta20, predictors10, meta10)
    except Exception as e:
        print("Wald validation failed (maybe shapes mismatch). Error:", e)

    # 5. Quick plots
    plt.figure(figsize=(12, 6))
    vmin, vmax = np.percentile(atprk_10, (2, 98))
    plt.subplot(1, 3, 1); plt.title("Regression pred (10m)"); plt.imshow(reg10, vmin=vmin, vmax=vmax); plt.axis('off')
    plt.subplot(1, 3, 2); plt.title("Kriged residuals (10m)"); plt.imshow(resid10, vmin=np.percentile(resid10, 2), vmax=np.percentile(resid10, 98)); plt.axis('off')
    plt.subplot(1, 3, 3); plt.title("Final ATPRK 10m"); plt.imshow(atprk_10, vmin=vmin, vmax=vmax); plt.axis('off')
    plt.tight_layout()
    plt.show()
