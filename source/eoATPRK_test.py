
'''
ATPRK downscale example (20m -> 10m) for Sentinel-2 using 4 x 10m bands as predictors.
'''


import numpy as np
import dask.array as da
from dask import delayed, compute
import eoImage as Img

def local_mapping_function(VNIRImg, TargetImg, x, y, WinSize=7):
    """
    Fit local linear mapping for a single pixel.
    Returns: coeffs (array), bias (float)
    """
    if WinSize < 3:
        WinSize = 3

    VNIR_Win   = Img.extract_window(VNIRImg, x, y, WinSize)
    target_Win = Img.extract_window(TargetImg, x, y, WinSize)

    flatVNIR   = VNIR_Win.reshape(-1, VNIR_Win.shape[2])
    flatTarget = target_Win.reshape(-1)

    X_design = np.hstack([flatVNIR, np.ones((flatVNIR.shape[0], 1))])
    params, _, _, _ = np.linalg.lstsq(X_design, flatTarget, rcond=None)
    
    coeffs = params[:-1]
    bias = params[-1]
    return coeffs, bias


def compute_all_mappings_dask(VNIRImg, TargetImg, Linear=True, WinSize=7, BlockSize=256):
    """
    Compute local mapping coefficients for all pixels using Dask blocks.
    """
    if WinSize < 3:
        WinSize = 3

    H, W, C = VNIRImg.shape
    if TargetImg.ndim == 3:
        TargetImg = np.squeeze(TargetImg, axis=2)
    
    if TargetImg.shape != (H, W):
        raise ValueError("TargetImg must match VNIRImg spatial dimensions.")

    wr = WinSize // 2

    # Pad images
    padded_VNIR = np.pad(VNIRImg, ((wr, wr), (wr, wr), (0, 0)), mode='edge')
    padded_Tgt  = np.pad(TargetImg, ((wr, wr), (wr, wr)), mode='edge')

    # Add nonlinear terms if needed
    if not Linear:
        red = padded_VNIR[:, :, 2]
        nir = padded_VNIR[:, :, 3]
        red_nir = (red*nir)[..., None]
        red_red = (red*red)[..., None]
        nir_nir = (nir*nir)[..., None]
        UsedImg = np.concatenate([padded_VNIR, red_nir, red_red, nir_nir], axis=2)
        nCoeffs = 7
    else:
        UsedImg = padded_VNIR
        nCoeffs = 4

    # Convert to Dask arrays
    dask_VNIR = da.from_array(UsedImg, chunks=(BlockSize, BlockSize, UsedImg.shape[2]))
    dask_Tgt  = da.from_array(padded_Tgt, chunks=(BlockSize, BlockSize))

    # Function to process one block
    def process_block(vnir_block, tgt_block):
        H_blk, W_blk, _ = vnir_block.shape
        A_blk = np.zeros((H_blk, W_blk, nCoeffs), dtype=np.float32)
        D_blk = np.zeros((H_blk, W_blk), dtype=np.float32)

        for i in range(H_blk):
            for j in range(W_blk):
                coeffs, bias = local_mapping_function(vnir_block, tgt_block, i, j, WinSize)
                A_blk[i, j, :] = coeffs
                D_blk[i, j] = bias
        return A_blk, D_blk

    # Create delayed tasks (do NOT compute yet)
    delayed_blocks = []
    for i in range(0, H + 2*wr, BlockSize):
        for j in range(0, W + 2*wr, BlockSize):
            v_block = dask_VNIR[i:i+BlockSize, j:j+BlockSize, :]
            t_block = dask_Tgt[i:i+BlockSize, j:j+BlockSize]
            delayed_blocks.append(delayed(process_block)(v_block, t_block))

    # Compute all blocks in parallel
    results = compute(*delayed_blocks, scheduler='threads')

    # Combine results into full arrays
    A_coeff = np.zeros((H + 2*wr, W + 2*wr, nCoeffs), dtype=np.float32)
    D_bias  = np.zeros((H + 2*wr, W + 2*wr), dtype=np.float32)

    blk_idx = 0
    for i in range(0, H + 2*wr, BlockSize):
        for j in range(0, W + 2*wr, BlockSize):
            a_blk, d_blk = results[blk_idx]
            H_blk, W_blk, _ = a_blk.shape
            A_coeff[i:i+H_blk, j:j+W_blk, :] = a_blk
            D_bias[i:i+H_blk, j:j+W_blk] = d_blk
            blk_idx += 1

    # Crop padded borders
    return A_coeff[wr:wr+H, wr:wr+W, :], D_bias[wr:wr+H, wr:wr+W]


def Piecewise_SuperResolution(DataDir, Month, TargetName='swir16_'):
    """
    Wrapper to load VNIR and target images and compute mapping coefficients.
    """
    HR_bands = ['blue_', 'green_', 'red_', 'nir08_']

    VNIR_Img, VNIR_mask, VNIR_profile = Img.load_TIF_files_to_npa(DataDir, HR_bands, Month)
    target_Img, target_mask, target_profile = Img.load_TIF_files_to_npa(DataDir, [TargetName], Month)

    # Compute mapping coefficients (linear)
    coeff_map, offset_map = compute_all_mappings_dask(VNIR_Img, target_Img, Linear=True, WinSize=7, BlockSize=256)

    return coeff_map, offset_map


# Example usage
DataDir = 'C:\\Work_Data\\S2_mosaic_vancouver2020_20m_for_testing_gap_filling'
coeff_map, offset_map = Piecewise_SuperResolution(DataDir, 'Aug', 'swir16_')
print("Coefficient map shape:", coeff_map.shape)
print("Bias map shape:", offset_map.shape)


