from math import ceil, floor
import rasterio
from rasterio.transform import from_bounds, Affine
from rasterio.windows import Window, from_bounds as window_from_bounds
from rasterio.enums import Resampling
#from rasterio.warp import reproject
from rasterio.features import rasterize
import numpy as np


def resample_geotiff_aligned(input_path, output_path, new_resolution, resampling=Resampling.average, dtype=np.float64, bigtiff=False):
    """
    Resamples a GeoTIFF to a new resolution (must be a multiple of the original),
    and aligns the origin point to a multiple of the new resolution.

    Parameters:
        input_path (str): Path to the input GeoTIFF file.
        output_path (str): Path to save the resampled GeoTIFF.
        new_resolution (float): Desired resolution (pixel size, e.g. in meters).
        resampling (): Resampling method.
    """
    with rasterio.open(input_path, BIGTIFF='YES' if bigtiff else 'IF_NEEDED') as src:
        # Original resolution
        original_res_x = src.transform.a
        original_res_y = -src.transform.e

        # Check that new resolution is a multiple of original
        if new_resolution % original_res_x != 0 or new_resolution % original_res_y != 0:
            raise ValueError("New resolution must be a multiple of the original resolution.")

        # Original bounds
        bounds = src.bounds

        # Align the origin to a multiple of new_resolution
        aligned_minx = floor(bounds.left  / new_resolution) * new_resolution
        aligned_maxy = ceil (bounds.top   / new_resolution) * new_resolution
        aligned_maxx = ceil (bounds.right / new_resolution) * new_resolution
        aligned_miny = floor(bounds.bottom/ new_resolution) * new_resolution

        # New dimensions
        new_width  = int((aligned_maxx - aligned_minx) / new_resolution)
        new_height = int((aligned_maxy - aligned_miny) / new_resolution)

        # New transform
        new_transform = Affine(
            new_resolution, 0.0, aligned_minx,
            0.0, -new_resolution, aligned_maxy
        )

        # Update profile
        profile = src.profile
        profile.update({
            'height': new_height,
            'width': new_width,
            'transform': new_transform
        })

        if dtype is not None:
            profile.update({ 'dtype': dtype })


        with rasterio.open(output_path, 'w', **profile) as dst:
            for i in range(1, src.count + 1):
                rasterio.warp.reproject(
                    source=rasterio.band(src, i),
                    destination=rasterio.band(dst, i),
                    src_transform=src.transform,
                    src_crs=src.crs,
                    dst_transform=new_transform,
                    dst_crs=src.crs,
                    resampling=resampling,
                    dtype=dtype
                )



def mask_pixels_with_lambda_in_place(geotiff_path, band_number, predicate):
    """
    geotiff_path : str
        Path to the input GeoTIFF. The file will be overwritten.
    band_number : int
        1-based band index.
    predicate : function
        A lambda or function f(value) -> bool that returns True where pixels
        should be set to nodata.
    """

    # Open input in read/write mode
    with rasterio.open(geotiff_path, "r+") as ds:
        nodata = ds.nodata
        if nodata is None:
            raise ValueError("The raster has no nodata value defined.")

        # Read only the given band
        data = ds.read(band_number)

        # Apply lambda to every pixel
        mask = np.vectorize(predicate)(data)

        # Set masked pixels to nodata
        data[mask] = nodata

        # Write the modified band back to file
        ds.write(data, band_number)



def mask_pixels_with_lambda(
    input_path,
    output_path,
    band_number,
    predicate
):
    """
    Create a new GeoTIFF where pixels satisfying the boolean predicate
    on the specified band are set to nodata.

    Parameters
    ----------
    input_path : str
        Path to input GeoTIFF.
    output_path : str
        Path where output GeoTIFF will be written.
    band_number : int
        1-based index of band to modify.
    predicate : callable
        Function or lambda value -> bool returning True for pixels
        to set to nodata.
    """

    with rasterio.open(input_path) as src:
        profile = src.profile.copy()

        # Ensure nodata is defined
        nodata = src.nodata
        if nodata is None:
            raise ValueError("The input raster has no nodata value defined.")

        # Read the full raster (all bands)
        data = src.read()

    # Apply predicate only to the selected band
    band = data[band_number - 1]

    # Apply pixel-wise lambda
    mask = np.vectorize(predicate)(band)

    # Assign nodata
    band[mask] = nodata

    # Put modified band back
    data[band_number - 1] = band

    # Write output file
    with rasterio.open(output_path, "w", **profile) as dst:
        dst.write(data)
