# prepare accessibility grid for gridviz mapping

from pygridmap import gridtiler_raster
import sys
import os
from rasterio.enums import Resampling
from datetime import datetime
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from geotiff import resample_geotiff_aligned, mask_pixels_with_lambda

#TODO
# apply mask_pixels_with_lambda to tiffs
# copy mask_pixels_with_lambda to pysco
# tile
# test
# publish
# use !


aggregate = True
set_no_data = True
tiling = False

input_dem = "/home/juju/geodata/gisco/EU_DEM_mosaic_1000K/eudem_dem_3035_europe.tif"
resolutions = [2000]  #[ 20000, 10000, 5000, 2000, 1000, 500, 200, 100 ] #, 50, 25
def modif_fun(x): return round(x, 1)


out_folder = "tmp/"
if not os.path.exists(out_folder): os.makedirs(out_folder)


if aggregate:
    print(datetime.now(), "aggregate")
    for resolution in resolutions:
        print(datetime.now(), resolution)
        output_dem = out_folder+"dem_"+str(resolution) + "m.tif"
        if os.path.exists(output_dem):
            print(datetime.now(), "exists, skip")
            continue
        resample_geotiff_aligned(input_dem, output_dem, resolution, Resampling.med)

if set_no_data:
    print(datetime.now(), "set no_data")
    for resolution in resolutions:
        print(datetime.now(), resolution)
        input_dem = out_folder+"dem_"+str(resolution) + "m.tif"
        output_dem = out_folder+"dem_"+str(resolution) + "m_nodata.tif"
        mask_pixels_with_lambda(input_dem, output_dem, 1, lambda x: x <= 0)


if tiling:
    print(datetime.now(), "tiling")
    for resolution in resolutions:

        print(datetime.now(), "Tiling", resolution)

        # make folder for resolution
        folder_ = out_folder+"tiles_"+str(resolution)+"/"
        if os.path.exists(folder_):
            print(datetime.now(), "exists, skip")
            continue
        if not os.path.exists(folder_): os.makedirs(folder_)

        # prepare dict for geotiff bands
        dict = {}
        dict["v"] = {"file":out_folder+"dem_"+str(resolution) + "m_nodata.tif", "band":1, "no_data_values": [0, None, -9999]}

        # launch tiling
        gridtiler_raster.tiling_raster(
            dict,
            folder_,
            crs="EPSG:3035",
            tile_size_cell = 256,
            format="parquet",
            num_processors_to_use = 10,
            modif_fun = modif_fun,
            )
