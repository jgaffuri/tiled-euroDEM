# prepare accessibility grid for gridviz mapping

from pygridmap import gridtiler_raster
import sys
import os
from rasterio.enums import Resampling
from datetime import datetime
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from geotiff import resample_geotiff_aligned

aggregate = True
tiling = True

input_dem = "/home/juju/geodata/gisco/EU_DEM_mosaic_1000K/eudem_dem_3035_europe.tif"
resolutions = [ 10000, 5000, 2000, 1000, 500, 200, 100, 50, 25 ]

out_folder = "tmp/"
if not os.path.exists(out_folder): os.makedirs(out_folder)


if aggregate:
    print(datetime.now(), "aggregate")
    for resolution in resolutions:
        print(datetime.now(), resolution)
        resample_geotiff_aligned(input_dem, out_folder+"dem_"+str(resolution) + "m_.tif", resolution, Resampling.med)


if tiling:
    print(datetime.now(), "tiling")
    for resolution in resolutions:

        print(datetime.now(), "Tiling", resolution)

        # make folder for resolution
        folder_ = out_folder+"tiles_"+str(resolution)+"/"
        if not os.path.exists(folder_): os.makedirs(folder_)

        # prepare dict for geotiff bands
        dict = {}
        dict["v"] = {"file":out_folder+"dem_"+str(resolution) + "m_.tif", "band":1}

        # launch tiling
        gridtiler_raster.tiling_raster(
            dict,
            folder_,
            crs="EPSG:3035",
            tile_size_cell = 256,
            format="parquet",
            num_processors_to_use = 10,
            modif_fun = lambda x:round(x,1),
            )

