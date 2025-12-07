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
resolutions = [ 20000, 10000, 5000, 2000, 1000, 500, 200, 100, 50, 25 ]
def modif_fun(x): return round(x, 1)


out_folder = "tmp/"
if not os.path.exists(out_folder): os.makedirs(out_folder)


if aggregate:
    print(datetime.now(), "aggregate")
    for resolution in resolutions:
        out = out_folder+"dem_"+str(resolution) + "m_.tif"
        if os.path.exists(out): continue
        print(datetime.now(), resolution)
        resample_geotiff_aligned(input_dem, out, resolution, Resampling.med)


if tiling:
    print(datetime.now(), "tiling")
    for resolution in resolutions:

        print(datetime.now(), "Tiling", resolution)

        # make folder for resolution
        folder_ = out_folder+"tiles_"+str(resolution)+"/"
        if os.path.exists(folder_): continue
        if not os.path.exists(folder_): os.makedirs(folder_)

        # prepare dict for geotiff bands
        dict = {}
        dict["v"] = {"file":out_folder+"dem_"+str(resolution) + "m_.tif", "band":1, "no_data_values": [0, None, -9999]}

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
