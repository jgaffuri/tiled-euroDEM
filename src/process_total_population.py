from pygridmap import gridtiler
from datetime import datetime
import os


aggregated_folder = "/home/juju/Bureau/aggregated/"
if not os.path.exists(aggregated_folder): os.makedirs(aggregated_folder)

transform = True
aggregate = True
tiling = True


# fid,GRD_ID,T,M,F,Y_LT15,Y_1564,Y_GE65,EMP,NAT,EU_OTH,OTH,SAME,CHG_IN,CHG_OUT,LAND_SURFACE,POPULATED,COUNT,
# T_CI,M_CI,F_CI,Y_LT15_CI,Y_1564_CI,Y_GE65_CI,EMP_CI,NAT_CI,EU_OTH_CI,OTH_CI,SAME_CI,CHG_IN_CI,CHG_OUT_CI

#transform

if transform:
    def tr(c):

        # skip non populated non confidential cells
        pop = c['T']
        ci = c['T_CI']
        if pop == "0" and ci != "-9999": return False
        #if pop == "0" and ci == "-9999": print("ok!")

        gid = c['GRD_ID'].replace("CRS3035RES1000mN", "").split('E')

        c.clear()
        c['T'] = pop
        c['x'] = gid[1]
        c['y'] = gid[0]

        #set confidentiality to 0 or 1
        if ci == "": c['T_CI'] = 0
        elif ci == "-9999": c['T_CI'] = 1
        else: print("Unexpected T_CI: ", ci)

        #initialise nb - to count the number of cells aggregated
        c['nb'] = 1

    gridtiler.grid_transformation("/home/juju/geodata/census/2021/ESTAT_Census_2021_V2.csv", tr, aggregated_folder+"1000.csv")



#aggregation
if aggregate:

    for a in [2,5,10]:
        print(datetime.now(), "aggregation to", a*1000, "m")
        gridtiler.grid_aggregation(input_file=aggregated_folder+"1000.csv", resolution=1000, output_file=aggregated_folder+str(a*1000)+".csv", a=a)
    for a in [2,5,10]:
        print(datetime.now(), "aggregation to", a*10000, "m")
        gridtiler.grid_aggregation(input_file=aggregated_folder+"10000.csv", resolution=10000, output_file=aggregated_folder+str(a*10000)+".csv", a=a)




#tiling
if tiling:
    for resolution in [1000, 2000, 5000, 10000, 20000, 50000, 100000]:
        print("tiling for resolution", resolution)

        #create output folder
        out_folder = 'pub/v2/parquet_total/' + str(resolution)
        if not os.path.exists(out_folder): os.makedirs(out_folder)

        gridtiler.grid_tiling(
            aggregated_folder+str(resolution)+".csv",
            out_folder,
            resolution,
            tile_size_cell = 256,
            x_origin = 0,
            y_origin = 0,
            format = "parquet"
        )
