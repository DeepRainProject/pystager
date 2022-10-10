from pystager import Pipeline, Pystager, Backend
from tasks import CDOMap, Mkdir, Filter_Years, Filter_Months
import argparse

def main():

    parser=argparse.ArgumentParser()
    parser.add_argument("--input_data_path", type=str, default="/p/scratch/deepacf/deeprain/radklim_process/netcdf/orig_grid/yw_hourly")
    parser.add_argument("--output_data_path", type=str, default="/p/scratch/deepacf/kreshpa1/radklim")
    parser.add_argument("--src_grid_path", type=str, default="/p/scratch/deepacf/deeprain/radklim_process/netcdf/grid_des/radklim_grid", help="Grid description file of source grid," + "i.e. describing RADKLIM's polarstereo. grid")
    parser.add_argument("--target_grid_path", type=str, default="/p/scratch/deepacf/deeprain/radklim_process/netcdf/grid_des/cde_grid", help="Grid description of target grid")
    parser.add_argument("--year_start", "--year_start", type=int, default=2011)
    parser.add_argument("--year_end", "--year_end", type=int, default=2012)
    parser.add_argument("--month_start", "--month_start", type=int, default=12)
    parser.add_argument("--month_end", "--month_end", type=int, default=1)

    args = parser.parse_args()

    pipeline = Pipeline(
            args.input_data_path, 
            args.output_data_path, 
            [Filter_Years(args), Mkdir(), Filter_Months(args), Mkdir(), CDOMap(args.src_grid_path, args.target_grid_path)],levels=[1,1,2,2,3],
            max_lvl=3,
    )
    Pystager(max_workers=4, backend=Backend.mpi.value).process(pipeline)

if __name__ == '__main__':
    main()
