import os, sys, getopt, itertools
import yaml
import pandas as pd
from utilities import check_to_rebuild
from utilities import json_to_df
from timer import Timer
from dict_to_parquet import d2p_baseline
from dict_to_parquet import d2p_floor_area

if __name__ == "__main__":

    # Default Values for command line arguments
    config_path = "config.yml"
    verbose = False

    # get arguments from the command line
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hv", ["help", "verbose", "config="])
    except getopt.GetoptError:
        print("Usage: main.py -h for help")
        sys.exit(2)

    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print("dash-test -r <ecm_results file> -p <ecm_prep file>")
            print("Options:")
            print("  -h --help     Print this help and exit")
            print("  -v --verbose  Print status messages")
            print("  --config      User defined path to config.yml file")
            sys.exit()
        elif opt in ("--config"):
            config_path = arg
        elif opt in ("-v", "--verbose"):
            verbose = True

    # import the config
    with open(config_path, 'r') as f:
        config = yaml.load(f, Loader = yaml.loader.SafeLoader)

    # Pre-process baseline data.
    if config.get("baseline") is None:
        raise Exception("baseline key in config file is missing.")

    # check if parquets need to be rebuilt, if so do so, else, just read the
    # parquets
    trgts = ["parquets/baseline.parquet", "parquets/floor_area.parquet"]
    prqts = [b.get('file') for b in config.get('baseline')]
    prqts.extend(['dict_to_parquet.py', config_path])
    if check_to_rebuild(trgts, prqts):
        with Timer("(Re)formatting baseline and floor area data", verbose = verbose):
            f = [b.get('file') for b in config.get('baseline')]
            s = [b.get('scenario') for b in config.get('baseline')]
            DFs = [json_to_df(path = p) for p in f]
            baseline = [d2p_baseline(df, s) for df, s in zip(DFs, s)]
            baseline = pd.concat(baseline)
            baseline.to_parquet('parquets/baseline.parquet')
            floor_area = [d2p_floor_area(df, s) for df, s in zip(DFs, s)]
            floor_area = pd.concat(floor_area)
            floor_area.to_parquet('parquets/floor_area.parquet')
    else:
        baseline = pd.read_parquet('parquets/baseline.parquet')
        floor_area = pd.read_parquet('parquets/floor_area.parquet')

    print(baseline)
    print(floor_area)


            

            

#[[i, j] for i, j, in zip([1, 2, 4], ['a', 'c', 'd'])]
#
#
    #
#[b.get('file') for b in config.get('baseline')] , ['dict_to_parquet.py']]




