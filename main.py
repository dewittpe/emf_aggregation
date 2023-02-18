import os, sys, getopt, itertools
import yaml
import pandas as pd
from utilities import check_to_rebuild
from utilities import json_to_df
from timer import Timer
from dict_to_parquet import d2p_baseline
from dict_to_parquet import d2p_floor_area
from dict_to_parquet import d2p_emm_to_states
from dict_to_parquet import d2p_emm_population_weights
from dict_to_parquet import d2p_emm_region_emission_prices
from dict_to_parquet import d2p_site_source_co2_conversion
from dict_to_parquet import d2p_ecm_results


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


    # verify the parquets directory exists
    if not os.path.exists("parquets"):
        os.makedirs("parquets")

    # check if parquets need to be rebuilt, if so do so, else, just read the
    # parquets

    # supporting data
    trgts = ['parquets/emm_to_states.parquet']
    prqts = ['convert_data/geo_map/EMM_State_RowSums.txt', 'dict_to_parquet.py']
    if check_to_rebuild(trgts, prqts):
        with Timer("Formatting EMM states", verbose = verbose):
            emm_to_states = d2p_emm_to_states()
    else:
        emm_to_states = pd.read_parquet(trgts[0])

    trgts = ['parquets/emm_populaiton_weights.parquet']
    prqts = ['convert_data/geo_map/EMM_National.txt', 'dict_to_parquet.py']
    if check_to_rebuild(trgts, prqts):
        with Timer("Formatting EMM Population Weights", verbose = verbose):
            emm_to_states = d2p_emm_population_weights()
    else:
        emm_populaiton_weights = pd.read_parquet(trgts[0])

    # the following contains both the 
    # co2_intensity_of_electricity and end_use_electricity_price
    trgts = ['parquets/emm_region_emissions_prices.parquet']
    prqts = ['convert_data/emm_region_emissions_prices.json.gz', 'dict_to_parquet.py']
    if check_to_rebuild(trgts, prqts):
        with Timer("Formatting EMM Region Emission Prices", verbose = verbose):
            emm_region_emissions_prices = d2p_emm_region_emission_prices()
    else:
        emm_region_emissions_prices = pd.read_parquet(trgts[0])

    trgts = ['parquets/site_source_co2_conversions.parquet']
    prqts = ['convert_data/site_source_co2_conversions.json.gz', 'dict_to_parquet.py']
    if check_to_rebuild(trgts, prqts):
        with Timer("Formatting Site Source CO2 Conversions", verbose = verbose):
            site_source_co2_conversions = d2p_site_source_co2_conversion()
    else:
        site_source_co2_conversions = pd.read_parquet(trgts[0])

    # baseline data as defined in the config file
    # Pre-process baseline data.
    if config.get("baseline") is None:
        raise Exception("baseline key in config file is missing.")

    trgts = ["parquets/baseline.parquet", "parquets/floor_area.parquet"]
    prqts = [b.get('file') for b in config.get('baseline')]
    prqts.extend(['dict_to_parquet.py', config_path])
    if check_to_rebuild(trgts, prqts):
        with Timer("Formatting baseline and floor area data", verbose = verbose):
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

    # Results Data
    # Pre-process baseline data.
    if config.get("ecm_results") is None:
        raise Exception("ecm_results key in config file is missing.")

    trgts = ['parquets/OnSiteGenerationByCategory.parquet',
             'parquets/OnSiteGenerationOverall.parquet',
             'parquets/MarketsSavingsByCategory.parquet',
             'parquets/MarketsSavingsOverall.parquet',
             'parquets/FilterVariables.parquet',
             'parquets/FinancialMetrics.parquet']
    prqts = [b.get('file') for b in config.get('ecm_results')]
    prqts.extend(['dict_to_parquet.py', config_path])
    if check_to_rebuild(trgts, prqts):
        with Timer("Formatting results data", verbose = verbose):
            f = [b.get('file') for b in config.get('ecm_results')]
            s = [b.get('scenario') for b in config.get('ecm_results')]
            DFs = [json_to_df(path = p) for p in f]
            DF = pd.concat(DFs, keys = s)
            DF = DF.reset_index(level = 0, names = ["Scenario"])
            d2p_ecm_results(DF)

    MarketsSavingsByCategory = pd.read_parquet("parquets/MarketsSavingsByCategory.parquet")

    print(MarketsSavingsByCategory)
            

            
