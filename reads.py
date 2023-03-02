import pandas as pd
from utilities import check_to_rebuild
from dict_to_parquet import d2p_baseline
from dict_to_parquet import d2p_floor_area
from dict_to_parquet import d2p_emm_to_states
from dict_to_parquet import d2p_emm_population_weights
from dict_to_parquet import d2p_emm_region_emission_prices
from dict_to_parquet import d2p_site_source_co2_conversion
from dict_to_parquet import d2p_ecm_results
from timer import Timer

def read_emm_to_states(verbose):
    trgts = ['parquets/emm_to_states.parquet']
    prqts = ['convert_data/geo_map/EMM_State_RowSums.txt', 'dict_to_parquet.py']
    if check_to_rebuild(trgts, prqts):
        with Timer("Formatting EMM states", verbose = verbose):
            emm_to_states = d2p_emm_to_states()
    else:
        with Timer("Reading " + trgts[0], verbose = verbose):
            emm_to_states = pd.read_parquet(trgts[0])
    return emm_to_states

def read_emm_population_weights(verbose):
    trgts = ['parquets/emm_population_weights.parquet']
    prqts = ['convert_data/geo_map/EMM_National.txt', 'dict_to_parquet.py']
    if check_to_rebuild(trgts, prqts):
        with Timer("Formatting EMM Population Weights", verbose = verbose):
            emm_population_weights = d2p_emm_population_weights()
    else:
        with Timer("Reading " + trgts[0], verbose = verbose):
            emm_population_weights = pd.read_parquet(trgts[0])
    return emm_population_weights

def read_emm_region_emission_prices(verbose):
    trgts = ['parquets/emm_region_emissions_prices.parquet']
    prqts = ['convert_data/emm_region_emissions_prices.json.gz', 'dict_to_parquet.py']
    if check_to_rebuild(trgts, prqts):
        with Timer("Formatting EMM Region Emission Prices", verbose = verbose):
            emm_region_emissions_prices = d2p_emm_region_emission_prices()
    else:
        with Timer("Reading " + trgts[0], verbose = verbose):
            emm_region_emissions_prices = pd.read_parquet(trgts[0])
    return emm_region_emissions_prices

def read_site_source_co2_conversions(verbose):
    trgts = ['parquets/site_source_co2_conversions.parquet']
    prqts = ['convert_data/site_source_co2_conversions.json.gz', 'dict_to_parquet.py']
    if check_to_rebuild(trgts, prqts):
        with Timer("Formatting Site Source CO2 Conversions", verbose = verbose):
            site_source_co2_conversions = d2p_site_source_co2_conversion()
    else:
        with Timer("Reading " + trgts[0], verbose = verbose):
            site_source_co2_conversions = pd.read_parquet(trgts[0])
    return site_source_co2_conversions

def read_baseline_and_floor_area(config_path, config, verbose):
    trgts = ["parquets/baseline.parquet", "parquets/floor_area.parquet"]
    prqts = [b.get('file') for b in config.get('baseline')]
    prqts.extend(['dict_to_parquet.py', config_path])
    if check_to_rebuild(trgts, prqts):
        with Timer("Formatting baseline and floor area data", verbose = verbose):
            f = [b.get('file') for b in config.get('baseline')]
            s = [b.get('scenario') for b in config.get('baseline')]
            DFs = [dict_to_df(path = p) for p in f]
            baseline = [d2p_baseline(df, s) for df, s in zip(DFs, s)]
            baseline = pd.concat(baseline)
            baseline.to_parquet('parquets/baseline.parquet')
            floor_area = [d2p_floor_area(df, s) for df, s in zip(DFs, s)]
            floor_area = pd.concat(floor_area)
            floor_area.to_parquet('parquets/floor_area.parquet')
    else:
        with Timer("Reading in baseline and floor_area parquets", verbose = verbose):
            baseline = pd.read_parquet('parquets/baseline.parquet')
            floor_area = pd.read_parquet('parquets/floor_area.parquet')
    return baseline, floor_area

def read_results(config_path, config, verbose):
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
            DFs = [dict_to_df(path = p) for p in f]
            DF = pd.concat(DFs, keys = s)
            DF = DF.reset_index(level = 0, names = ["Scenario"])
            rtn = d2p_ecm_results(DF)
    else:
        with Timer("Reading results data", verbose = verbose):
            rtn = [pd.read_parquet(x) for x in trgts]
    return rtn
