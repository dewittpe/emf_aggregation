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
from scout_emf_mappings import ScoutEMFMappings
from scout_concepts import ScoutConcepts
from scout_concepts import ScoutMappings


if __name__ == "__main__":

    # Default Values for command line arguments
    config_path = "config.yml"
    verbose = False

    # needed mappings and concepts
    scout_emf_mappings = ScoutEMFMappings()
    scout_concecpts = ScoutConcepts()
    scout_mappings = ScoutMappings()

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
        with Timer("Reading " + trgts[0], verbose = verbose):
            emm_to_states = pd.read_parquet(trgts[0])

    trgts = ['parquets/emm_population_weights.parquet']
    prqts = ['convert_data/geo_map/EMM_National.txt', 'dict_to_parquet.py']
    if check_to_rebuild(trgts, prqts):
        with Timer("Formatting EMM Population Weights", verbose = verbose):
            emm_population_weights = d2p_emm_population_weights()
    else:
        with Timer("Reading " + trgts[0], verbose = verbose):
            emm_population_weights = pd.read_parquet(trgts[0])

    # the following contains both the
    # co2_intensity_of_electricity and end_use_electricity_price
    trgts = ['parquets/emm_region_emissions_prices.parquet']
    prqts = ['convert_data/emm_region_emissions_prices.json.gz', 'dict_to_parquet.py']
    if check_to_rebuild(trgts, prqts):
        with Timer("Formatting EMM Region Emission Prices", verbose = verbose):
            emm_region_emissions_prices = d2p_emm_region_emission_prices()
    else:
        with Timer("Reading " + trgts[0], verbose = verbose):
            emm_region_emissions_prices = pd.read_parquet(trgts[0])

    trgts = ['parquets/site_source_co2_conversions.parquet']
    prqts = ['convert_data/site_source_co2_conversions.json.gz', 'dict_to_parquet.py']
    if check_to_rebuild(trgts, prqts):
        with Timer("Formatting Site Source CO2 Conversions", verbose = verbose):
            site_source_co2_conversions = d2p_site_source_co2_conversion()
    else:
        with Timer("Reading " + trgts[0], verbose = verbose):
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
        with Timer("Reading in baseline and floor_area parquets", verbose = verbose):
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
    
    with Timer("Reading in MarketsSavingsByCategory", verbose = verbose):
        MarketsSavingsByCategory = pd.read_parquet("parquets/MarketsSavingsByCategory.parquet")

    # Prepare the data for aggregation
    with Timer("Prepare Data For Aggregation", verbose = verbose):
        # sub set to the years endingin 5 or 0
        baseline = baseline[baseline.year % 5 == 0].reset_index(drop = True)
        MarketsSavingsByCategory = (
                MarketsSavingsByCategory[MarketsSavingsByCategory.year % 5 == 0]
                .reset_index(drop = True)
                )

        ##########
        # baseline
        baseline = (
                baseline
                .merge(scout_mappings.building_type_class)
                .merge(scout_emf_mappings.emf_end_uses,
                       how = 'left',
                       left_on = 'end_use',
                       right_on = 'scout_end_use')
                .drop('scout_end_use', axis = 1)
                .merge(scout_emf_mappings.other_fuel_type,
                      how = 'left',
                       left_on = ['fuel_type', 'end_use', 'technology'],
                       right_on = ['scout_other_fuel_type', 'scout_end_use', 'scout_technology']
                       )
                .drop(['scout_other_fuel_type', 'scout_end_use', 'scout_technology'], axis = 1)
                .merge(scout_emf_mappings.non_other_fuel_type,
                       how = 'left',
                       left_on = ['fuel_type'],
                       right_on = ['scout_non_other_fuel_type'],
                       suffixes = ("", "_2")
                       )
                .drop(['scout_non_other_fuel_type'], axis = 1)
                .merge(scout_emf_mappings.emf_direct_indirect_fuel,
                       how = 'left',
                       left_on = 'fuel_type',
                       right_on = 'scout_fuel_type')
                .merge(
                    emm_region_emissions_prices.query('conversion == "CO2 intensity of electricity"')[["region", "year", "conversion_factor"]],
                    #CO2_intensity_of_electricity[["region", "year", "conversion_factor"]],
                       how = 'left',
                       on = ['region', 'year'])
                .rename(columns = {"conversion_factor" : "CO2_intensity_of_electricity"})
            )

        # Above merges do have extra columns resulting from the non_other and other fuel
        # types.  Coalesce these columns and drop the extra columns.
        baseline.emf_fuel_type = baseline.emf_fuel_type.fillna(baseline.emf_fuel_type_2)
        baseline.EJ_to_mt_CO2 = baseline.EJ_to_mt_CO2.fillna(baseline.EJ_to_mt_CO2_2)
        baseline = baseline.drop(['emf_fuel_type_2', 'EJ_to_mt_CO2_2'], axis = 1)

        # In the above merge the CO2_intensity_of_electricity is not relevent when the
        # EMF fuel type is not electricity.  Correct that here by settting that factor
        # to 1.0 when emf_fuel_type != "Electricity"
        idx = baseline.query("emf_fuel_type != 'Electricity'").index
        baseline.loc[idx, "CO2_intensity_of_electricity"] = 1.0

        # create a EJ and CO2 columns
        baseline["EJ"] = baseline["value"] * scout_emf_mappings.MMBtu_to_EJ

        baseline["CO2"] = (
                baseline.EJ *
                baseline.EJ_to_mt_CO2 *
                baseline.CO2_intensity_of_electricity
            )

        ##########
        # results
        MarketsSavingsByCategory = (
                MarketsSavingsByCategory
                .merge(scout_emf_mappings.emf_base_strings, how = "inner")
                .merge(scout_mappings.building_type_class, how = "left", on = "building_type")
                .merge(scout_emf_mappings.emf_end_uses,
                       how = "left",
                       left_on = "end_use",
                       right_on = "scout_end_use")
                .drop("scout_end_use", axis = 1)
                .merge(scout_emf_mappings.emf_direct_indirect_fuel,
                       how = "left",
                       left_on = "fuel_type",
                       right_on = "scout_fuel_type")
                .drop("scout_fuel_type", axis = 1)
                .merge(scout_emf_mappings.non_other_fuel_type,
                       how = 'left',
                       left_on = ['fuel_type'],
                       right_on = ['scout_non_other_fuel_type'],
                       suffixes = ("", "_2")
                       )
                .drop(['scout_non_other_fuel_type'], axis = 1)
                # TODO: build out end use, technology mappings
                )

        idx = MarketsSavingsByCategory.query('outcome_metric.str.contains("MMBtu")').index
        MarketsSavingsByCategory.loc[idx, "value"] *= scout_emf_mappings.MMBtu_to_EJ
        MarketsSavingsByCategory.outcome_metric = (
                MarketsSavingsByCategory
                .outcome_metric
                .str.replace("MMBtu", "EJ")
                )

    with Timer("Aggregate Baseline Data for EMF Summary", verbose = verbose):
        groupings = [
                {
                    'emf_base_string' : 'Final Energy|Buildings',
                    'acol' : 'EJ',
                    'groups' : [
                        ["Scenario", "region",                                                   "year"],
                        ["Scenario", "region",                                  "emf_fuel_type", "year"],
                        ["Scenario", "region", "building_class",                                 "year"],
                        ["Scenario", "region", "building_class",                "emf_fuel_type", "year"],
                        ["Scenario", "region", "building_class", "emf_end_use", "emf_fuel_type", "year"]
                    ]
                },
                {
                    'emf_base_string' : 'Emissions|CO2|Energy|Demand|Buildings',
                    'acol' : 'CO2',
                    'groups' : [
                            ["Scenario", "region",                                                              "year"],
                            ["Scenario", "region",                                  "emf_direct_indirect_fuel", "year"],
                            ["Scenario", "region", "building_class",                                            "year"],
                            ["Scenario", "region", "building_class",                "emf_direct_indirect_fuel", "year"],
                            ["Scenario", "region", "building_class", "emf_end_use",                             "year"],
                            ["Scenario", "region", "building_class", "emf_end_use", "emf_direct_indirect_fuel", "year"]
                        ]
                }
                ]

        baseline_aggs = [
                    baseline
                    .groupby(grouping)
                    .agg({g['acol']:"sum"})
                    .rename({g['acol']:"value"}, axis = 1)
                    .assign(Variable = lambda x: g['emf_base_string'])
                    .reset_index()
                    for g in groupings
                    for grouping in g["groups"]
                ]

        baseline_aggs = pd.concat(baseline_aggs)

        baseline_aggs["Variable"] = (
                (
                baseline_aggs["Variable"] +
                "|" + baseline_aggs.building_class.fillna("") +
                "|" + baseline_aggs.emf_end_use.fillna("") +
                "|" + baseline_aggs.emf_fuel_type.fillna("") +
                "|" + baseline_aggs.emf_direct_indirect_fuel.fillna("")
                )
                .str.replace("\|{2,}", "|", regex = True)
                .str.replace("\|$", "", regex = True)
                )

    with Timer("Aggregate MarketsSavingsByCategory for EMF Summary", verbose = verbose):
        querys_groups = [
                {
                    'query' : 'emf_base_string.isin(["Emissions|CO2|Energy|Demand|Buildings", "Final Energy|Buildings"])',
                    'groups' : [
                            ['Scenario', 'region', 'emf_base_string', 'year'],
                            ['Scenario', 'region', 'emf_base_string', 'building_class',                                                             'year'],
                            ['Scenario', 'region', 'emf_base_string', 'building_class', 'emf_end_use',                                              'year']
                        ]
                },
                {
                    'query' : 'emf_base_string == "Emissions|CO2|Energy|Demand|Buildings"',
                    'groups' : [
                            ['Scenario', 'region', 'emf_base_string',                                  'emf_direct_indirect_fuel',                  'year'],
                            ['Scenario', 'region', 'emf_base_string', 'building_class',                'emf_direct_indirect_fuel',                  'year'],
                            ['Scenario', 'region', 'emf_base_string', 'building_class', 'emf_end_use', 'emf_direct_indirect_fuel',                  'year']
                        ]
                },
                {
                    'query' : 'emf_base_string == "Final Energy|Buildings"',
                    'groups' : [
                            ['Scenario', 'region', 'emf_base_string',                                                              'emf_fuel_type', 'year'],
                            ['Scenario', 'region', 'emf_base_string', 'building_class',                                            'emf_fuel_type', 'year'],
                            ['Scenario', 'region', 'emf_base_string', 'building_class', 'emf_end_use',                             'emf_fuel_type', 'year']
                        ]
                }
                ]

        aggs = [
                MarketsSavingsByCategory
                .query(q["query"])
                .groupby(g)
                .agg({"value":"sum"})
                .reset_index()
                for q in querys_groups
                for g in q["groups"]
                ]

        aggs = pd.concat(aggs)

        aggs["Variable"] = (
                    (
                        aggs.emf_base_string +
                        "|" + aggs.building_class.fillna("") +
                        "|" + aggs.emf_end_use.fillna("") +
                        "|" + aggs.emf_direct_indirect_fuel.fillna("") +
                        "|" + aggs.emf_fuel_type.fillna("")
                    )
                    .str.replace("\|{2,}", "|", regex = True)
                    .str.replace("\|$", "", regex = True)
                )

    with Timer("Clean up Aggregated Results", verbose = verbose):
        aggs = (
                pd.concat([baseline_aggs, aggs])
                .rename({"region":"Region"}, axis = 1)
               )
        aggs.to_csv("emf_output/aggs.csv")

        # add model name and units columns
        aggs["Model"] = config.get("scoutversion")
        # for units, set all rows to the units for 'Final Energy' and then correct the
        # needed rows for 'Emmissions'
        aggs["Units"] = "EJ/yr"
        idx = aggs.query("Variable.str.contains('Emmissions')").index
        aggs.loc[idx, "Units"] = "Mt CO2\yr"

        # Remove extraneous rows from baseline and modify CO2 emissions
        # to report direct emissions without the 'Direct' tag (and drop
        # the total direct + indirect emissions values)

        totco2_regex = '^Emissions\|.*\|(?:Direct|Indirect)$|^(?!Emissions).*'

        aggs = (
            aggs
            .query('~((Scenario == "NT.Ref.R2") & Variable.str.contains(@totco2_regex, regex = True))')
            .query('~((Scenario == "NT.Ref.R2") & Variable.str.endswith("|Cooling|Gas"))')
            )

        idx = aggs.query('Scenario == "NT.Ref.R2"').index
        aggs.loc[idx, "Variable"] = (
                aggs
                .loc[idx, "Variable"]
                .str.replace("\|Direct", "", regex = True)
            )


    with Timer("Translate to State Level results", verbose = verbose):
        state_aggs = (
                aggs
                .merge(emm_to_states,
                       how = 'left',
                       left_on = 'Region',
                       right_on = 'EMM')
                .drop("EMM", axis = 1)
                .merge(emm_population_weights,
                       how = 'left',
                       left_on = 'Region',
                       right_on = 'EMM')
                .drop("EMM", axis = 1)
                .assign(value = lambda x : x.value * x.emm_to_state_factor)
                .groupby(["Model", "Scenario", "year", "Variable", "Units", "State"])
                .agg({"value":"sum"})
                .reset_index()
                .rename(columns = {"State":"Region"})
                )
        state_aggs.to_csv("emf_output/state_aggs.csv")

    with Timer("Translate to National results", verbose = verbose):
        national_aggs = (
                aggs
                .merge(emm_population_weights,
                       how = 'left',
                       left_on = 'Region',
                       right_on = 'EMM')
                .drop("EMM", axis = 1)
                .assign(value = lambda x: x.value * x.weight)
                .groupby(["Model", "Scenario", "year", "Variable", "Units"])
                .agg({"value":"sum"})
                .assign(Region = lambda x: 'United States')
                .reset_index()
                )
        national_aggs.to_csv("emf_output/national_aggs.csv")

    with Timer("Pivot State and National Data for IAMC"):
        iamc = pd.concat([national_aggs, state_aggs])
        iamc.to_csv("emf_output/IAMC_long.csv")

        iamc = pd.pivot_table(iamc,
                              values = "value",
                              index = ["Model", "Scenario", "Region", "Variable", "Units"],
                              columns = "year"
                              )
        iamc.to_csv("emf_output/IAMC_wide.csv")

    with Timer("Write to files", verbose = verbose):
        if not os.path.exists("emf_output"):
            os.makedirs("emf_output")

        iamc.to_excel("emf_output/IAMC_format.xlsx")











