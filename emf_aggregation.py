import pandas as pd
import sys
from timer import Timer
from scout_emf_mappings import ScoutEMFMappings
from scout_concepts import ScoutConcepts
from scout_concepts import ScoutMappings

scout_emf_mappings = ScoutEMFMappings()
scout_concecpts = ScoutConcepts()
scout_mappings = ScoutMappings()

timer = Timer()
timer.tic("EMF Aggregation")

################################################################################
with Timer('Import parquet files'):
    baseline = pd.read_parquet("parquets/baseline.parquet")
    MarketsSavingsByCategory = pd.read_parquet("parquets/MarketsSavingsByCategory.parquet")
    CO2_intensity_of_electricity = pd.read_parquet("parquets/co2_intensity_of_electricity.parquet")
    site_source_co2_conversions = pd.read_parquet("parquets/site_source_co2_conversions.parquet")
    end_use_electricity_price = pd.read_parquet("parquets/end_use_electricity_price.parquet")
    floor_area = pd.read_parquet("parquets/floor_area.parquet")
    emm_to_states = pd.read_parquet("parquets/emm_to_states.parquet")
    emm_populaiton_weights = pd.read_parquet("parquets/emm_populaiton_weights.parquet")
    #petro_df = pd.read_parquet("parquets/site_source_co2_conversions.parquet")
    #elec_df = pd.read_parquet("parquets/end_use_electricity_price.parquet")

################################################################################
with Timer("Subset to years % 5 == 0"):
    baseline = baseline[baseline.year % 5 == 0].reset_index(drop = True)
    MarketsSavingsByCategory = (
            MarketsSavingsByCategory[MarketsSavingsByCategory.year % 5 == 0]
            .reset_index(drop = True)
            )

################################################################################
with Timer("Subset to needed outcome_metrics and merge on EMF related columns"):
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

################################################################################
with Timer("Convert MMBtu to Exajoules for MarketsSavingsByCategory"):
    idx = MarketsSavingsByCategory.query('outcome_metric.str.contains("MMBtu")').index
    MarketsSavingsByCategory.loc[idx, "value"] *= scout_emf_mappings.MMBtu_to_EJ
    MarketsSavingsByCategory.outcome_metric = (
            MarketsSavingsByCategory
            .outcome_metric
            .str.replace("MMBtu", "EJ")
            )

################################################################################
with Timer("Prepare Baseline Data for Aggregration"):
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
            .merge(CO2_intensity_of_electricity[["region", "year", "conversion_factor"]],
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

################################################################################
with Timer("Aggregate Baseline data for EMF Summary") as t:
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

################################################################################
with Timer("Aggregate MarketsSavingsByCategory for EMF Summary"):
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

################################################################################
with Timer("Concat Aggregations, process, and write out"):

    aggs = (
            pd.concat([baseline_aggs, aggs])#[baseline_energy_aggs, baseline_emission_aggs, aggs])
            .rename({"region":"Region"}, axis = 1)
           )

    # add model name and units columns
    aggs["Model"] = "Scout v0.8"

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





# elec_df is end_use_electricity_price
# petro_df is the site_source_co2_conversions
# price_floor_area_df is a concatnation of elec_df, petro_df, floor_area




# FOR DEV WORK....
aggs.to_parquet("aggs.parquet")
aggs = pd.read_parquet("aggs.parquet")


################################################################################


################################################################################
timer.toc()
################################################################################
#                                 End of File                                  #
################################################################################

