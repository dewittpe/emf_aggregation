import pandas as pd
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
timer.tic("Import parquets")
baseline = pd.read_parquet("parquets/baseline.parquet")
MarketsSavingsByCategory = pd.read_parquet("parquets/MarketsSavingsByCategory.parquet")
CO2_intensity_of_electricity = pd.read_parquet("parquets/CO2_intensity_of_electricity.parquet")
site_source_co2_conversions = pd.read_parquet("parquets/site_source_co2_conversions.parquet")
timer.toc()

################################################################################
timer.tic("Subset to years % 5 == 0")
baseline = baseline[baseline.year % 5 == 0].reset_index(drop = True)
MarketsSavingsByCategory = MarketsSavingsByCategory[MarketsSavingsByCategory.year % 5 == 0].reset_index(drop = True)
timer.toc()

################################################################################
timer.tic("Subset to needed outcome_metrics and merge on EMF related columns")

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
        #.merge(scout_emf_mappings.non_other_fuel_type,
        #       how = "left",
        #       left_on = ['fuel_type'],
        #       right_on = ['scout_non_other_fuel_type'])
        #.merge(scout_emf_mappings.other_fuel_type,
        #       how = "left",
        #       left_on = ['fuel_type', 'end_use'], #, 'technology'],
        #       right_on = ['scout_other_fuel_type', 'scout_end_use'], #, 'scout_technology'],
        #       suffixes = ("_1", "_2"))
        #.merge(scout_emf_mappings.other_fuel_type,
        #      how = 'left',
        #       left_on = ['fuel_type', 'end_use', 'technology'],
        #       right_on = ['scout_other_fuel_type', 'scout_end_use', 'scout_technology']
        #       )
        #.drop(['scout_other_fuel_type', 'scout_end_use', 'scout_technology'], axis = 1)
        )

MarketsSavingsByCategory.query('emf_fuel_type.isna()')
timer.toc()

################################################################################
timer.tic("Convert MMBtu to Exajoules for MarketsSavingsByCategory")
idx = MarketsSavingsByCategory.query('outcome_metric.str.contains("MMBtu")').index

MarketsSavingsByCategory.loc[idx, "value"] *= scout_emf_mappings.MMBtu_to_EJ
MarketsSavingsByCategory.outcome_metric = MarketsSavingsByCategory.outcome_metric.str.replace("MMBtu", "EJ")

timer.toc()

################################################################################
timer.tic("Prepare Baseline Data for Aggregration")
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
        .merge(CO2_intensity_of_electricity,
               how = 'left',
               on = ['region', 'year'])
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

timer.toc()

################################################################################
timer.tic("Aggregate Baseline data for EMF Summary")

########################################
timer.tic("Aggregate Final Energy|Buildings")

grps = [
        ["Scenario", "region",                                                   "year"],
        ["Scenario", "region",                                  "emf_fuel_type", "year"],
        ["Scenario", "region", "building_class",                                 "year"],
        ["Scenario", "region", "building_class",                "emf_fuel_type", "year"],
        ["Scenario", "region", "building_class", "emf_end_use", "emf_fuel_type", "year"]
    ]

baseline_energy_aggs = [
        baseline
        .groupby(g)
        .agg({"EJ":"sum"})
        .rename({"EJ":"value"}, axis = 1)
        .reset_index()
        for g in grps
        ]

baseline_energy_aggs = pd.concat(baseline_energy_aggs)

baseline_energy_aggs["Variable"] = (
        (
        "*Final Energy|Buildings" +
        "|" + baseline_energy_aggs.building_class.fillna("") +
        "|" + baseline_energy_aggs.emf_end_use.fillna("") +
        "|" + baseline_energy_aggs.emf_fuel_type.fillna("")
        )
        .str.replace("\|{2,}", "|", regex = True)
        .str.replace("\|$", "", regex = True)
        )

timer.toc()

########################################
timer.tic("Aggregate Emissions|CO2|Energy|Demand|Buildings")

grps = [
    ["Scenario", "region",                                                              "year"],
    ["Scenario", "region",                                  "emf_direct_indirect_fuel", "year"],
    ["Scenario", "region", "building_class",                                            "year"],
    ["Scenario", "region", "building_class",                "emf_direct_indirect_fuel", "year"],
    ["Scenario", "region", "building_class", "emf_end_use",                             "year"],
    ["Scenario", "region", "building_class", "emf_end_use", "emf_direct_indirect_fuel", "year"]
        ]

baseline_emission_aggs = [
        baseline
        .groupby(g)
        .agg({"CO2":"sum"})
        .rename({"CO2":"value"}, axis = 1)
        .reset_index()
        for g in grps
        ]

baseline_emission_aggs = pd.concat(baseline_emission_aggs)

baseline_emission_aggs["Variable"] = (
        (
        "*Final Energy|Buildings" +
        "|" + baseline_emission_aggs.building_class.fillna("") +
        "|" + baseline_emission_aggs.emf_end_use.fillna("") +
        "|" + baseline_emission_aggs.emf_direct_indirect_fuel.fillna("")
        )
        .str.replace("\|{2,}", "|", regex = True)
        .str.replace("\|$", "", regex = True)
        )

timer.toc()
timer.toc()

################################################################################
timer.tic("Aggregate MarketsSavingsByCategory for EMF Summary")

grps0 = [
    ['Scenario', 'region', 'emf_base_string',                                                                               'year'],
    ['Scenario', 'region', 'emf_base_string', 'building_class',                                                             'year'],
    ['Scenario', 'region', 'emf_base_string', 'building_class', 'emf_end_use',                                              'year']
        ]
grps1 = [
    ['Scenario', 'region', 'emf_base_string',                                  'emf_direct_indirect_fuel',                  'year'],
    ['Scenario', 'region', 'emf_base_string', 'building_class',                'emf_direct_indirect_fuel',                  'year'],
    ['Scenario', 'region', 'emf_base_string', 'building_class', 'emf_end_use', 'emf_direct_indirect_fuel',                  'year']
        ]
grps2 = [
    ['Scenario', 'region', 'emf_base_string',                                                              'emf_fuel_type', 'year'],
    ['Scenario', 'region', 'emf_base_string', 'building_class',                                            'emf_fuel_type', 'year'],
    ['Scenario', 'region', 'emf_base_string', 'building_class', 'emf_end_use',                             'emf_fuel_type', 'year']
        ]

aggs0 = [
        MarketsSavingsByCategory
        .groupby(g)
        .agg({"value":"sum"})
        .reset_index() 
        for g in grps0
        ]

aggs1 = [
        MarketsSavingsByCategory
        .query('emf_base_string == "*Emissions|CO2|Energy|Demand|Buildings"')
        .groupby(g)
        .agg({"value":"sum"})
        .reset_index() 
        for g in grps1
        ]

aggs2 = [
        MarketsSavingsByCategory
        .query('emf_base_string == "*Final Energy|Buildings"')
        .groupby(g)
        .agg({"value":"sum"})
        .reset_index() 
        for g in grps2
        ]

aggs = [pd.concat(a) for a in [aggs0, aggs1, aggs2]]
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

timer.toc()

################################################################################
timer.tic("Concat Aggregations, process, and write out")

aggs = (
        pd.concat([baseline_energy_aggs, baseline_emission_aggs, aggs])
        .rename({"region":"Region"}, axis = 1)
       )

# add model name and units columns
aggs["Model"] = "Scout v0.8"

# for units, set all rows to the units for 'Final Energy' and then correct the
# needed rows for 'Emmissions'
aggs["Units"] = "EJ/yr"
idx = aggs.query("Variable.str.contains('Emmissions')").index
aggs.loc[idx, "Units"] = "Mt CO2\yr"

print(aggs)

aggs.to_parquet("aggs.parquet")

aggs = pd.read_parquet("aggs.parquet")

aggs

#baseline_agg_wide = (
#        baseline_agg.astype({'year':'str'})
#        .pivot_table(
#            index = ["emf_string"],
#            columns = ["year"],
#            values = ["value"]
#            )
#        )
#
#baseline_agg_wide.columns = baseline_agg_wide.columns.droplevel(0)
#baseline_agg_wide = baseline_agg_wide.reset_index()
#baseline_agg_wide.to_csv("baseline_agg_wide.csv", index = False)
#baseline_agg.to_csv("baseline_agg.csv", index = False)

timer.toc()

################################################################################


################################################################################
timer.toc()
################################################################################
#                                 End of File                                  #
################################################################################

