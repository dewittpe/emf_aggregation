import pandas as pd
from timer import Timer
from Scout_to_EMF_mappings import EMF_BASE_STRINGS
from Scout_to_EMF_mappings import MMBtu_to_EJ
from Scout_to_EMF_mappings import BUILDING_TYPE_CLASS
from Scout_to_EMF_mappings import EMF_END_USES
from Scout_to_EMF_mappings import EMF_DIRECT_INDIRECT_FUEL
from Scout_to_EMF_mappings import EMF_FUEL_TYPE

timer = Timer()
timer.tic("EMF Aggregation")

################################################################################
timer.tic("Import parquets")
baseline = pd.read_parquet("baseline.parquet")
MarketsSavingsByCategory = pd.read_parquet("MarketsSavingsByCategory.parquet")
CO2_intensity_of_electricity = pd.read_parquet("CO2_intensity_of_electricity.parquet")
timer.toc()

################################################################################
timer.tic("Subset to needed outcome_metrics and merge on EMF related columns")

MarketsSavingsByCategory = (

d = (
        MarketsSavingsByCategory
        .merge(EMF_BASE_STRINGS, how = "inner")
        .merge(BUILDING_TYPE_CLASS, how = "left", on = "building_type")
        .merge(EMF_END_USES,
               how = "left",
               left_on = "end_use",
               right_on = "scout_end_use")
        .drop("scout_end_use", axis = 1)
        .merge(EMF_DIRECT_INDIRECT_FUEL,
               how = "left",
               left_on = "fuel_type",
               right_on = "scout_fuel_type")
        .drop("scout_fuel_type", axis = 1)
        .merge(EMF_FUEL_TYPE,
               how = "left",
               left_on = ['fuel_type', 'end_use', 'technology'],
               right_on = ['scout_fuel_type', 'scout_end_use', 'scout_technology'])
        )
d.iloc[[0]]

EMF_FUEL_TYPE

timer.toc()

################################################################################
timer.tic("Convert MMBtu to Exajoules")
idx = MarketsSavingsByCategory.query('outcome_metric.str.contains("MMBtu")').index

MarketsSavingsByCategory.loc[idx, "value"] *= MMBtu_to_EJ
MarketsSavingsByCategory.outcome_metric = MarketsSavingsByCategory.outcome_metric.str.replace("MMBtu", "EJ")

timer.toc()

################################################################################
timer.tic("Aggregate ECM results for EMF Summary")

grps = [
        ['result_file', 'region', 'emf_base_string', 'year'],
        ['result_file', 'region', 'emf_base_string', 'building_class', 'year'],
        ['result_file', 'region', 'emf_base_string', 'building_class', 'emf_end_use', 'year'],

        ['result_file', 'region', 'emf_base_string', 'emf_direct_indirect_fuel', 'year'],
        ['result_file', 'region', 'emf_base_string', 'building_class', 'emf_direct_indirect_fuel', 'year'],
        ['result_file', 'region', 'emf_base_string', 'building_class', 'emf_end_use', 'emf_direct_indirect_fuel', 'year'],

        ['result_file', 'region', 'emf_base_string', 'emf_fuel_type', 'year'],
        ['result_file', 'region', 'emf_base_string', 'building_class', 'emf_fuel_type', 'year'],
        ['result_file', 'region', 'emf_base_string', 'building_class', 'emf_end_use', 'emf_fuel_type', 'year'],
       ]

aggs = [MarketsSavingsByCategory.groupby(g).agg({"value":"sum"}).reset_index() for g in grps]

aggs

MarketsSavingsByCategory

################################################################################
timer.toc()
################################################################################
#                                 End of File                                  #
################################################################################

