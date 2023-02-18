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

state_aggs = (
        aggs
        .merge(emm_to_states,
               how = 'left',
               left_on = 'Region',
               right_on = 'EMM')
        .drop("EMM", axis = 1)
        .merge(emm_populaiton_weights,
               how = 'left',
               left_on = 'Region',
               right_on = 'EMM')
        .drop("EMM", axis = 1)
        .assign(value = lambda x : x.value * x.emm_to_state_factor)
        .groupby(["Scenario", "year", "Variable", "Units", "State"])
        .agg({"value":"sum"})
        .reset_index()
        )

state_aggs


aggs
emm_to_states
emm_populaiton_weights
state_aggs


# FOR DEV WORK....
aggs.to_parquet("aggs.parquet")
aggs = pd.read_parquet("aggs.parquet")


################################################################################


################################################################################
timer.toc()
################################################################################
#                                 End of File                                  #
################################################################################

