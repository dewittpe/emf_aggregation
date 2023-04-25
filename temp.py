import os, sys, getopt, itertools
import yaml
import pandas as pd
from timer import Timer
from scout_emf_mappings import ScoutEMFMappings
from scout_concepts import ScoutMappings
from reads import *

emm_to_states = read_emm_to_states(verbose = False)
emm_population_weights = read_emm_population_weights(verbose = False)

baseline_agg = pd.read_csv("./emf_output/baseline_agg.csv").drop(["index"], axis = 1)
results_agg = pd.read_csv("./emf_output/results_agg.csv").drop(["index"], axis = 1)

aggs = pd.concat([baseline_agg, results_agg]).reset_index(drop = True)

# rename the value column in the agg DataFrame to emm_value so it is clear what
# values are being created and how in the following
aggs = aggs.rename(columns = {"value":"emm_value"})

aggs = aggs.drop(["emf_fuel_type", "building_class", "emf_end_use", "emf_direct_indirect_fuel","emf_base_string"], axis = 1)


# does the emm_to_state_factor seem reasonable?  
# If so, then all the factors should sum to one over the EMM
emm_sums = (
        emm_to_states
        .groupby(["EMM"])
        .agg({"emm_to_state_factor":"sum"})
        ).emm_to_state_factor

assert all((emm_sums > 0.9999999) & (emm_sums < 1.0000001))


# Explore the state summary
# rename the value column in the agg DataFrame to emm_value so it is clear what
# values are being created and how in the following
aggs = aggs.rename(columns = {"value":"emm_value"})
aggs

# The following code is/was in the main.py module and needs to be explored.
#state_aggs = (
#        aggs
#        .merge(emm_to_states,
#               how = 'left',
#               left_on = 'Region',
#               right_on = 'EMM')
#        .drop("EMM", axis = 1)
#        .merge(emm_population_weights,
#               how = 'left',
#               left_on = 'Region',
#               right_on = 'EMM')
#        .drop("EMM", axis = 1)
#        .assign(value = lambda x : x.value * x.emm_to_state_factor * x.weight)
#        .groupby(["Model", "Scenario", "year", "Variable", "Units", "State"])
#        .agg({"value":"sum"})
#        .reset_index()
#        .rename(columns = {"State":"Region"})
#        )

# Merge on the emm_to_state_factor to the agg 
prep_for_state_aggs = (
    aggs
    .merge(emm_to_states, how = "left", left_on = "region", right_on = "EMM")
    .drop("EMM", axis = 1)
    )
prep_for_state_aggs

# create a state_value column
#prep_for_state_aggs = prep_for_state_aggs.assign(state_value = lambda x : x.emm_value * x.emm_to_state_factor)
#prep_for_state_aggs[prep_for_state_aggs.state_value > 0]

# aggregate to set state sums
state_aggs_v1 = (
    prep_for_state_aggs
    .assign(state_value = lambda x : x.emm_value * x.emm_to_state_factor)
    .groupby(["Scenario", "year", "Variable", "State"])
    .agg({"state_value":"sum"})
    .reset_index()
    )

# aggregate by emm region then apply emm to state factor and reaggregate by
# state.
state_aggs_v2 = (
    prep_for_state_aggs
    .groupby(["Scenario", "year", "Variable", "region"])
    .agg({"emm_value":"sum"})
    .reset_index()
    .merge(emm_to_states, how = "left", left_on = "region", right_on = "EMM")
    .drop("EMM", axis = 1)
    .assign(state_value = lambda x : x.emm_value * x.emm_to_state_factor)
    .groupby(["Scenario", "year", "Variable", "State"])
    .agg({"state_value":"sum"})
    .reset_index()
    )

# no emm to state weight multiplier
state_aggs_v3 = (
    prep_for_state_aggs
    .groupby(["Scenario", "year", "Variable", "region"])
    .agg({"emm_value":"sum"})
    .reset_index()
    .merge(emm_to_states, how = "left", left_on = "region", right_on = "EMM")
    .drop("EMM", axis = 1)
    .groupby(["Scenario", "year", "Variable", "State"])
    .agg({"emm_value":"sum"})
    .rename(columns = {"emm_value":"state_value"})
    .reset_index()
    )

state_aggs_v1
state_aggs_v2
state_aggs_v3

(
    state_aggs_v1.rename(columns = {"state_value" : "state_value_v1"})
    .merge(state_aggs_v2.rename(columns = {"state_value" : "state_value_v2"}), how = "outer", on = ['Scenario', 'year', 'Variable', "State"])
    .merge(state_aggs_v3.rename(columns = {"state_value" : "state_value_v3"}), how = "outer", on = ['Scenario', 'year', 'Variable', "State"])
)




# WHY ARE THERE NEGATIVE VALUES????
prep_for_state_aggs[prep_for_state_aggs.State == "AL"][prep_for_state_aggs.state_value > 0]
prep_for_state_aggs[prep_for_state_aggs.State == "AL"][prep_for_state_aggs.state_value < 0]


# Looking at results form the SCOUT repo I see
#Model       Scenario        Region  Variable                                Unit        2020        2025        2030        2035        2040        2045        2050
#Scout v0.8  0by50.BSG.1.R2  AL      Emissions|CO2|Energy|Demand|Buildings   Mt CO2/yr   4.596032369 4.73419159  4.242014226 3.646477774 3.058057903 2.466516951 1.849124997

state_aggs.query('Scenario == "0by50.BSG.1.R2" & State == "AL" & Variable == "Emissions|CO2|Energy|Demand|Buildings"')

# Final Aggrgations
#Model       Region  Variable                                                     Unit       Scenario   2020         2025         2030         2035         2040         2045         2050
#Scout v0.8  BASN    Emissions|CO2|Energy|Demand|Buildings|Commercial|Appliances  Mt CO2/yr  NT.Ref.R2  1.458974056  1.506480015  1.590253982  1.679325619  1.771505518  1.867802847  1.963049348
aggs.query('Scenario == "NT.Ref.R2" & region == "BASN" & Variable == "Emissions|CO2|Energy|Demand|Buildings|Commercial|Appliances"')

################################################################################
#                              -- end of file --                               #
################################################################################
