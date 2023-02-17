import pandas as pd
import yaml
from utilities import json_to_df
from utilities import isfloat
from utilities import whats_in_a_series
from scout_concepts import ScoutConcepts
from timer import Timer

scout_concetps = ScoutConcepts()

with open('config.yml', 'r') as f:
    config = yaml.load(f, Loader = yaml.loader.SafeLoader)
    print(config)


config
config.Node()


timer = Timer()
timer.tic("Process ecm_results_1-1.json, ecm_results_2.json, and ecm_results_3-1.json")

################################################################################
# Read in the baseline data and format as a DataFrame
timer.tic("Read in json files and formate as one DataFrame")
timer.tic("reading in ecm_results_1-1.json (0by50.BSG.1.R2)")
DF1 = json_to_df(path = 'ecm_results/ecm_results_1-1.json')
timer.toc()
timer.tic("reading in ecm_results_2.json (0by50.BSG.2.R2)")
DF2 = json_to_df(path = 'ecm_results/ecm_results_2.json')
timer.toc()
timer.tic("reading in ecm_results_3-1.json (0by50.BSG.Adv.R2)")
DF3 = json_to_df(path = 'ecm_results/ecm_results_3-1.json')
timer.toc()

timer.tic("Building one data set from all result files")
DF = pd.concat([DF1, DF2, DF3],
               keys = ["0by50.BSG.1.R2", "0by50.BSG.2.R2", "0by50.BSG.Adv.R2"])
DF = DF.reset_index(level = 0, names = ["Scenario"])
del DF1
del DF2
del DF3
timer.toc()
timer.toc()

################################################################################
# We Need to split DF into several different sets

timer.tic("Split results by concept:")
queries = {
        'OnSiteGenerationByCategory': 'lvl0 == "On-site Generation" & lvl2 == "By Category"',
        'OnSiteGenerationOverall': 'lvl0 == "On-site Generation" & lvl2 == "Overall"',
        'FinancialMetrics': 'lvl1 == "Financial Metrics"',
        'FilterVariables' : 'lvl1 == "Filter Variables"',
        'MarketsSavingsByCategory': 'lvl1 == "Markets and Savings (by Category)"',
        'MarketsSavingsOverall': 'lvl1 == "Markets and Savings (Overall)"',
        }

timer.tic("OnSiteGenerationByCategory")
OnSiteGenerationByCategory = DF.query(queries["OnSiteGenerationByCategory"]).copy()
OnSiteGenerationByCategory = OnSiteGenerationByCategory.reset_index(drop = True)
timer.toc()

timer.tic("OnSiteGenerationOverall")
OnSiteGenerationOverall = DF.query(queries["OnSiteGenerationOverall"]).copy()
OnSiteGenerationOverall = OnSiteGenerationOverall.reset_index(drop = True)
timer.toc()

timer.tic("FinancialMetrics")
FinancialMetrics = DF.query(queries["FinancialMetrics"]).copy()
FinancialMetrics = FinancialMetrics.reset_index(drop = True)
timer.toc()

timer.tic("MarketsSavingsByCategory")
MarketsSavingsByCategory = DF.query(queries["MarketsSavingsByCategory"]).copy()
MarketsSavingsByCategory = MarketsSavingsByCategory.reset_index(drop = True)
timer.toc()

timer.tic("MarketsSavingsOverall")
MarketsSavingsOverall = DF.query(queries["MarketsSavingsOverall"]).copy()
MarketsSavingsOverall = MarketsSavingsOverall.reset_index(drop = True)
timer.toc()

timer.tic("FilterVariables")
FilterVariables = DF.query(queries["FilterVariables"]).copy()
FilterVariables = FilterVariables.reset_index(drop = True)
timer.toc()

# verify all rows of DF have been accounted for.  A set of anti joins here is
# useful, but can take a bit of time to compute.  To reduce overall
# computational time try to do the anti joins in order of the largest (most
# rows) to smallest (fewest rows) of the noted sets.

timer.tic("verifying all rows in the results are accounted for")
d = DF.copy()
d.shape

outer = d.merge(MarketsSavingsByCategory, how = "outer", indicator = True)
d = outer.query('_merge == "left_only"').drop("_merge", axis = 1)
d.shape

outer = d.merge(MarketsSavingsOverall, how = "outer", indicator = True)
d = outer.query('_merge == "left_only"').drop("_merge", axis = 1)
d.shape

outer = d.merge(OnSiteGenerationByCategory, how = "outer", indicator = True)
d = outer.query('_merge == "left_only"').drop("_merge", axis = 1)
d.shape

outer = d.merge(FinancialMetrics, how = "outer", indicator = True)
d = outer.query('_merge == "left_only"').drop("_merge", axis = 1)
d.shape

outer = d.merge(FilterVariables, how = "outer", indicator = True)
d = outer.query('_merge == "left_only"').drop("_merge", axis = 1)
d.shape

outer = d.merge(OnSiteGenerationOverall, how = "outer", indicator = True)
d = outer.query('_merge == "left_only"').drop("_merge", axis = 1)
d.shape

assert d.shape == (0, 11), "Not all rows in DF have been accounted for"
del d
timer.toc()
timer.toc()

################################################################################
timer.tic("Clean up the DataFrames")

########################################
timer.tic("remove columns that have no data")
def remove_empty_colums(df):
    for c in df.columns:
        if df[c].isna().all():
            df = df.drop(columns = c)
    return df

OnSiteGenerationByCategory = remove_empty_colums(OnSiteGenerationByCategory)
OnSiteGenerationOverall    = remove_empty_colums(OnSiteGenerationOverall)
FinancialMetrics           = remove_empty_colums(FinancialMetrics)
MarketsSavingsByCategory   = remove_empty_colums(MarketsSavingsByCategory)
MarketsSavingsOverall      = remove_empty_colums(MarketsSavingsOverall)
FilterVariables            = remove_empty_colums(FilterVariables)

timer.toc()

########################################
# On-site Generation By Category
timer.tic("Cleaning up OnSiteGenerationByCategory")
if (OnSiteGenerationByCategory.lvl0 == "On-site Generation").all():
    OnSiteGenerationByCategory = OnSiteGenerationByCategory.drop(columns = "lvl0")

if OnSiteGenerationByCategory.lvl1.isin(scout_concetps.outcome_metrics).all():
    OnSiteGenerationByCategory = OnSiteGenerationByCategory.rename(columns = {"lvl1" : "outcome_metric"})

if (OnSiteGenerationByCategory.lvl2 == "By Category").all():
    OnSiteGenerationByCategory = OnSiteGenerationByCategory.drop(columns = "lvl2")

if OnSiteGenerationByCategory.lvl3.isin(scout_concetps.regions).all():
    OnSiteGenerationByCategory = OnSiteGenerationByCategory.rename(columns = {"lvl3": "region"})

if OnSiteGenerationByCategory.lvl4.isin(scout_concetps.building_types).all():
    OnSiteGenerationByCategory = OnSiteGenerationByCategory.rename(columns = {"lvl4": "building_type"})

if OnSiteGenerationByCategory.lvl5.isin(scout_concetps.years).all():
    OnSiteGenerationByCategory = OnSiteGenerationByCategory.rename(columns = {"lvl5": "year", "lvl6": "value"})
    OnSiteGenerationByCategory.year = OnSiteGenerationByCategory.year.astype("Int64")
    OnSiteGenerationByCategory.value = OnSiteGenerationByCategory.value.astype(float)

# verity all the columns have been mapped to a concept
assert ~OnSiteGenerationByCategory.columns.str.match("lvl").any(),\
        "A column in OnSiteGenerationByCategory has not been mapped to a conceptual construct"

timer.toc()

########################################
# On-site Generation Overall
timer.tic("Cleaning up OnSiteGenerationOverall")
if (OnSiteGenerationOverall.lvl0 == "On-site Generation").all():
    OnSiteGenerationOverall = OnSiteGenerationOverall.drop(columns = "lvl0")

if OnSiteGenerationOverall.lvl1.isin(scout_concetps.outcome_metrics).all():
    OnSiteGenerationOverall = OnSiteGenerationOverall.rename(columns = {"lvl1" : "outcome_metric"})

if (OnSiteGenerationOverall.lvl2 == "Overall").all():
    OnSiteGenerationOverall = OnSiteGenerationOverall.drop(columns = "lvl2")

if OnSiteGenerationOverall.lvl3.isin(scout_concetps.years).all():
    OnSiteGenerationOverall = OnSiteGenerationOverall.rename(columns = {"lvl3": "year", "lvl4": "value"})
    OnSiteGenerationOverall.year = OnSiteGenerationOverall.year.astype("Int64")
    OnSiteGenerationOverall.value = OnSiteGenerationOverall.value.astype(float)

# verity all the columns have been mapped to a concept
assert ~OnSiteGenerationOverall.columns.str.match("lvl").any(),\
        "A column in OnSiteGenerationOverall has not been mapped to a conceptual construct"

timer.toc()

########################################
# FinancialMetrics
timer.tic("Cleaning up FinancialMetrics")
if FinancialMetrics.lvl0.isin(scout_concetps.ecms).all():
    FinancialMetrics = FinancialMetrics.rename(columns = {"lvl0" : "ecm"})

if (FinancialMetrics.lvl1 == "Financial Metrics").all():
    FinancialMetrics = FinancialMetrics.drop("lvl1", axis = 1)

if FinancialMetrics.lvl2.isin(scout_concetps.outcome_metrics).all():
    FinancialMetrics = FinancialMetrics.rename(columns = {"lvl2": "outcome_metric"})

if FinancialMetrics.lvl3.isin(scout_concetps.years).all():
    FinancialMetrics = FinancialMetrics.rename(columns = {"lvl3": "year", "lvl4": "value"})
    FinancialMetrics.year = FinancialMetrics.year.astype("Int64")
    FinancialMetrics.value = FinancialMetrics.value.astype(float)

assert ~FinancialMetrics.columns.str.match("lvl").any(),\
        "A column in FinancialMetrics has not been mapped to a conceptual construct"

timer.toc()

########################################
# MarketsSavingsByCategory
timer.tic("Cleaning up MarketsSavingsByCategory")
if MarketsSavingsByCategory.lvl0.isin(scout_concetps.ecms).all():
    MarketsSavingsByCategory = MarketsSavingsByCategory.rename(columns = {"lvl0" : "ecm"})

if (MarketsSavingsByCategory.lvl1 == "Markets and Savings (by Category)").all():
    MarketsSavingsByCategory = MarketsSavingsByCategory.drop("lvl1", axis = 1)

if MarketsSavingsByCategory.lvl2.isin(scout_concetps.scenarios).all():
    MarketsSavingsByCategory = MarketsSavingsByCategory.rename(columns = {"lvl2": "adoption_scenario"})

if MarketsSavingsByCategory.lvl3.isin(scout_concetps.outcome_metrics).all():
    MarketsSavingsByCategory = MarketsSavingsByCategory.rename(columns = {"lvl3": "outcome_metric"})

if MarketsSavingsByCategory.lvl4.isin(scout_concetps.regions).all():
    MarketsSavingsByCategory = MarketsSavingsByCategory.rename(columns = {"lvl4": "region"})

if MarketsSavingsByCategory.lvl5.isin(scout_concetps.building_types).all():
    MarketsSavingsByCategory = MarketsSavingsByCategory.rename(columns = {"lvl5": "building_type"})

if MarketsSavingsByCategory.lvl6.isin(scout_concetps.end_uses).all():
    MarketsSavingsByCategory = MarketsSavingsByCategory.rename(columns = {"lvl6": "end_use"})

# there are some rows with no fuel_type, lvl7 has year values.  move the year
# and values
MarketsSavingsByCategory.query('lvl7.isin(@scout_concetps.years)')
idx = MarketsSavingsByCategory.query('lvl7.isin(@scout_concetps.years)').index
MarketsSavingsByCategory.loc[idx, "lvl9"] = MarketsSavingsByCategory.loc[idx, "lvl8"]
MarketsSavingsByCategory.loc[idx, "lvl8"] = MarketsSavingsByCategory.loc[idx, "lvl7"]
MarketsSavingsByCategory.loc[idx, "lvl7"] = pd.NA

if MarketsSavingsByCategory.lvl7.isin(scout_concetps.fuel_types + [pd.NA]).all():
    MarketsSavingsByCategory = MarketsSavingsByCategory.rename(columns = {"lvl7": "fuel_type"})

if MarketsSavingsByCategory.lvl8.isin(scout_concetps.years).all():
    MarketsSavingsByCategory = MarketsSavingsByCategory.rename(columns = {"lvl8": "year", "lvl9": "value"})
    MarketsSavingsByCategory.year = MarketsSavingsByCategory.year.astype("Int64")
    MarketsSavingsByCategory.value = MarketsSavingsByCategory.value.astype(float)

assert ~MarketsSavingsByCategory.columns.str.match("lvl").any(),\
        "A column in MarketsSavingsByCategory has not been mapped to a conceptual construct"

timer.toc()

########################################
# MarketsSavingsOverall
timer.tic("Cleaning up MarketsSavingsOverall")
if MarketsSavingsOverall.lvl0.isin(scout_concetps.ecms).all():
    MarketsSavingsOverall = MarketsSavingsOverall.rename(columns = {"lvl0" : "ecm"})

if (MarketsSavingsOverall.lvl1 == "Markets and Savings (Overall)").all():
    MarketsSavingsOverall = MarketsSavingsOverall.drop("lvl1", axis = 1)

if MarketsSavingsOverall.lvl2.isin(scout_concetps.scenarios).all():
    MarketsSavingsOverall = MarketsSavingsOverall.rename(columns = {"lvl2": "adoption_scenario"})

if MarketsSavingsOverall.lvl3.isin(scout_concetps.outcome_metrics).all():
    MarketsSavingsOverall = MarketsSavingsOverall.rename(columns = {"lvl3": "outcome_metric"})

if MarketsSavingsOverall.lvl4.isin(scout_concetps.years).all():
    MarketsSavingsOverall = MarketsSavingsOverall.rename(columns = {"lvl4": "year", "lvl5": "value"})
    MarketsSavingsOverall.year = MarketsSavingsOverall.year.astype("Int64")
    MarketsSavingsOverall.value = MarketsSavingsOverall.value.astype(float)

assert ~MarketsSavingsOverall.columns.str.match("lvl").any(),\
        "A column in MarketsSavingsOverall has not been mapped to a conceptual construct"

timer.toc()

########################################
# FilterVariables
timer.tic("Cleaning up FilterVariables")
if FilterVariables.lvl0.isin(scout_concetps.ecms).all():
    FilterVariables = FilterVariables.rename(columns = {"lvl0" : "ecm"})

if (FilterVariables.lvl1 == "Filter Variables").all():
    FilterVariables = FilterVariables.drop("lvl1", axis = 1)

FilterVariables = FilterVariables.rename(columns = {"lvl2": "concept", "lvl3": "values"})

assert ~FilterVariables.columns.str.match("lvl").any(),\
        "A column in FilterVariables has not been mapped to a conceptual construct"

timer.toc()
timer.toc()

################################################################################
# Output
timer.tic("Writting parquet files")

OnSiteGenerationByCategory.to_parquet('parquets/OnSiteGenerationByCategory.parquet')
OnSiteGenerationOverall.to_parquet('parquets/OnSiteGenerationOverall.parquet')
MarketsSavingsByCategory.to_parquet('parquets/MarketsSavingsByCategory.parquet')
MarketsSavingsOverall.to_parquet('parquets/MarketsSavingsOverall.parquet')
FilterVariables.to_parquet('parquets/FilterVariables.parquet')
FinancialMetrics.to_parquet('parquets/FinancialMetrics.parquet')

timer.toc()

################################################################################
timer.toc()
################################################################################
#                                 End of File                                  #
################################################################################

