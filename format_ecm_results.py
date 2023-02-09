import pandas as pd
from utilities import json_to_df
from utilities import isfloat
from utilities import whats_in_a_series
from utilities import ECMS
from utilities import SCENARIOS
from utilities import REGIONS
from utilities import BUILDING_TYPES
from utilities import FUEL_TYPES
from utilities import END_USES
from utilities import SUPPLY_DEMAND
from utilities import ENERGY_STOCK
from utilities import TECHNOLOGIES
from utilities import YEARS
from utilities import OUTCOME_METRICS
from utilities import DEFINED_VALUES

# Read in the baseline data and format as a DataFrame
DF1 = json_to_df(path = 'ecm_results_1-1.json')
DF2 = json_to_df(path = 'ecm_results_2.json')
DF3 = json_to_df(path = 'ecm_results_3-1.json')

DF = pd.concat([DF1, DF2, DF3],
               keys = ["ecm_results_1-1", "ecm_results_2", "ecm_results_3-1"])
DF = DF.reset_index(level = 0, names = ["result_file"])

################################################################################
# We Need to split DF into several different sets

queries = {
        'OnSiteGenerationByCategory': 'lvl0 == "On-site Generation" & lvl2 == "By Category"',
        'OnSiteGenerationOverall': 'lvl0 == "On-site Generation" & lvl2 == "Overall"',
        'FinancialMetrics': 'lvl1 == "Financial Metrics"',
        'FilterVariables' : 'lvl1 == "Filter Variables"',
        'MarketsSavingsByCategory': 'lvl1 == "Markets and Savings (by Category)"',
        'MarketsSavingsOverall': 'lvl1 == "Markets and Savings (Overall)"',
        }


OnSiteGenerationByCategory = DF.query(queries["OnSiteGenerationByCategory"]).copy()
OnSiteGenerationOverall    = DF.query(queries["OnSiteGenerationOverall"]).copy()
FinancialMetrics           = DF.query(queries["FinancialMetrics"]).copy()
MarketsSavingsByCategory   = DF.query(queries["MarketsSavingsByCategory"]).copy()
MarketsSavingsOverall      = DF.query(queries["MarketsSavingsOverall"]).copy()
FilterVariables            = DF.query(queries["FilterVariables"]).copy()

# verify all rows of DF have been accounted for.  A set of anti joins here is
# useful, but can take a bit of time to compute.  To reduce overall
# computational time try to do the anti joins in order of the largest (most
# rows) to smallest (fewest rows) of the noted sets.

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

################################################################################
# Clean up the DataFrames

########################################
# remove columns that have no data
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

########################################
# On-site Generation By Category
if (OnSiteGenerationByCategory.lvl0 == "On-site Generation").all():
    OnSiteGenerationByCategory = OnSiteGenerationByCategory.drop(columns = "lvl0")

if OnSiteGenerationByCategory.lvl1.isin(OUTCOME_METRICS).all():
    OnSiteGenerationByCategory = OnSiteGenerationByCategory.rename(columns = {"lvl1" : "outcome_metric"})

if (OnSiteGenerationByCategory.lvl2 == "By Category").all():
    OnSiteGenerationByCategory = OnSiteGenerationByCategory.drop(columns = "lvl2")

if OnSiteGenerationByCategory.lvl3.isin(REGIONS).all():
    OnSiteGenerationByCategory = OnSiteGenerationByCategory.rename(columns = {"lvl3": "region"})

if OnSiteGenerationByCategory.lvl4.isin(BUILDING_TYPES).all():
    OnSiteGenerationByCategory = OnSiteGenerationByCategory.rename(columns = {"lvl4": "building_type"})

if OnSiteGenerationByCategory.lvl5.isin(YEARS).all():
    OnSiteGenerationByCategory = OnSiteGenerationByCategory.rename(columns = {"lvl5": "year", "lvl6": "value"})
    OnSiteGenerationByCategory.year = OnSiteGenerationByCategory.year.astype("Int64")
    OnSiteGenerationByCategory.value = OnSiteGenerationByCategory.value.astype(float)

# verity all the columns have been mapped to a concept
assert ~OnSiteGenerationByCategory.columns.str.match("lvl").any(),\
        "A column in OnSiteGenerationByCategory has not been mapped to a conceptual construct"

########################################
# On-site Generation Overall
if (OnSiteGenerationOverall.lvl0 == "On-site Generation").all():
    OnSiteGenerationOverall = OnSiteGenerationOverall.drop(columns = "lvl0")

if OnSiteGenerationOverall.lvl1.isin(OUTCOME_METRICS).all():
    OnSiteGenerationOverall = OnSiteGenerationOverall.rename(columns = {"lvl1" : "outcome_metric"})

if (OnSiteGenerationOverall.lvl2 == "Overall").all():
    OnSiteGenerationOverall = OnSiteGenerationOverall.drop(columns = "lvl2")

if OnSiteGenerationOverall.lvl3.isin(YEARS).all():
    OnSiteGenerationOverall = OnSiteGenerationOverall.rename(columns = {"lvl3": "year", "lvl4": "value"})
    OnSiteGenerationOverall.year = OnSiteGenerationOverall.year.astype("Int64")
    OnSiteGenerationOverall.value = OnSiteGenerationOverall.value.astype(float)

# verity all the columns have been mapped to a concept
assert ~OnSiteGenerationOverall.columns.str.match("lvl").any(),\
        "A column in OnSiteGenerationOverall has not been mapped to a conceptual construct"

########################################
# FinancialMetrics
if FinancialMetrics.lvl0.isin(ECMS).all():
    FinancialMetrics = FinancialMetrics.rename(columns = {"lvl0" : "ecm"})

if (FinancialMetrics.lvl1 == "Financial Metrics").all():
    FinancialMetrics = FinancialMetrics.drop("lvl1", axis = 1)

if FinancialMetrics.lvl2.isin(OUTCOME_METRICS).all():
    FinancialMetrics = FinancialMetrics.rename(columns = {"lvl2": "outcome_metric"})

if FinancialMetrics.lvl3.isin(YEARS).all():
    FinancialMetrics = FinancialMetrics.rename(columns = {"lvl3": "year", "lvl4": "value"})
    FinancialMetrics.year = FinancialMetrics.year.astype("Int64")
    FinancialMetrics.value = FinancialMetrics.value.astype(float)

assert ~FinancialMetrics.columns.str.match("lvl").any(),\
        "A column in FinancialMetrics has not been mapped to a conceptual construct"

########################################
# MarketsSavingsByCategory
if MarketsSavingsByCategory.lvl0.isin(ECMS).all():
    MarketsSavingsByCategory = MarketsSavingsByCategory.rename(columns = {"lvl0" : "ecm"})

if (MarketsSavingsByCategory.lvl1 == "Markets and Savings (by Category)").all():
    MarketsSavingsByCategory = MarketsSavingsByCategory.drop("lvl1", axis = 1)

if MarketsSavingsByCategory.lvl2.isin(SCENARIOS).all():
    MarketsSavingsByCategory = MarketsSavingsByCategory.rename(columns = {"lvl2": "adoption_scenario"})

if MarketsSavingsByCategory.lvl3.isin(OUTCOME_METRICS).all():
    MarketsSavingsByCategory = MarketsSavingsByCategory.rename(columns = {"lvl3": "outcome_metric"})

if MarketsSavingsByCategory.lvl4.isin(REGIONS).all():
    MarketsSavingsByCategory = MarketsSavingsByCategory.rename(columns = {"lvl4": "region"})

if MarketsSavingsByCategory.lvl5.isin(BUILDING_TYPES).all():
    MarketsSavingsByCategory = MarketsSavingsByCategory.rename(columns = {"lvl5": "building_type"})

if MarketsSavingsByCategory.lvl6.isin(END_USES).all():
    MarketsSavingsByCategory = MarketsSavingsByCategory.rename(columns = {"lvl6": "end_use"})

if MarketsSavingsByCategory.lvl7.isin(FUEL_TYPES).all():
    MarketsSavingsByCategory = MarketsSavingsByCategory.rename(columns = {"lvl7": "fuel_type"})

if MarketsSavingsByCategory.lvl8.isin(YEARS).all():
    MarketsSavingsByCategory = MarketsSavingsByCategory.rename(columns = {"lvl8": "year", "lvl9": "value"})
    MarketsSavingsByCategory.year = MarketsSavingsByCategory.year.astype("Int64")
    MarketsSavingsByCategory.value = MarketsSavingsByCategory.value.astype(float)

assert ~MarketsSavingsByCategory.columns.str.match("lvl").any(),\
        "A column in MarketsSavingsByCategory has not been mapped to a conceptual construct"

########################################
# MarketsSavingsOverall
if MarketsSavingsOverall.lvl0.isin(ECMS).all():
    MarketsSavingsOverall = MarketsSavingsOverall.rename(columns = {"lvl0" : "ecm"})

if (MarketsSavingsOverall.lvl1 == "Markets and Savings (Overall)").all():
    MarketsSavingsOverall = MarketsSavingsOverall.drop("lvl1", axis = 1)

if MarketsSavingsOverall.lvl2.isin(SCENARIOS).all():
    MarketsSavingsOverall = MarketsSavingsOverall.rename(columns = {"lvl2": "adoption_scenario"})

if MarketsSavingsOverall.lvl3.isin(OUTCOME_METRICS).all():
    MarketsSavingsOverall = MarketsSavingsOverall.rename(columns = {"lvl3": "outcome_metric"})

if MarketsSavingsOverall.lvl4.isin(YEARS).all():
    MarketsSavingsOverall = MarketsSavingsOverall.rename(columns = {"lvl4": "year", "lvl5": "value"})
    MarketsSavingsOverall.year = MarketsSavingsOverall.year.astype("Int64")
    MarketsSavingsOverall.value = MarketsSavingsOverall.value.astype(float)

assert ~MarketsSavingsOverall.columns.str.match("lvl").any(),\
        "A column in MarketsSavingsOverall has not been mapped to a conceptual construct"

########################################
# FilterVariables
if FilterVariables.lvl0.isin(ECMS).all():
    FilterVariables = FilterVariables.rename(columns = {"lvl0" : "ecm"})

if (FilterVariables.lvl1 == "Filter Variables").all():
    FilterVariables = FilterVariables.drop("lvl1", axis = 1)

FilterVariables = FilterVariables.rename(columns = {"lvl2": "concept", "lvl3": "values"})

assert ~FilterVariables.columns.str.match("lvl").any(),\
        "A column in FilterVariables has not been mapped to a conceptual construct"

################################################################################
# Output

OnSiteGenerationByCategory.to_parquet('OnSiteGenerationByCategory.parquet')
OnSiteGenerationOverall.to_parquet('OnSiteGenerationOverall.parquet')
MarketsSavingsByCategory.to_parquet('MarketsSavingsByCategory.parquet')
MarketsSavingsOverall.to_parquet('MarketsSavingsOverall.parquet')
FilterVariables.to_parquet('FilterVariables.parquet')
FinancialMetrics.to_parquet('FinancialMetrics.parquet')




