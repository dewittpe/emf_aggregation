# The ecm results files contain information for two major categories:
#
# * On-site Generation, and
# * Markets and Savings.
#
# Both of these have data presented in terms of "By Category" and "Overall."
# The purpose of this script is to verify that the information in the "Overall"
# set is redundant to the "By Category" set.

import pandas as pd
import numpy as np
import sys
from timer import Timer
from utilities import fns

original_stdout = sys.stdout

timer = Timer()
timer.tic(task = "Compare the 'By Category' and 'Overall' data sets.")

################################################################################
timer.tic(task = "Import all parquet files")

timer.tic(task = "Import OnSiteGenerationByCategory")
OnSiteGenerationByCategory = pd.read_parquet("OnSiteGenerationByCategory.parquet")
timer.toc()

timer.tic(task = "Import OnSiteGenerationOverall")
OnSiteGenerationOverall = pd.read_parquet("OnSiteGenerationOverall.parquet")
timer.toc()

timer.tic(task = "MarketsSavingsByCategory")
MarketsSavingsByCategory = pd.read_parquet("MarketsSavingsByCategory.parquet")
timer.toc()

timer.tic(task = "MarketsSavingsOverall")
MarketsSavingsOverall = pd.read_parquet("MarketsSavingsOverall.parquet")
timer.toc()

timer.toc()

################################################################################
#
timer.tic("Compare OnSiteGenerationByCategory to OnSiteGenerationOverall")

timer.tic("aggregate OnSiteGenerationByCategory")
OnSiteGenerationByCategory_agg = (
    OnSiteGenerationByCategory
        .groupby(['result_file', 'outcome_metric', 'year'])
        .agg({"value" : "sum"})
        .reset_index()
        )
timer.toc()

timer.tic("merge aggregated data with Overall data")
d = (
    OnSiteGenerationByCategory_agg
        .merge(OnSiteGenerationOverall,
               how = "outer",
               on = ['result_file', 'outcome_metric', 'year'],
               suffixes = ("_ByCat_agg", "_overall"),
               indicator = True)
    )
d["Abs_diff"] = abs(d.value_ByCat_agg - d.value_overall)
d["Abs_percent_diff"] = 100 * abs((d.value_ByCat_agg - d.value_overall) / d.value_overall)
d.sort_values(by = ['result_file', 'outcome_metric', 'year'])
timer.toc()


with open("by_category_vs_overall.log", "w") as f:
    sys.stdout = f
    print("OnSiteGenerationByCategory vs OnSiteGenerationOverall".center(80))
    print('\n\nThe following rows are only in the ByCategory set:')
    print(d.query('_merge == "left_only"').drop("_merge", axis = 1))

    print('\n\nThe following rows are only in the Overll set:')
    print(d.query('_merge == "right_only"').drop("_merge", axis = 1))

    print(('\n\nThe five number summary for the absolute value of the differences'
           'between the aggregated ByCategory values and the reported Overall'
           'values:'))
    fns(d.Abs_diff, ignorenan = True)

    print("\n\nThe five number summary for the |(ByCat_agg - Overall) / Overall| * 100")
    fns(d.Abs_percent_diff, ignorenan = True)
    sys.stdout = original_stdout

timer.toc()
################################################################################
#
timer.tic("Compare MarketsSavingsByCategory to MarketsSavingsOverall")

timer.tic("aggregate MarketsSavingsByCategory")
MarketsSavingsByCategory_agg = (
    MarketsSavingsByCategory
        .groupby(['result_file', 'ecm', 'adoption_scenario', 'outcome_metric', 'year'])
        .agg({"value" : "sum"})
        .reset_index()
        )
timer.toc()

timer.tic("merge aggregated data with Overall data")
d = (
    MarketsSavingsByCategory_agg
        .merge(MarketsSavingsOverall,
               how = "outer",
               on = ['result_file', 'ecm', 'adoption_scenario', 'outcome_metric', 'year'],
               suffixes = ("_ByCat_agg", "_overall"),
               indicator = True)
    )
d["Abs_diff"] = abs(d.value_ByCat_agg - d.value_overall)
d["Abs_percent_diff"] = 100 * abs((d.value_ByCat_agg - d.value_overall) / d.value_overall)
d.sort_values(by = ['result_file', 'outcome_metric', 'year'])
timer.toc()

with open("by_category_vs_overall.log", "a") as f:
    sys.stdout = f
    print("\n\nMarketsSavingsByCategory vs MarketsSavingsOverall".center(80))
    print('\n\nThe following rows are only in the ByCategory set:')
    print(d.query('_merge == "left_only"').drop("_merge", axis = 1))

    print('\n\nThe following rows are only in the Overll set:')
    print(d.query('_merge == "right_only"').drop("_merge", axis = 1))

    print(('\n\nThe five number summary for the absolute value of the differences'
           'between the aggregated ByCategory values and the reported Overall'
           'values:'))
    fns(d.Abs_diff, ignorenan = True)

    print("\n\nThe five number summary for the |(ByCat_agg - Overall) / Overall| * 100")
    fns(d.Abs_percent_diff, ignorenan = True)
    sys.stdout = original_stdout

timer.toc()

################################################################################
# end of the primary task
timer.toc()
################################################################################
#                                 End of File                                  #
################################################################################
