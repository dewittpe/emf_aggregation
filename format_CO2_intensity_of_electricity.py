import pandas as pd
from utilities import json_to_df
from timer import Timer

timer = Timer()
timer.tic("Process emm_region_emissions_prices.json to a columized data form")

################################################################################
timer.tic("Read in the baseline data and format as a DataFrame")
DF = json_to_df(path = 'emm_region_emissions_prices.json')
timer.toc()

################################################################################
timer.tic("Format CO2_intensity_of_electricity data")

DF = (
        DF.query('lvl0 == "CO2 intensity of electricity"')
        .query('lvl1 == "data"')
        .drop(["lvl0", "lvl1", "lvl5"], axis = 1)
        .rename({"lvl2" : "region",
                 "lvl3": "year",
                 "lvl4": "CO2_intensity_of_electricity"},
                axis = 1)
        )

DF.CO2_intensity_of_electricity = DF.CO2_intensity_of_electricity.astype(float)
DF.year = DF.year.astype("Int64")

timer.toc()

################################################################################
timer.tic("Write CO2_intensity_of_electricity.parquet")
DF.to_parquet('CO2_intensity_of_electricity.parquet')
timer.toc()

################################################################################
timer.toc()
################################################################################
#                                 End of File                                  #
################################################################################
