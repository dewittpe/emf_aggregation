import pandas as pd
from utilities import json_to_df
from timer import Timer

timer = Timer()
timer.tic("Process site_source_co2_conversions.json to a columized data form")

################################################################################
timer.tic("Read in the baseline data and format as a DataFrame")
DF = json_to_df(path = 'convert_data/site_source_co2_conversions.json')
timer.toc()

################################################################################
timer.tic("Format site_source_co2_conversions data")
DF = (
        DF
        .query('lvl2 == "data"')
        .drop(["lvl2"], axis = 1)
        .rename({"lvl0" : "fuel_type",
                 "lvl1" : "conversion",
                 "lvl3" : "building_class",
                 "lvl4": "year",
                 "lvl5": "conversion_factor"},
                axis = 1)
    )

# There are rows where the building class column contains year data,
# shift the year and conversion_factor data over one column to the right
idx = DF.query('~building_class.isin(["residential", "commercial"])').index
DF.loc[idx, "conversion_factor"] = DF.loc[idx, "year"]
DF.loc[idx, "year"] = DF.loc[idx, "building_class"]
DF.loc[idx, "building_class"] = pd.NA

# set the building_class to title case to match the general concept else where
DF.building_class = DF.building_class.str.title()

DF.conversion_factor = DF.conversion_factor.astype(float)
DF.year = DF.year.astype("Int64")

timer.toc()

################################################################################
timer.tic("Write site_source_co2_conversions.parquet")
DF.to_parquet('parquets/site_source_co2_conversions.parquet')
timer.toc()

################################################################################
timer.toc()
################################################################################
#                                 End of File                                  #
################################################################################
