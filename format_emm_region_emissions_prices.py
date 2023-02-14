import pandas as pd
from utilities import json_to_df
from timer import Timer

timer = Timer()
timer.tic("Process emm_region_emissions_prices.json to a columized data form")

################################################################################
with Timer("Read in the provided json file and format as a DataFrame"):
    DF = json_to_df(path = 'convert_data/emm_region_emissions_prices.json')

################################################################################
with Timer("Format data"):
    DF = (
            DF
            .query('lvl1 == "data"')
            .drop(["lvl1"], axis = 1)
            .rename({"lvl0" : "conversion",
                     "lvl2" : "building_class",
                     "lvl3" : "region",
                     "lvl4": "year",
                     "lvl5": "conversion_factor"},
                    axis = 1)
        )

    # There are rows where the building class column contains region data,
    # shift the year and conversion_factor data over one column to the right
    idx = DF.query('~building_class.isin(["residential", "commercial"])').index
    DF.loc[idx, "conversion_factor"] = DF.loc[idx, "year"]
    DF.loc[idx, "year"] = DF.loc[idx, "region"]
    DF.loc[idx, "region"] = DF.loc[idx, "building_class"]
    DF.loc[idx, "building_class"] = pd.NA

    # set the building_class to title case to match the general concept else where
    DF.building_class = DF.building_class.str.title()

    DF.conversion_factor = DF.conversion_factor.astype(float)
    DF.year = DF.year.astype("Int64")

    # Some new columns:
    DF["Variable"] = pd.NA
    DF["Unit"] = pd.NA

    # For End-use electricity prices
    kWh_GJ_conv = 1e6/3600  # convert from kWh to GJ
    USD_pres_val_conv = 1/1.023  # convert from 2019 to 2018 dollars (Scout data are in US$2019 not US$2018)

    idx = DF.query("conversion == 'End-use electricity price'").index
    DF.loc[idx, "conversion_factor"] *= kWh_GJ_conv * USD_pres_val_conv
    DF.loc[idx, "Variable"] = "Price|Final Energy|" + DF.loc[idx, "building_class"] + "|Electricity"
    DF.loc[idx, "Unit"] = "US$2018/GJ"

################################################################################
with Timer("Write parquet files"):
    (
        DF
        .query("conversion == 'End-use electricity price'")
        .to_parquet('parquets/end_use_electricity_price.parquet')
    )
    (
        DF
        .query("conversion == 'CO2 intensity of electricity'")
        .to_parquet('parquets/co2_intensity_of_electricity.parquet')
    )

################################################################################
timer.toc()
################################################################################
#                                 End of File                                  #
################################################################################
