# dict_to_parquet.py
#
# Methods for transforming the Scout "json" files, which are more like python
# dict than json, into pandas DataFrames.

import pandas as pd
from utilities import json_to_df
from utilities import isfloat
from scout_concepts import ScoutConcepts

################################################################################
def d2p_baseline(df, scenario):
    """ Dict to Parquet: Baseline Data

    Args:
        df a DataFrame returned from a calling json_to_df
        scenario

    Return:
        A DataFrame
    """
    scout_concepts = ScoutConcepts()

    df = (
            df
            .query('lvl2.isin(@scout_concepts.fuel_types)')
            .reset_index(drop = True)
            .rename({'lvl0':'region',
                     'lvl1':'building_type',
                     'lvl2':'fuel_type',
                     'lvl3':'end_use',
                     'lvl4':'supply_demand',
                     'lvl5':'technology',
                     'lvl6':'energy_stock',
                     'lvl7':'year',
                     'lvl8':'value'},
                    axis = 1)
            )

    # if technology.isin(['energy', 'stock']) then shift values over one column and
    # set the technology to NA
    idx = df.technology.isin(scout_concepts.energy_stock)
    df.loc[idx]
    df.loc[idx, 'value'] = df.loc[idx, 'year']
    df.loc[idx, 'year']  = df.loc[idx, 'energy_stock']
    df.loc[idx, 'energy_stock']  = df.loc[idx, 'technology']
    df.loc[idx, 'technology']  = pd.NA

    # do a similar thing for energy, stock values in the supply_demand colum
    idx = df.supply_demand.isin(scout_concepts.energy_stock)
    df.loc[idx]
    df.loc[idx, 'value'] = df.loc[idx, 'energy_stock']
    df.loc[idx, 'year']  = df.loc[idx, 'technology']
    df.loc[idx, 'energy_stock']  = df.loc[idx, 'supply_demand']
    df.loc[idx, 'technology']  = pd.NA
    df.loc[idx, 'supply_demand']  = pd.NA

    # if technology is na and supply_demand is not in ['supply', 'demand'] then flip
    # the values in the columns
    df.query("supply_demand.notna() & not supply_demand.isin(@scout_concepts.supply_demand)")
    idx = df.query("supply_demand.notna() & not supply_demand.isin(@scout_concepts.supply_demand) & technology.isna()").index
    df.loc[idx]
    df.loc[idx, "technology"] = df.loc[idx, "supply_demand"]
    df.loc[idx, "supply_demand"] = pd.NA

    # There are "NA" values in the year column
    df.loc[df.query('year == "NA"').index, "year"] = pd.NA

    # Check column contents
    if not df.region.isin(scout_concepts.regions).all():
        raise Exception("df.region contains more than just expected region values")

    if not df.building_type.isin(scout_concepts.building_types).all():
        raise Exception("df.building_type contains more than just expected building_type values")

    if not df.fuel_type.isin(scout_concepts.fuel_types).all():
        raise Exception("df.fuel_type contains more than just expected fuel_type values")

    if not df.end_use.isin(scout_concepts.end_uses).all():
        print(set(df.end_use))
        raise Exception("df.end_use contains more than just expected end_use values")

    if not df.supply_demand.isin(scout_concepts.supply_demand + [pd.NA]).all():
        raise Exception("df.supply_demand contains more than just expected supply_demand values")

    if not df.technology.isin(scout_concepts.technologies + [pd.NA]).all():
        print(list(set(df.query("~technology.isin(@scout_concepts.technologies)").technology)))
        raise Exception("df.technology contains more than just expected technology values")

    if not df.energy_stock.isin(scout_concepts.energy_stock + [pd.NA]).all():
        raise Exception("df.energy_stock contains more than just expected energy_stock values")

    if not df.year.isin(scout_concepts.years + [pd.NA]).all():
        print(list(set(df.query("~year.isin(@scout_concepts.years)").year)))
        raise Exception("df.year contains more than just expected year values")

    if not df.value.apply(isfloat).all():
        if not df[~df.value.isna()].value.apply(isfloat).all():
            raise Exception("df.value contains more than just expected floating point values")

    # The year and the value columns can/should have the dtype changed to integer
    # and float respectively.  There is a string value "NA" in the year column that
    # needs to be modified first
    df.year = df.year.astype("Int64")
    df.value = pd.to_numeric(df.value)

    # Add on the EMF Scenario
    df["Scenario"] = scenario

    return df

################################################################################
def d2p_floor_area(df, scenario):
    scout_concepts = ScoutConcepts()
    floor_area = (
            df
            .query('~lvl2.isin(@scout_concepts.fuel_types)')
            .reset_index(drop = True)
            .drop(["lvl5", "lvl6", "lvl7", "lvl8"], axis = 1)
            .rename({'lvl0':'region',
                     'lvl1':'building_type',
                     'lvl2':'metric',
                     'lvl3':'year',
                     'lvl4':'value'},
                    axis = 1)
            )
    floor_area.year = floor_area.year.astype("Int64")
    floor_area.value = pd.to_numeric(floor_area.value)
    floor_area["Scenario"] = scenario
    return floor_area

################################################################################
#                                 END OF FILE                                  #
################################################################################

