import pandas as pd
from utilities import json_to_df
from utilities import isfloat
from utilities import whats_in_a_series
from utilities import REGIONS
from utilities import BUILDING_TYPES
from utilities import FUEL_TYPES
from utilities import END_USES
from utilities import SUPPLY_DEMAND
from utilities import ENERGY_STOCK
from utilities import TECHNOLOGIES
from utilities import YEARS
from utilities import DEFINED_VALUES
from timer import Timer

timer = Timer()
timer.tic("Process mseg_res_com_emm.json to a columized data form")

timer.tic("Read in the baseline data and format as a DataFrame")
DF = json_to_df(path = 'mseg_res_com_emm.json')
timer.toc()

timer.tic(task = "Explore and clean the data set")
# explore the columns and get good names
DF.query("lvl8.notna()").lvl0.value_counts() # likely region
DF.query("lvl8.notna()").lvl1.value_counts() # likely building_type
DF.query("lvl8.notna()").lvl2.value_counts() # likely fuel_type
DF.query("lvl8.notna()").lvl3.value_counts() # likely end_use
DF.query("lvl8.notna()").lvl4.value_counts() # likely supply/demand
DF.query("lvl8.notna()").lvl5.value_counts() # likely technology
DF.query("lvl8.notna()").lvl6.value_counts() # likely energy/stock
DF.query("lvl8.notna()").lvl7.value_counts() # likely year
DF.query("lvl8.notna()").lvl8.value_counts() # likely value

# rename columns that can be renamed
if DF.lvl0.isin(REGIONS).all():
    DF = DF.rename({"lvl0": "region"}, axis = 1)

if DF.lvl1.isin(BUILDING_TYPES).all():
    DF = DF.rename({"lvl1": "building_type"}, axis = 1)

# lvl2 has more than just the expected fuel_type
DF.lvl2.isin(FUEL_TYPES).any()

# looks like meta data about residential buidings
# split this off to another DataFrame
DF[ ~DF.lvl2.isin(FUEL_TYPES) ].lvl2.value_counts()

DF_metadata = DF[ ~DF.lvl2.isin(FUEL_TYPES) ].reset_index(drop = True).copy()

# set DF to be only the rows with a fuel type in position 2
DF = DF[ DF.lvl2.isin(FUEL_TYPES) ].reset_index(drop = True)

if DF.lvl2.isin(FUEL_TYPES).all():
    DF = DF.rename({"lvl2": "fuel_type"}, axis = 1)

# what is in lvl3?
if DF.lvl3.isin(END_USES).all():
    DF = DF.rename({"lvl3": "end_use"}, axis = 1)

# what is in lvl4?
whats_in_a_series(DF.lvl4).query('any_obs')
# building_type, end_use, supply_demand, energy_stock, defined and undefined
# values.

# So now we need to shift data from one column to another.
DF.query('lvl8.isna()')

# It seems reasonable to assume that lvl8 should be the value column and lvl7
# the year.  Based on the other observations it seems reasonable to rename
# columns lvl4 through lvl8 as:
DF = DF.rename(columns = {
    "lvl4": "supply_demand",
    "lvl5": "technology",
    "lvl6": "energy_stock",
    "lvl7": "year",
    "lvl8": "value"
    })

# if technology.isin(['energy', 'stock']) then shift values over one column and
# set the technology to NA
idx = DF.technology.isin(ENERGY_STOCK)
DF.loc[idx]
DF.loc[idx, 'value'] = DF.loc[idx, 'year']
DF.loc[idx, 'year']  = DF.loc[idx, 'energy_stock']
DF.loc[idx, 'energy_stock']  = DF.loc[idx, 'technology']
DF.loc[idx, 'technology']  = pd.NA

# do a similar thing for energy, stock values in the supply_demand colum
idx = DF.supply_demand.isin(ENERGY_STOCK)
DF.loc[idx]
DF.loc[idx, 'value'] = DF.loc[idx, 'energy_stock']
DF.loc[idx, 'year']  = DF.loc[idx, 'technology']
DF.loc[idx, 'energy_stock']  = DF.loc[idx, 'supply_demand']
DF.loc[idx, 'technology']  = pd.NA
DF.loc[idx, 'supply_demand']  = pd.NA

# if technology is na and supply_demand is not in ['supply', 'demand'] then flip
# the values in the columns
DF.query("supply_demand.notna() & not supply_demand.isin(@SUPPLY_DEMAND)")
idx = DF.query("supply_demand.notna() & not supply_demand.isin(@SUPPLY_DEMAND) & technology.isna()").index
DF.loc[idx]
DF.loc[idx, "technology"] = DF.loc[idx, "supply_demand"]
DF.loc[idx, "supply_demand"] = pd.NA

# There are "NA" values in the year column
DF.loc[DF.query('year == "NA"').index, "year"] = pd.NA

timer.toc()

timer.tic(task = "Check the column contents")
if not DF.region.isin(REGIONS).all():
    raise Exception("DF.region contains more than just expected region values")

if not DF.building_type.isin(BUILDING_TYPES).all():
    raise Exception("DF.building_type contains more than just expected building_type values")

if not DF.fuel_type.isin(FUEL_TYPES).all():
    raise Exception("DF.fuel_type contains more than just expected fuel_type values")

if not DF.end_use.isin(END_USES).all():
    print(set(DF.end_use))
    raise Exception("DF.end_use contains more than just expected end_use values")

if not DF.supply_demand.isin(SUPPLY_DEMAND + [pd.NA]).all():
    raise Exception("DF.supply_demand contains more than just expected supply_demand values")

if not DF.technology.isin(TECHNOLOGIES + [pd.NA]).all():
    print(list(set(DF.query("~technology.isin(@TECHNOLOGIES)").technology)))
    raise Exception("DF.technology contains more than just expected technology values")

if not DF.energy_stock.isin(ENERGY_STOCK + [pd.NA]).all():
    raise Exception("DF.energy_stock contains more than just expected energy_stock values")

if not DF.year.isin(YEARS + [pd.NA]).all():
    print(list(set(DF.query("~year.isin(@YEARS)").year)))
    raise Exception("DF.year contains more than just expected year values")

if not DF.value.apply(isfloat).all():
    if not DF[~DF.value.isna()].value.apply(isfloat).all():
        raise Exception("DF.value contains more than just expected floating point values")

timer.toc()

# The year and the value columns can/should have the dtype changed to integer
# and float respectively.  There is a string value "NA" in the year column that
# needs to be modified first
DF.year = DF.year.astype("Int64")
DF.value = pd.to_numeric(DF.value)

timer.tic("Write baseline.parquet")
DF.to_parquet('baseline.parquet')
timer.toc()
timer.toc()

