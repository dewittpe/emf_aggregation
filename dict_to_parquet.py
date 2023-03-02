# dict_to_parquet.py
#
# Methods for transforming the Scout "json" files, which are more like python
# dict than json, into pandas DataFrames.

import pandas as pd
from utilities import dict_to_df
from utilities import isfloat
from utilities import remove_empty_colums
from scout_concepts import ScoutConcepts

################################################################################
def d2p_baseline(df, scenario):
    """ Dict to Parquet: Baseline Data

    Args:
        df a DataFrame returned from a calling dict_to_df
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
def d2p_emm_to_states():
    emm_to_states = (
            pd.read_csv('convert_data/geo_map/EMM_State_RowSums.txt', sep = '\t')
            .rename({"State" : "EMM"}, axis = 1)
            .melt(id_vars = "EMM",
                  var_name = "State",
                  value_name = 'emm_to_state_factor')
            )

    emm_to_states.to_parquet("parquets/emm_to_states.parquet")
    return emm_to_states

################################################################################
def d2p_emm_population_weights():
    emm_population_weights = (
            pd.read_csv('convert_data/geo_map/EMM_National.txt', sep = '\t')
            .rename(columns = {"Total Population" : "population",
                               "Population Weight" : "weight"})
            )

    emm_population_weights.to_parquet("parquets/emm_population_weights.parquet")
    return emm_population_weights

################################################################################
def d2p_emm_region_emission_prices():
    df = dict_to_df(path = 'convert_data/emm_region_emissions_prices.json.gz')
    df = (
            df
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
    idx = df.query('~building_class.isin(["residential", "commercial"])').index
    df.loc[idx, "conversion_factor"] = df.loc[idx, "year"]
    df.loc[idx, "year"] = df.loc[idx, "region"]
    df.loc[idx, "region"] = df.loc[idx, "building_class"]
    df.loc[idx, "building_class"] = pd.NA

    # set the building_class to title case to match the general concept else where
    df.building_class = df.building_class.str.title()

    df.conversion_factor = df.conversion_factor.astype(float)
    df.year = df.year.astype("Int64")

    # Some new columns:
    df["Variable"] = pd.NA
    df["Unit"] = pd.NA

    # For End-use electricity prices
    kWh_GJ_conv = 1e6/3600  # convert from kWh to GJ
    USD_pres_val_conv = 1/1.023  # convert from 2019 to 2018 dollars (Scout data are in US$2019 not US$2018)

    idx = df.query("conversion == 'End-use electricity price'").index
    df.loc[idx, "conversion_factor"] *= kWh_GJ_conv * USD_pres_val_conv
    df.loc[idx, "Variable"] = "Price|Final Energy|" + df.loc[idx, "building_class"] + "|Electricity"
    df.loc[idx, "Unit"] = "US$2018/GJ"

    # write out parquet
    df.to_parquet('parquets/emm_region_emissions_prices.parquet')

    # return the df
    return df

################################################################################
def d2p_site_source_co2_conversion():
    df = dict_to_df(path = 'convert_data/site_source_co2_conversions.json.gz')

    df = (
            df
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
    idx = df.query('~building_class.isin(["residential", "commercial"])').index
    df.loc[idx, "conversion_factor"] = df.loc[idx, "year"]
    df.loc[idx, "year"] = df.loc[idx, "building_class"]
    df.loc[idx, "building_class"] = pd.NA

    # set the building_class to title case to match the general concept else where
    df.building_class = df.building_class.str.title()

    df.conversion_factor = df.conversion_factor.astype(float)
    df.year = df.year.astype("Int64")

    df.to_parquet('parquets/site_source_co2_conversions.parquet')

    return df

################################################################################
def d2p_ecm_results(df):
    scout_concepts = ScoutConcepts()

    # Split up the df into several conceptually different data sets
    queries = {
            'OnSiteGenerationByCategory': 'lvl0 == "On-site Generation" & lvl2 == "By Category"',
            'OnSiteGenerationOverall': 'lvl0 == "On-site Generation" & lvl2 == "Overall"',
            'FinancialMetrics': 'lvl1 == "Financial Metrics"',
            'FilterVariables' : 'lvl1 == "Filter Variables"',
            'MarketsSavingsByCategory': 'lvl1 == "Markets and Savings (by Category)"',
            'MarketsSavingsOverall': 'lvl1 == "Markets and Savings (Overall)"',
            }

    OnSiteGenerationByCategory = (
            df
            .query(queries["OnSiteGenerationByCategory"])
            .copy()
            .reset_index(drop = True)
            )

    OnSiteGenerationOverall = (
            df
            .query(queries["OnSiteGenerationOverall"])
            .copy()
            .reset_index(drop = True)
            )

    FinancialMetrics = (
            df
            .query(queries["FinancialMetrics"])
            .copy()
            .reset_index(drop = True)
            )

    MarketsSavingsByCategory = (
            df
            .query(queries["MarketsSavingsByCategory"])
            .copy()
            .reset_index(drop = True)
            )

    MarketsSavingsOverall = (
            df
            .query(queries["MarketsSavingsOverall"])
            .copy()
            .reset_index(drop = True)
            )

    FilterVariables = (
            df
            .query(queries["FilterVariables"])
            .copy()
            .reset_index(drop = True)
            )

    # remove columns that have no data
    OnSiteGenerationByCategory = remove_empty_colums(OnSiteGenerationByCategory)
    OnSiteGenerationOverall    = remove_empty_colums(OnSiteGenerationOverall)
    FinancialMetrics           = remove_empty_colums(FinancialMetrics)
    MarketsSavingsByCategory   = remove_empty_colums(MarketsSavingsByCategory)
    MarketsSavingsOverall      = remove_empty_colums(MarketsSavingsOverall)
    FilterVariables            = remove_empty_colums(FilterVariables)


    ########################################
    # Clean On-site Generation By Category
    OnSiteGenerationByCategory = (
            OnSiteGenerationByCategory
            .drop(columns = ["lvl0", "lvl2"])
            .rename(columns = {"lvl1":"outcome_metric",
                               "lvl3":"region",
                               "lvl4":"building_type",
                               "lvl5":"year",
                               "lvl6":"value"})
            )
    OnSiteGenerationByCategory.year = OnSiteGenerationByCategory.year.astype("Int64")
    OnSiteGenerationByCategory.value = OnSiteGenerationByCategory.value.astype(float)

    ########################################
    # On-site Generation Overall
    OnSiteGenerationOverall = (
            OnSiteGenerationOverall
            .drop(columns = ["lvl0", "lvl2"])
            .rename(columns = {"lvl1":"outcome_metric",
                               "lvl3":"year",
                               "lvl4":"value"})
            )
    OnSiteGenerationOverall.year = OnSiteGenerationOverall.year.astype("Int64")
    OnSiteGenerationOverall.value = OnSiteGenerationOverall.value.astype(float)


    ########################################
    # FinancialMetrics
    FinancialMetrics = (
            FinancialMetrics
            .drop(columns = ["lvl1"])
            .rename(columns = {"lvl0":"ecm",
                               "lvl2":"outcome_metric",
                               "lvl3":"year",
                               "lvl4":"value"})
            )
    FinancialMetrics.year = FinancialMetrics.year.astype("Int64")
    FinancialMetrics.value = FinancialMetrics.value.astype(float)

    ########################################
    # MarketsSavingsByCategory
    MarketsSavingsByCategory = (
            MarketsSavingsByCategory
            .drop(columns = ["lvl1"])
            .rename(columns = {"lvl0" : "ecm",
                               "lvl2" : "adoption_scenario",
                               "lvl3" : "outcome_metric",
                               "lvl4" : "region",
                               "lvl5" : "building_type",
                               "lvl6" : "end_use"
                               })
            )


    # there are some rows with no fuel_type, lvl7 has year values.  move the year
    # and values
    MarketsSavingsByCategory.query('lvl7.isin(@scout_concepts.years)')
    idx = MarketsSavingsByCategory.query('lvl7.isin(@scout_concepts.years)').index
    MarketsSavingsByCategory.loc[idx, "lvl9"] = MarketsSavingsByCategory.loc[idx, "lvl8"]
    MarketsSavingsByCategory.loc[idx, "lvl8"] = MarketsSavingsByCategory.loc[idx, "lvl7"]
    MarketsSavingsByCategory.loc[idx, "lvl7"] = pd.NA

    MarketsSavingsByCategory = (
            MarketsSavingsByCategory
            .rename(columns = {"lvl7":"fuel_type",
                               "lvl8":"year",
                               "lvl9":"value"})
            )

    MarketsSavingsByCategory.year = MarketsSavingsByCategory.year.astype("Int64")
    MarketsSavingsByCategory.value = MarketsSavingsByCategory.value.astype(float)


    ########################################
    # MarketsSavingsOverall
    MarketsSavingsOverall = (
            MarketsSavingsOverall
            .drop(columns = ["lvl1"])
            .rename(columns = {"lvl0":"ecm",
                               "lvl2":"adoption_scenario",
                               "lvl3":"outcome_metric",
                               "lvl4":"year",
                               "lvl5":"value"
                               })
            )
    MarketsSavingsOverall.year = MarketsSavingsOverall.year.astype("Int64")
    MarketsSavingsOverall.value = MarketsSavingsOverall.value.astype(float)

    ########################################
    # FilterVariables
    FilterVariables = (
            FilterVariables
            .drop(columns = ["lvl1"])
            .rename(columns = {"lvl0" : "ecm",
                               "lvl2" : "concept",
                               "lvl3" : "values"})
            )

    ########################################
    # Writting parquet files
    OnSiteGenerationByCategory.to_parquet('parquets/OnSiteGenerationByCategory.parquet')
    OnSiteGenerationOverall.to_parquet('parquets/OnSiteGenerationOverall.parquet')
    MarketsSavingsByCategory.to_parquet('parquets/MarketsSavingsByCategory.parquet')
    MarketsSavingsOverall.to_parquet('parquets/MarketsSavingsOverall.parquet')
    FilterVariables.to_parquet('parquets/FilterVariables.parquet')
    FinancialMetrics.to_parquet('parquets/FinancialMetrics.parquet')

    return [
               OnSiteGenerationByCategory,
               OnSiteGenerationOverall,
               MarketsSavingsByCategory,
               MarketsSavingsOverall,
               FilterVariables,
               FinancialMetrics
            ]

################################################################################
# emm_to_states


################################################################################
#                                 End of File                                  #
################################################################################

