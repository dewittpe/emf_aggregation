import pandas as pd
from timer import Timer

timer = Timer()
timer.tic("EMF Aggregation")

################################################################################
MMBtu_to_EJ           = 1.05505585262e-9
EJ_to_quad            = 0.9478
pound_to_mt           = 0.000453592
EJ_to_twh             = 277.778
EJ_to_mt_co2_propane  = EJ_to_quad * 62.88
EJ_to_mt_co2_kerosene = EJ_to_quad * 73.38
EJ_to_mt_co2_gas      = EJ_to_quad * 53.056
EJ_to_mt_co2_oil      = EJ_to_quad * 74.14
EJ_to_mt_co2_bio      = EJ_to_quad * 96.88

################################################################################
timer.tic("Import parquets")
baseline = pd.read_parquet("baseline.parquet")
MarketsSavingsByCategory = pd.read_parquet("MarketsSavingsByCategory.parquet")
timer.toc()

################################################################################
timer.tic("Subset to needed outcome_metrics and set emf_base_string")

emf_base_strings = pd.DataFrame(
    [
        {
            "outcome_metric" : "Avoided CO\u2082 Emissions (MMTons)",
            "emf_base_string": "*Emissions|CO2|Energy|Demand|Buildings"
        },
        {
            "outcome_metric" : "Energy Savings (MMBtu)",
            "emf_base_string": "*Final Energy|Buildings"
        }
    ]
    )

MarketsSavingsByCategory = (
        MarketsSavingsByCategory
        .merge(emf_base_strings, how = "inner")
        )

timer.toc()

################################################################################
timer.tic("Convert MMBtu to Exajoules")
idx = MarketsSavingsByCategory.query('outcome_metric.str.contains("MMBtu")').index

MarketsSavingsByCategory.loc[idx, "value"] *= MMBtu_to_EJ
MarketsSavingsByCategory.outcome_metric = MarketsSavingsByCategory.outcome_metric.str.replace("MMBtu", "EJ")

timer.toc()

################################################################################

MarketsSavingsByCategory

################################################################################
timer.toc()
################################################################################
#                                 End of File                                  #
################################################################################

