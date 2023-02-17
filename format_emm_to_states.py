import pandas as pd

emm_to_states = (
        pd.read_csv('convert_data/geo_map/EMM_State_RowSums.txt', sep = '\t')
        .rename({"State" : "EMM"}, axis = 1)
        .melt(id_vars = "EMM", var_name = "State")
        )

emm_to_states.to_parquet("parquets/emm_to_states.parquet")
