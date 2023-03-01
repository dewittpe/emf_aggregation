import pandas as pd
import math

emm_to_states = (
        pd.read_csv('convert_data/geo_map/EMM_State_RowSums.txt', sep = '\t')
        .rename({"State" : "EMM"}, axis = 1)
        .melt(id_vars = "EMM",
              var_name = "State",
              value_name = 'emm_to_state_factor')
        )

# Verify that the sum of the emm_to_state_factor colum is 1 for each EMM
assert (
        emm_to_states
        .groupby(["EMM"])
        .agg({"emm_to_state_factor":"sum"})
        .emm_to_state_factor
        .apply(lambda x : math.isclose(a = x, b = 1.0, rel_tol = 1e-8))
        .all()
         )

emm_to_states.to_parquet("parquets/emm_to_states.parquet")
