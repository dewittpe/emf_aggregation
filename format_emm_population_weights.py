import pandas as pd
import numpy as np

emm_populaiton_weights = (
        pd.read_csv('convert_data/geo_map/EMM_National.txt', sep = '\t')
        )

# check
assert np.isclose(
        emm_populaiton_weights["Population Weight"].to_numpy(),
        np.array(emm_populaiton_weights["Total Population"] / emm_populaiton_weights["Total Population"].sum()),
        rtol = 1e-3).all()

emm_populaiton_weights.to_parquet("parquets/emm_populaiton_weights.parquet")
