import pandas as pd

emm_populaiton_weights = (
        pd.read_csv('convert_data/geo_map/EMM_National.txt', sep = '\t')
        )

emm_populaiton_weights.to_parquet("parquets/emm_populaiton_weights.parquet")
