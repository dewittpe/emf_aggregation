PARQUETS  = baseline.parquet
PARQUETS += OnSiteGenerationByCategory.parquet
PARQUETS += OnSiteGenerationOverall.parquet
PARQUETS += MarketsSavingsByCategory.parquet
PARQUETS += MarketsSavingsOverall.parquet
PARQUETS += FilterVariables.parquet
PARQUETS += FinancialMetrics.parquet

.PHONY : all clean

all : $(PARQUETS)

baseline.parquet : format_baseline.py utilities.py mseg_res_com_emm.json
	python $<

OnSiteGenerationByCategory.parquet OnSiteGenerationOverall.parquet MarketsSavingsByCategory.parquet MarketsSavingsOverall.parquet FilterVariables.parquet FinancialMetrics.parquet &: format_ecm_results.py utilities.py ecm_results_1-1.json ecm_results_2.json ecm_results_3-1.json
	python $<

%.json : %.json.gz
	gunzip -dk $<
