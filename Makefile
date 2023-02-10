.PHONY : all clean

all: by_category_vs_overall.log emf_aggregation.log

baseline.parquet: format_baseline.py utilities.py Scout_Concepts.py mseg_res_com_emm.json
	python $<

CO2_intensity_of_electricity.parquet: format_CO2_intensity_of_electricity.py utilities.py Scout_Concepts.py emm_region_emissions_prices.json
	python $<

site_source_co2_conversions.parquet: format_site_source_co2_conversions.py utilities.py timer.py site_source_co2_conversions.json
	python $<

OnSiteGenerationByCategory.parquet OnSiteGenerationOverall.parquet MarketsSavingsByCategory.parquet MarketsSavingsOverall.parquet FilterVariables.parquet FinancialMetrics.parquet &: format_ecm_results.py utilities.py Scout_Concepts.py ecm_results_1-1.json ecm_results_2.json ecm_results_3-1.json
	python $<

%.json: %.json.gz
	gunzip -dk $<

by_category_vs_overall.log: by_category_vs_overall.py OnSiteGenerationByCategory.parquet OnSiteGenerationOverall.parquet MarketsSavingsOverall.parquet MarketsSavingsByCategory.parquet
	python $<

emf_aggregation.log: emf_aggregation.py baseline.parquet MarketsSavingsByCategory.parquet CO2_intensity_of_electricity.parquet site_source_co2_conversions.parquet
	python $<

clean:
	/bin/rm -f $(PARQUETS) *.log *.json
