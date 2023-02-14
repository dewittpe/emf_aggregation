.PHONY : all clean

all: by_category_vs_overall.log aggs.parquet

parquets/baseline.parquet:\
	format_baseline.py\
	utilities.py\
	timer.py\
	scout_concepts.py\
	stock_energy_tech_data/mseg_res_com_emm.json
	python $<

parquets/co2_intensity_of_electricity.parquet parquets/end_use_electricity_price.parquet:\
	format_emm_region_emissions_prices.py\
	utilities.py\
	timer.py\
	convert_data/emm_region_emissions_prices.json
	python $<

parquets/site_source_co2_conversions.parquet:\
	format_site_source_co2_conversions.py\
	utilities.py\
	timer.py\
	convert_data/site_source_co2_conversions.json
	python $<

parquets/OnSiteGenerationByCategory.parquet parquets/OnSiteGenerationOverall.parquet parquets/MarketsSavingsByCategory.parquet parquets/MarketsSavingsOverall.parquet parquets/FilterVariables.parquet parquets/FinancialMetrics.parquet &: \
	format_ecm_results.py\
	utilities.py\
	scout_concepts.py\
	timer.py\
	ecm_results/ecm_results_1-1.json\
	ecm_results/ecm_results_2.json\
	ecm_results/ecm_results_3-1.json
	python $<

%.json: %.json.gz
	gunzip -dk $<

by_category_vs_overall.log:\
	by_category_vs_overall.py\
	timer.py\
	utilities.py\
	parquets/OnSiteGenerationByCategory.parquet\
	parquets/OnSiteGenerationOverall.parquet\
	parquets/MarketsSavingsOverall.parquet\
	parquets/MarketsSavingsByCategory.parquet
	python $<

aggs.parquet:\
	emf_aggregation.py\
	scout_emf_mappings.py\
	scout_concepts.py\
	timer.py\
	parquets/baseline.parquet\
	parquets/MarketsSavingsByCategory.parquet\
	parquets/CO2_intensity_of_electricity.parquet\
	parquets/site_source_co2_conversions.parquet\
	parquets/emm_region_emissions_prices.parquet
	python $<

clean:
	/bin/rm -rf __pycache__
	/bin/rm -f parquets/*.parquet
	/bin/rm -f *.log 
	/bin/rm -f convert_data/*.json
	/bin/rm -f ecm_results/*.json
	/bin/rm -f stock_energy_tech_data/*.json
