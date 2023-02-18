


parquets_dir:
	mkdir -p parquets

%.json: %.json.gz
	gunzip -dkf $<

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
	parquets/co2_intensity_of_electricity.parquet\
	parquets/site_source_co2_conversions.parquet\
	parquets/end_use_electricity_price.parquet\
	parquets/floor_area.parquet\
	parquets/emm_to_states.parquet\
	parquets/emm_populaiton_weights.parquet
	python $<

clean:
	/bin/rm -rf __pycache__
	/bin/rm -f parquets/*.parquet
	/bin/rm -f *.log
	/bin/rm -f convert_data/*.json
	/bin/rm -f ecm_results/*.json
	/bin/rm -f stock_energy_tech_data/*.json
