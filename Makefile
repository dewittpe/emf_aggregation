
.PHONY : all clean

all : baseline.parquet

%.parquet : format_%.py utilities.py mseg_res_com_emm.json
	python $<

mseg_res_com_emm.json : mseg_res_com_emm.json.gz
	gunzip -dk $<
