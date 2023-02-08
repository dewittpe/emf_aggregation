
.PHONY : all clean

all : baseline.parquet

%.parquet : format_%.py utilities.py
	python $<
