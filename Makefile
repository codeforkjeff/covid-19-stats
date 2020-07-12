# less than ideal makefile

modules = covid19stats/common.py

all: \
	$(modules) covid19stats/exports.py \
	stage/csse.loaded stage/reference_data.loaded stage/dimensional_models.loaded
	python3 -m covid19stats.exports

stage/dimensional_models.loaded: \
	$(modules) covid19stats/dimensional_tables.py \
	stage/csse.loaded stage/reference_data.loaded
	python3 -m covid19stats.dimensional_tables

stage/csse.loaded: \
	$(modules) covid19stats/csse.py \
	../COVID-19/csse_covid_19_data/csse_covid_19_daily_reports/*.csv
	python3 -m covid19stats.csse

stage/reference_data.loaded: \
	$(modules) covid19stats/reference_data.py
	python3 -m covid19stats.reference_data

clean:
	rm -rf stage/*.loaded
