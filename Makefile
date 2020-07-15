# 
# use empty .loaded files to track the last time portions of the database were updated
# since make can't peek inside the database for dependencies.

modules = covid19stats/common.py

.PHONY: depend

all: \
	depend \
	$(modules) covid19stats/exports.py \
	stage/csse.loaded stage/covidtracking.loaded stage/reference_data.loaded stage/dimensional_models.loaded

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
	$(modules) covid19stats/reference_data.py \
	reference/*

	python3 -m covid19stats.reference_data

stage/covidtracking.loaded: input/covidtracking_states.csv

	python3 -m covid19stats.covidtracking

# in the context of this build, 'depend' is for downloading/updating input data
depend:

	@if ! [ -d ~/COVID-19 ]; then echo "COVID-19 directory doesn't exist! clone that repo first"; exit 1; fi

	cd ~/COVID-19 && git pull

	python3 -m covid19stats.covidtracking_download

	python3 -m covid19stats.reference_data_download

clean:
	rm -rf stage/*.loaded
