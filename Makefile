# 
# we use stage/*.loaded targets to trigger reloading data into the database
# when a dataset needs to be refreshed.
# TODO: this doesn't work if a source table got dropped somehow and needs to be reloaded.

SHELL := /bin/bash

modules = covid19stats/common.py


extract:

	@if ! [ -d ../COVID-19 ]; then echo "COVID-19 directory doesn't exist! clone that repo first"; exit 1; fi

	cd ../COVID-19 && git pull

	python3 -m covid19stats.covidtracking_download

	python3 -m covid19stats.cdc_deaths_download

	python3 -m covid19stats.cdc_states_download

	python3 -m covid19stats.cdc_surveillance_cases_download	

	python3 -m covid19stats.reference_data_download

load: \
	stage/csse.loaded stage/reference_data.loaded stage/covidtracking.loaded stage/cdc_deaths.loaded stage/cdc_states.loaded stage/cdc_surveillance_cases.loaded
	echo "Loaded"

stage/csse.loaded: \
	$(modules) covid19stats/csse_load.py \
	../COVID-19/csse_covid_19_data/csse_covid_19_daily_reports/*.csv

	python3 -m covid19stats.csse_load

stage/cdc_deaths.loaded: \
	$(modules) covid19stats/cdc_deaths_load.py \
	stage/cdc_deaths_2019_2020.txt

	python3 -m covid19stats.cdc_deaths_load

stage/cdc_states.loaded: \
	$(modules) covid19stats/cdc_states_load.py \
	stage/cdc_states.tsv

	python3 -m covid19stats.cdc_states_load

stage/cdc_surveillance_cases.loaded: \
	$(modules) covid19stats/cdc_surveillance_cases_load.py \
	stage/cdc_surveillance_cases.tsv

	python3 -m covid19stats.cdc_surveillance_cases_load

stage/reference_data.loaded: \
	$(modules) covid19stats/reference_data_load.py \
	reference/*

	python3 -m covid19stats.reference_data_load

stage/covidtracking.loaded: \
	$(modules) covid19stats/covidtracking_load.py \
	stage/covidtracking_states.csv

	python3 -m covid19stats.covidtracking_load

transform:

	python3 -m covid19stats.transform_updated

stage/last_exported: \
	stage/transforms.updated

	python3 -m covid19stats.exports

export: \
	$(modules) covid19stats/exports.py \
	stage/last_exported

	echo "Exported"

clean:
	rm -rf stage/*.loaded
