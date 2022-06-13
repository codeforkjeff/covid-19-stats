# 
# we use stage/*.loaded targets to trigger reloading data into the database
# when a dataset needs to be refreshed.
# TODO: this doesn't work if a source table got dropped somehow and needs to be reloaded.

SHELL := /bin/bash

modules = covid19stats/common.py


extract:

	mkdir -p data/stage
	mkdir -p data/reference

	@if ! [ -d data/COVID-19 ]; then echo "COVID-19 directory doesn't exist! clone that repo first"; exit 1; fi

	cd data/COVID-19 && git pull

	python3 -m covid19stats.covidtracking_download

	python3 -m covid19stats.cdc_deaths_download

	python3 -m covid19stats.dhhs_testing_download

	python3 -m covid19stats.cdc_states_download

	python3 -m covid19stats.cdc_surveillance_cases_download	

	python3 -m covid19stats.reference_data_download

load: \
	data/stage/csse.loaded data/stage/reference_data.loaded data/stage/covidtracking.loaded data/stage/cdc_deaths.loaded data/stage/cdc_states.loaded data/stage/dhhs_testing.loaded data/stage/cdc_surveillance_cases.loaded
	echo "Loaded"

data/stage/csse.loaded: \
	$(modules) covid19stats/csse_load.py \
	data/COVID-19/csse_covid_19_data/csse_covid_19_daily_reports/*.csv

	python3 -m covid19stats.csse_load

data/stage/cdc_deaths.loaded: \
	$(modules) covid19stats/cdc_deaths_load.py \
	data/stage/cdc_deaths_2020_2021.txt

	python3 -m covid19stats.cdc_deaths_load

data/stage/dhhs_testing.loaded: \
	$(modules) covid19stats/dhhs_testing_load.py \
	data/stage/dhhs_testing.tsv

	python3 -m covid19stats.dhhs_testing_load

data/stage/cdc_states.loaded: \
	$(modules) covid19stats/cdc_states_load.py \
	data/stage/cdc_states.tsv

	python3 -m covid19stats.cdc_states_load

data/stage/cdc_surveillance_cases.loaded: \
	$(modules) covid19stats/cdc_surveillance_cases_load.py \
	data/stage/cdc_surveillance_cases.tsv

	python3 -m covid19stats.cdc_surveillance_cases_load

data/stage/reference_data.loaded: \
	$(modules) covid19stats/reference_data_load.py \
	reference/raw_state_abbreviations.csv \
	data/reference/*

	python3 -m covid19stats.reference_data_load

data/stage/covidtracking.loaded: \
	$(modules) covid19stats/covidtracking_load.py \
	data/stage/covidtracking_states.csv

	python3 -m covid19stats.covidtracking_load

transform:

	python3 -m covid19stats.transform_updated

data/stage/last_exported: \
	data/stage/transforms.updated

	python3 -m covid19stats.exports

export: \
	$(modules) covid19stats/exports.py \
	data/stage/last_exported

	echo "Exported"

clean:
	rm -rf data/stage/*.loaded
