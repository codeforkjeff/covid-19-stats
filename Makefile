# 
# we use stage/*.loaded targets to trigger reloading data into the database
# when a dataset needs to be refreshed.
#
# TODO: communicate to dbt what models need to be run

modules = covid19stats/common.py

all: \
	$(modules) covid19stats/exports.py \
	stage/transforms.updated

	python3 -m covid19stats.exports

stage/transforms.updated: \
	stage/csse.loaded stage/reference_data.loaded stage/covidtracking.loaded stage/cdc_deaths.loaded

	dbt run
	touch stage/transforms.updated

stage/csse.loaded: \
	$(modules) covid19stats/csse.py \
	../COVID-19/csse_covid_19_data/csse_covid_19_daily_reports/*.csv

	python3 -m covid19stats.csse

stage/cdc_deaths.loaded: \
	$(modules) covid19stats/cdc_deaths.py \
	input/cdc_deaths_2019_2020.txt

	python3 -m covid19stats.cdc_deaths

stage/reference_data.loaded: \
	$(modules) covid19stats/reference_data.py \
	reference/*

	python3 -m covid19stats.reference_data

stage/covidtracking.loaded: input/covidtracking_states.csv

	python3 -m covid19stats.covidtracking

# 'depend' is the extract phase of ELT
depend:

	@if ! [ -d ../COVID-19 ]; then echo "COVID-19 directory doesn't exist! clone that repo first"; exit 1; fi

	cd ../COVID-19 && git pull

	python3 -m covid19stats.covidtracking_download

	python3 -m covid19stats.cdc_deaths_download

	python3 -m covid19stats.reference_data_download

clean:
	rm -rf stage/*.loaded
