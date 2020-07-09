# less than ideal makefile

modules = covid19stats.py

all: $(modules) stage/csse.loaded stage/reference_data.loaded stage/dimensional_models.loaded
	python3 -c "from covid19stats import *; create_dimensional_tables();"
	python3 -c "from covid19stats import *; create_exports();"

stage/csse.loaded: $(modules) ../COVID-19/csse_covid_19_data/csse_covid_19_daily_reports/*.csv
	python3 -c "from covid19stats import *; load_csse();"

stage/reference_data.loaded: $(modules)
	python3 -c "from covid19stats import *; load_reference_data();"

clean:
	rm -rf stage/*.loaded
