
# COVID-19-stats

Experimentation with COVID-19 data. You shouldn't use this for anything
except satisfying your personal curiosity.

What's in this repo:
- ELT for cleaning COVID-19 data from various sources and transforming it into dimensional models
- web interfaces for viewing the data in various ways

(Note: on 11/14/2020, I removed the large output files that were being
committed daily (ugh) from the repo's history, reducing its size by a few
hundred megabytes. If you have been tracking this project, it's a good
idea to do a fresh clone from scratch.)

# How to Run This

You'll need BigQuery.

```
cd /path/to/covid-19-stats

# build image for running the download scripts
docker build . -t covid-19-stats-download

# pull meltano image
docker pull meltano/meltano:v1.90.1

# create Google service account credentials file
vi service-account.json

# make sure your ~/.ssh dir exists and key is recognized by github 

# edit meltano.yml and set the project_id and dataset_id for meltano

# initialize meltano project
./meltano_docker.sh install

# do initial load of everything
./meltano_docker.sh elt tap-spreadsheets-anywhere target-bigquery

# these lines add to crontab:

# twice a day
0 5,17 * * * cd /home/jeff/covid-19-stats && ./download_docker.sh python3 -m covid19stats.csse_load >> download.log 2>> download.log
0 6,18 * * * cd /home/jeff/covid-19-stats && ./download_docker.sh python3 -m covid19stats.dhhs_testing_download >> download.log 2>> download.log
0 7,19 * * * cd /home/jeff/covid-19-stats && ./download_docker.sh python3 -m covid19stats.cdc_states_download >> download.log 2>> download.log
0 8,21 * * * cd /home/jeff/covid-19-stats && ./meltano_docker.sh schedule run load-csse >> meltano.log 2>> meltano.log
# once a week
0 20 * * 7 cd /home/jeff/covid-19-stats && ./download_docker.sh python3 -m covid19stats.cdc_deaths_2020_download >> download.log 2>> download.log
# once a month
30 3 1 * * cd /home/jeff/covid-19-stats && ./download_docker.sh python3 -m covid19stats.cdc_deaths_2018_download >> download.log 2>> download.log
30 4 1 * * cd /home/jeff/covid-19-stats && ./download_docker.sh python3 -m covid19stats.cdc_surveillance_cases_download >> download.log 2>> download.log
30 5 1 * * cd /home/jeff/covid-19-stats && ./download_docker.sh python3 -m covid19stats.reference_data_download >> download.log 2>> download.log
```

# Charts and Tables

## Choropleth Map of Two Week Trends by County

Shows trends at the county level over the last two weeks.

<https://codeforkjeff.github.io/covid-19-stats/counties_trends.html>

## Simple Choropleth Map of Outbreaks

This uses the metric of "25 or higher new cases per 100k in the last 2 weeks." This is one of the measures used by WA
state at the start of the pandemic to determine which counties could move to later stages of reopening. So it's a
a helpful working definition of "outbreak."

<https://codeforkjeff.github.io/covid-19-stats/outbreaks_simple.html>

## Overly Complicated Map of Outbreaks

This was an initial effort that ended up way too busy-looking and complicated to understand. What I still like about it,
though, is the color coding to show which counties have been trending upwards or downwards over the last 2 weeks. You
can't tell this information at a glance in the simpler choropleth map.

<https://codeforkjeff.github.io/covid-19-stats/outbreaks.html>

## Tables of Progress by County and by State

<https://codeforkjeff.github.io/covid-19-stats/counties_progress.html>

<https://codeforkjeff.github.io/covid-19-stats/states_progress.html>

# Working with the data

Key tables in the database:

`fact_counties_base` - a table containing daily snapshot info for each U.S.
county. Key into the table is Date and FIPS code.

`fact_counties_progress` - a more extensive version of `fact_counties_base`
containing numerous progress measures. Key into the table is Date and FIPS
code.

`fact_states` - state-level facts. Key into this table is Date and State.

`fact_nation` - national-level facts.

`dim_county` - county attributes, including population and lat/lng for
geographic center. Key into this table is FIPS code.

`dim_state` - state attributes, including population. Key into this table
is State name.

`dim_date` - dates and related useful 'milestone' dates (e.g. 1 week ago, 1 month ago, etc.)

Other tables:

`raw_*` - raw data

`final_*` - cleaned versions of raw tables

`stage_*` - tables used to stage data to create dim and fact tables

