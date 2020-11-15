
# COVID-19-stats

Experimentation with COVID-19 data. You shouldn't use this for anything
except satisfying your personal curiosity.

What's in this repo:
- ELT for cleaning COVID-19 data from various sources and transforming it into dimensional models
- web interfaces for viewing the data in various ways

# How to Run This

This is set up to run nightly in a docker container, using BigQuery for the
data warehouse and Google Cloud Storage to store files. 

If you want to run this yourself, your best best is to use the docker setup
and tweak it. See this repo:

https://github.com/codeforkjeff/docker-covid-19-stats

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

## Various Charts

Obsolete: this is no longer being maintained.

This was my earliest effort, intended to view data adjusted for population size. With time, these charts aren't very
informative anymore. They're all on a single page, so it takes a while to load.

<https://codeforkjeff.github.io/covid-19-stats/covid-19-stats.html>

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

