
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

Create a new project on the Google Cloud Platform. Within it, create a BigQuery project
and two Cloud Storage buckets.

Under IAM, create a service account and a key for it, making sure to download
the [key file](https://cloud.google.com/iam/docs/creating-managing-service-account-keys). 
Save this file as `service-account.json` in this directory.

Update `profiles.yml` with information about your BigQuery project. See the 
[dbt docs](https://docs.getdbt.com/reference/warehouse-profiles/bigquery-profile).

Edit `run_docker.sh` and set the bucket names accordingly.

(Re)build the image using the Dockerfile:

```
docker build . -t covid-19-stats-image
```

Run the ELT code. This can be put in a cron job.

```
./run_docker.sh
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

