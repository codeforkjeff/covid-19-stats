
# COVID-19-stats

This is me just playing around with data. You shouldn't use this for anything
except satisfying your personal curiosity.

What all this does:
- cleans and transforms COVID-19 data from JHU into dimensional models
for easier reporting
- provides web interfaces for viewing the data in various ways

# How to Run This

Set up an environment using `requirements.txt`

Clone the following repo in the same directory where you cloned this repo, to get
the data files:
https://github.com/CSSEGISandData/COVID-19

Run `make`

You should end up with a SQLite database, `stage/covid19.db`, and files in
the `data/` directory.

# Charts and Tables

A map of outbreaks:

https://codeforkjeff.github.io/covid-19-stats/outbreaks.html

Progress by county and by state:

https://codeforkjeff.github.io/covid-19-stats/counties_progress.html

https://codeforkjeff.github.io/covid-19-stats/states_progress.html

Charts for COVID-19 statistics for the United States, adjusted for population
size, by county and by state. This was my earliest effort; I'm no longer
maintaining it or updating the data for it, though it still works.

https://codeforkjeff.github.io/covid-19-stats/covid-19-stats.html

# Working with the data

Key tables in the SQLite database:

`fact_counties_base` - a table containing daily snapshot info for each U.S.
county. Key into the table is Date and FIPS code.

`fact_counties_progress` - a more extensive version of `fact_counties_base`
containing numerous progress measures.Key into the table is Date and FIPS
code.

`dim_county` - county attributes, including population and lat/lng for
geographic center. Key into this table is FIPS code.

`dim_state` - state attributes, including population. Key into this table
is State name.
