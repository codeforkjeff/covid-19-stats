
# COVID-19-stats

This is me just playing around with data. You shouldn't use this for anything except satisfying your personal curiosity.

# Charts

Charts for COVID-19 statistics for the United States, adjusted for population size, by county and by state:

https://codeforkjeff.github.io/covid-19-stats/covid-19-stats.html

Progress by county and by state:

https://codeforkjeff.github.io/covid-19-stats/counties_progress.html

https://codeforkjeff.github.io/covid-19-stats/states_progress.html

# Running this

Set up an environment using `requirements.txt`

Clone the repo below in the same directory where you cloned this repo, to get the data files.
https://github.com/CSSEGISandData/COVID-19

Run this:

```
python -c "from covid19stats import *; load_all_data();"
```
