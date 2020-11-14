
import codecs
import csv

from .common import timer, touch_file, bq_load


@timer
def load_covidtracking_states():

    bq_load("stage/covidtracking_states.csv", "gs://covid-19-sources/covidtracking_states.csv", 'source_tables.raw_covidtracking_states')

    touch_file('stage/covidtracking.loaded')


if __name__ == "__main__":
    load_covidtracking_states()

