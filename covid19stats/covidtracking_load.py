
import codecs
import csv

from .common import timer, touch_file, bq_load, sources_bucket


@timer
def load_covidtracking_states():

    bq_load("data/stage/covidtracking_states.csv", f"gs://{sources_bucket}/covidtracking_states.csv", 'source_tables.raw_covidtracking_states')

    touch_file('data/stage/covidtracking.loaded')


if __name__ == "__main__":
    load_covidtracking_states()

