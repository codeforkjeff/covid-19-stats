
import codecs
import csv

from .common import get_db_conn, timer, touch_file, load_flat_file, bq_load_flat_file


@timer
def load_covidtracking_states():

    conn = get_db_conn()

    # load_flat_file("input/covidtracking_states.csv", conn, 'raw_covidtracking_states')

    bq_load_flat_file("input/covidtracking_states.csv", 'source_tables.raw_covidtracking_states')

    touch_file('stage/covidtracking.loaded')


if __name__ == "__main__":
    load_covidtracking_states()

