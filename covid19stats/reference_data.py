
import codecs
import csv
import os.path

from .common import get_db_conn, timer, touch_file, load_flat_file, load_table


@timer
def load_county_population(conn):

    load_flat_file("reference/co-est2019-alldata.csv", conn, 'raw_county_population', encoding='latin1')


@timer
def load_county_gazetteer(conn):
    """ load geographic lat/lng data for counties """

    # I forgot we already get lat/lng from csse, so we don' actually need this.
    # keeping it around anyway

    # https://www.census.gov/geographies/reference-files/time-series/geo/gazetteer-files.html

    load_flat_file("reference/2019_Gaz_counties_national.txt", conn, 'raw_county_gazetteer', delimiter="\t")


@timer
def load_county_acs_vars(conn):
    """ load county-level variables from ACS """

    # https://api.census.gov/data/2018/acs/acs5/cprofile/variables.html

    path = "reference/county_acs_2018.json"

    print("Loading county vars into database")

    column_names = []

    with codecs.open(path, encoding='latin1') as f:
        rows = eval(f.read())
        column_names = rows[0]
        rows = rows[1:]

    load_table(conn, 'raw_county_acs', column_names, rows)


def load_state_info(conn):

    load_flat_file("reference/nst-est2019-alldata.csv", conn, 'raw_nst_population', encoding='latin1')


def load_reference_data():

    conn = get_db_conn()

    load_state_info(conn)

    load_county_population(conn)

    load_county_acs_vars(conn)

    load_county_gazetteer(conn)

    touch_file('stage/reference_data.loaded')


if __name__ == "__main__":
    load_reference_data()
