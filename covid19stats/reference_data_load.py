
import codecs
import csv
import datetime
import os.path

from .common import timer, touch_file, bq_load


@timer
def load_county_population(conn):

    bq_load("reference/co-est2019-alldata.csv", "gs://covid-19-sources/co-est2019-alldata.csv", 'source_tables.raw_county_population', encoding='latin1')

@timer
def load_county_gazetteer(conn):
    """ load geographic lat/lng data for counties """

    # I forgot we already get lat/lng from csse, so we don' actually need this.
    # keeping it around anyway

    # https://www.census.gov/geographies/reference-files/time-series/geo/gazetteer-files.html

    bq_load("reference/2019_Gaz_counties_national.txt", "gs://covid-19-sources/2019_Gaz_counties_national.txt", 'source_tables.raw_county_gazetteer', delimiter="\t")


@timer
def load_county_acs_vars(conn):
    """ load county-level variables from ACS """

    # https://api.census.gov/data/2018/acs/acs5/cprofile/variables.html

    path = "reference/county_acs_2018.json"

    print("Loading county vars into database")

    column_names = []

    with codecs.open(path, encoding='latin1') as f:
        rows = eval(f.read())

        with codecs.open("reference/county_acs_2018.csv", 'w', encoding='latin1') as out:
            for row in rows:
                out.write(",".join(row))
                out.write("\n")

    bq_load("reference/county_acs_2018.csv", "gs://covid-19-sources/county_acs_2018.csv", 'source_tables.raw_county_acs', encoding='latin1')


def load_state_info(conn):

    bq_load("reference/nst-est2019-alldata.csv", "gs://covid-19-sources/nst-est2019-alldata.csv", 'source_tables.raw_nst_population', encoding='latin1')


def load_raw_date():
    """
    this exists because BigQuery doesn't support recursive CTEs
    """

    rows = []
    d = datetime.date(2020,1,1)
    last = datetime.date(2021,12,31)

    td_1day = datetime.timedelta(days=1)
    td_7days = datetime.timedelta(days=7)
    td_14days = datetime.timedelta(days=14)
    td_30days = datetime.timedelta(days=30)

    while d < last:
        rows.append([
            d.isoformat(),
            (d - td_1day).isoformat(),
            (d - td_7days).isoformat(),
            (d - td_14days).isoformat(),
            (d - td_30days).isoformat(),
        ])
        d = d + td_1day

    with codecs.open('stage/raw_date.csv', 'w', encoding='utf-8') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(['Date', 'Minus1Day', 'Minus7Days', 'Minus14Days', 'Minus30Days'])
        for row in rows:
            writer.writerow(row)

    bq_load("stage/raw_date.csv", "gs://covid-19-sources/raw_date.csv", 'source_tables.raw_date')


def load_raw_state_abbreviations():

    bq_load("reference/raw_state_abbreviations.csv", "gs://covid-19-sources/raw_state_abbreviations.csv", 'source_tables.raw_state_abbreviations')


def load_reference_data():

    conn = None# get_db_conn()

    load_state_info(conn)

    load_county_population(conn)

    load_county_acs_vars(conn)

    load_county_gazetteer(conn)

    load_raw_date()

    load_raw_state_abbreviations()

    touch_file('stage/reference_data.loaded')


if __name__ == "__main__":
    load_reference_data()