
import codecs
import csv
import os.path
import urllib.request
import zipfile

from .common import timer, download_and_update


def download_county_population():

    # this file doesn't change often, if at all, so every month
    threshold = 60 * 60 * 24 * 30

    download_and_update( \
        'https://www2.census.gov/programs-surveys/popest/datasets/2010-2019/counties/totals/co-est2019-alldata.csv', \
        "reference/co-est2019-alldata.csv", \
        threshold=threshold)


def download_county_gazetteer():

    zip_path = "reference/2019_Gaz_counties_national.zip"

    # this file doesn't change often, if at all, so every month
    threshold = 60 * 60 * 24 * 30

    updated = download_and_update( \
        "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2019_Gazetteer/2019_Gaz_counties_national.zip", \
        zip_path, \
        threshold=threshold)

    if updated:
        with zipfile.ZipFile(zip_path) as zipf:
            zipf.extractall("reference/")


def download_county_acs_vars():
    """ load county-level variables from ACS """

    # https://api.census.gov/data/2018/acs/acs5/cprofile/variables.html

    # this file doesn't change often, if at all, so every month
    threshold = 60 * 60 * 24 * 30

    updated = download_and_update( \
        "https://api.census.gov/data/2018/acs/acs5/cprofile?get=GEO_ID,CP03_2014_2018_062E,CP05_2014_2018_018E&for=county:*", \
        "reference/county_acs_2018.json", \
        threshold=threshold)


def download_state_info():

    # this file doesn't change often, if at all, so every month
    threshold = 60 * 60 * 24 * 30

    download_and_update( \
        'http://www2.census.gov/programs-surveys/popest/datasets/2010-2019/national/totals/nst-est2019-alldata.csv', \
        "reference/nst-est2019-alldata.csv", \
        threshold=threshold)


@timer
def download_reference_data():

    download_county_population()

    download_county_gazetteer()

    download_county_acs_vars()

    download_state_info()


if __name__ == "__main__":

    download_reference_data()
