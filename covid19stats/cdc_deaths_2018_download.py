
from .common import timer, download_and_update

@timer
def download_cdc_deaths_2018_data():

    # this file doesn't change often, if at all, so every month
    threshold = 60 * 60 * 24 * 30

    download_and_update( \
        'https://data.cdc.gov/api/views/3yf8-kanr/rows.csv?accessType=DOWNLOAD', \
        "stage/cdc_deaths_2014_2018.csv", \
        threshold=threshold)


if __name__ == "__main__":

    download_cdc_deaths_2018_data()
