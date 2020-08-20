
from .common import timer, download_and_update

@timer
def download_cdc_deaths_data():

    # this file doesn't change often, if at all, so every month
    threshold = 60 * 60 * 24 * 30

    download_and_update( \
        'https://data.cdc.gov/api/views/muzy-jte6/rows.tsv?accessType=DOWNLOAD&bom=true', \
        "input/cdc_deaths_2019_2020.txt", \
        threshold=threshold)

    download_and_update( \
        'https://data.cdc.gov/api/views/3yf8-kanr/rows.tsv?accessType=DOWNLOAD&bom=true', \
        "input/cdc_deaths_2014_2018.txt", \
        threshold=threshold)


if __name__ == "__main__":

    download_cdc_deaths_data()
