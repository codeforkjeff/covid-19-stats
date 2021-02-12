
from .common import timer, download_and_update

@timer
def download_cdc_surveillance_cases_data():

    # every month
    threshold = 60 * 60 * 24 * 30

    download_and_update( \
        'https://data.cdc.gov/api/views/vbim-akqf/rows.tsv?accessType=DOWNLOAD&bom=true', \
        "stage/cdc_surveillance_cases.tsv", \
        threshold=threshold)


if __name__ == "__main__":

    download_cdc_surveillance_cases_data()
