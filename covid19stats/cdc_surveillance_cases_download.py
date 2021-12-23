
from .common import timer, download_and_update

@timer
def download_cdc_surveillance_cases_data():

    # every month
    threshold = 60 * 60 * 24 * 30

    # https://data.cdc.gov/Case-Surveillance/COVID-19-Case-Surveillance-Public-Use-Data/vbim-akqf

    download_and_update( \
        'https://data.cdc.gov/api/views/vbim-akqf/rows.csv?accessType=DOWNLOAD', \
        "stage/cdc_surveillance_cases.csv", \
        threshold=threshold)


if __name__ == "__main__":

    download_cdc_surveillance_cases_data()
