
from .common import timer, download_and_update

@timer
def download_dhhs_testing_data():

    # every 12h
    threshold = 60 * 60 * 12

    # https://healthdata.gov/dataset/COVID-19-Diagnostic-Laboratory-Testing-PCR-Testing/j8mb-icvb
    download_and_update( \
        'https://healthdata.gov/api/views/j8mb-icvb/rows.tsv?accessType=DOWNLOAD&bom=true', \
        "stage/dhhs_testing.tsv", \
        threshold=threshold)


if __name__ == "__main__":

    download_dhhs_testing_data()
