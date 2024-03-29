
from .common import timer, download_and_update

@timer
def download_cdc_states_data():

    # every 12h
    threshold = 60 * 60 * 12

    download_and_update( \
        'https://data.cdc.gov/api/views/9mfq-cb36/rows.tsv?accessType=DOWNLOAD&bom=true', \
        "data/stage/cdc_states.tsv", \
        threshold=threshold)


if __name__ == "__main__":

    download_cdc_states_data()
