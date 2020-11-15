
from .common import timer, touch_file, bq_load, sources_bucket


@timer
def load_cdc_deaths():

    bq_load("stage/cdc_deaths_2019_2020.txt", f"gs://{sources_bucket}/cdc_deaths_2019_2020.txt", "source_tables.raw_cdc_deaths_2019_2020", delimiter="\t", encoding='utf-8-sig')

    bq_load("stage/cdc_deaths_2014_2018.txt", f"gs://{sources_bucket}/cdc_deaths_2014_2018.txt", "source_tables.raw_cdc_deaths_2014_2018", delimiter="\t", encoding='utf-8-sig')

    touch_file('stage/cdc_deaths.loaded')


if __name__ == "__main__":
    load_cdc_deaths()

