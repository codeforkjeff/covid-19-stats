
from .common import timer, touch_file, bq_load, sources_bucket


@timer
def load_cdc_deaths():

    bq_load("data/stage/cdc_deaths_2020_2021.txt", f"gs://{sources_bucket}/cdc_deaths_2020_2021.txt", "source_tables.raw_cdc_deaths_2020_2021", delimiter="\t", encoding='utf-8-sig')

    bq_load("data/stage/cdc_deaths_2014_2019.txt", f"gs://{sources_bucket}/cdc_deaths_2014_2019.txt", "source_tables.raw_cdc_deaths_2014_2019", delimiter="\t", encoding='utf-8-sig')

    touch_file('data/stage/cdc_deaths.loaded')


if __name__ == "__main__":
    load_cdc_deaths()

