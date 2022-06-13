

from .common import timer, touch_file, bq_load, sources_bucket


@timer
def load_cdc_positivity():

    bq_load("data/stage/dhhs_testing.tsv", f"gs://{sources_bucket}/dhhs_testing.tsv", "source_tables.raw_dhhs_testing", delimiter="\t", encoding='utf-8-sig')

    touch_file('data/stage/dhhs_testing.loaded')


if __name__ == "__main__":
    load_cdc_positivity()

