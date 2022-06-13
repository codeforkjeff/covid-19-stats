
import codecs
import csv

from .common import timer, touch_file, bq_load, sources_bucket, sync_to_bucket


@timer
def load_cdc_surveillance_cases():

    bq_load("data/stage/cdc_surveillance_cases.tsv", f"gs://{sources_bucket}/cdc_surveillance_cases.tsv", "source_tables.raw_cdc_surveillance_cases", delimiter="\t", encoding='utf-8-sig')

    touch_file('data/stage/cdc_surveillance_cases.loaded')


if __name__ == "__main__":
    load_cdc_surveillance_cases()

