

from .common import timer, touch_file, bq_load, sources_bucket


@timer
def load_cdc_states():

    bq_load("stage/cdc_states.tsv", f"gs://{sources_bucket}/cdc_states.tsv", "source_tables.raw_cdc_states", delimiter="\t", encoding='utf-8-sig')

    touch_file('stage/cdc_states.loaded')


if __name__ == "__main__":
    load_cdc_states()

