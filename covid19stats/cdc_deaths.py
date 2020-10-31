
from .common import blanks_to_none, get_db_conn, timer, touch_file, load_flat_file


@timer
def load_cdc_deaths():

    conn = get_db_conn()

    load_flat_file("input/cdc_deaths_2019_2020.txt", conn, "raw_cdc_deaths_2019_2020", delimiter="\t", encoding='utf-8-sig')

    load_flat_file("input/cdc_deaths_2014_2018.txt", conn, "raw_cdc_deaths_2014_2018", delimiter="\t", encoding='utf-8-sig')

    touch_file('stage/cdc_deaths.loaded')


if __name__ == "__main__":
    load_cdc_deaths()

