
import codecs
import csv
import re

from .common import blanks_to_none, get_db_conn, timer, touch_file


def clean_column_name(name):
    return re.sub(r"[^a-zA-Z0-9]", "_", name.strip())


def load_tab_delim_file(path, table_name):

    with codecs.open(path, encoding='utf-8-sig') as f:
        reader = csv.reader(f, delimiter="\t")
        rows = [blanks_to_none(row) for row in reader]
        column_names = [clean_column_name(name) for name in rows[0]]
        rows = rows[1:]

    conn = get_db_conn()

    c = conn.cursor()

    c.execute(f"DROP TABLE IF EXISTS {table_name};")

    # Create table
    c.execute(f"CREATE TABLE {table_name} (" + ",".join([col + " text" for col in column_names]) + ")")

    c.executemany(f"INSERT INTO {table_name} VALUES (" + ",".join(["?"] * len(column_names)) + ')', rows)

    conn.commit()


@timer
def load_cdc_deaths():

    load_tab_delim_file("input/cdc_deaths_2019_2020.txt", "raw_cdc_deaths_2019_2020")

    load_tab_delim_file("input/cdc_deaths_2014_2018.txt", "raw_cdc_deaths_2014_2018")

    ####

    conn = get_db_conn()

    c = conn.cursor()

    c.execute('DROP TABLE IF EXISTS final_cdc_deaths')

    c.execute('''
        CREATE TABLE final_cdc_deaths (
            State text,
            Year int,
            Week_Of_Year int,
            Week_Ending_Date text,
            All_Cause int
        )
    ''')

    c.execute('''
        INSERT INTO final_cdc_deaths
        SELECT
	    Jurisdiction_of_Occurrence,
            MMWR_Year,
            MMWR_Week,
                substr(Week_Ending_Date, -4, 4) || '-' ||
           	substr('00' || substr(Week_Ending_Date, 1, instr(Week_Ending_Date, '/') - 1), -2, 2) || '-' ||
          	substr('00' || replace(substr(Week_Ending_Date, instr(Week_Ending_Date, '/') + 1), '/2020', ''), -2, 2)
            AS Week_Ending_Date,
	    All_Cause
        FROM raw_cdc_deaths_2019_2020;
    ''')

    c.execute('''
        INSERT INTO final_cdc_deaths
        SELECT
	    Jurisdiction_of_Occurrence,
            MMWR_Year,
            MMWR_Week,
                substr(Week_Ending_Date, -4, 4) || '-' ||
           	substr('00' || substr(Week_Ending_Date, 1, instr(Week_Ending_Date, '/') - 1), -2, 2) || '-' ||
          	substr('00' || replace(substr(Week_Ending_Date, instr(Week_Ending_Date, '/') + 1), '/2020', ''), -2, 2)
            AS Week_Ending_Date,
	    All__Cause
        FROM raw_cdc_deaths_2014_2018;
    ''')

    conn.commit()

    touch_file('stage/cdc_deaths.loaded')


if __name__ == "__main__":
    load_cdc_deaths()

