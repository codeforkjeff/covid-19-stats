
import codecs
import csv
import re

from .common import blanks_to_none, get_db_conn, timer, touch_file


def clean_column_name(name):
    return re.sub(r"[^a-zA-Z0-9]", "_", name.strip())


@timer
def load_cdc_deaths():

    path = "input/cdc_deaths_2019_2020.txt"

    with codecs.open(path, encoding='utf-8-sig') as f:
        reader = csv.reader(f, delimiter="\t")
        rows = [blanks_to_none(row) for row in reader]
        column_names = [clean_column_name(name) for name in rows[0]]
        rows = rows[1:]

    conn = get_db_conn()

    c = conn.cursor()

    c.execute('''
        DROP TABLE IF EXISTS raw_cdc_deaths_2019_2020;
    ''')

    # Create table
    c.execute('''
        CREATE TABLE raw_cdc_deaths_2019_2020 ('''
            + ",".join([col + " text" for col in column_names]) +
        ''')
    ''')

    print(column_names)

    c.executemany('INSERT INTO raw_cdc_deaths_2019_2020 VALUES (' + ",".join(["%s"] * len(column_names)) + ')', rows)

    conn.commit()

    ####

    path = "input/cdc_deaths_2014_2018.txt"

    with codecs.open(path, encoding='utf-8-sig') as f:
        reader = csv.reader(f, delimiter="\t")
        rows = [blanks_to_none(row) for row in reader]
        column_names = [clean_column_name(name) for name in rows[0]]
        rows = rows[1:]

    conn = get_db_conn()

    c = conn.cursor()

    c.execute('''
        DROP TABLE IF EXISTS raw_cdc_deaths_2014_2018;
    ''')

    # Create table
    c.execute('''
        CREATE TABLE raw_cdc_deaths_2014_2018 ('''
            + ",".join([col + " text" for col in column_names]) +
        ''')
    ''')

    print(column_names)

    c.executemany('INSERT INTO raw_cdc_deaths_2014_2018 VALUES (' + ",".join(["%s"] * len(column_names)) + ')', rows)

    conn.commit()

    ####

    c.execute('DROP TABLE IF EXISTS final_cdc_deaths')

    c.execute('''
        CREATE TABLE final_cdc_deaths (
            State text,
            Year int,
            Week_Of_Year int,
            Week_Ending_Date date,
            All_Cause int
        )
    ''')

    c.execute('''
        INSERT INTO final_cdc_deaths
        SELECT
	    Jurisdiction_of_Occurrence,
            cast(MMWR_Year as int),
            cast(MMWR_Week as int),
            to_date(
                right(Week_Ending_Date, 4) || '-' ||
           	right(substr(Week_Ending_Date, 1, position('/' in Week_Ending_Date) - 1), 2) || '-' ||
          	right(replace(substr(Week_Ending_Date, position('/' in Week_Ending_Date) + 1), '/2020', ''), 2)
            ,'YYYY-MM-DD') AS Week_Ending_Date,
	    cast(All_Cause as int)
        FROM raw_cdc_deaths_2019_2020;
    ''')

    c.execute('''
        INSERT INTO final_cdc_deaths
        SELECT
	    Jurisdiction_of_Occurrence,
            cast(MMWR_Year as int),
            cast(MMWR_Week as int),
            to_date(
                right(Week_Ending_Date, 4) || '-' ||
           	right(substr(Week_Ending_Date, 1, position('/' in Week_Ending_Date) - 1), 2) || '-' ||
          	right(replace(substr(Week_Ending_Date, position('/' in Week_Ending_Date) + 1), '/2020', ''), 2)
            ,'YYYY-MM-DD') AS Week_Ending_Date,
	    cast(All__Cause as int)
        FROM raw_cdc_deaths_2014_2018;
    ''')

    conn.commit()

    touch_file('stage/cdc_deaths.loaded')


if __name__ == "__main__":
    load_cdc_deaths()

