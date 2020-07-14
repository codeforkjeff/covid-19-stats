
import codecs
import csv

from .common import get_db_conn, timer, touch_file


def blanks_to_none(row):
    return [(val if val != '' else None) for val in row]


@timer
def load_covidtracking_states():

    path = "input/covidtracking_states.csv"

    with codecs.open(path, encoding='utf8') as f:
        reader = csv.reader(f)
        rows = [blanks_to_none(row) for row in reader]
        column_names = [name.strip() for name in rows[0]]
        rows = rows[1:]

    conn = get_db_conn()

    c = conn.cursor()

    c.execute('''
        DROP TABLE IF EXISTS raw_covidtracking_states;
    ''')

    # Create table
    c.execute('''
        CREATE TABLE raw_covidtracking_states ('''
            + ",".join([col + " text" for col in column_names]) +
        ''')
    ''')

    c.executemany('INSERT INTO raw_covidtracking_states VALUES (' + ",".join(["?"] * len(column_names)) + ')', rows)

    conn.commit()

    touch_file('stage/covidtracking.loaded')


if __name__ == "__main__":
    load_covidtracking_states()

