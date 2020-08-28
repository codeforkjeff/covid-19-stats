
import codecs
import csv

from .common import blanks_to_none, get_db_conn, fast_bulk_insert, timer, touch_file


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

    # CREATE UNLOGGED TABLE
    c.execute('''
        CREATE UNLOGGED TABLE raw_covidtracking_states ('''
            + ",".join([col + " text" for col in column_names]) +
        ''')
    ''')

    #c.executemany('INSERT INTO raw_covidtracking_states VALUES (' + ",".join(["%s"] * len(column_names)) + ')', rows)
    fast_bulk_insert(conn, rows, 'raw_covidtracking_states')

    conn.commit()

    touch_file('stage/covidtracking.loaded')


if __name__ == "__main__":
    load_covidtracking_states()

