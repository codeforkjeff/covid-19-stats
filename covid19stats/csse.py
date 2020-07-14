
import codecs
import csv
import glob
import multiprocessing
import os
import os.path

from .common import get_db_conn, timer, touch_file, Path


ordered_fields = [
    "FIPS",
    "Admin2",
    "Province_State",
    "Country_Region",
    "Last_Update",
    "Lat",
    "Long_",
    "Confirmed",
    "Deaths",
    "Recovered",
    "Active",
    "Combined_Key",
]


def get_sortable_date(path):
    basename = os.path.basename(path)
    return basename[6:10] + basename[0:2] + basename[3:5]


def get_rows_from_csse_file(path):
    result = []
    print(f"Loading from {path.path}")
    with codecs.open(path.path, encoding='utf8') as f:
        reader = csv.reader(f)
        field_order_in_file = []
        first_row = True
        for row in reader:
            if first_row:
                field_order_in_file = row
                # strip out BOM
                if field_order_in_file[0].encode().startswith(codecs.BOM_UTF8):
                    # this is ridiculous
                    field_order_in_file[0] = field_order_in_file[0].encode()[len(codecs.BOM_UTF8):].decode()
            else:
                reordered = []
                for field in ordered_fields:
                    reordered.append(row[field_order_in_file.index(field)])

                row = [path.date] + reordered

                result.append(row)

            first_row = False
    return result


@timer
def load_csse():

    conn = get_db_conn()

    # assumes COVID-19 repo has been cloned to home directory
    spec = os.path.join(os.getenv("HOME"), 'COVID-19/csse_covid_19_data/csse_covid_19_daily_reports/*.csv')
    print(f"Looking for files: {spec}")
    paths = [Path(path, get_sortable_date(path)) for path in glob.glob(spec)]

    filtered_paths = [p for p in paths if p.date >= '20200322']

    all_rows = []

    p = multiprocessing.Pool(20)
    for result in p.map(get_rows_from_csse_file, filtered_paths):
        all_rows = all_rows + result
    p.close()


    print(f"Writing {len(all_rows)} rows to the database")

    c = conn.cursor()

    #c.execute("PRAGMA synchronous=OFF")
    c.execute("PRAGMA cache_size=10000000")
    c.execute("PRAGMA journal_mode = OFF")
    #c.execute("PRAGMA locking_mode = EXCLUSIVE")
    c.execute("PRAGMA temp_store = MEMORY")

    c.execute('''
        DROP TABLE IF EXISTS final_csse;
    ''')

    # Create table
    c.execute('''
        CREATE TABLE final_csse (
            Date text,
            FIPS text,
            Admin2 text,
            Province_State text,
            Country_Region text,
            Last_Update text,
            Lat text,
            Long_ text,
            Confirmed int,
            Deaths int,
            Recovered int,
            Active int,
            Combined_Key text,
            ShouldHaveFIPS int
            )
    ''')

    c.executemany('INSERT INTO final_csse VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?, 0)', all_rows)

    conn.commit()

    c.execute('''
        UPDATE final_csse
        SET ShouldHaveFIPS = 1
        WHERE
            Country_Region = 'US'
            AND Admin2 <> 'Unassigned'
            AND Admin2 <> 'Unknown'
            AND Admin2 not like 'Out of%'
            AND Admin2 not like 'Out-of-%'
            And Province_State <> 'Recovered'
    ''')

    # fix some FIPS codes that aren't properly zero-padded
    c.execute('''
        UPDATE final_csse
        SET
            FIPS = substr('0000000000' || FIPS, -5, 5)
        WHERE
            ShouldHaveFIPS = 1
            AND length(fips) <> 5
            AND length(fips) > 0
    ''')

    c.execute('''
        CREATE INDEX idx ON final_csse (FIPS);
    ''')

    conn.commit()

    # -- find rows that should havbe a fips code but doesn't.
    # -- I think these are actually okay to let by.
    #
    # select distinct combined_key
    # from final_csse
    # where
    #     shouldhavefips = 1
    #     and (fips is null or length(fips) <> 5 or fips = '00000')

    c.close()

    touch_file('stage/csse.loaded')


if __name__ == "__main__":
    load_csse()
