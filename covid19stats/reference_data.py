
import codecs
import csv
import os.path

from .common import get_db_conn, timer, touch_file


@timer
def load_county_population(conn):

    path = "reference/co-est2019-alldata.csv"

    print("Loading county data into database")

    column_names = []

    with codecs.open(path, encoding='latin1') as f:
        reader = csv.reader(f)
        rows = [row for row in reader]
        column_names = rows[0]
        rows = rows[1:]

    c = conn.cursor()

    c.execute('''
        DROP TABLE IF EXISTS raw_county_population;
    ''')

    # Create table
    c.execute('''
        CREATE TABLE raw_county_population ('''
            + ",".join([col + " text" for col in column_names]) +
        ''')
    ''')

    c.executemany('INSERT INTO raw_county_population VALUES (' + ",".join(["?"] * len(column_names)) + ')', rows)

    conn.commit()

    c.execute('DROP TABLE IF EXISTS final_fips_population')

    c.execute('''
        CREATE TABLE final_fips_population
        AS SELECT
            STNAME,
            CTYNAME,
            STATE || COUNTY AS FIPS,
            CAST(POPESTIMATE2019 as INT) as Population
        FROM raw_county_population;
    ''')

    # Patch Puerto Rico
    c.execute('''
        INSERT INTO final_fips_population
        SELECT
            NAME, 
            NAME,
            '00072',
            CAST(POPESTIMATE2019 as INT) as Population
        FROM raw_nst_population
        WHERE NAME = 'Puerto Rico'
    ''')

    # Patch NYC: CSSE aggregates all 5 counties of NYC. Reusing code 36061
    # is misleading, IMO, but that's how it is. so we patch our pop count
    # to follow suit

    c.execute('''
        DROP TABLE IF EXISTS nyc_patch
    ''');

    c.execute('''
        CREATE TABLE nyc_patch
        AS SELECT
            SUM(Population) as Population
        FROM final_fips_population
        WHERE FIPS IN (
            '36061', -- New York County
            '36005', -- Bronx County
            '36047', -- Kings County
            '36081', -- Queens County
            '36085' -- Richmond County
        ) 
    ''')

    c.execute('''
        UPDATE final_fips_population
        SET
            Population = (SELECT Population from nyc_patch)
        WHERE
            FIPS = '36061'
    ''')


    c.executescript('''
        DROP TABLE nyc_patch;

        CREATE INDEX idx_final_fips_population ON final_fips_population (FIPS);
    ''')

    conn.commit()

    c.close()


@timer
def load_county_gazetteer(conn):
    """ load geographic lat/lng data for counties """

    # I forgot we already get lat/lng from csse, so we don' actually need this.
    # keeping it around anyway

    # https://www.census.gov/geographies/reference-files/time-series/geo/gazetteer-files.html

    path = "reference/2019_Gaz_counties_national.txt"

    with codecs.open(path, encoding='utf8') as f:
        reader = csv.reader(f, delimiter="\t")
        rows = [row for row in reader]
        column_names = [name.strip() for name in rows[0]]
        rows = rows[1:]

    c = conn.cursor()

    c.execute('''
        DROP TABLE IF EXISTS raw_county_gazetteer;
    ''')

    # Create table
    c.execute('''
        CREATE TABLE raw_county_gazetteer ('''
            + ",".join([col + " text" for col in column_names]) +
        ''')
    ''')

    c.executemany('INSERT INTO raw_county_gazetteer VALUES (' + ",".join(["?"] * len(column_names)) + ')', rows)


@timer
def load_county_acs_vars(conn):
    """ load county-level variables from ACS """

    # https://api.census.gov/data/2018/acs/acs5/cprofile/variables.html

    path = "reference/county_acs_2018.csv"

    print("Loading county vars into database")

    column_names = []

    with codecs.open(path, encoding='latin1') as f:
        rows = eval(f.read())
        column_names = rows[0]
        rows = rows[1:]

    c = conn.cursor()

    c.execute('''
        DROP TABLE IF EXISTS raw_county_acs;
    ''')

    # Create table
    c.execute('''
        CREATE TABLE raw_county_acs ('''
            + ",".join([col + " text" for col in column_names]) +
        ''')
    ''')

    c.executemany('INSERT INTO raw_county_acs VALUES (' + ",".join(["?"] * len(column_names)) + ')', rows)

    c.executescript('''
        DROP TABLE IF EXISTS final_county_acs;

        CREATE TABLE final_county_acs AS
            SELECT
                *
                ,state || county AS state_and_county
            FROM raw_county_acs;

        CREATE INDEX idx_final_county_acs ON final_county_acs (state_and_county)
    ''')

    conn.commit()

    c.close()


def load_state_info(conn):

    path = "reference/nst-est2019-alldata.csv"

    print("Loading state data into database")

    column_names = []

    with codecs.open(path, encoding='latin1') as f:
        reader = csv.reader(f)
        rows = [row for row in reader]
        column_names = rows[0]
        rows = rows[1:]

    c = conn.cursor()

    c.execute('''
        DROP TABLE IF EXISTS raw_nst_population;
    ''')

    # Create table
    c.execute('''
        CREATE TABLE raw_nst_population ('''
            + ",".join([col + " text" for col in column_names]) +
        ''')
    ''')

    c.executemany('INSERT INTO raw_nst_population VALUES (' + ",".join(["?"] * len(column_names)) + ')', rows)

    conn.commit()

    c.execute('DROP TABLE IF EXISTS raw_state_abbreviations')

    c.execute('CREATE TABLE raw_state_abbreviations ( State text , Abbreviation text )')

    c.execute('''
        INSERT INTO raw_state_abbreviations
        SELECT *
        FROM
        (
        VALUES
            ('Alabama', 'AL'),
            ('Alaska', 'AK'),
            ('Arizona', 'AZ'),
            ('Arkansas', 'AR'),
            ('California', 'CA'),
            ('Colorado', 'CO'),
            ('Connecticut', 'CT'),
            ('Delaware', 'DE'),
            ('District of Columbia', 'DC'),
            ('Florida', 'FL'),
            ('Georgia', 'GA'),
            ('Hawaii', 'HI'),
            ('Idaho', 'ID'),
            ('Illinois', 'IL'),
            ('Indiana', 'IN'),
            ('Iowa', 'IA'),
            ('Kansas', 'KS'),
            ('Kentucky', 'KY'),
            ('Louisiana', 'LA'),
            ('Maine', 'ME'),
            ('Maryland', 'MD'),
            ('Massachusetts', 'MA'),
            ('Michigan', 'MI'),
            ('Minnesota', 'MN'),
            ('Mississippi', 'MS'),
            ('Missouri', 'MO'),
            ('Montana', 'MT'),
            ('Nebraska', 'NE'),
            ('Nevada', 'NV'),
            ('New Hampshire', 'NH'),
            ('New Jersey', 'NJ'),
            ('New Mexico', 'NM'),
            ('New York', 'NY'),
            ('North Carolina', 'NC'),
            ('North Dakota', 'ND'),
            ('Ohio', 'OH'),
            ('Oklahoma', 'OK'),
            ('Oregon', 'OR'),
            ('Pennsylvania', 'PA'),
            ('Rhode Island', 'RI'),
            ('South Carolina', 'SC'),
            ('South Dakota', 'SD'),
            ('Tennessee', 'TN'),
            ('Texas', 'TX'),
            ('Utah', 'UT'),
            ('Vermont', 'VT'),
            ('Virginia', 'VA'),
            ('Washington', 'WA'),
            ('West Virginia', 'WV'),
            ('Wisconsin', 'WI'),
            ('Wyoming', 'WY')
        ) AS t;
    ''')

    c.execute('CREATE INDEX idx_raw_state_abbreviations ON raw_state_abbreviations (State)')

    conn.commit()

    c.close()


def load_reference_data():

    conn = get_db_conn()

    load_state_info(conn)

    load_county_population(conn)

    load_county_acs_vars(conn)

    load_county_gazetteer(conn)

    touch_file('stage/reference_data.loaded')


if __name__ == "__main__":
    load_reference_data()
