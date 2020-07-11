
# python >= 3.7.4

import codecs
from collections import namedtuple
import csv
import glob
import io
import functools
import json
import multiprocessing
import os
import os.path
import pathlib
import sqlite3
import sys
import time
import urllib.request
import zipfile

Path = namedtuple('Path', ['path', 'date'])


def timer(func):
    """Print the runtime of the decorated function"""
    @functools.wraps(func)
    def wrapper_timer(*args, **kwargs):
        start_time = time.perf_counter()
        value = func(*args, **kwargs)
        end_time = time.perf_counter()
        run_time = end_time - start_time
        print(f"Finished {func.__name__!r} in {run_time:.4f} secs")
        return value
    return wrapper_timer


def get_sortable_date(path):
    basename = os.path.basename(path)
    return basename[6:10] + basename[0:2] + basename[3:5]


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
        DROP TABLE IF EXISTS csse;
    ''')

    # Create table
    c.execute('''
        CREATE TABLE csse (
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

    c.executemany('INSERT INTO csse VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?, 0)', all_rows)

    conn.commit()

    c.execute('''
        UPDATE csse
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
        UPDATE csse
        SET
            FIPS = substr('0000000000' || FIPS, -5, 5)
        WHERE
            ShouldHaveFIPS = 1
            AND length(fips) <> 5
            AND length(fips) > 0
    ''')

    c.execute('''
        CREATE INDEX idx ON csse (FIPS);
    ''')

    conn.commit()

    # -- find rows that should havbe a fips code but doesn't.
    # -- I think these are actually okay to let by.
    #
    # select distinct combined_key
    # from csse
    # where
    #     shouldhavefips = 1
    #     and (fips is null or length(fips) <> 5 or fips = '00000')

    c.close()

    pathlib.Path('stage/csse.loaded').touch()


@timer
def load_county_population(conn):

    path = "stage/co-est2019-alldata.csv"

    if not os.path.exists(path):
        print("Downloading county info into stage dir")
        with urllib.request.urlopen('https://www2.census.gov/programs-surveys/popest/datasets/2010-2019/counties/totals/co-est2019-alldata.csv') as f:
            data = f.read()
            with open(path, 'wb') as output:
                output.write(data)

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

    c.execute('DROP TABLE IF EXISTS fips_population')

    c.execute('''
        CREATE TABLE fips_population
        AS SELECT
            STNAME,
            CTYNAME,
            STATE || COUNTY AS FIPS,
            CAST(POPESTIMATE2019 as INT) as Population
        FROM raw_county_population;
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
        FROM fips_population
        WHERE FIPS IN (
            '36061', -- New York County
            '36005', -- Bronx County
            '36047', -- Kings County
            '36081', -- Queens County
            '36085' -- Richmond County
        ) 
    ''')

    c.execute('''
        UPDATE fips_population
        SET
            Population = (SELECT Population from nyc_patch)
        WHERE
            FIPS = '36061'
    ''')


    c.executescript('''
        DROP TABLE nyc_patch;

        CREATE INDEX idx_fips_population ON fips_population (FIPS);
    ''')

    conn.commit()

    c.close()


@timer
def load_county_gazetteer(conn):
    """ load geographic lat/lng data for counties """

    # I forgot we already get lat/lng from csse, so we don' actually need this.
    # keeping it around anyway

    # https://www.census.gov/geographies/reference-files/time-series/geo/gazetteer-files.html

    zip_path = "stage/2019_Gaz_counties_national.zip"
    path = "stage/2019_Gaz_counties_national.txt"

    if not os.path.exists(path):
        print("Downloading Gazetter county file into stage dir")
        with urllib.request.urlopen("https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2019_Gazetteer/2019_Gaz_counties_national.zip") as f:
            data = f.read()
            with open(zip_path, 'wb') as output:
                output.write(data)

        with zipfile.ZipFile(zip_path) as zipf:
            zipf.extractall("stage/")


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

    path = "stage/county_acs_2018.csv"

    if not os.path.exists(path):
        print("Downloading county ACS file into stage dir")
        with urllib.request.urlopen("https://api.census.gov/data/2018/acs/acs5/cprofile?get=GEO_ID,CP03_2014_2018_062E,CP05_2014_2018_018E&for=county:*") as f:
            data = f.read()
            with open(path, 'wb') as output:
                output.write(data)

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
        DROP TABLE IF EXISTS county_acs;

        CREATE TABLE county_acs AS
            SELECT
                *
                ,state || county AS state_and_county
            FROM raw_county_acs;

        CREATE INDEX idx_county_acs ON county_acs (state_and_county)
    ''')

    conn.commit()

    c.close()


def load_state_info(conn):

    path = "stage/nst-est2019-alldata.csv"

    if not os.path.exists(path):
        print("Downloading state info into stage dir")
        with urllib.request.urlopen('http://www2.census.gov/programs-surveys/popest/datasets/2010-2019/national/totals/nst-est2019-alldata.csv') as f:
            data = f.read()
            with open(path, 'wb') as output:
                output.write(data)

    print("Loading state data into database")

    column_names = []

    with codecs.open(path, encoding='latin1') as f:
        reader = csv.reader(f)
        rows = [row for row in reader]
        column_names = rows[0]
        rows = rows[1:]

    c = conn.cursor()

    c.execute('''
        DROP TABLE IF EXISTS nst_population;
    ''')

    # Create table
    c.execute('''
        CREATE TABLE nst_population ('''
            + ",".join([col + " text" for col in column_names]) +
        ''')
    ''')

    c.executemany('INSERT INTO nst_population VALUES (' + ",".join(["?"] * len(column_names)) + ')', rows)

    conn.commit()

    c.execute('DROP TABLE IF EXISTS state_abbreviations')

    c.execute('CREATE TABLE state_abbreviations ( State text , Abbreviation text )')

    c.execute('''
        INSERT INTO state_abbreviations
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

    c.execute('CREATE INDEX idx_state_abbreviations ON state_abbreviations (State)')

    conn.commit()

    c.close()


@timer
def create_dimensional_tables():

    conn = get_db_conn()

    c = conn.cursor()

    c.execute('DROP TABLE IF EXISTS dim_state')

    c.execute('''
        CREATE TABLE dim_state
        AS SELECT
            NAME AS State
            ,Abbreviation AS StateAbbrev
            ,CAST(POPESTIMATE2019 as INT) as Population
        FROM nst_population t1
        LEFT JOIN state_abbreviations t2
            ON t1.NAME = t2.State
        WHERE CAST(t1.STATE AS INT) > 0
    ''')

    conn.commit()

    print("stage_regions_with_data")

    c.executescript('''
        DROP TABLE IF EXISTS stage_regions_with_data;

        CREATE TABLE stage_regions_with_data AS
            -- only take U.S. regions that have data
            select FIPS
            from csse
            where
                Country_Region = 'US'
                AND FIPS <> '00078' -- duplicate for Virgin Islands
                AND FIPS <> '00066' -- duplicate for Guam
                AND LENGTH(FIPS) > 0
            group by FIPS
            Having SUM(Confirmed > 0) or SUM(Deaths) > 0 or SUM(Recovered) > 0 or sum(active) > 0;

        CREATE INDEX idx_stage_regions_with_data ON stage_regions_with_data (FIPS);
    ''')


    print("stage_csse_filtered_pre")

    c.executescript('''

        DROP TABLE IF EXISTS stage_csse_filtered_pre;

        CREATE TABLE stage_csse_filtered_pre AS
            SELECT
                t1.*
            FROM csse t1
            WHERE
                ShouldHaveFIPS = 1
                AND EXISTS (SELECT 1 FROM stage_regions_with_data t2 WHERE t1.FIPS = t2.FIPS);

        CREATE INDEX idx_stage_csse_filtered_pre ON stage_csse_filtered_pre (FIPS, Date, Confirmed, Deaths);

    ''')


    print("stage_csse_filtered_deduped")

    c.executescript('''
        DROP TABLE IF EXISTS stage_csse_filtered_deduped;

        CREATE TABLE stage_csse_filtered_deduped AS
            -- there's duplication for the same FIPS and Date;
            -- for some cases, it looks like counts got spread out across 2 rows;
            -- in other cases, it looks like there are rows where values are 0.
            -- for simplicity, just pick the row with higher numbers
            SELECT
                *
                ,ROW_NUMBER() OVER (PARTITION BY FIPS, Date ORDER BY Confirmed DESC, Deaths DESC) AS RN
            FROM stage_csse_filtered_pre;

        CREATE INDEX idx_stage_csse_filtered_deduped ON stage_csse_filtered_deduped (RN);

    ''')


    print("stage_csse_filtered")

    c.executescript('''
        DROP TABLE IF EXISTS stage_csse_filtered;

        CREATE TABLE stage_csse_filtered (
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
            );

        INSERT INTO stage_csse_filtered
        SELECT
            substr(Date,1,4) || '-' || substr(Date,5,2) ||  '-' || substr(Date,7,2) AS Date,
            FIPS,
            Admin2,
            Province_State,
            Country_Region,
            Last_Update,
            Lat,
            Long_,
            Confirmed,
            Deaths,
            Recovered,
            Active,
            Combined_Key,
            ShouldHaveFIPS
        FROM stage_csse_filtered_deduped
        WHERE RN = 1;

        CREATE INDEX idx_stage_csse_filtered ON stage_csse_filtered (FIPS, Date);
    ''')


    print("dim_county")

    c.executescript('''
        DROP TABLE IF EXISTS dim_county;

        CREATE TABLE dim_county (
            FIPS text,
            County text,
            State text,
            Lat text,
            Long_ text,
            Combined_Key text,
            Population int,
            MedianIncome int,
            MedianAge float,
            primary key (FIPS)
        );

        CREATE UNIQUE INDEX idx_dim_county ON dim_county (County, State);
    
        WITH MostRecent AS (
            SELECT
                *
                ,ROW_NUMBER() OVER (PARTITION BY FIPS ORDER BY Date DESC) AS DateRank
            FROM stage_csse_filtered
        )
        INSERT INTO dim_county (
            FIPS,
            County,
            State,
            Lat,
            Long_,
            Combined_Key,
            Population,
            MedianIncome,
            MedianAge
        )
        SELECT DISTINCT
            t1.FIPS,
            Admin2 AS County,
            Province_State AS State,
            Lat,
            Long_,
            Combined_Key,
            Population,
            CP03_2014_2018_062E AS MedianIncome,
            CP05_2014_2018_018E AS MedianAge
        FROM MostRecent t1
        LEFT JOIN fips_population t2
            ON t1.FIPS = t2.FIPS
        LEFT JOIN county_acs t3
            ON t1.FIPS = t3.state_and_county
        WHERE t1.DateRank = 1
    ''')


    print("stage_with_population")

    c.executescript('''
        DROP TABLE IF EXISTS stage_with_population_pre;

        DROP TABLE IF EXISTS stage_with_population;

        CREATE TABLE stage_with_population_pre AS
            SELECT
                t1.*
                ,Population
                ,CAST(Confirmed AS REAL) / (CAST(Population AS REAL) / 1000000) as ConfirmedPer1M
                ,CAST(Deaths AS REAL) / (CAST(Population AS REAL) / 1000000) as DeathsPer1M
                ,ROW_NUMBER() OVER (PARTITION BY t1.FIPS ORDER BY Date DESC) As DateRank
            FROM stage_csse_filtered t1
            LEFT JOIN dim_county t2
                ON t1.FIPS = t2.FIPS;

        CREATE TABLE stage_with_population AS
            SELECT
                *,
                DateRank - 1 AS DateRankMinus1,
                DateRank - 5 AS DateRankMinus5
            FROM stage_with_population_pre;

        DROP TABLE IF EXISTS stage_with_population_pre;

        CREATE INDEX idx_stage_with_population ON stage_with_population (FIPS, DateRank);
        CREATE INDEX idx_stage_with_population2 ON stage_with_population (FIPS, DateRankMinus1);
        CREATE INDEX idx_stage_with_population3 ON stage_with_population (FIPS, DateRankMinus5);
     ''')


    print("stage_with_deltas")

    c.executescript('''
        DROP TABLE IF EXISTS stage_with_deltas;

        CREATE TABLE stage_with_deltas as
            select
                t1.*
                ,t1.ConfirmedPer1M - t2.ConfirmedPer1M as DeltaConfirmedPer1M
                ,t1.DeathsPer1M - t2.DeathsPer1M as DeltaDeathsPer1M
                ,(t1.ConfirmedPer1M - t3.ConfirmedPer1M) / 5.0 as Avg5DaysConfirmedPer1M
                ,(t1.DeathsPer1M - t3.DeathsPer1M) / 5.0 as Avg5DaysDeathsPer1M
            from stage_with_population t1
            left join stage_with_population t2 
                on t1.fips = t2.fips
                and t1.DateRank = t2.DateRankMinus1
            left join stage_with_population t3
                on t1.fips = t3.fips
                and t1.DateRank = t3.DateRankMinus5
        ;

        CREATE INDEX idx_stage_with_deltas ON stage_with_deltas (FIPS);
    ''')


    print("stage_latest")

    c.executescript('''
        DROP TABLE IF EXISTS stage_latest;

        CREATE TABLE stage_latest AS
            SELECT
                FIPS
                ,MAX(ConfirmedPer1M) AS LatestConfirmedPer1M
                ,MAX(DeathsPer1M) AS LatestDeathsPer1M
            FROM stage_with_deltas
            WHERE DateRank = 1
            GROUP BY FIPS;

        CREATE INDEX idx_stage_latest ON stage_latest (FIPS);
    ''')


    print("stage_with_latest")

    c.executescript('''
        DROP TABLE IF EXISTS stage_with_latest;

        CREATE TABLE stage_with_latest AS
            SELECT
                t1.*
                ,date(t1.Date, '+1 day') as DatePlus1Day
                ,t2.LatestConfirmedPer1M
                ,t2.LatestDeathsPer1M
            FROM stage_with_deltas t1
            LEFT JOIN stage_latest t2
                ON t1.FIPS = t2.FIPS;

        CREATE INDEX idx_stage_with_latest ON stage_with_latest (FIPS, LatestConfirmedPer1M, LatestDeathsPer1M);
        CREATE INDEX idx_stage_with_latest2 ON stage_with_latest (FIPS, DatePlus1Day);
        CREATE INDEX idx_stage_with_latest3 ON stage_with_latest (LatestConfirmedPer1M);
        CREATE INDEX idx_stage_with_latest4 ON stage_with_latest (LatestDeathsPer1M);
     ''')


    print("stage_with_rank")

    c.executescript('''
        DROP TABLE IF EXISTS stage_with_rank;

        CREATE TABLE stage_with_rank AS
            SELECT
                FIPS
                ,LatestConfirmedPer1M
                ,LatestDeathsPer1M
                ,ROW_NUMBER() OVER (ORDER BY LatestConfirmedPer1M DESC) As ConfirmedRank
                ,ROW_NUMBER() OVER (ORDER BY LatestDeathsPer1M DESC) As DeathRank
            FROM stage_with_latest
            GROUP BY FIPS, LatestConfirmedPer1M, LatestDeathsPer1M;

        CREATE INDEX idx_stage_with_rank ON stage_with_rank (FIPS, ConfirmedRank);
    ''')


    print("fact_counties_ranked")

    c.executescript('''
        DROP TABLE IF EXISTS fact_counties_ranked;

        CREATE TABLE fact_counties_ranked (
            Date text,
            FIPS text,
            Confirmed int,
            ConfirmedIncrease int,
            Deaths int,
            DeathsIncrease int,
            ConfirmedPer1M real,
            DeathsPer1M real,
            DeltaConfirmedPer1M real,
            DeltaDeathsPer1M real,
            Avg5DaysConfirmedPer1M real,
            Avg5DaysDeathsPer1M real,
            ConfirmedRank int,
            DeathRank int
        );

        INSERT INTO fact_counties_ranked
        SELECT
            t1.Date
            ,t1.FIPS
            ,t1.Confirmed
            ,t1.Confirmed - t3.Confirmed AS ConfirmedIncrease
            ,t1.Deaths
            ,t1.Deaths - t3.Deaths AS DeathsIncrease
            ,t1.ConfirmedPer1M
            ,t1.DeathsPer1M
            ,t1.DeltaConfirmedPer1M
            ,t1.DeltaDeathsPer1M
            ,t1.Avg5DaysConfirmedPer1M
            ,t1.Avg5DaysDeathsPer1M
            ,t2.ConfirmedRank
            ,t2.DeathRank
        FROM stage_with_latest t1
        LEFT JOIN stage_with_rank t2
            ON t1.FIPS = t2.FIPS
        LEFT JOIN stage_with_latest t3
            ON t1.FIPS = t3.FIPS
            AND t1.Date = t3.DatePlus1Day

        ORDER BY ConfirmedRank;

        CREATE UNIQUE INDEX idx_fact_counties_ranked ON fact_counties_ranked (FIPS, Date);

    ''')

    c.executescript('''
        DROP TABLE IF EXISTS fact_counties_7day_avg;

        CREATE TABLE fact_counties_7day_avg AS
        select
            fc1.fips,
            fc1.date,
            sum(fc2.ConfirmedIncrease) / 7.0 as Avg7DayConfirmedIncrease, 
            sum(fc2.DeathsIncrease) / 7.0 as Avg7DayDeathsIncrease,
            ROW_NUMBER() OVER (PARTITION BY fc1.FIPS ORDER BY fc1.Date) AS RankDateAsc,
            ROW_NUMBER() OVER (PARTITION BY fc1.FIPS ORDER BY fc1.Date DESC) AS RankDateDesc
        from fact_counties_ranked fc1
        join fact_counties_ranked fc2
            ON fc1.fips = fc2.fips
            AND fc2.date >= date(fc1.date, '-6 days')
            AND fc2.date <= fc1.date
        GROUP BY
            fc1.fips,
            fc1.date;
    ''')


    c.executescript('''

        DROP TABLE IF EXISTS stage_counties_7dayavg_month_change_overall;

        CREATE TABLE stage_counties_7dayavg_month_change_overall AS
            SELECT
                t1.FIPS
                ,t2.Date
                ,t2.Avg7DayConfirmedIncrease - t1.Avg7DayConfirmedIncrease AS MonthAvg7DayConfirmedIncrease
                ,(t2.Avg7DayConfirmedIncrease - t1.Avg7DayConfirmedIncrease) / t1.Avg7DayConfirmedIncrease AS MonthAvg7DayConfirmedIncreasePct
                ,t2.Avg7DayDeathsIncrease - t1.Avg7DayDeathsIncrease AS MonthAvg7DayDeathsIncrease
                ,(t2.Avg7DayDeathsIncrease - t1.Avg7DayDeathsIncrease) / t1.Avg7DayDeathsIncrease AS MonthAvg7DayDeathsIncreasePct
            FROM fact_counties_7day_avg t1
            JOIN fact_counties_7day_avg t2
                ON t1.FIPS = t2.FIPS
                AND t1.Date = date(t2.Date, '-30 days')
        ;

        CREATE UNIQUE INDEX idx_stage_counties_7dayavg_month_change_overall ON stage_counties_7dayavg_month_change_overall (FIPS, Date);

        --

        DROP TABLE IF EXISTS stage_counties_7dayavg_twoweek_change_overall;

        CREATE TABLE stage_counties_7dayavg_twoweek_change_overall AS
            SELECT
                t1.FIPS
                ,t2.Date
                ,t2.Avg7DayConfirmedIncrease - t1.Avg7DayConfirmedIncrease AS TwoWeekAvg7DayConfirmedIncrease
                ,(t2.Avg7DayConfirmedIncrease - t1.Avg7DayConfirmedIncrease) / t1.Avg7DayConfirmedIncrease AS TwoWeekAvg7DayConfirmedIncreasePct
                ,t2.Avg7DayDeathsIncrease - t1.Avg7DayDeathsIncrease AS TwoWeekAvg7DayDeathsIncrease
                ,(t2.Avg7DayDeathsIncrease - t1.Avg7DayDeathsIncrease) / t1.Avg7DayDeathsIncrease AS TwoWeekAvg7DayDeathsIncreasePct
            FROM fact_counties_7day_avg t1
            JOIN fact_counties_7day_avg t2
                ON t1.FIPS = t2.FIPS
                AND t1.Date = date(t2.Date, '-14 days')
        ;

        CREATE UNIQUE INDEX idx_stage_counties_7dayavg_twoweek_change_overall ON stage_counties_7dayavg_twoweek_change_overall (FIPS, Date);

        --

        DROP TABLE IF EXISTS stage_counties_one_week_change;

        CREATE TABLE stage_counties_one_week_change AS
            SELECT
                t1.FIPS
                ,t2.Date
                ,t2.Confirmed - coalesce(t1.Confirmed, 0) AS OneWeekConfirmedIncrease
                ,(t2.Confirmed - t1.Confirmed) / CAST(t1.Confirmed AS REAL) AS OneWeekConfirmedIncreasePct
            FROM fact_counties_ranked t1
            JOIN fact_counties_ranked t2
                ON t1.FIPS = t2.FIPS
                AND t1.Date = date(t2.Date, '-7 days')
        ;

        CREATE UNIQUE INDEX idx_stage_counties_one_week_change ON stage_counties_one_week_change (FIPS, Date);

        --

        DROP TABLE IF EXISTS stage_counties_two_week_change;

        CREATE TABLE stage_counties_two_week_change AS
            SELECT
                t1.FIPS
                ,t2.Date
                ,t2.Confirmed - coalesce(t1.Confirmed, 0) AS TwoWeekConfirmedIncrease
                ,(t2.Confirmed - t1.Confirmed) / CAST(t1.Confirmed AS REAL) AS TwoWeekConfirmedIncreasePct
            FROM fact_counties_ranked t1
            JOIN fact_counties_ranked t2
                ON t1.FIPS = t2.FIPS
                AND t1.Date = date(t2.Date, '-14 days')
        ;

        CREATE UNIQUE INDEX idx_stage_counties_two_week_change ON stage_counties_two_week_change (FIPS, Date);

        --

        DROP TABLE IF EXISTS stage_counties_month_change;

        CREATE TABLE stage_counties_month_change AS
            SELECT
                t1.FIPS
                ,t2.Date
                ,t2.Confirmed - coalesce(t1.Confirmed,0) AS MonthConfirmedIncrease
                ,(t2.Confirmed - t1.Confirmed) / CAST(t1.Confirmed AS REAL) AS MonthConfirmedIncreasePct
            FROM fact_counties_ranked t1
            JOIN fact_counties_ranked t2
                ON t1.FIPS = t2.FIPS
                AND t1.Date = date(t2.Date, '-30 days')
        ;

        CREATE UNIQUE INDEX idx_stage_counties_month_change ON stage_counties_month_change (FIPS, Date);

        --

        DROP TABLE IF EXISTS stage_counties_doubling_time;

        CREATE TABLE stage_counties_doubling_time AS
            SELECT
                FIPS
                ,Date
                --,Confirmed
                --,EarlierConfirmed
                --,DateDiffDays
                ,ROUND(Confirmed / ((Confirmed - EarlierConfirmed) / CAST(DateDiffDays AS REAL)) / 2, 2) AS DoublingTimeDays
            FROM
            (
                SELECT
                    t1.FIPS
                    ,t2.Date
                    ,t2.Confirmed
                    ,t1.Date AS EarlierDate
                    ,t1.Confirmed AS EarlierConfirmed
                    ,cast(julianday(t2.Date) as int) - cast(julianday(date(t1.Date)) as int) AS DateDiffDays
                    ,ROW_NUMBER() OVER (PARTITION BY
                            t1.FIPS
                            ,t2.Date
                        ORDER BY t1.Date DESC) AS Rank
                FROM fact_counties_ranked t1
                JOIN fact_counties_ranked t2
                    ON t1.FIPS = t2.FIPS
                    AND t1.Date < t2.Date
                    AND t1.Confirmed <= t2.Confirmed / 2
            ) T
            WHERE Rank = 1
        ;

        CREATE UNIQUE INDEX idx_stage_counties_doubling_time ON stage_counties_doubling_time (FIPS, Date);

        --

        DROP TABLE IF EXISTS stage_counties_outbreak;

        CREATE TABLE stage_counties_outbreak AS
            SELECT
                t1.FIPS
                ,t2.Date
                ,CASE WHEN t1.Confirmed > 100 AND t2.Avg7DayConfirmedIncrease >= t1_avg.Avg7DayConfirmedIncrease * 2 THEN 1 ELSE 0 END AS OutbreakFlag
            FROM fact_counties_ranked t1
            JOIN fact_counties_7day_avg t1_avg
                ON t1.FIPS = t1_avg.FIPS
                AND t1.Date = t1_avg.Date
            JOIN fact_counties_7day_avg t2
                ON t1.FIPS = t2.FIPS
                AND t1.Date = date(t2.Date, '-14 days')
        ;

        CREATE UNIQUE INDEX idx_stage_counties_outbreak ON stage_counties_outbreak (FIPS, Date);

        --

        DROP TABLE IF EXISTS fact_counties_progress;

        CREATE TABLE fact_counties_progress AS
        SELECT
            t.FIPS,
            t.Date,
            t.Confirmed,
            t.ConfirmedIncrease,
            CAST(t.ConfirmedIncrease as REAL) / (t.Confirmed - t.ConfirmedIncrease) AS ConfirmedIncreasePct,
            Avg7DayConfirmedIncrease,
            coalesce(OneWeekConfirmedIncrease, 0) AS OneWeekConfirmedIncrease,
            coalesce(OneWeekConfirmedIncreasePct, 0) AS OneWeekConfirmedIncreasePct,
            coalesce(TwoWeekConfirmedIncrease, 0) AS TwoWeekConfirmedIncrease,
            coalesce(TwoWeekConfirmedIncreasePct, 0) AS TwoWeekConfirmedIncreasePct,
            coalesce(MonthConfirmedIncrease, 0) AS MonthConfirmedIncrease,
            coalesce(MonthConfirmedIncreasePct, 0) AS MonthConfirmedIncreasePct,
            coalesce(TwoWeekAvg7DayConfirmedIncrease, 0) as TwoWeekAvg7DayConfirmedIncrease,
            coalesce(TwoWeekAvg7DayConfirmedIncreasePct, 0) as TwoWeekAvg7DayConfirmedIncreasePct,
            coalesce(MonthAvg7DayConfirmedIncrease, 0) AS MonthAvg7DayConfirmedIncrease,
            coalesce(MonthAvg7DayConfirmedIncreasePct, 0) AS MonthAvg7DayConfirmedIncreasePct,
            t.Deaths,
            t.DeathsIncrease,
            CAST(t.DeathsIncrease as REAL) / (t.Deaths - t.DeathsIncrease) AS DeathsIncreasePct,
            MonthAvg7DayDeathsIncrease,
            MonthAvg7DayDeathsIncreasePct,
            DoublingTimeDays,
            OutbreakFlag
        FROM fact_counties_ranked t
        JOIN dim_county c
            ON t.FIPS = c.FIPS
        JOIN fact_counties_7day_avg current_7dayavg
            ON t.FIPS = current_7dayavg.FIPS AND t.Date = current_7dayavg.Date
        LEFT JOIN stage_counties_7dayavg_month_change_overall o
            ON t.FIPS = o.FIPS AND t.Date = o.Date
        LEFT JOIN stage_counties_7dayavg_twoweek_change_overall two_7dayavg
            ON t.FIPS = two_7dayavg.FIPS AND t.Date = two_7dayavg.Date
        LEFT JOIN stage_counties_one_week_change one
            ON t.FIPS = one.FIPS AND t.Date = one.Date
        LEFT JOIN stage_counties_two_week_change two
            ON t.FIPS = two.FIPS AND t.Date = two.Date
        LEFT JOIN stage_counties_month_change mon
            ON t.FIPS = mon.FIPS AND t.Date = mon.Date
        LEFT JOIN stage_counties_doubling_time doub
            ON t.FIPS = doub.FIPS AND t.Date = doub.Date
        LEFT JOIN stage_counties_outbreak outbreak
            ON t.FIPS = outbreak.FIPS AND t.Date = outbreak.Date
        WHERE 
            c.state is not null
            AND lower(County) <> 'unassigned'
            AND County not like 'Out of%'
        ;

        CREATE UNIQUE INDEX idx_fact_counties_progress ON fact_counties_progress (FIPS, Date);
    ''')

    c.executescript('''
        DROP TABLE stage_csse_filtered;
        DROP TABLE stage_with_population;
        DROP TABLE stage_with_deltas;
        DROP TABLE stage_latest;
        DROP TABLE stage_with_latest;
        DROP TABLE stage_with_rank;
    ''')
    conn.commit()

    c.close()

    pathlib.Path('stage/dimensional_models.loaded').touch()


def row_to_dict(row):
    d = {}
    for column in row.keys():
        d[column] = row[column]
    return d


@timer
def export_counties_ranked():

    conn = get_db_conn()

    print("exporting output_counties_ranked")

    c = conn.cursor()

    c.executescript('''
        DROP VIEW IF EXISTS output_counties_ranked;

        -- TODO: don't mangle names here; this change needs to be coordinated with JS
        CREATE VIEW output_counties_ranked AS
        SELECT
            substr(Date, 1, 4) || substr(Date, 6, 2) || substr(Date, 9, 2) AS Date,
            c.FIPS,
            County AS Admin2,
            c.State AS Province_State,
            StateAbbrev AS StateAbbrev,
            Confirmed,
            Deaths,
            c.Population,
            ConfirmedPer1M,
            DeathsPer1M,
            DeltaConfirmedPer1M,
            DeltaDeathsPer1M,
            Avg5DaysConfirmedPer1M,
            Avg5DaysDeathsPer1M,
            ConfirmedRank,
            DeathRank
        FROM fact_counties_ranked cr
        JOIN dim_county c
            ON cr.FIPS = c.FIPS
        JOIN dim_state s
            ON c.State = s.State
    ''')

    c.execute('''
        SELECT
            *
        FROM output_counties_ranked
        ORDER BY FIPS, Date;
    ''')



    rows = c.fetchall()

    blank_if_none = lambda value: str(value) if value is not None else ""

    buf = io.StringIO()
    for column in rows[0].keys():
        buf.write(column)
        buf.write("\t")
    buf.write("\n")
    for row in rows[1:]:
        buf.write("\t".join([blank_if_none(value) for value in row]) + "\n")

    with codecs.open("data/counties_ranked.csv", "w", encoding='utf8') as f:
        f.write(buf.getvalue())


    with codecs.open("data/counties_ranked.json", "w", encoding='utf8') as f:
        f.write(json.dumps([row_to_dict(row) for row in rows]))

    conn.close()


@timer
def export_counties_rate_of_change():

    conn = get_db_conn()

    print("exporting counties_rate_of_change")

    c = conn.cursor()

    c.execute('''
        SELECT
            t.FIPS,
            Date,
            c.County,
            s.StateAbbrev as State,
            c.Lat,
            c.Long_,
            Confirmed,
            ConfirmedIncrease,
            ConfirmedIncreasePct,
            Avg7DayConfirmedIncrease,
            OneWeekConfirmedIncrease,
            OneWeekConfirmedIncreasePct,
            TwoWeekConfirmedIncrease,
            TwoWeekConfirmedIncreasePct,
            MonthConfirmedIncrease,
            MonthConfirmedIncreasePct,
            TwoWeekAvg7DayConfirmedIncrease,
            TwoWeekAvg7DayConfirmedIncreasePct,
            MonthAvg7DayConfirmedIncrease,
            MonthAvg7DayConfirmedIncreasePct,
            Deaths,
            DeathsIncrease,
            DeathsIncreasePct,
            MonthAvg7DayDeathsIncrease,
            MonthAvg7DayDeathsIncreasePct,
            c.Population,
            OutbreakFlag
        FROM fact_counties_progress t
        JOIN dim_county c
            ON t.FIPS = c.FIPS
        JOIN dim_state s
            ON c.State = s.State
        WHERE
            Date = (SELECT MAX(Date) FROM fact_counties_progress)
        ORDER BY t.FIPS;
    ''')

    rows = c.fetchall()

    with codecs.open("data/counties_rate_of_change.json", "w", encoding='utf8') as f:
        f.write(json.dumps([row_to_dict(row) for row in rows]))

    conn.close()


@timer
def export_counties_7day_avg():

    conn = get_db_conn()

    print("exporting counties_7day_avg")

    c = conn.cursor()

    #### for sparklines

    c.execute('''
        SELECT
            FIPS, Date, Avg7DayConfirmedIncrease, Avg7DayDeathsIncrease
        FROM fact_counties_7day_avg
        WHERE
            Date >= date((SELECT MAX(date) FROM fact_counties_ranked), '-30 days')
        ORDER BY date;
    ''')

    rows = c.fetchall()

    # with codecs.open("data/counties_7day_avg.json", "w", encoding='utf8') as f:
    #     f.write(json.dumps([row_to_dict(row) for row in rows]))

    buf = io.StringIO()
    for column in rows[0].keys():
        buf.write(column)
        buf.write("\t")
    buf.write("\n")
    for row in rows[1:]:
        for value in row:
            buf.write(str(value))
            buf.write("\t")
        buf.write("\n")

    with codecs.open("data/counties_7day_avg.txt", "w", encoding='utf8') as f:
        f.write(buf.getvalue())

    conn.close()


@timer
def export_state_info():

    conn = get_db_conn()

    c = conn.cursor()

    c.execute('''
        SELECT *
        FROM dim_state;
    ''')

    rows = c.fetchall()

    with codecs.open("data/state_population.json", "w", encoding='utf8') as f:
        f.write(json.dumps([row_to_dict(row) for row in rows]))

    conn.close()


def get_db_conn():
    conn = sqlite3.connect('stage/covid19.db')
    conn.row_factory = sqlite3.Row
    return conn


def load_reference_data():

    conn = get_db_conn()

    load_county_population(conn)

    load_county_acs_vars(conn)

    load_county_gazetteer(conn)

    load_state_info(conn)

    pathlib.Path('stage/reference_data.loaded').touch()


def create_exports():

    processes = [
        multiprocessing.Process(target=export_state_info)
        ,multiprocessing.Process(target=export_counties_rate_of_change)
        ,multiprocessing.Process(target=export_counties_7day_avg)
    ]

    for p in processes:
        p.start()

    for p in processes:
        p.join()


def load_all_data():

    #### load source data

    load_csse()

    load_reference_data()

    #### transform into dim/fact

    create_dimensional_tables()

    #### exports

    create_exports()
