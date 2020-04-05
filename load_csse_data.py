
# python >= 3.7.4

import codecs
from collections import namedtuple
import csv
import glob
import json
import os
import os.path
import sqlite3
import sys
import urllib.request


Path = namedtuple('Path', ['path', 'date'])


def get_sortable_date(path):
    basename = os.path.basename(path)
    return basename[6:10] + basename[0:2] + basename[3:5]


def load_csse(conn):

    # assumes COVID-19 repo has been cloned to home directory
    spec = os.path.join(os.getenv("HOME"), 'COVID-19/csse_covid_19_data/csse_covid_19_daily_reports/*.csv')
    print(f"Looking for files: {spec}")
    paths = [Path(path, get_sortable_date(path)) for path in glob.glob(spec)]

    filtered_paths = [p for p in paths if p.date >= '20200322']

    all_rows = []

    for path in filtered_paths:
        print(f"Loading from {path.path}")
        with codecs.open(path.path, encoding='utf8') as f:
            reader = csv.reader(f)
            rows = [[path.date] + row for row in reader]
            rows = rows[1:]

            all_rows += rows

    print(f"Writing {len(all_rows)} rows to the database")

    c = conn.cursor()

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


def load_county_info(conn):

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
        DROP TABLE IF EXISTS county_population;
    ''')

    # Create table
    c.execute('''
        CREATE TABLE county_population ('''
            + ",".join([col + " text" for col in column_names]) +
        ''')
    ''')

    c.executemany('INSERT INTO county_population VALUES (' + ",".join(["?"] * len(column_names)) + ')', rows)

    conn.commit()

    c.execute('DROP TABLE IF EXISTS fips_population')

    c.execute('''
        CREATE TABLE fips_population
        AS SELECT
            STNAME,
            CTYNAME,
            STATE || COUNTY AS FIPS,
            CAST(POPESTIMATE2019 as INT) as Population
        FROM county_population;
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

    conn.commit()

    c.execute('DROP TABLE IF EXISTS state_population')

    c.execute('''
        CREATE TABLE state_population
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

    c.close()


def create_counties_ranked(conn):

    c = conn.cursor()

    c.execute('''
        DROP TABLE IF EXISTS counties_ranked;
    ''')

    c.execute('''
        CREATE TABLE counties_ranked (
            Date text,
            FIPS text,
            Admin2 text,
            Province_State text,
            StateAbbrev text,
            Country_Region text,
            Confirmed int,
            Deaths int,
            Population int,
            ConfirmedPer1M real,
            DeathsPer1M real,
            DeltaConfirmedPer1M real,
            DeltaDeathsPer1M real,
            Avg5DaysConfirmedPer1M real,
            Avg5DaysDeathsPer1M real,
            ConfirmedRank int,
            DeathRank int
        )
    ''')

    c.execute('''
        WITH RegionsWithData AS (
            -- only take U.S. regions that have data
            select FIPS
            from csse
            where
                Country_Region = 'US'
                AND LENGTH(FIPS) > 0
            group by FIPS
            Having SUM(Confirmed > 0) or SUM(Deaths) > 0 or SUM(Recovered) > 0 or sum(active) > 0
        )
        ,Filtered AS (
            SELECT
                t1.*
            FROM csse t1
            JOIN RegionsWithData t2
                ON t1.FIPS = t2.FIPS
        )
        ,WithPopulation AS (
            SELECT
                t1.*
                ,Population
                ,CAST(Confirmed AS REAL) / (CAST(Population AS REAL) / 1000000) as ConfirmedPer1M
                ,CAST(Deaths AS REAL) / (CAST(Population AS REAL) / 1000000) as DeathsPer1M
                ,ROW_NUMBER() OVER (PARTITION BY t1.FIPS ORDER BY Date DESC) As DateRank
            FROM Filtered t1
            LEFT JOIN fips_population t2
                ON t1.FIPS = t2.FIPS 
        )
        ,WithDeltas as (
            select
                t1.*
                ,t1.ConfirmedPer1M - t2.ConfirmedPer1M as DeltaConfirmedPer1M
                ,t1.DeathsPer1M - t2.DeathsPer1M as DeltaDeathsPer1M
                ,(t1.ConfirmedPer1M - t3.ConfirmedPer1M) / 5.0 as Avg5DaysConfirmedPer1M
                ,(t1.DeathsPer1M - t3.DeathsPer1M) / 5.0 as Avg5DaysDeathsPer1M
            from WithPopulation t1
            left join WithPopulation t2 
                on t1.fips = t2.fips
                and t1.DateRank = t2.DateRank - 1
            left join WithPopulation t3
                on t1.fips = t3.fips
                and t1.DateRank = t3.DateRank - 5
        )
        ,Latest AS (
            SELECT
                FIPS
                ,MAX(ConfirmedPer1M) AS LatestConfirmedPer1M
                ,MAX(DeathsPer1M) AS LatestDeathsPer1M
            FROM WithDeltas
            WHERE DateRank = 1
            GROUP BY FIPS
        )
        ,WithLatest as (
            SELECT
                t1.*
                ,t2.LatestConfirmedPer1M
                ,t2.LatestDeathsPer1M
            FROM WithDeltas t1
            LEFT JOIN Latest t2
                ON t1.FIPS = t2.FIPS
        )
        ,WithRank AS (
            SELECT
                FIPS
                ,LatestConfirmedPer1M
                ,LatestDeathsPer1M
                ,ROW_NUMBER() OVER (ORDER BY LatestConfirmedPer1M DESC) As ConfirmedRank
                ,ROW_NUMBER() OVER (ORDER BY LatestDeathsPer1M DESC) As DeathRank
            FROM WithLatest
            GROUP BY FIPS, LatestConfirmedPer1M, LatestDeathsPer1M
        )
        INSERT INTO counties_ranked
        SELECT
            Date
            ,t1.FIPS
            ,Admin2
            ,Province_State
            ,Abbreviation as StateAbbrev
            ,Country_Region
            ,Confirmed
            ,Deaths
            ,Population
            ,ConfirmedPer1M
            ,DeathsPer1M
            ,DeltaConfirmedPer1M
            ,DeltaDeathsPer1M
            ,Avg5DaysConfirmedPer1M
            ,Avg5DaysDeathsPer1M
            ,ConfirmedRank
            ,DeathRank
        FROM WithLatest t1
        LEFT JOIN WithRank t2
            ON t1.FIPS = t2.FIPS
        LEFT JOIN state_abbreviations t3
            ON t1.Province_State = t3.State
        ORDER BY ConfirmedRank
    ''')

    conn.commit()

    c.close()


def row_to_dict(row):
    d = {}
    for column in row.keys():
        d[column] = row[column]
    return d


def export_counties_ranked(conn):

    c = conn.cursor()

    c.execute('''
        SELECT * FROM counties_ranked ORDER BY FIPS, Date;
    ''')

    rows = c.fetchall()

    with codecs.open("data/counties_ranked.csv", "w", encoding='utf8') as f:
        for column in rows[0].keys():
            f.write(column)
            f.write("\t")
        f.write("\n")
        for row in rows[1:]:
            for value in row:
                f.write(str(value))
                f.write("\t")
            f.write("\n")

    with codecs.open("data/counties_ranked.json", "w", encoding='utf8') as f:
        f.write(json.dumps([row_to_dict(row) for row in rows]))


def export_state_info(conn):

    c = conn.cursor()

    c.execute('''
        SELECT * FROM state_population;
    ''')

    rows = c.fetchall()

    with codecs.open("data/state_population.json", "w", encoding='utf8') as f:
        f.write(json.dumps([row_to_dict(row) for row in rows]))


conn = sqlite3.connect('stage/covid19.db')
conn.row_factory = sqlite3.Row

load_csse(conn)
load_county_info(conn)
create_counties_ranked(conn)
export_counties_ranked(conn)

load_state_info(conn)
export_state_info(conn)

conn.close()

