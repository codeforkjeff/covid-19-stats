
import codecs
import csv
import io
import json
import multiprocessing

from google.cloud import bigquery

from .common import get_db_conn, timer, row_to_dict, get_bq_client, sync_to_bucket, public_bucket, get_gcs_client

public_bucket = 'codeforkjeff-covid-19-public'

@timer
def export_counties_ranked():

    conn = get_db_conn()

    print("exporting output_counties_ranked")

    c = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

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
    for row in rows:
        buf.write("\t".join([blank_if_none(value) for value in row]) + "\n")

    with codecs.open("data/counties_ranked.csv", "w", encoding='utf8') as f:
        f.write(buf.getvalue())


    with codecs.open("data/counties_ranked.json", "w", encoding='utf8') as f:
        f.write(json.dumps([row_to_dict(row) for row in rows]))

    conn.close()


@timer
def export_counties_rate_of_change():

    client = get_bq_client()

    print("exporting counties_rate_of_change")

    query_job = client.query('''
        SELECT
            t.FIPS,
            FORMAT_DATE("%F", Date) AS Date,
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
            CasesPer100k,
            OneWeekCasesPer100kChange,
            OneWeekCasesPer100kChangePct
        FROM fact_counties_progress t
        JOIN dim_county c
            ON t.FIPS = c.FIPS
        JOIN dim_state s
            ON c.State = s.State
        WHERE
            Date = (SELECT MAX(Date) FROM fact_counties_progress)
        ORDER BY t.FIPS, Date;
    ''')

    rows = query_job.result()

    # preserve camelcase

    columns = [
        'FIPS',
        'Date',
        'County',
        'State',
        'Lat',
        'Long_',
        'Confirmed',
        'ConfirmedIncrease',
        'ConfirmedIncreasePct',
        'Avg7DayConfirmedIncrease',
        'OneWeekConfirmedIncrease',
        'OneWeekConfirmedIncreasePct',
        'TwoWeekConfirmedIncrease',
        'TwoWeekConfirmedIncreasePct',
        'MonthConfirmedIncrease',
        'MonthConfirmedIncreasePct',
        'TwoWeekAvg7DayConfirmedIncrease',
        'TwoWeekAvg7DayConfirmedIncreasePct',
        'MonthAvg7DayConfirmedIncrease',
        'MonthAvg7DayConfirmedIncreasePct',
        'Deaths',
        'DeathsIncrease',
        'DeathsIncreasePct',
        'MonthAvg7DayDeathsIncrease',
        'MonthAvg7DayDeathsIncreasePct',
        'Population',
        'CasesPer100k',
        'OneWeekCasesPer100kChange',
        'OneWeekCasesPer100kChangePct'
    ]

    new_rows = []

    for row in rows:
        new_row = {}
        for col in columns:
            new_row[col] = row[col]
        new_rows.append(new_row)

    with codecs.open("data/counties_rate_of_change.json", "w", encoding='utf8') as f:
        f.write(json.dumps([row_to_dict(row) for row in new_rows]))

    client.close()

    sync_to_bucket("data/counties_rate_of_change.json", f"gs://{public_bucket}/counties_rate_of_change.json")


@timer
def export_counties_7day_avg():

    client = get_bq_client()

    print("exporting counties_7day_avg")

    #### for sparklines

    query_job = client.query('''
        SELECT
            FIPS, Date, Avg7DayConfirmedIncrease, Avg7DayDeathsIncrease
        FROM fact_counties_base
        WHERE
            Date >= DATE_SUB((SELECT MAX(date) FROM fact_counties_base), interval 30 day)
        ORDER BY date, FIPS;
    ''')

    rows = query_job.result()

    # with codecs.open("data/counties_7day_avg.json", "w", encoding='utf8') as f:
    #     f.write(json.dumps([row_to_dict(row) for row in rows]))

    buf = io.StringIO()
    # for column in rows[0].keys():
    #     buf.write(column)
    #     buf.write("\t")
    # buf.write("\n")
    buf.write("FIPS\tDate\tAvg7DayConfirmedIncrease\tAvg7DayDeathsIncrease\n")
    for row in rows:
        buf.write("\t".join([str(value) for value in row]))
        buf.write("\n")

    with codecs.open("data/counties_7day_avg.txt", "w", encoding='utf8') as f:
        f.write(buf.getvalue())

    client.close()

    sync_to_bucket("data/counties_7day_avg.txt", f"gs://{public_bucket}/counties_7day_avg.txt")


@timer
def export_counties_casesper100k():

    client = get_bq_client()

    print("exporting counties_casesper100k")

    #### for sparklines

    query_job = client.query('''
        SELECT
            FIPS, Date, CasesPer100k
        FROM fact_counties_progress
        WHERE
            Date >= DATE_SUB((SELECT MAX(date) FROM fact_counties_base), interval 30 day)
        ORDER BY FIPS, date;
    ''')

    rows = query_job.result()

    buf = io.StringIO()
    # for column in rows[0].keys():
    #     buf.write(column)
    #     buf.write("\t")
    # buf.write("\n")
    buf.write("FIPS\tDate\tCasesPer100k\n")
    for row in rows:
        buf.write("\t".join([str(value) for value in row]))
        buf.write("\n")

    with codecs.open("data/counties_casesper100k.txt", "w", encoding='utf8') as f:
        f.write(buf.getvalue())

    client.close()

    sync_to_bucket("data/counties_casesper100k.txt", f"gs://{public_bucket}/counties_casesper100k.txt")


@timer
def export_state_info():

    client = get_bq_client()

    sql = """
        SELECT *
        FROM models.dim_state
        ORDER BY state
    """
    query_job = client.query(sql)
    rows = query_job.result()

    with codecs.open("data/state_population.json", "w", encoding='utf8') as f:
        f.write(json.dumps([row_to_dict(row) for row in rows]))

    client.close()

    sync_to_bucket("data/state_population.json", f"gs://{public_bucket}/state_population.json")


def upload_shapefile():
    # not an export, but maps need this file
    sync_to_bucket("data/gz_2010_us_050_00_500k.json", f"gs://{public_bucket}/gz_2010_us_050_00_500k.json")


def set_allow_cors_on_bucket(bucket_name):

    bucket = get_gcs_client().get_bucket(bucket_name)
    bucket.cors = [
        {
            "origin": ["*"],
            "responseHeader": [
                "Content-Type",
                "x-goog-resumable"],
            "method": ['GET'],
            "maxAgeSeconds": 3600
        }
    ]
    bucket.patch()


def create_exports():

    processes = [
        multiprocessing.Process(target=export_state_info)
        ,multiprocessing.Process(target=export_counties_rate_of_change)
        ,multiprocessing.Process(target=export_counties_7day_avg)
        ,multiprocessing.Process(target=export_counties_casesper100k)
        ,multiprocessing.Process(target=upload_shapefile)
    ]

    for p in processes:
        p.start()

    for p in processes:
        p.join()

    # this really only needs to happen once ever on the bucket
    # but we set it every time this fn is called, just in case
    set_allow_cors_on_bucket(public_bucket)


if __name__ == "__main__":
    create_exports()
