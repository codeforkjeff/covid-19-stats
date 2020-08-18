
import codecs
import csv
import io
import json
import multiprocessing

from .common import get_db_conn, timer, row_to_dict


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
        FROM fact_counties_base
        WHERE
            Date >= date((SELECT MAX(date) FROM fact_counties_base), '-30 days')
        ORDER BY date, FIPS;
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
def export_counties_casesper100k():

    conn = get_db_conn()

    print("exporting counties_casesper100k")

    c = conn.cursor()

    #### for sparklines

    c.execute('''
        SELECT
            FIPS, Date, CasesPer100k
        FROM fact_counties_progress
        WHERE
            Date >= date((SELECT MAX(date) FROM fact_counties_progress), '-30 days')
        ORDER BY FIPS, date;
    ''')

    rows = c.fetchall()

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

    with codecs.open("data/counties_casesper100k.txt", "w", encoding='utf8') as f:
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


def create_exports():

    processes = [
        multiprocessing.Process(target=export_state_info)
        ,multiprocessing.Process(target=export_counties_rate_of_change)
        ,multiprocessing.Process(target=export_counties_7day_avg)
        ,multiprocessing.Process(target=export_counties_casesper100k)
    ]

    for p in processes:
        p.start()

    for p in processes:
        p.join()

if __name__ == "__main__":
    create_exports()
