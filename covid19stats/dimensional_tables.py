
from .common import get_db_conn, timer, touch_file


def create_dim_state(conn):

    print("dim_state")

    c = conn.cursor()

    c.execute('DROP TABLE IF EXISTS dim_state')

    c.execute('''
        CREATE TABLE dim_state
        AS SELECT
            NAME AS State
            ,Abbreviation AS StateAbbrev
            ,CAST(POPESTIMATE2019 as INT) as Population
        FROM raw_nst_population t1
        LEFT JOIN raw_state_abbreviations t2
            ON t1.NAME = t2.State
        WHERE CAST(t1.STATE AS INT) > 0
    ''')

    conn.commit()


def create_dim_date(conn):

    print("dim_date")

    c = conn.cursor()

    c.executescript('''
        DROP TABLE IF EXISTS dim_date;

        CREATE TABLE dim_date (
            Date text,
            Minus1Day text,
            Minus7Days text,
            Minus14Days text,
            Minus30Days text
        );

        WITH RECURSIVE dates(Date) AS (
          VALUES('2020-01-01')
          UNION ALL
          SELECT
            date(date, '+1 day')
          FROM dates
          WHERE date < '2020-12-31'
        )
        INSERT INTO dim_date (Date)
        SELECT date FROM dates;


        UPDATE dim_date
        SET
            Minus1Day = date(date, '-1 day')
            ,Minus7Days = date(date, '-7 days')
            ,Minus14Days = date(date, '-14 days')
            ,Minus30Days = date(date, '-30 days')
        ;

        CREATE INDEX ix_dim_date1 ON dim_date(Date);
        CREATE INDEX ix_dim_date2 ON dim_date(Date, Minus1Day);
        CREATE INDEX ix_dim_date3 ON dim_date(Date, Minus7Days);
        CREATE INDEX ix_dim_date4 ON dim_date(Date, Minus14Days);
        CREATE INDEX ix_dim_date5 ON dim_date(Date, Minus30Days);
    ''')

    conn.commit()


def create_dim_county_and_fact_counties_base(conn):

    c = conn.cursor()

    print("stage_regions_with_data")

    c.executescript('''
        DROP TABLE IF EXISTS stage_regions_with_data;

        CREATE TABLE stage_regions_with_data AS
            -- only take U.S. regions that have data
            select FIPS
            from final_csse
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
            FROM final_csse t1
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
        LEFT JOIN final_fips_population t2
            ON t1.FIPS = t2.FIPS
        LEFT JOIN final_county_acs t3
            ON t1.FIPS = t3.state_and_county
        WHERE t1.DateRank = 1
    ''')

    c.executescript('''
        DROP TABLE IF EXISTS fact_counties_base;

        CREATE TABLE fact_counties_base (
            Date TEXT,
            FIPS TEXT,
            Confirmed INT,
            ConfirmedIncrease INT,
            Deaths INT,
            DeathsIncrease INT,
            Recovered INT,
            Active INT,
            Avg7DayConfirmedIncrease REAL,
            Avg7DayDeathsIncrease REAL
        );

        INSERT INTO fact_counties_base (
            Date,
            FIPS,
            Confirmed,
            ConfirmedIncrease,
            Deaths,
            DeathsIncrease,
            Recovered,
            Active,
            Avg7DayConfirmedIncrease,
            Avg7DayDeathsIncrease
        )
        SELECT
            t1.Date,
            t1.FIPS,
            t1.Confirmed,
            t1.Confirmed - earlier.Confirmed AS ConfirmedIncrease,
            t1.Deaths,
            t1.Deaths - earlier.Deaths AS DeathsIncrease,
            t1.Recovered,
            t1.Active,
            NULL AS Avg7DayConfirmedIncrease,
            NULL AS Avg7DayDeathsIncrease
        FROM stage_csse_filtered t1
        JOIN dim_date t1_date
            ON t1.Date = t1_date.Date
        LEFT JOIN stage_csse_filtered earlier
            ON t1.FIPS = earlier.FIPS
            AND t1_date.Minus1Day = earlier.Date
        ;
        
        CREATE UNIQUE INDEX idx_fact_counties_base ON fact_counties_base (FIPS, Date);
     ''')


    c.executescript('''
        DROP TABLE IF EXISTS stage_counties_7day_avg;

        CREATE TABLE stage_counties_7day_avg AS
        select
            fc1.fips,
            fc1.date,
            sum(fc2.ConfirmedIncrease) / 7.0 as Avg7DayConfirmedIncrease, 
            sum(fc2.DeathsIncrease) / 7.0 as Avg7DayDeathsIncrease
        from fact_counties_base fc1
        join dim_date fc1_date
            ON fc1.date = fc1_date.date
        join fact_counties_base fc2
            ON fc1.fips = fc2.fips
            AND fc2.date > fc1_date.Minus7Days
            AND fc2.date <= fc1.date
        GROUP BY
            fc1.fips,
            fc1.date;

        CREATE UNIQUE INDEX idx_stage_counties_7day_avg ON stage_counties_7day_avg (FIPS, Date);

        UPDATE fact_counties_base
        SET
            Avg7DayConfirmedIncrease = (
                SELECT Avg7DayConfirmedIncrease
                FROM stage_counties_7day_avg t
                WHERE
                    fact_counties_base.FIPS = t.FIPS
                    AND fact_counties_base.Date = t.Date
                )
            ,Avg7DayDeathsIncrease = (
                SELECT Avg7DayDeathsIncrease
                FROM stage_counties_7day_avg t
                WHERE
                    fact_counties_base.FIPS = t.FIPS
                    AND fact_counties_base.Date = t.Date
                )
        ;
    ''')

    conn.commit()


def create_fact_counties_ranked(conn):

    c = conn.cursor()

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
            FROM fact_counties_base t1
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
                --,date(t1.Date, '+1 day') as DatePlus1Day
                ,t2.LatestConfirmedPer1M
                ,t2.LatestDeathsPer1M
            FROM stage_with_deltas t1
            LEFT JOIN stage_latest t2
                ON t1.FIPS = t2.FIPS;

        CREATE INDEX idx_stage_with_latest ON stage_with_latest (FIPS, LatestConfirmedPer1M, LatestDeathsPer1M);
        CREATE INDEX idx_stage_with_latest2 ON stage_with_latest (FIPS, Date);
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
        JOIN dim_date t1_date
            ON t1.date = t1_date.date
        LEFT JOIN stage_with_rank t2
            ON t1.FIPS = t2.FIPS
        LEFT JOIN stage_with_latest t3
            ON t1.FIPS = t3.FIPS
            AND t1_date.Minus1Day = t3.Date

        ORDER BY ConfirmedRank;

        CREATE UNIQUE INDEX idx_fact_counties_ranked ON fact_counties_ranked (FIPS, Date);

    ''')

    conn.commit()


def create_fact_counties_progress(conn):

    print("fact_counties_progress")

    c = conn.cursor()

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
            FROM fact_counties_base t1
            JOIN fact_counties_base t2
                ON t1.FIPS = t2.FIPS
            JOIN dim_date t2_date
                ON t2.date = t2_date.date
                AND t1.Date = t2_date.Minus30Days
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
            FROM fact_counties_base t1
            JOIN fact_counties_base t2
                ON t1.FIPS = t2.FIPS
            JOIN dim_date t2_date
                ON t2.date = t2_date.date
                AND t1.Date = t2_date.Minus14Days
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
            FROM fact_counties_base t1
            JOIN fact_counties_base t2
                ON t1.FIPS = t2.FIPS
            JOIN dim_date t2_date
                ON t2.date = t2_date.date
                AND t1.Date = t2_date.Minus7Days
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
            FROM fact_counties_base t1
            JOIN fact_counties_base t2
                ON t1.FIPS = t2.FIPS
            JOIN dim_date t2_date
                ON t2.date = t2_date.date
                AND t1.Date = t2_date.Minus14Days
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
            FROM fact_counties_base t1
            JOIN fact_counties_base t2
                ON t1.FIPS = t2.FIPS
            JOIN dim_date t2_date
                ON t2.date = t2_date.date
                AND t1.Date = t2_date.Minus30Days
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
                FROM fact_counties_base t1
                JOIN fact_counties_base t2
                    ON t1.FIPS = t2.FIPS
                    AND t1.Date < t2.Date
                    AND t1.Confirmed <= t2.Confirmed / 2
            ) T
            WHERE Rank = 1
        ;

        CREATE UNIQUE INDEX idx_stage_counties_doubling_time ON stage_counties_doubling_time (FIPS, Date);

        --

    ''')

    c.executescript('''

        DROP TABLE IF EXISTS fact_counties_progress;

        CREATE TABLE fact_counties_progress (
            FIPS TEXT,
            Date TEXT,
            Confirmed INT,
            ConfirmedIncrease INT,
            ConfirmedIncreasePct REAL,
            Avg7DayConfirmedIncrease REAL,
            OneWeekConfirmedIncrease INT,
            OneWeekConfirmedIncreasePct REAL,
            TwoWeekConfirmedIncrease INT,
            TwoWeekConfirmedIncreasePct REAL,
            MonthConfirmedIncrease INT,
            MonthConfirmedIncreasePct REAL,
            TwoWeekAvg7DayConfirmedIncrease REAL,
            TwoWeekAvg7DayConfirmedIncreasePct REAL,
            MonthAvg7DayConfirmedIncrease REAL,
            MonthAvg7DayConfirmedIncreasePct REAL,
            Deaths INT,
            DeathsIncrease INT,
            DeathsIncreasePct REAL,
            MonthAvg7DayDeathsIncrease REAL,
            MonthAvg7DayDeathsIncreasePct REAL,
            DoublingTimeDays REAL,
            CasesPer100k REAL,
            OneWeekCasesPer100kChange REAL,
            OneWeekCasesPer100kChangePct REAL
        );

        INSERT INTO fact_counties_progress
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
            coalesce(TwoWeekConfirmedIncrease, 0) * cast(100000 as real) / c.Population as CasesPer100k,
            NULL AS OneWeekCasesPer100kChange,
            NULL AS OneWeekCasesPer100kChangePct
        FROM fact_counties_base t
        JOIN dim_county c
            ON t.FIPS = c.FIPS
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
        ;

        CREATE UNIQUE INDEX idx_fact_counties_progress ON fact_counties_progress (FIPS, Date);
    ''')

    c.executescript('''

        DROP TABLE IF EXISTS stage_counties_cases_per_100k_change;

        CREATE TABLE stage_counties_cases_per_100k_change AS
            SELECT
                t1.FIPS
                ,t2.Date
                ,coalesce(t2.CasesPer100k, 0) - coalesce(t1.CasesPer100k, 0) AS OneWeekCasesPer100kChange
                ,(coalesce(t2.CasesPer100k, 0) - coalesce(t1.CasesPer100k, 0)) / t1.CasesPer100k AS OneWeekCasesPer100kChangePct
            FROM fact_counties_progress t1
            JOIN fact_counties_progress t2
                ON t1.FIPS = t2.FIPS
            JOIN dim_date t2_date
                ON t2.date = t2_date.date
                AND t1.Date = t2_date.Minus7Days
        ;

        CREATE UNIQUE INDEX idx_stage_counties_cases_per_100k_change ON stage_counties_cases_per_100k_change (FIPS, Date);

        UPDATE fact_counties_progress
        SET
            OneWeekCasesPer100kChange = coalesce((
                SELECT OneWeekCasesPer100kChange
                FROM stage_counties_cases_per_100k_change t
                WHERE
                    fact_counties_progress.FIPS = t.FIPS
                    AND fact_counties_progress.Date = t.Date
                ), 0)
            ,OneWeekCasesPer100kChangePct = coalesce((
                SELECT coalesce(OneWeekCasesPer100kChangePct, 0)
                FROM stage_counties_cases_per_100k_change t
                WHERE
                    fact_counties_progress.FIPS = t.FIPS
                    AND fact_counties_progress.Date = t.Date
                ), 0)
        ;
    ''')

    conn.commit()


@timer
def create_dimensional_tables():

    conn = get_db_conn()

    create_dim_state(conn)

    create_dim_date(conn)

    create_dim_county_and_fact_counties_base(conn)

    # this is obsolete. going forward, nothing should use this.
    create_fact_counties_ranked(conn)

    create_fact_counties_progress(conn)

    touch_file('stage/dimensional_models.loaded')


if __name__ == "__main__":
    create_dimensional_tables()

