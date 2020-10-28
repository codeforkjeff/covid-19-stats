
from .common import get_db_conn, timer, touch_file


def create_dim_state(conn):

    print("dim_state")

    c = conn.cursor()

    c.execute('DROP TABLE IF EXISTS dim_state')

    c.execute('''
        CREATE UNLOGGED TABLE dim_state
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

    c.execute('''
        DROP TABLE IF EXISTS dim_date;

        CREATE UNLOGGED TABLE dim_date (
            Date date,
            Minus1Day date,
            Minus7Days date,
            Minus14Days date,
            Minus30Days date
        );

        WITH RECURSIVE dates(Date) AS (
          VALUES(to_date('20200101', 'YYYYMMDD'))
          UNION ALL
          SELECT
              (date + interval '1 day')::date
          FROM dates
          WHERE date < date '2020-12-31'
        )
        INSERT INTO dim_date (Date)
        SELECT date FROM dates;


        UPDATE dim_date
        SET
            Minus1Day = date - interval '1 day'
            ,Minus7Days = date - interval '7 days'
            ,Minus14Days = date - interval '14 days'
            ,Minus30Days = date - interval '30 days'
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

    c.execute('''
        DROP TABLE IF EXISTS stage_regions_with_data;

        CREATE UNLOGGED TABLE stage_regions_with_data AS
            -- only take U.S. regions that have data
            select FIPS
            from final_csse
            where
                Country_Region = 'US'
                AND FIPS <> '00078' -- duplicate for Virgin Islands
                AND FIPS <> '00066' -- duplicate for Guam
                AND LENGTH(FIPS) > 0
            group by FIPS
            Having SUM(Confirmed) > 0 or SUM(Deaths) > 0 or SUM(Recovered) > 0 or sum(active) > 0;

        CREATE INDEX idx_stage_regions_with_data ON stage_regions_with_data (FIPS);
    ''')


    print("stage_csse_filtered_pre")

    c.execute('''

        DROP TABLE IF EXISTS stage_csse_filtered_pre;

        CREATE UNLOGGED TABLE stage_csse_filtered_pre AS
            SELECT
                t1.*
            FROM final_csse t1
            WHERE
                ShouldHaveFIPS = 1
                AND EXISTS (SELECT 1 FROM stage_regions_with_data t2 WHERE t1.FIPS = t2.FIPS);

        CREATE INDEX idx_stage_csse_filtered_pre ON stage_csse_filtered_pre (FIPS, Date, Confirmed, Deaths);

    ''')


    print("stage_csse_filtered_deduped")

    c.execute('''
        DROP TABLE IF EXISTS stage_csse_filtered_deduped;

        CREATE UNLOGGED TABLE stage_csse_filtered_deduped AS
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

    c.execute('''
        DROP TABLE IF EXISTS stage_csse_filtered;

        CREATE UNLOGGED TABLE stage_csse_filtered (
            Date date,
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
            to_date(Date, 'YYYYMMDD') AS Date,
            --substr(Date,1,4) || '-' || substr(Date,5,2) ||  '-' || substr(Date,7,2)
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

    conn.commit()

    print("dim_county")

    c.execute('''
        DROP TABLE IF EXISTS dim_county;

        CREATE UNLOGGED TABLE dim_county (
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
            CAST(CP03_2014_2018_062E as integer) AS MedianIncome,
            cast(CP05_2014_2018_018E as float) AS MedianAge
        FROM MostRecent t1
        LEFT JOIN final_fips_population t2
            ON t1.FIPS = t2.FIPS
        LEFT JOIN final_county_acs t3
            ON t1.FIPS = t3.state_and_county
        WHERE t1.DateRank = 1;

        CREATE UNIQUE INDEX idx_dim_county ON dim_county (County, State);
    ''')

    print("fact_counties_base")

    c.execute('''
        DROP TABLE IF EXISTS fact_counties_base;

        CREATE UNLOGGED TABLE fact_counties_base (
            Date date,
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
            greatest(t1.Confirmed - coalesce(earlier.Confirmed, 0), 0) AS ConfirmedIncrease,
            t1.Deaths,
            greatest(t1.Deaths - coalesce(earlier.Deaths, 0), 0) AS DeathsIncrease,
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


    c.execute('''
        DROP TABLE IF EXISTS stage_counties_7day_avg;

        CREATE UNLOGGED TABLE stage_counties_7day_avg AS
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

    c.execute('''
        DROP TABLE IF EXISTS stage_with_population_pre;

        DROP TABLE IF EXISTS stage_with_population;

        CREATE UNLOGGED TABLE stage_with_population_pre AS
            SELECT
                t1.*
                ,Population
                ,CAST(Confirmed AS REAL) / (CAST(Population AS REAL) / 1000000) as ConfirmedPer1M
                ,CAST(Deaths AS REAL) / (CAST(Population AS REAL) / 1000000) as DeathsPer1M
                ,ROW_NUMBER() OVER (PARTITION BY t1.FIPS ORDER BY Date DESC) As DateRank
            FROM fact_counties_base t1
            LEFT JOIN dim_county t2
                ON t1.FIPS = t2.FIPS;

        CREATE UNLOGGED TABLE stage_with_population AS
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

    c.execute('''
        DROP TABLE IF EXISTS stage_with_deltas;

        CREATE UNLOGGED TABLE stage_with_deltas as
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

    c.execute('''
        DROP TABLE IF EXISTS stage_latest;

        CREATE UNLOGGED TABLE stage_latest AS
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

    c.execute('''
        DROP TABLE IF EXISTS stage_with_latest;

        CREATE UNLOGGED TABLE stage_with_latest AS
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

    c.execute('''
        DROP TABLE IF EXISTS stage_with_rank;

        CREATE UNLOGGED TABLE stage_with_rank AS
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

    c.execute('''
        DROP TABLE IF EXISTS fact_counties_ranked;

        CREATE UNLOGGED TABLE fact_counties_ranked (
            Date date,
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

    c.execute('''

        DROP TABLE IF EXISTS stage_counties_7dayavg_month_change_overall;

        CREATE UNLOGGED TABLE stage_counties_7dayavg_month_change_overall AS
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
    ''')

    conn.commit()

    c.execute('''

        DROP TABLE IF EXISTS stage_counties_7dayavg_twoweek_change_overall;

        CREATE UNLOGGED TABLE stage_counties_7dayavg_twoweek_change_overall AS
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

    ''')

    conn.commit()

    c.execute('''

        DROP TABLE IF EXISTS stage_counties_one_week_change;

        CREATE UNLOGGED TABLE stage_counties_one_week_change AS
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

    ''')

    conn.commit()

    c.execute('''

        DROP TABLE IF EXISTS stage_counties_two_week_change;

        CREATE UNLOGGED TABLE stage_counties_two_week_change AS
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

    ''')

    conn.commit()

    c.execute('''

        DROP TABLE IF EXISTS stage_counties_month_change;

        CREATE UNLOGGED TABLE stage_counties_month_change AS
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
    ''')

    conn.commit()

    c.execute('''

        DROP TABLE IF EXISTS stage_counties_doubling_time;

        CREATE UNLOGGED TABLE stage_counties_doubling_time AS
            SELECT
                FIPS
                ,Date
                --,Confirmed
                --,EarlierConfirmed
                --,DateDiffDays
                ,ROUND(cast(Confirmed / nullif((Confirmed - EarlierConfirmed) / nullif(CAST(DateDiffDays AS REAL), 0), 0) / 2 as numeric), 2) AS DoublingTimeDays
            FROM
            (
                SELECT
                    t1.FIPS
                    ,t2.Date
                    ,t2.Confirmed
                    ,t1.Date AS EarlierDate
                    ,t1.Confirmed AS EarlierConfirmed
                    --,cast(julianday(t2.Date) as int) - cast(julianday(date(t1.Date)) as int) AS DateDiffDays
                    ,cast(t2.Date - t1.Date as int) AS DateDiffDays
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

    conn.commit()

    c.execute('''

        DROP TABLE IF EXISTS fact_counties_progress;

        CREATE UNLOGGED TABLE fact_counties_progress (
            FIPS TEXT,
            Date date,
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
            CAST(t.ConfirmedIncrease as REAL) / nullif(t.Confirmed - t.ConfirmedIncrease, 0) AS ConfirmedIncreasePct,
            Avg7DayConfirmedIncrease,
            coalesce(OneWeekConfirmedIncrease, 0) AS OneWeekConfirmedIncrease,
            coalesce(OneWeekConfirmedIncreasePct, 0) AS OneWeekConfirmedIncreasePct,
            coalesce(TwoWeekConfirmedIncrease, 0) AS TwoWeekConfirmedIncrease,
            coalesce(TwoWeekConfirmedIncreasePct, 0) AS TwoWeekConfirmedIncreasePct,
            coalesce(MonthConfirmedIncrease, 0) AS MonthConfirmedIncrease,
            coalesce(MonthConfirmedIncreasePct, 0) AS MonthConfirmedIncreasePct,
            coalesce(TwoWeekAvg7DayConfirmedIncrease, 0) as TwoWeekAvg7DayConfirmedIncrease,
            TwoWeekAvg7DayConfirmedIncreasePct as TwoWeekAvg7DayConfirmedIncreasePct,
            coalesce(MonthAvg7DayConfirmedIncrease, 0) AS MonthAvg7DayConfirmedIncrease,
            coalesce(MonthAvg7DayConfirmedIncreasePct, 0) AS MonthAvg7DayConfirmedIncreasePct,
            t.Deaths,
            t.DeathsIncrease,
            CAST(t.DeathsIncrease as REAL) / nullif(t.Deaths - t.DeathsIncrease, 0) AS DeathsIncreasePct,
            MonthAvg7DayDeathsIncrease,
            MonthAvg7DayDeathsIncreasePct,
            DoublingTimeDays,
            greatest(coalesce(coalesce(TwoWeekConfirmedIncrease, 0) * cast(100000 as real) / nullif(c.Population, 0), 0), 0) as CasesPer100k,
            --coalesce(Avg7DayConfirmedIncrease, 0) * cast(100000 as real) / nullif(c.Population, 0) as CasesPer100k2,
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

    conn.commit()

    c.execute('''

        DROP TABLE IF EXISTS stage_counties_cases_per_100k_change;

        CREATE UNLOGGED TABLE stage_counties_cases_per_100k_change AS
            SELECT
                t1.FIPS
                ,t2.Date
                ,coalesce(t2.CasesPer100k, 0) - coalesce(t1.CasesPer100k, 0) AS OneWeekCasesPer100kChange
                ,(coalesce(t2.CasesPer100k, 0) - coalesce(t1.CasesPer100k, 0)) / nullif(t1.CasesPer100k, 0) AS OneWeekCasesPer100kChangePct
            FROM fact_counties_progress t1
            JOIN fact_counties_progress t2
                ON t1.FIPS = t2.FIPS
            JOIN dim_date t2_date
                ON t2.date = t2_date.date
                AND t1.Date = t2_date.Minus7Days
        ;

        CREATE UNIQUE INDEX idx_stage_counties_cases_per_100k_change ON stage_counties_cases_per_100k_change (FIPS, Date);

    ''')

    conn.commit()

    c.execute('''
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


def create_fact_states(conn):

    print("create_fact_states")

    c = conn.cursor()

    c.execute('''
        DROP TABLE IF EXISTS fact_states;

        CREATE UNLOGGED TABLE fact_states AS
            select
                --date(substr(Date,1,4) || '-' || substr(Date,5,2) ||  '-' || substr(Date,7,2)) AS Date,
                to_date(Date, 'YYYYMMDD') AS Date,
                state,
                CAST(positive AS INT) AS positive,
                CASE WHEN CAST(positiveIncrease AS INT) < 0 THEN 0 ELSE CAST(positiveIncrease AS INT) END AS positiveIncrease,
                CAST(death AS INT) AS death,
                CAST(deathIncrease AS INT) AS deathIncrease,
                CAST(hospitalized AS INT) AS hospitalized,
                CAST(hospitalizedIncrease AS INT) AS hospitalizedIncrease,
                CAST(totalTestResultsIncrease AS INT) AS totalTestResultsIncrease,
                CAST(positiveIncrease AS REAL) / nullif(CAST(totalTestResultsIncrease AS REAL), 0) AS PositivityRate,
                CAST(NULL AS REAL) AS CasesPer100k
            from raw_covidtracking_states s
        ;

        CREATE UNIQUE INDEX idx_fact_states ON fact_states (state, Date);

        WITH two_week_increase as (
            select
                s.date,
                s.State,
                cast((s.positive - two_weeks_ago.positive) AS REAL) * 100000 / nullif(Population,0) AS CasesPer100k
            from fact_states s
            join dim_date s_date
                ON s.date = s_date.date
            join dim_state ds
                ON s.state = ds.StateAbbrev
            left join fact_states two_weeks_ago
                ON two_weeks_ago.State = s.State
                AND two_weeks_ago.Date = s_date.Minus14Days
        )
        UPDATE fact_states
        SET CasesPer100k =
            (
            select
                CasesPer100k
            from two_week_increase
            where
                two_week_increase.state = fact_states.state
                AND two_week_increase.Date = fact_states.Date
            )
        ;
    ''')

    conn.commit()


def create_fact_state_deaths(conn):

    print("create_fact_state_deaths")

    c = conn.cursor()

    c.execute('''
        DROP TABLE IF EXISTS fact_state_deaths;

        CREATE TABLE fact_state_deaths (
            State text,
            Week_Of_Year int,
            Week_Ending_Date text,
            AvgDeaths_2014_2019 int,
            Deaths2019 int,
            Deaths2020 int,
            Excess int,
            Pct float,
            Covid19DeathsForWeek int
        );

        with end_dates as (
            Select distinct
                Week_Ending_Date AS end_date
            FROM final_cdc_deaths
            WHERE
                Year = 2020
        ),
        covid19deaths as (
            select
                state,
                end_date,
                sum(deathIncrease) as Covid19DeathsForWeek
            from fact_states
            cross join end_dates
            WHERE
                DATE <= end_date
                AND DATE >= end_date - interval '6 days'
            group by state, end_date
        ),
        avg_by_week as (
            select
                State,
                Week_Of_Year,
                avg(All_Cause) As AvgDeaths_2014_2019
            from final_cdc_deaths
            where
                Year <= 2019
            group by
                State,
                Week_Of_Year
        )
        INSERT INTO fact_state_deaths
        select
            cd.State,
            cd.Week_Of_Year,
            cd2.Week_Ending_Date,
            a.AvgDeaths_2014_2019,
            cd.All_Cause as Deaths2019,
            cd2.All_Cause as Deaths2020,
            coalesce(cd2.All_Cause,0) - coalesce(cd.All_Cause,0) AS Excess,
            cast(coalesce(cd2.All_Cause,0) - coalesce(cd.All_Cause,0) as float) / cd.All_Cause * 100.0 as Pct,
            d.Covid19DeathsForWeek
        from final_cdc_deaths cd
        join final_cdc_deaths cd2
            ON cd.State = cd2.State
            AND cd.Year + 1 = cd2.Year
            AND cd.Week_Of_Year = cd2.Week_Of_Year
        left join avg_by_week a
            on cd.State = a.State
            and cd.Week_Of_Year = a.Week_Of_year
        left join raw_state_abbreviations rsa
            on cd.State = rsa.State
        left join covid19deaths d
            on rsa.Abbreviation = d.state
            and cd2.Week_Ending_Date = d.end_date
        where
            cd.Year = 2019
            and cd.State <> 'United States'
    ''')

    conn.commit()


def create_fact_nation(conn):

    print("create_fact_nation")

    c = conn.cursor()

    c.execute('''
        DROP TABLE IF EXISTS fact_nation;

        CREATE UNLOGGED TABLE fact_nation AS
            select
                date,
                sum(positive) as positive,
                sum(positiveIncrease) as positiveIncrease,
                sum(totalTestResultsIncrease) as totalTestResultsIncrease,
                CAST(NULL AS REAL) as Avg7DayPositiveIncrease,
                CAST(NULl AS REAL) as Avg7DayTotalTestResultsIncrease,
                CAST(NULL AS REAL) AS CasesPer100k
            from fact_states
            group by date
        ;

        WITH
        us_pop as (
            select sum(Population) as Population
            from dim_state
            where 
                StateAbbrev in (select state from fact_states)
        )
        ,two_week_increase as (
            select
                n.date,
                n.positive - two_weeks_ago.positive as TwoWeekIncrease
            from fact_nation n
            left join fact_nation two_weeks_ago
                ON two_weeks_ago.Date = n.Date - interval '14 days'
        )
        UPDATE fact_nation
        SET CasesPer100k = 
            (
            select
                cast(TwoWeekIncrease AS REAL) * 100000 / nullif(Population, 0) AS CasesPer100k
            from two_week_increase
            cross join us_pop
            where two_week_increase.Date = fact_nation.Date
            )
        ;

        WITH one_week AS (
            select
                n.date,
                SUM(one_week_period.positiveIncrease) / 7.0 AS Avg7DayPositiveIncrease,
                SUM(one_week_period.totalTestResultsIncrease) / 7.0 AS Avg7DayTotalTestResultsIncrease
            from fact_nation n
            left join fact_nation one_week_period
                ON one_week_period.Date > n.Date - interval '7 days'
                AND one_week_period.Date <= n.date
            GROUP BY n.date
        )
        UPDATE fact_nation
        SET Avg7DayPositiveIncrease =
            (
            select
                Avg7DayPositiveIncrease
            from one_week
            where one_week.Date = fact_nation.Date
            )
            ,Avg7DayTotalTestResultsIncrease = 
            (
            select
                Avg7DayTotalTestResultsIncrease
            from one_week
            where one_week.Date = fact_nation.Date
            )

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

    create_fact_states(conn)

    create_fact_state_deaths(conn)

    create_fact_nation(conn)

    touch_file('stage/dimensional_models.loaded')


if __name__ == "__main__":
    create_dimensional_tables()

