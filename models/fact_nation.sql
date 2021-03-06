

WITH
us_pop as (
    select sum(Population) as Population
    from {{ ref('dim_state') }}
    where 
        StateAbbrev in (select state from {{ ref('fact_states') }})
)
,base as (
    select
        date,
        sum(positive) as positive,
        sum(positiveIncrease) as positiveIncrease,
        sum(totalTestResultsIncrease) as totalTestResultsIncrease,
        CAST(NULL AS FLOAT64) as Avg7DayPositiveIncrease,
        CAST(NULl AS FLOAT64) as Avg7DayTotalTestResultsIncrease
        --CAST(NULL AS FLOAT64) AS CasesPer100k
    from {{ ref('fact_states') }}
    group by date
)
,two_week_increase as (
    select
        n.date,
        n.positive - two_weeks_ago.positive as TwoWeekIncrease
    from base n
    left join base two_weeks_ago
        ON two_weeks_ago.Date = date_sub(n.Date, interval 14 day)
)
,cases_per_100k AS (
    select
        date,
        cast(TwoWeekIncrease AS FLOAT64) * 100000 / nullif(Population, 0) AS CasesPer100k
    from two_week_increase
    cross join us_pop
)
,one_week AS (
    select
        n.date,
        SUM(one_week_period.positiveIncrease) / 7.0 AS Avg7DayPositiveIncrease,
        SUM(one_week_period.totalTestResultsIncrease) / 7.0 AS Avg7DayTotalTestResultsIncrease
    from base n
    JOIN base one_week_period
    ON one_week_period.Date between date_sub(n.Date, interval 6 day) and n.date
    GROUP BY n.date
)
SELECT
    b.date,
    positive,
    positiveIncrease,
    totalTestResultsIncrease,
    o.Avg7DayPositiveIncrease,
    o.Avg7DayTotalTestResultsIncrease,
    c.CasesPer100k
FROM base b
LEFT JOIN cases_per_100k c
    ON b.date = c.date
LEFT JOIN one_week o
    ON b.date = o.date
