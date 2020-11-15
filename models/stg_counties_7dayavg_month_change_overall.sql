
{{
  config({
    "materialized": 'view'
    })
}}

SELECT
    t1.FIPS
    ,t2.Date
    ,CAST(t2.Avg7DayConfirmedIncrease - t1.Avg7DayConfirmedIncrease AS FLOAT64) AS MonthAvg7DayConfirmedIncrease
    ,CAST(case when t1.Avg7DayConfirmedIncrease > 0 then (t2.Avg7DayConfirmedIncrease - t1.Avg7DayConfirmedIncrease) / t1.Avg7DayConfirmedIncrease end AS FLOAT64) AS MonthAvg7DayConfirmedIncreasePct
    ,CAST(t2.Avg7DayDeathsIncrease - t1.Avg7DayDeathsIncrease AS FLOAT64) AS MonthAvg7DayDeathsIncrease
    ,CAST(case when t1.Avg7DayDeathsIncrease > 0 then (t2.Avg7DayDeathsIncrease - t1.Avg7DayDeathsIncrease) / t1.Avg7DayDeathsIncrease end AS FLOAT64) AS MonthAvg7DayDeathsIncreasePct
FROM {{ ref('fact_counties_base') }} t1
JOIN {{ ref('fact_counties_base') }} t2
    ON t1.FIPS = t2.FIPS
JOIN {{ ref('dim_date') }} t2_date
    ON t2.date = t2_date.date
    AND t1.Date = t2_date.Minus30Days
