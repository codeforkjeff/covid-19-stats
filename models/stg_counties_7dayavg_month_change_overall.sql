
{{
  config({
    "pre-hook": 'drop index if exists idx_{{ this.table }}',
    "post-hook": 'create unique index if not exists idx_{{ this.table }} on {{ this }} (FIPS, Date)'
    })
}}

SELECT
    t1.FIPS
    ,t2.Date
    ,t2.Avg7DayConfirmedIncrease - t1.Avg7DayConfirmedIncrease AS MonthAvg7DayConfirmedIncrease
    ,case when t1.Avg7DayConfirmedIncrease > 0 then (t2.Avg7DayConfirmedIncrease - t1.Avg7DayConfirmedIncrease) / t1.Avg7DayConfirmedIncrease end AS MonthAvg7DayConfirmedIncreasePct
    ,t2.Avg7DayDeathsIncrease - t1.Avg7DayDeathsIncrease AS MonthAvg7DayDeathsIncrease
    ,case when t1.Avg7DayDeathsIncrease > 0 then (t2.Avg7DayDeathsIncrease - t1.Avg7DayDeathsIncrease) / t1.Avg7DayDeathsIncrease end AS MonthAvg7DayDeathsIncreasePct
FROM {{ ref('fact_counties_base') }} t1
JOIN {{ ref('fact_counties_base') }} t2
    ON t1.FIPS = t2.FIPS
JOIN {{ ref('dim_date') }} t2_date
    ON t2.date = t2_date.date
    AND t1.Date = t2_date.Minus30Days
