
{{
  config({
    "materialized": 'view'
    })
}}

SELECT
    t1.FIPS
    ,t2.Date
    ,t2.Confirmed - coalesce(t1.Confirmed, 0) AS OneWeekConfirmedIncrease
    ,case when t1.Confirmed > 0 then (t2.Confirmed - t1.Confirmed) / CAST(t1.Confirmed AS FLOAT64) end AS OneWeekConfirmedIncreasePct
FROM {{ ref('fact_counties_base') }} t1
JOIN {{ ref('fact_counties_base') }} t2
    ON t1.FIPS = t2.FIPS
JOIN {{ ref('dim_date') }} t2_date
    ON t2.date = t2_date.date
    AND t1.Date = t2_date.Minus7Days
