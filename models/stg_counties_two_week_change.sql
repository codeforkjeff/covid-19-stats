
{{
  config({
    "pre-hook": 'drop index if exists idx_{{ this.table }}',
    "post-hook": 'create unique index if not exists idx_{{ this.table }} on {{ this }} (FIPS, Date)'
    })
}}

SELECT
    t1.FIPS
    ,t2.Date
    ,t2.Confirmed - coalesce(t1.Confirmed, 0) AS TwoWeekConfirmedIncrease
    ,case when t1.Confirmed > 0 then (t2.Confirmed - t1.Confirmed) / CAST(t1.Confirmed AS REAL) end AS TwoWeekConfirmedIncreasePct
FROM {{ ref('fact_counties_base') }} t1
JOIN {{ ref('fact_counties_base') }} t2
    ON t1.FIPS = t2.FIPS
JOIN {{ ref('dim_date') }} t2_date
    ON t2.date = t2_date.date
    AND t1.Date = t2_date.Minus14Days
