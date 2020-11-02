
{{
  config({
    "materialized": 'view'
    })
}}

select
    fc1.fips,
    fc1.date,
    AVG(ConfirmedIncrease) OVER (
        PARTITION BY fips ORDER BY date
        ROWS BETWEEN 6 PRECEDING AND 0 FOLLOWING
    ) AS Avg7DayConfirmedIncrease,
    AVG(DeathsIncrease) OVER (
        PARTITION BY fips ORDER BY date
        ROWS BETWEEN 6 PRECEDING AND 0 FOLLOWING
    ) AS Avg7DayDeathsIncrease
from {{ ref('stg_counties_base') }} fc1
