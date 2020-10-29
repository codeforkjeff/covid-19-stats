
{{
  config({
    "pre-hook": 'drop index if exists idx_{{ this.table }}',
    "post-hook": 'create unique index if not exists idx_{{ this.table }} on {{ this }} (FIPS, Date)'
    })
}}

SELECT
    b.Date,
    b.FIPS,
    Confirmed,
    ConfirmedIncrease,
    Deaths,
    DeathsIncrease,
    Recovered,
    Active,
    t1.Avg7DayConfirmedIncrease,
    t2.Avg7DayDeathsIncrease
FROM {{ ref('stg_counties_base') }} b
LEFT JOIN {{ ref('stg_counties_7day_avg') }} t1
     ON b.FIPS = t1.FIPS
     AND b.Date = t1.Date
LEFT JOIN {{ ref('stg_counties_7day_avg') }} t2
     ON b.FIPS = t2.FIPS
     AND b.Date = t2.Date
