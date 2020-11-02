
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
    t1.Avg7DayDeathsIncrease
FROM {{ ref('stg_counties_base') }} b
LEFT JOIN {{ ref('stg_counties_7day_avg') }} t1
     ON b.FIPS = t1.FIPS
     AND b.Date = t1.Date
