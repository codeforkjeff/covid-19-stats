
SELECT
    p.FIPS,
    p.Date,
    Confirmed,
    ConfirmedIncrease,
    ConfirmedIncreasePct,
    Avg7DayConfirmedIncrease,
    OneWeekConfirmedIncrease,
    OneWeekConfirmedIncreasePct,
    TwoWeekConfirmedIncrease,
    TwoWeekConfirmedIncreasePct,
    MonthConfirmedIncrease,
    MonthConfirmedIncreasePct,
    TwoWeekAvg7DayConfirmedIncrease,
    TwoWeekAvg7DayConfirmedIncreasePct,
    MonthAvg7DayConfirmedIncrease,
    MonthAvg7DayConfirmedIncreasePct,
    Deaths,
    DeathsIncrease,
    DeathsIncreasePct,
    Avg7DayDeathsIncrease,
    TwoWeekAvg7DayDeathsIncrease,
    TwoWeekAvg7DayDeathsIncreasePct,
    MonthAvg7DayDeathsIncrease,
    MonthAvg7DayDeathsIncreasePct,
--    DoublingTimeDays,
    CasesPer100k,
    t1.OneWeekCasesPer100kChange,
    t1.OneWeekCasesPer100kChangePct
FROM {{ ref('stg_counties_progress') }} p
JOIN {{ ref('stg_counties_cases_per_100k_change') }} t1
    ON p.FIPS = t1.FIPS
    AND p.Date = t1.Date
