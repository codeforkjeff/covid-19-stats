
{{
  config({
    "pre-hook": 'drop index if exists idx_{{ this.table }}',
    "post-hook": 'create unique index if not exists idx_{{ this.table }} on {{ this }} (FIPS, Date)'
    })
}}

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
    greatest(coalesce(coalesce(TwoWeekConfirmedIncrease, 0) * cast(100000 as real) / nullif(c.Population, 0), 0), 0) as CasesPer100k
    --coalesce(Avg7DayConfirmedIncrease, 0) * cast(100000 as real) / nullif(c.Population, 0) as CasesPer100k2,
FROM {{ ref('fact_counties_base') }} t
JOIN {{ ref('dim_county') }} c
    ON t.FIPS = c.FIPS
LEFT JOIN {{ ref('stg_counties_7dayavg_month_change_overall') }} o
    ON t.FIPS = o.FIPS AND t.Date = o.Date
LEFT JOIN {{ ref('stg_counties_7dayavg_twoweek_change_overall') }} two_7dayavg
    ON t.FIPS = two_7dayavg.FIPS AND t.Date = two_7dayavg.Date
LEFT JOIN {{ ref('stg_counties_one_week_change') }} one
    ON t.FIPS = one.FIPS AND t.Date = one.Date
LEFT JOIN {{ ref('stg_counties_two_week_change') }} two
    ON t.FIPS = two.FIPS AND t.Date = two.Date
LEFT JOIN {{ ref('stg_counties_month_change') }} mon
    ON t.FIPS = mon.FIPS AND t.Date = mon.Date
LEFT JOIN {{ ref('stg_counties_doubling_time') }} doub
    ON t.FIPS = doub.FIPS AND t.Date = doub.Date
