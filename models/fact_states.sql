{{
  config({
    "pre-hook": 'drop index if exists idx_{{ this.table }}',
    "post-hook": 'create unique index if not exists idx_{{ this.table }} on {{ this }} (state, Date)'
    })
}}

WITH s AS (
    select
        --date(substr(Date,1,4) || '-' || substr(Date,5,2) ||  '-' || substr(Date,7,2)) AS Date,
        to_date(Date, 'YYYYMMDD') AS Date,
        state,
        CAST(positive AS INT) AS positive,
        CASE WHEN CAST(positiveIncrease AS INT) < 0 THEN 0 ELSE CAST(positiveIncrease AS INT) END AS positiveIncrease,
        CAST(death AS INT) AS death,
        CAST(deathIncrease AS INT) AS deathIncrease,
        CAST(hospitalized AS INT) AS hospitalized,
        CAST(hospitalizedIncrease AS INT) AS hospitalizedIncrease,
        CAST(totalTestResultsIncrease AS INT) AS totalTestResultsIncrease,
        CAST(positiveIncrease AS REAL) / nullif(CAST(totalTestResultsIncrease AS REAL), 0) AS PositivityRate
    from raw_covidtracking_states s
)
,two_week_increase as (
    select
        s.date,
        s.State,
        cast((s.positive - two_weeks_ago.positive) AS REAL) * 100000 / nullif(Population,0) AS CasesPer100k
    from s
    join {{ ref('dim_date') }} s_date
        ON s.date = s_date.date
    join {{ ref('dim_state') }} ds
        ON s.state = ds.StateAbbrev
    left join s two_weeks_ago
        ON two_weeks_ago.State = s.State
        AND two_weeks_ago.Date = s_date.Minus14Days
)
SELECT
    s.Date,
    s.state,
    positive,
    positiveIncrease,
    death,
    deathIncrease,
    hospitalized,
    hospitalizedIncrease,
    totalTestResultsIncrease,
    PositivityRate,
    CasesPer100k
FROM s
LEFT JOIN two_week_increase
    ON two_week_increase.state = s.state
    AND two_week_increase.Date = s.Date
