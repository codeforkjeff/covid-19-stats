
WITH
covidtracking AS (
    select
        cast(substr(Date,1,4) || '-' || substr(Date,5,2) ||  '-' || substr(Date,7,2) AS DATE) AS Date,
        state,
        CAST(positive AS INT64) AS positive,
        CASE WHEN CAST(positiveIncrease AS INT64) < 0 THEN 0 ELSE CAST(positiveIncrease AS INT64) END AS positiveIncrease,
        CAST(death AS INT64) AS death,
        CAST(deathIncrease AS INT64) AS deathIncrease,
        CAST(hospitalized AS INT64) AS hospitalized,
        CAST(hospitalizedIncrease AS INT64) AS hospitalizedIncrease,
        CAST(totalTestResultsIncrease AS INT64) AS totalTestResultsIncrease,
        CAST(positiveIncrease AS FLOAT64) / nullif(CAST(totalTestResultsIncrease AS FLOAT64), 0) AS PositivityRate
    from {{ source('public', 'raw_covidtracking_states') }} s
)
,cdc as (
    select 
        parse_date("%m/%d/%Y", submission_date) AS Date,
        state,
        CAST(tot_cases AS INT64) AS positive,
        CASE WHEN CAST(new_case AS INT64) < 0 THEN 0 ELSE CAST(new_case AS INT64) END AS positiveIncrease,
        CAST(tot_death AS INT64) AS death,
        CAST(new_death AS INT64) AS deathIncrease,
        NULL AS hospitalized,
        NULL AS hospitalizedIncrease,
        NULL AS totalTestResultsIncrease
    from {{ source('public', 'raw_cdc_states') }} s

)
,states as (
    -- SELECT *
    -- FROM covidtracking

    SELECT *
    FROM cdc
)
,two_week_increase as (
    select
        s.date,
        s.State,
        cast((s.positive - two_weeks_ago.positive) AS FLOAT64) * 100000 / nullif(Population,0) AS CasesPer100k
    from states s
    join {{ ref('dim_date') }} s_date
        ON s.date = s_date.date
    join {{ ref('dim_state') }} ds
        ON s.state = ds.StateAbbrev
    left join states two_weeks_ago
        ON two_weeks_ago.State = s.State
        AND two_weeks_ago.Date = s_date.Minus14Days
)
,two_week_avg as (
    select
        s1.state,
        s1.date,
        cast(sum(s2.positiveIncrease) / 7.0 as FLOAT64) as Avg7DayPositiveIncrease, 
        cast(sum(s2.DeathIncrease) / 7.0 as FLOAT64) as Avg7DayDeathIncrease
    from states s1
    join {{ ref('dim_date') }} s1_date
        ON s1.date = s1_date.date
    join states s2
        ON s1.state = s2.state
        AND s2.date > s1_date.Minus7Days
        AND s2.date <= s1.date
    GROUP BY
        s1.state,
        s1.date
)
SELECT
    s.Date,
    s.state,
    s.positive,
    positiveIncrease,
    death,
    deathIncrease,
    hospitalized,
    hospitalizedIncrease,
    totalTestResultsIncrease,
    pos.positivityRate,
    CasesPer100k,
    Avg7DayPositiveIncrease,
    Avg7DayDeathIncrease,
    Avg7DayPositivityRate
FROM states s
LEFT JOIN two_week_increase
    ON two_week_increase.state = s.state
    AND two_week_increase.Date = s.Date
LEFT JOIN two_week_avg
    ON two_week_avg.state = s.state
    AND two_week_avg.Date = s.Date
LEFT JOIN {{ ref('stg_states_positivity') }} pos
    ON s.state = pos.state
    AND s.date = pos.date
