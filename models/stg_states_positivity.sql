WITH
pivotted AS (
    select
        *
        ,COALESCE(Negative, 0) + COALESCE(Positive, 0) + COALESCE(Inconclusive, 0) AS TotalTests
    FROM (
        SELECT
            state,
            date,
            overall_outcome,
            new_results_reported 
        FROM {{ source('public', 'raw_dhhs_testing') }}
    )
    PIVOT (
        SUM(cast(new_results_reported as int64)) FOR overall_outcome IN ('Negative', 'Positive', 'Inconclusive')
    )
)
,WithRate AS (
    select
        state,
        CAST(replace(date, '/', '-') AS date) as Date,
        negative,
        positive,
        inconclusive,
        totalTests,
        CASE
            WHEN totalTests > 0
            THEN positive / totalTests 
            ELSE 0
        END AS positivityRate
    FROM pivotted
)
,Avg7Day AS (
    select
        s1.state,
        s1.date,
        cast(SUM(s2.positivityRate) / 7.0 as FLOAT64) as Avg7DayPositivityRate
    from WithRate s1
    join {{ ref('dim_date') }} s1_date
        ON s1.date = s1_date.date
    join WithRate s2
        ON s1.state = s2.state
        AND s2.date > s1_date.Minus7Days
        AND s2.date <= s1.date
    GROUP BY
        s1.state,
        s1.date
)
SELECT
    t1.*
    ,t2.Avg7DayPositivityRate
FROM WithRate t1
LEFT JOIN Avg7Day t2
    ON t1.state = t2.state
    AND t1.date = t2.date
