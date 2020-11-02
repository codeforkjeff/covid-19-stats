
SELECT
    FIPS
    ,Date
    --,Confirmed
    --,EarlierConfirmed
    --,DateDiffDays
    ,ROUND(cast(Confirmed / nullif((Confirmed - EarlierConfirmed) / nullif(CAST(DateDiffDays AS REAL), 0), 0) / 2 as numeric), 2) AS DoublingTimeDays
FROM
(
    SELECT
        t1.FIPS
        ,t2.Date
        ,t2.Confirmed
        ,t1.Date AS EarlierDate
        ,t1.Confirmed AS EarlierConfirmed
        --,cast(julianday(t2.Date) as int) - cast(julianday(date(t1.Date)) as int) AS DateDiffDays
        ,cast(t2.Date - t1.Date as int) AS DateDiffDays
        ,ROW_NUMBER() OVER (PARTITION BY
                t1.FIPS
                ,t2.Date
            ORDER BY t1.Date DESC) AS Rank
    FROM {{ ref('fact_counties_base') }} t1
    JOIN {{ ref('fact_counties_base') }} t2
        ON t1.FIPS = t2.FIPS
        AND t1.Date < t2.Date
        AND t1.Confirmed <= t2.Confirmed / 2
) T
WHERE Rank = 1
