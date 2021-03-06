
WITH MostRecent AS (
    SELECT
        *
        ,ROW_NUMBER() OVER (PARTITION BY FIPS ORDER BY Date DESC) AS DateRank
    FROM {{ ref('stg_csse_filtered') }}
)
SELECT DISTINCT
    t1.FIPS,
    Admin2 AS County,
    Province_State AS State,
    Lat,
    Long_,
    Combined_Key,
    Population,
    CAST(CP03_2014_2018_062E as INT64) AS MedianIncome,
    cast(CP05_2014_2018_018E as FLOAT64) AS MedianAge
FROM MostRecent t1
LEFT JOIN {{ ref('final_fips_population') }} t2
    ON t1.FIPS = t2.FIPS
LEFT JOIN {{ ref('final_county_acs') }} t3
    ON t1.FIPS = t3.state_and_county
WHERE t1.DateRank = 1
