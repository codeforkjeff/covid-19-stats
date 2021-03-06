
{{
  config({
    "materialized": 'view'
    })
}}

-- there's duplication for the same FIPS and Date;
-- for some cases, it looks like counts got spread out across 2 rows;
-- in other cases, it looks like there are rows where values are 0.
-- for simplicity, just pick the row with higher numbers

SELECT
    cast(substr(Date,1,4) || '-' || substr(Date,5,2) ||  '-' || substr(Date,7,2) as DATE) as Date,
    FIPS,
    Admin2,
    Province_State,
    Country_Region,
    Last_Update,
    Lat,
    Long_,
    Confirmed,
    Deaths,
    Recovered,
    Active,
    Combined_Key,
    ShouldHaveFIPS
FROM
(
    SELECT
        *
        ,ROW_NUMBER() OVER (PARTITION BY FIPS, Date ORDER BY Confirmed DESC, Deaths DESC) AS RN
    FROM {{ ref('stg_csse_filtered_pre') }}
) T
WHERE RN = 1
