
WITH t as (

SELECT
    *,
    CASE WHEN
         Country_Region = 'US'
         AND coalesce(Admin2, '') <> 'Unassigned'
         AND coalesce(Admin2, '') <> 'Unknown'
         AND coalesce(Admin2, '') not like 'Out of%'
         AND coalesce(Admin2, '') not like 'Out-of-%'
         And Province_State <> 'Recovered'
    THEN 1 ELSE 0 END AS ShouldHaveFIPS
FROM {{ source('public', 'raw_csse') }}

)
SELECT
    Date,
    CASE WHEN
        ShouldHaveFIPS = 1
        AND length(fips) <> 5
        AND length(fips) > 0
    THEN lpad(CAST(CAST(CAST(FIPS as FLOAT64) as INT64) as STRING), 5, '0')
    ELSE FIPS
    END AS FIPS,
    Admin2,
    Province_State,
    Country_Region,
    Last_Update,
    Lat,
    Long_,
    CAST(CAST(Confirmed AS FLOAT64) AS INT64) AS Confirmed,
    CAST(CAST(Deaths AS FLOAT64) AS INT64) AS Deaths,
    CAST(CAST(Recovered AS FLOAT64) AS INT64) AS Recovered,
    CAST(CAST(Active AS FLOAT64) AS INT64) AS Active,
    Combined_Key,
    ShouldHaveFIPS
FROM t
