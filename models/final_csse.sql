
{{
  config({
    "pre-hook": 'drop index if exists idx_{{ this.table }}',
    "post-hook": 'create index if not exists idx_{{ this.table }} on {{ this }} (FIPS)'
    })
}}

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
    THEN lpad(CAST(CAST(CAST(FIPS as FLOAT) as INT) as VARCHAR), 5, '0')
    ELSE FIPS
    END AS FIPS,
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
FROM t
