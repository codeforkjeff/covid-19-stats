
{{
  config({
    "materialized": 'view'
    })
}}

select FIPS
from {{ ref('final_csse') }}
where
    Country_Region = 'US'
    AND FIPS <> '00078' -- duplicate for Virgin Islands
    AND FIPS <> '00066' -- duplicate for Guam
    AND LENGTH(FIPS) > 0
group by FIPS
Having SUM(Confirmed) > 0 or SUM(Deaths) > 0 or SUM(Recovered) > 0 or sum(active) > 0
