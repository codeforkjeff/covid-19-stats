
SELECT
    *
    ,state || county AS state_and_county
FROM {{ source('public', 'raw_county_acs') }}

