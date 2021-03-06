
{{
  config({
    "materialized": 'view'
    })
}}

SELECT
    t1.*
FROM {{ ref('final_csse') }} t1
WHERE
    ShouldHaveFIPS = 1
    AND EXISTS (SELECT 1 FROM {{ ref('stg_regions_with_data') }} t2 WHERE t1.FIPS = t2.FIPS)
