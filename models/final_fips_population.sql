
{{
  config({
    "pre-hook": 'drop index if exists idx_{{ this.table }}',
    "post-hook": 'create index if not exists idx_{{ this.table }} on {{ this }} (FIPS)'
    })
}}

WITH t AS (
        SELECT
            STNAME,
            CTYNAME,
            STATE || COUNTY AS FIPS,
            CAST(POPESTIMATE2019 as INT) as Population
        FROM {{ source('public', 'raw_county_population') }}
        UNION ALL
        SELECT
            NAME, 
            NAME,
            '00072',
            CAST(POPESTIMATE2019 as INT) as Population
        FROM {{ source('public', 'raw_nst_population') }}
        WHERE NAME = 'Puerto Rico'
)
,nyc_patch AS (
   -- Patch NYC: CSSE aggregates all 5 counties of NYC. Reusing code 36061
   -- is misleading, IMO, but that's how it is. so we patch our pop count
   -- to follow suit

   SELECT
        '36061' as FIPS,
        SUM(Population) as Population
    FROM t
    WHERE FIPS IN (
        '36061', -- New York County
        '36005', -- Bronx County
        '36047', -- Kings County
        '36081', -- Queens County
        '36085' -- Richmond County
    ) 
)
select
    STNAME,
    CTYNAME,
    t.FIPS,
    COALESCE(nyc_patch.Population, t.Population) AS Population
from t
left join nyc_patch
    ON t.fips = nyc_patch.fips
