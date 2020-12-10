
{{
  config({
    "materialized": 'view'
    })
}}
        
SELECT
    t1.Date,
    t1.FIPS,
    t1.Confirmed,
    greatest(t1.Confirmed - coalesce(earlier.Confirmed, 0), 0) AS ConfirmedIncrease,
    t1.Deaths,
    greatest(t1.Deaths - coalesce(earlier.Deaths, 0), 0) AS DeathsIncrease,
    t1.Recovered,
    t1.Active
FROM {{ ref('stg_csse_filtered') }} t1
LEFT JOIN {{ ref('stg_csse_filtered') }} earlier
    ON t1.FIPS = earlier.FIPS
    AND DATE_SUB(t1.Date, INTERVAL 1 day) = earlier.Date
