
{{
  config({
    "pre-hook": 'drop index if exists idx_{{ this.table }}',
    "post-hook": 'create unique index if not exists idx_{{ this.table }} on {{ this }} (FIPS, Date)'
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
JOIN {{ ref('dim_date') }} t1_date
    ON t1.Date = t1_date.Date
LEFT JOIN {{ ref('stg_csse_filtered') }} earlier
    ON t1.FIPS = earlier.FIPS
    AND t1_date.Minus1Day = earlier.Date
