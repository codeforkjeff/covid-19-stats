
{{
  config({
    "materialized": 'view'
    })
}}

SELECT
    t1.FIPS
    ,t2.Date
    ,coalesce(t2.CasesPer100k, 0) - coalesce(t1.CasesPer100k, 0) AS OneWeekCasesPer100kChange
    ,(coalesce(t2.CasesPer100k, 0) - coalesce(t1.CasesPer100k, 0)) / nullif(t1.CasesPer100k, 0) AS OneWeekCasesPer100kChangePct
FROM {{ ref('stg_counties_progress') }} t1
JOIN {{ ref('stg_counties_progress') }} t2
    ON t1.FIPS = t2.FIPS
    AND t1.Date = DATE_SUB(t2.Date, INTERVAL 7 day)
