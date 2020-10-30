
{{
  config({
    "pre-hook": 'drop index if exists idx_{{ this.table }}',
    "post-hook": 'create unique index if not exists idx_{{ this.table }} on {{ this }} (FIPS, Date)'
    })
}}

select
    fc1.fips,
    fc1.date,
    cast(sum(fc2.ConfirmedIncrease) / 7.0 as REAL) as Avg7DayConfirmedIncrease, 
    cast(sum(fc2.DeathsIncrease) / 7.0 as REAL) as Avg7DayDeathsIncrease
from {{ ref('stg_counties_base') }} fc1
join {{ ref('dim_date') }} fc1_date
    ON fc1.date = fc1_date.date
join {{ ref('stg_counties_base') }} fc2
    ON fc1.fips = fc2.fips
    AND fc2.date > fc1_date.Minus7Days
    AND fc2.date <= fc1.date
GROUP BY
    fc1.fips,
    fc1.date
    
