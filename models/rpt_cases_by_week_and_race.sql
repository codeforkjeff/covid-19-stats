
with
period_totals AS (
    select 
        week_ending_date,
        count(*) as cases_total,
        sum(death_flag) as death_total
    from {{ ref('final_cdc_surveillance_cases') }}
    group by week_ending_date
)
,totals_for_race as (
    select 
        week_ending_date,
        race_ethnicity_combined,
        count(*) as cases,
        sum(death_flag) as death,
        sum(icu_flag) as icu,
    from {{ ref('final_cdc_surveillance_cases') }}
    group by week_ending_date, race_ethnicity_combined
)
,joined aS (
  select 
  	t1.week_ending_date
    ,race_ethnicity_combined
    ,cases
    ,death
    ,icu
    ,t2.cases_total
    ,cases / cases_total as cases_pct
    ,death  / death_total as death_pct
  from totals_for_race t1
  left join period_totals t2
    on t1.week_ending_date = t2.week_ending_date
)
select *
from joined
order by week_ending_date, race_ethnicity_combined
