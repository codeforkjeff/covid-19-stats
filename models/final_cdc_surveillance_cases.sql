
select
    PARSE_DATE("%Y/%m/%d", cdc_case_earliest_dt) AS cdc_case_earliest_dt
    ,extract(YEAR FROM PARSE_DATE("%Y/%m/%d", cdc_case_earliest_dt)) as year
    ,extract(WEEK FROM PARSE_DATE("%Y/%m/%d", cdc_case_earliest_dt)) as week_of_year
    ,DATE_ADD(PARSE_DATE("%Y/%m/%d", cdc_case_earliest_dt),
        INTERVAL 7 - extract(DAYOFWEEK FROM PARSE_DATE("%Y/%m/%d", cdc_case_earliest_dt)) DAY) AS week_ending_date
 	,PARSE_DATE("%Y/%m/%d", cdc_report_dt) AS cdc_report_dt
	,PARSE_DATE("%Y/%m/%d", pos_spec_dt) AS pos_spec_dt
	,PARSE_DATE("%Y/%m/%d", onset_dt) AS onset_dt
    ,current_status
    ,sex
    ,age_group
    ,race_ethnicity_combined
    ,hosp_yn
    ,icu_yn
    ,death_yn
    ,medcond_yn
    -- flatten to 0/1: these don't account for distinctions among no/missing/na
    ,case when hosp_yn = 'Yes' then 1 else 0 end as hosp_flag
    ,case when icu_yn = 'Yes' then 1 else 0 end as icu_flag
    ,case when death_yn = 'Yes' then 1 else 0 end as death_flag
    ,case when medcond_yn = 'Yes' then 1 else 0 end as medcond_flag
from {{ source('public', 'raw_cdc_surveillance_cases') }}
