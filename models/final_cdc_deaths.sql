
SELECT
    Jurisdiction_of_Occurrence As State,
    cast(MMWR_Year as int64) AS Year,
    cast(MMWR_Week as int64) AS Week_Of_Year,
    cast(Week_Ending_Date AS date) AS Week_Ending_Date,
    cast(All_Cause as int64) as All_Cause
FROM {{ source('public', 'raw_cdc_deaths_2019_2020') }}
UNION ALL
SELECT
    Jurisdiction_of_Occurrence AS State,
    cast(MMWR_Year as int64) as Year,
    cast(MMWR_Week as int64) AS Week_Of_Year,
    cast(
        right(Week_Ending_Date, 4) || '-' ||
   	right(substr(Week_Ending_Date, 1, strpos(Week_Ending_Date,'/') - 1), 2) || '-' ||
  	right(replace(substr(Week_Ending_Date, strpos(Week_Ending_Date,'/') + 1), '/2020', ''), 2)
    AS Date) AS Week_Ending_Date,
    cast(All__Cause as int64) as All_Cause
FROM {{ source('public', 'raw_cdc_deaths_2014_2018') }}

