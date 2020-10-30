
SELECT
    Jurisdiction_of_Occurrence As State,
    cast(MMWR_Year as int) AS Year,
    cast(MMWR_Week as int) AS Week_Of_Year,
    to_date(Week_Ending_Date, 'YYYY-MM-DD') AS Week_Ending_Date,
    cast(All_Cause as int) as All_Cause
FROM {{ source('public', 'raw_cdc_deaths_2019_2020') }}
UNION ALL
SELECT
    Jurisdiction_of_Occurrence AS State,
    cast(MMWR_Year as int) as Year,
    cast(MMWR_Week as int) AS Week_Of_Year,
    to_date(
        right(Week_Ending_Date, 4) || '-' ||
   	right(substr(Week_Ending_Date, 1, position('/' in Week_Ending_Date) - 1), 2) || '-' ||
  	right(replace(substr(Week_Ending_Date, position('/' in Week_Ending_Date) + 1), '/2020', ''), 2)
    ,'YYYY-MM-DD') AS Week_Ending_Date,
    cast(All__Cause as int) as All_Cause
FROM {{ source('public', 'raw_cdc_deaths_2014_2018') }}

