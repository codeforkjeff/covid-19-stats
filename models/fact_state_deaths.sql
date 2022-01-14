
with end_dates as (
    Select distinct
        Week_Ending_Date AS end_date
    FROM {{ ref('final_cdc_deaths') }}
    WHERE
        Year = 2020
),
covid19deaths as (
    select
        state,
        end_date,
        sum(deathIncrease) as Covid19DeathsForWeek
    from {{ ref('fact_states') }}
    cross join end_dates
    WHERE
        DATE <= end_date
        AND DATE >= DATE_SUB(end_date, interval 6 day)
    group by state, end_date
),
avg_by_week as (
    select
        State,
        Week_Of_Year,
        avg(All_Cause) As AvgDeaths_2014_2019
    from {{ ref('final_cdc_deaths') }}
    where
        Year <= 2019
    group by
        State,
        Week_Of_Year
)
select
    cd.State,
    cd.Week_Of_Year,
    a.AvgDeaths_2014_2019,
    cd2.Week_Ending_Date2020,
    cd3.Week_Ending_Date2021,
    cd.All_Cause as Deaths2019,
    cd2.All_Cause as Deaths2020,
    cd3.All_Cause as Deaths2021,
    coalesce(cd2.All_Cause,0) - coalesce(a.AvgDeaths_2014_2019,0) AS Excess2020,
    cast(coalesce(cd2.All_Cause,0) - coalesce(a.AvgDeaths_2014_2019,0) as float64) / a.AvgDeaths_2014_2019 * 100.0 as ExcessPct2020,
    coalesce(cd3.All_Cause,0) - coalesce(a.AvgDeaths_2014_2019,0) AS Excess2021,
    cast(coalesce(cd3.All_Cause,0) - coalesce(a.AvgDeaths_2014_2019,0) as float64) / a.AvgDeaths_2014_2019 * 100.0 as ExcessPct2021,
    d.Covid19DeathsForWeek
from {{ ref('final_cdc_deaths') }} cd
join {{ ref('final_cdc_deaths') }} cd2
    ON cd.State = cd2.State
    AND cd.Year + 1 = cd2.Year
    AND cd.Week_Of_Year = cd2.Week_Of_Year
left join {{ ref('final_cdc_deaths') }} cd3
    ON cd.State = cd3.State
    AND cd.Year + 2 = cd3.Year
    AND cd.Week_Of_Year = cd3.Week_Of_Year
left join avg_by_week a
    on cd.State = a.State
    and cd.Week_Of_Year = a.Week_Of_year
left join {{ ref('final_state_abbreviations') }} rsa
    on cd.State = rsa.State
left join covid19deaths d
    on rsa.Abbreviation = d.state
    and cd2.Week_Ending_Date = d.end_date
where
    cd.Year = 2019
    and cd.State <> 'United States'
