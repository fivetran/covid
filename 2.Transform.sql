drop table if exists cdc_dates;
drop table if exists combined;

create table cdc_dates as
with distinct_weeks as (select distinct year, week, cast(dense_rank() over (order by year, week) as integer) as week_number from ilinet_visits)
select year, week, date '2020-01-04' + 7 * (week_number - (select week_number from distinct_weeks where year = 2020 and week = 1)) as date 
from distinct_weeks
order by year, week;

create table combined as
with tests as (
    select 
        date, 
        region as state, 
        -- Most states report nulls instead of 0 when they have no specimens, except Puerto Rico.
        coalesce(total_specimens, 0) as total_specimens, 
        coalesce(total_a + total_b, 0) as total_positive
    from clinical_labs
    join cdc_dates using (year, week)
    where region <> 'New York City' -- always null
    union select 
        date, 
        region as state, 
        -- Most states report nulls instead of 0 when they have no specimens, except Virgin Islands.
        coalesce(total_specimens, 0) as total_specimens, 
        coalesce(a_h1n1 + a_h1 + a_h3 + a_no_subtype + a_unable_to_subtype + b + h3n2v, 0) as total_positive
    from combined_labs
    join cdc_dates using (year, week)
    where region <> 'New York City' -- always null
), patients as (
    -- ILINet reports New York City separately.
    select 
        date, 
        case region when 'New York City' then 'New York' else region end as state, 
        sum(ili_total) as ili_total, 
        sum(num_providers) as num_providers, 
        sum(total_patients) as total_patients
    from ilinet_visits
    join cdc_dates using (year, week)
    group by 1, 2
), cases as (
    -- Convert cumulative cases/deaths to weekly new cases/deaths
    select 
        date, 
        state, 
        cases - coalesce(lag(cases) over (partition by state order by date), 0) as cases, 
        deaths - coalesce(lag(deaths) over (partition by state order by date), 0) as deaths
    from nyt_cases
    where date in (select date from cdc_dates)
)
select 
    date as "Date", 
    extract(year from date) - case when extract(month from date) < 8 then 1 else 0 end as "Flu Season",
    state as "State", 
    population as "Population",
    total_specimens as "Total Flu Tests", 
    total_positive as "Positive Flu Tests", 
    total_patients as "Total Patients",
    ili_total as "Patients with ILI", 
    num_providers as "ILINet Providers", 
    coalesce(cases, 0) as "COVID Cases", 
    coalesce(deaths, 0) as "COVID Deaths"
from tests 
join patients using (date, state)
join cdc_dates using (date)
join census_population using (year, state)
left join cases using (date, state)
order by state, date;

copy combined to './results/Combined.csv' with header;