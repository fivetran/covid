create or replace table covid.cdc_dates as
with distinct_weeks as (select distinct year, week, cast(dense_rank() over (order by year, week) as int64) as week_number from covid.ilinet_visits)
select year, week, date_add('2020-01-04', interval week_number - (select week_number from distinct_weeks where year = 2020 and week = 1) week) as date 
from distinct_weeks
order by year, week;

create or replace table covid.combined as
with tests as (
    select 
        date, 
        region, 
        total_specimens, 
        total_a + total_b as total_positive
    from covid.clinical_labs
    join covid.cdc_dates using (year, week)
    union all select 
        date, 
        region, 
        total_specimens, 
        a_h1n1 + a_h3 + a_no_subtype + b + b_yam + b_vic + h3n2v as total_positive
    from covid.public_health_labs
    join covid.cdc_dates using (year, week)
    union all select 
        date, 
        region, 
        total_specimens, 
        a_h1n1 + a_h1 + a_h3 + a_no_subtype + a_unable_to_subtype + b + h3n2v as total_positive
    from covid.combined_labs
    join covid.cdc_dates using (year, week)
), patients as (
    -- ILINet reports New York City separately.
    select 
        date, 
        region, 
        sum(ili_total) as ili_total, 
        sum(num_providers) as num_providers, 
        sum(total_patients) as total_patients
    from covid.ilinet_visits
    join covid.cdc_dates using (year, week)
    group by 1, 2
), new_cases_by_state as (
    -- Convert cumulative cases/deaths to weekly new cases/deaths
    select
        date, 
        state, 
        cases - coalesce(lag(cases) over (partition by state order by date), 0) as cases, 
        deaths - coalesce(lag(deaths) over (partition by state order by date), 0) as deaths
    from covid.nyt_cases
    where date in (select date from covid.cdc_dates)
), new_cases_by_region as (
    select date, region, sum(cases) as cases, sum(deaths) as deaths
    from new_cases_by_state
    join covid.hhs_regions using (state)
    group by 1, 2
), infer_2020_population as (
    -- 2020 population isn't available yet, so use 2019 population
    select year, state, population from covid.census_population
    union all select 2020 as year, state, population from covid.census_population where year = 2019
), census_population_by_region as (
    select year, region, sum(population) as population
    from infer_2020_population
    join covid.hhs_regions using (state)
    group by 1, 2
)
select 
    date,
    extract(year from date) - case when extract(month from date) < 8 then 1 else 0 end as flu_season,
    region,
    population,
    total_specimens,
    total_positive,
    total_patients,
    ili_total,
    num_providers,
    coalesce(cases, 0) as cases,
    coalesce(deaths, 0) as deaths
from tests 
join patients using (date, region)
join covid.cdc_dates using (date)
join census_population_by_region using (year, region)
left join new_cases_by_region using (date, region)
order by region, date;