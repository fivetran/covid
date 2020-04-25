create or replace table covid.cdc_dates as
with distinct_weeks as (select distinct year, week, cast(dense_rank() over (order by year, week) as int64) as week_number from covid.ilinet_visits)
select year, week, date_add('2020-01-04', interval week_number - (select week_number from distinct_weeks where year = 2020 and week = 1) week) as date 
from distinct_weeks
order by year, week;

create or replace table covid.tests as 
with duplicates as (
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
)
select date, region, sum(total_specimens) as total_specimens, sum(total_positive) as total_positive
from duplicates
group by region, date
order by region, date;

create or replace table covid.patients as
select 
    date, 
    region, 
    sum(num_providers) as num_providers, 
    sum(total_patients) as total_patients,
    sum(ili_total) as ili_total,
    sum(coalesce(age_0_4, 0) + coalesce(age_5_24, 0)) as ili_under_25,
    sum(coalesce(age_25_49, 0) + coalesce(age_25_64, 0) + coalesce(age_50_64, 0) + coalesce(age_65, 0)) as ili_over_25
from covid.ilinet_visits
join covid.cdc_dates using (year, week)
group by region, date
order by region, date;

create or replace table covid.new_cases_by_region as 
with new_cases_by_state as (
    -- Convert cumulative cases/deaths to weekly new cases/deaths
    select
        date, 
        state, 
        cases - coalesce(lag(cases) over (partition by state order by date), 0) as cases, 
        deaths - coalesce(lag(deaths) over (partition by state order by date), 0) as deaths
    from covid.nyt_cases
    where date in (select date from covid.cdc_dates)
)
select date, region, sum(cases) as cases, sum(deaths) as deaths
from new_cases_by_state
join covid.hhs_regions using (state)
group by 1, 2;

create or replace table covid.census_population_by_region as
with infer_2020_population as (
    -- 2020 population isn't available yet, so use 2019 population
    select year, state, population from covid.census_population
    union all select 2020 as year, state, population from covid.census_population where year = 2019
), sum_by_region as (
    select year, region, sum(population) as population
    from infer_2020_population
    join covid.hhs_regions using (state)
    group by 1, 2
)
select date, region, population
from covid.cdc_dates 
join sum_by_region on extract(year from cdc_dates.date) = sum_by_region.year
order by region, date;