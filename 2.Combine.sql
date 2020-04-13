drop table if exists cdc_dates;
drop table if exists flu_tests;
drop table if exists surveillance;
drop table if exists weekly_cases;
drop table if exists region_population;

-- Translate the CDCs dumb (year, week) format into dates.
create table cdc_dates as
with distinct_dates as (
    select distinct year, week 
    from ilinet_visits
),
week_numbers as (
    select *, row_number() over (order by year, week) as n 
    from distinct_dates
),
week_offset as (
    select *, n - (select n from week_numbers where year = 2020 and week = 14) as weeks_since_apr_4_2020 
    from week_numbers
)
select 
    year, 
    week, 
    case when weeks_since_apr_4_2020 > 0 then date('2020-04-04', '+'||(7*weeks_since_apr_4_2020)||' day') 
        when weeks_since_apr_4_2020 < 0 then date('2020-04-04', ''||(7*weeks_since_apr_4_2020)||' day')
        else date('2020-04-04') end as d 
from week_offset;

-- Combine flu testing data into a single table.
create table flu_tests as
with post_2015_clinical as (
    select d, region, total_specimens, total_a + total_b as total_positive
    from clinical_labs
    join cdc_dates using (year, week)
), post_2015_public_health as (
    select d, region, total_specimens, a_h1n1 + a_h3 + a_no_subtype + b + b_vic + b_yam + h3n2v as total_positive
    from public_health_labs
    join cdc_dates using (year, week)
), pre_2015 as (
    select d, region, total_specimens, a_h1n1 + a_h1 + a_h3 + a_no_subtype + a_unable_to_subtype + b + h3n2v as total_positive
    from combined_labs 
    join cdc_dates using (year, week)
), combined as (
    select *, null as public_health_specimens, null as public_health_positive, null as clinical_specimens, null as clinical_positive from pre_2015
    union select *, 0 as public_health_specimens, 0 as public_health_positive, total_specimens as clinical_specimens, total_positive as clinical_positive from post_2015_clinical
    union select *, total_specimens as public_health_specimens, total_positive as public_health_positive, 0 as clinical_specimens, 0 as clinical_positive from post_2015_public_health
)
select 
    d, 
    region, 
    sum(total_specimens) as total_specimens, 
    sum(total_positive) as total_positive,
    sum(public_health_specimens) as public_health_specimens, 
    sum(public_health_positive) as public_health_positive,
    sum(clinical_specimens) as clinical_specimens, 
    sum(clinical_positive) as clinical_positive
from combined 
group by d, region;

-- Combine ILINet data with testing data.
create table surveillance as
select flu_tests.*, ili_total, num_providers, total_patients
from ilinet_visits
join cdc_dates on ilinet_visits.year = cdc_dates.year and ilinet_visits.week = cdc_dates.week
join flu_tests on cdc_dates.d = flu_tests.d and ilinet_visits.region = flu_tests.region;

-- Summarize cases and deaths by week, ending Saturday (like CDC).
create table weekly_cases as 
select 
    date(d, 'weekday 6') as d, 
    region,
    sum(cases) as cases,
    sum(deaths) as deaths 
from nyt_cases
join hhs_regions using (state)
group by 1, 2;

-- Summarize population by region.
create table region_population as
select region, sum(population) as population
from census_population
join hhs_regions using (state)
group by 1;