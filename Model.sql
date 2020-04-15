create or replace table covid.ilinet_visits (
    -- REGION TYPE
    region_type string,
    -- REGION
    -- Note that "New York City" is a separate region.
    region string,
    -- YEAR
    year int64,
    -- WEEK
    week int64,
    -- % WEIGHTED ILI
    weighted_ili string,
    -- %UNWEIGHTED ILI
    unweighted_ili string,
    -- AGE 0-4
    age_0_4 string,
    -- AGE 25-49
    age_25_29 string,
    -- AGE 25-64
    age_25_64 string,
    -- AGE 5-24
    age_5_24 string,
    -- AGE 50-64
    age_50_64 string,
    -- AGE 65
    age_65 string,
    -- ILITOTAL
    ili_total int64,
    -- NUM. OF PROVIDERS
    num_providers int64,
    -- TOTAL PATIENTS
    total_patients int64
);

-- After 2015, flu testing data is reported separately for clinical labs and public health labs.
-- Clinical lab data is reported by state, while public health lab data is only reported by region.
-- Clinical labs report ~5x more samples than public health labs.
create or replace table covid.clinical_labs (
    -- REGION TYPE
    region_type string,
    -- REGION
    region string,
    -- YEAR
    year int64,
    -- WEEK
    week int64,
    -- TOTAL SPECIMENS
    total_specimens int64,
    -- TOTAL A
    total_a int64,
    -- TOTAL B
    total_b int64,
    -- PERCENT POSITIVE
    percent_positive float64,
    -- PERCENT A
    percent_a float64,
    -- PERCENT B
    percent_b float64
);

create or replace table covid.public_health_labs (
    -- REGION TYPE
    region_type string,
    -- REGION
    region string,
    -- YEAR
    year int64,
    -- WEEK
    week int64,
    -- TOTAL SPECIMENS
    total_specimens int64,
    -- A (2009 H1N1)
    a_h1n1 int64,
    -- A (H3)
    a_h3 int64,
    -- A (Subtyping not Performed)
    a_no_subtype int64,
    -- B
    b int64,
    -- BVic
    b_yam int64,
    -- BYam
    b_vic int64,
    -- H3N2v
    h3n2v int64
);

create or replace table covid.combined_labs (
    -- REGION TYPE
    region_type string,
    -- REGION
    region string,
    -- YEAR
    year int64,
    -- WEEK
    week int64,
    -- TOTAL SPECIMENS
    total_specimens int64,
    -- PERCENT POSITIVE
    percent_positive float64,
    -- A (2009 H1N1)
    a_h1n1 int64,
    -- A (H1)
    a_h1 int64,
    -- A (H3)
    a_h3 int64,
    -- A (Subtyping not Performed)
    a_no_subtype int64,
    -- A (Unable to Subtype)
    a_unable_to_subtype int64,
    -- B
    b int64,
    -- H3N2v
    h3n2v int64
);

create or replace table covid.nyt_cases (
    date date,
    state string,
    fips int64,
    cases int64,
    deaths int64
);

create or replace table covid.census_population (
    state string,
    year int64,
    population int64
);

create or replace table covid.hhs_regions (
    region string,
    state string
);

/*
bq --project_id fivetran-covid load --skip_leading_rows 2 --null_marker 'X' covid.ilinet_visits './data/ILINet.csv'
bq --project_id fivetran-covid load --skip_leading_rows 2 --null_marker 'X' covid.clinical_labs './data/WHO_NREVSS_Clinical_Labs.csv'
bq --project_id fivetran-covid load --skip_leading_rows 2 --null_marker 'X' covid.public_health_labs './data/WHO_NREVSS_Public_Health_Labs.csv'
bq --project_id fivetran-covid load --skip_leading_rows 2 --null_marker 'X' covid.combined_labs './data/WHO_NREVSS_Combined_prior_to_2015_16.csv'
bq --project_id fivetran-covid load --skip_leading_rows 1 covid.nyt_cases './data/NYT_Cases.csv'
bq --project_id fivetran-covid load --skip_leading_rows 1 covid.census_population './data/Census_Population.csv'
bq --project_id fivetran-covid load --skip_leading_rows 1 covid.hhs_regions './data/HHS_Regions.csv'
*/

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
    sum(ili_total) as ili_total, 
    sum(num_providers) as num_providers, 
    sum(total_patients) as total_patients
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
)
select year, region, sum(population) as population
from infer_2020_population
join covid.hhs_regions using (state)
group by 1, 2;

/*
fit ili_rate ~ seasonal_trend + b * flu_positive_rate
*/

create temp function rate(x int64, y int64) as (
    case y when 0 then 0 else x / y end
);
create or replace table covid.features as 
select 
    date,
    sum(total_specimens) as total_specimens,
    sum(total_positive) as total_positive,
    sum(num_providers) as num_providers,
    sum(total_patients) as total_patients,
    sum(ili_total) as ili_total,
    rate(sum(ili_total), sum(total_patients)) as ili_per_patient,
    rate(sum(total_positive), sum(total_specimens)) as positive_per_specimen,
    format('Month %d', extract(month from date)) as seasonal_trend,
from covid.patients
join covid.tests using (date, region)
group by date
order by date;

create or replace model covid.national_model
options (model_type = 'linear_reg', input_label_cols = ['ili_per_patient']) as
select ili_per_patient, positive_per_specimen, seasonal_trend
from covid.features
where extract(year from date) <> 2020;

select * except (ili_per_patient, positive_per_specimen, seasonal_trend, predicted_ili_per_patient), predicted_ili_per_patient * total_patients as predicted_ili
from ml.predict(model covid.national_model, table covid.features);

