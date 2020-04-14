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
Our goal is to fit the model:

ili(region) ~ patients(region) * (a * seasonal_trend + b * flu_per_speciment(region1) + c * flu_per_specimen(region2) + ...)

*/

create or replace table covid.features as 
with rates as (
    select 
        date, 
        region, 
        case total_specimens when 0 then 0 else total_positive / total_specimens end as positive_per_specimen
    from covid.tests
)
select 
    date, 
    region,
    num_providers,
    total_patients,
    -- Dependent variable.
    ili_total,
    -- Dummy variable for each month so we can fit seasonal trend.
    total_patients * if(extract(month from date) = 1, 1, 0) as month1,
    total_patients * if(extract(month from date) = 2, 1, 0) as month2,
    total_patients * if(extract(month from date) = 3, 1, 0) as month3,
    total_patients * if(extract(month from date) = 4, 1, 0) as month4,
    total_patients * if(extract(month from date) = 5, 1, 0) as month5,
    total_patients * if(extract(month from date) = 6, 1, 0) as month6,
    total_patients * if(extract(month from date) = 7, 1, 0) as month7,
    total_patients * if(extract(month from date) = 8, 1, 0) as month8,
    total_patients * if(extract(month from date) = 9, 1, 0) as month9,
    total_patients * if(extract(month from date) = 10, 1, 0) as month10,
    total_patients * if(extract(month from date) = 11, 1, 0) as month11,
    total_patients * if(extract(month from date) = 12, 1, 0) as month12,
    -- ILI in each region is related to positive test rate from every region.
    tests_region1_lag0.positive_per_specimen as positive_per_speciment_region1_lag0,
    tests_region2_lag0.positive_per_specimen as positive_per_speciment_region2_lag0,
    tests_region3_lag0.positive_per_specimen as positive_per_speciment_region3_lag0,
    tests_region4_lag0.positive_per_specimen as positive_per_speciment_region4_lag0,
    tests_region5_lag0.positive_per_specimen as positive_per_speciment_region5_lag0,
    tests_region6_lag0.positive_per_specimen as positive_per_speciment_region6_lag0,
    tests_region7_lag0.positive_per_specimen as positive_per_speciment_region7_lag0,
    tests_region8_lag0.positive_per_specimen as positive_per_speciment_region8_lag0,
    tests_region9_lag0.positive_per_specimen as positive_per_speciment_region9_lag0,
    tests_region10_lag0.positive_per_specimen as positive_per_speciment_region10_lag0,
    tests_region1_lag1.positive_per_specimen as positive_per_speciment_region1_lag1,
    tests_region2_lag1.positive_per_specimen as positive_per_speciment_region2_lag1,
    tests_region3_lag1.positive_per_specimen as positive_per_speciment_region3_lag1,
    tests_region4_lag1.positive_per_specimen as positive_per_speciment_region4_lag1,
    tests_region5_lag1.positive_per_specimen as positive_per_speciment_region5_lag1,
    tests_region6_lag1.positive_per_specimen as positive_per_speciment_region6_lag1,
    tests_region7_lag1.positive_per_specimen as positive_per_speciment_region7_lag1,
    tests_region8_lag1.positive_per_specimen as positive_per_speciment_region8_lag1,
    tests_region9_lag1.positive_per_specimen as positive_per_speciment_region9_lag1,
    tests_region10_lag1.positive_per_specimen as positive_per_speciment_region10_lag1,
    tests_region1_lag2.positive_per_specimen as positive_per_speciment_region1_lag2,
    tests_region2_lag2.positive_per_specimen as positive_per_speciment_region2_lag2,
    tests_region3_lag2.positive_per_specimen as positive_per_speciment_region3_lag2,
    tests_region4_lag2.positive_per_specimen as positive_per_speciment_region4_lag2,
    tests_region5_lag2.positive_per_specimen as positive_per_speciment_region5_lag2,
    tests_region6_lag2.positive_per_specimen as positive_per_speciment_region6_lag2,
    tests_region7_lag2.positive_per_specimen as positive_per_speciment_region7_lag2,
    tests_region8_lag2.positive_per_specimen as positive_per_speciment_region8_lag2,
    tests_region9_lag2.positive_per_specimen as positive_per_speciment_region9_lag2,
    tests_region10_lag2.positive_per_specimen as positive_per_speciment_region10_lag2
from covid.patients 
join (select date, positive_per_specimen from rates where region = 'Region 1') as tests_region1_lag0 using (date)
join (select date, positive_per_specimen from rates where region = 'Region 2') as tests_region2_lag0 using (date)
join (select date, positive_per_specimen from rates where region = 'Region 3') as tests_region3_lag0 using (date)
join (select date, positive_per_specimen from rates where region = 'Region 4') as tests_region4_lag0 using (date)
join (select date, positive_per_specimen from rates where region = 'Region 5') as tests_region5_lag0 using (date)
join (select date, positive_per_specimen from rates where region = 'Region 6') as tests_region6_lag0 using (date)
join (select date, positive_per_specimen from rates where region = 'Region 7') as tests_region7_lag0 using (date)
join (select date, positive_per_specimen from rates where region = 'Region 8') as tests_region8_lag0 using (date)
join (select date, positive_per_specimen from rates where region = 'Region 9') as tests_region9_lag0 using (date)
join (select date, positive_per_specimen from rates where region = 'Region 10') as tests_region10_lag0 using (date)
join (select lag(date, 1) over (order by date) as date, positive_per_specimen from rates where region = 'Region 1') as tests_region1_lag1 using (date)
join (select lag(date, 1) over (order by date) as date, positive_per_specimen from rates where region = 'Region 2') as tests_region2_lag1 using (date)
join (select lag(date, 1) over (order by date) as date, positive_per_specimen from rates where region = 'Region 3') as tests_region3_lag1 using (date)
join (select lag(date, 1) over (order by date) as date, positive_per_specimen from rates where region = 'Region 4') as tests_region4_lag1 using (date)
join (select lag(date, 1) over (order by date) as date, positive_per_specimen from rates where region = 'Region 5') as tests_region5_lag1 using (date)
join (select lag(date, 1) over (order by date) as date, positive_per_specimen from rates where region = 'Region 6') as tests_region6_lag1 using (date)
join (select lag(date, 1) over (order by date) as date, positive_per_specimen from rates where region = 'Region 7') as tests_region7_lag1 using (date)
join (select lag(date, 1) over (order by date) as date, positive_per_specimen from rates where region = 'Region 8') as tests_region8_lag1 using (date)
join (select lag(date, 1) over (order by date) as date, positive_per_specimen from rates where region = 'Region 9') as tests_region9_lag1 using (date)
join (select lag(date, 1) over (order by date) as date, positive_per_specimen from rates where region = 'Region 10') as tests_region10_lag1 using (date)
join (select lag(date, 2) over (order by date) as date, positive_per_specimen from rates where region = 'Region 1') as tests_region1_lag2 using (date)
join (select lag(date, 2) over (order by date) as date, positive_per_specimen from rates where region = 'Region 2') as tests_region2_lag2 using (date)
join (select lag(date, 2) over (order by date) as date, positive_per_specimen from rates where region = 'Region 3') as tests_region3_lag2 using (date)
join (select lag(date, 2) over (order by date) as date, positive_per_specimen from rates where region = 'Region 4') as tests_region4_lag2 using (date)
join (select lag(date, 2) over (order by date) as date, positive_per_specimen from rates where region = 'Region 5') as tests_region5_lag2 using (date)
join (select lag(date, 2) over (order by date) as date, positive_per_specimen from rates where region = 'Region 6') as tests_region6_lag2 using (date)
join (select lag(date, 2) over (order by date) as date, positive_per_specimen from rates where region = 'Region 7') as tests_region7_lag2 using (date)
join (select lag(date, 2) over (order by date) as date, positive_per_specimen from rates where region = 'Region 8') as tests_region8_lag2 using (date)
join (select lag(date, 2) over (order by date) as date, positive_per_specimen from rates where region = 'Region 9') as tests_region9_lag2 using (date)
join (select lag(date, 2) over (order by date) as date, positive_per_specimen from rates where region = 'Region 10') as tests_region10_lag2 using (date)
order by region, date;

create temp function flu_season(exact date) as (
    extract(year from exact) - if(extract(month from exact) < 8, 1, 0)
);

create or replace model covid.linear_model
options (model_type = 'linear_reg', input_label_cols = ['ili_total']) as
select
    ili_total,
    month1,
    month2,
    month3,
    month4,
    month5,
    month6,
    month7,
    month8,
    month9,
    month10,
    month11,
    month12,
    positive_per_speciment_region1_lag0,
    positive_per_speciment_region2_lag0,
    positive_per_speciment_region3_lag0,
    positive_per_speciment_region4_lag0,
    positive_per_speciment_region5_lag0,
    positive_per_speciment_region6_lag0,
    positive_per_speciment_region7_lag0,
    positive_per_speciment_region8_lag0,
    positive_per_speciment_region9_lag0,
    positive_per_speciment_region10_lag0,
    positive_per_speciment_region1_lag1,
    positive_per_speciment_region2_lag1,
    positive_per_speciment_region3_lag1,
    positive_per_speciment_region4_lag1,
    positive_per_speciment_region5_lag1,
    positive_per_speciment_region6_lag1,
    positive_per_speciment_region7_lag1,
    positive_per_speciment_region8_lag1,
    positive_per_speciment_region9_lag1,
    positive_per_speciment_region10_lag1,
    positive_per_speciment_region1_lag2,
    positive_per_speciment_region2_lag2,
    positive_per_speciment_region3_lag2,
    positive_per_speciment_region4_lag2,
    positive_per_speciment_region5_lag2,
    positive_per_speciment_region6_lag2,
    positive_per_speciment_region7_lag2,
    positive_per_speciment_region8_lag2,
    positive_per_speciment_region9_lag2,
    positive_per_speciment_region10_lag2
from covid.features
where region = 'Region 2'
and flu_season(date) < 2019;

select * 
from ml.predict(model covid.linear_model, (
    select *
    from covid.features
    where region = 'Region 2'
));