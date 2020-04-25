-- Fit the model ili_rate ~ seasonal_trend + b * flu_positive_rate
create or replace table covid.features as 
select 
    date,
    region,
    total_specimens,
    total_positive,
    num_providers,
    total_patients,
    ili_total,
    population,
    coalesce(cases, 0) as cases,
    coalesce(deaths, 0) as deaths,
    ili_total / total_patients as ili_per_patient,
    total_positive / total_specimens positive_per_specimen,
    format('Month %d', extract(month from date)) as seasonal_trend,
from covid.patients
join covid.tests using (date, region)
join covid.census_population_by_region using (date, region)
left join covid.new_cases_by_region using (date, region)
where total_patients > 0 and total_specimens > 0
order by region, date;

create or replace model covid.national_model
options (model_type = 'linear_reg', input_label_cols = ['ili_per_patient']) as
select
    ili_per_patient,
    positive_per_specimen,
    region,
    seasonal_trend
from covid.features
where extract(year from date) <> 2020;

-- Evaluate the model for making charts.
create or replace table covid.prediction as
select *
from ml.predict(model `fivetran-covid.covid.national_model`, table `fivetran-covid.covid.features`);