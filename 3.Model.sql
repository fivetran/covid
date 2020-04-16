-- Fit the model ili_rate ~ seasonal_trend + b * flu_positive_rate
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
    sum(population) as population,
    coalesce(sum(cases), 0) as cases,
    coalesce(sum(deaths), 0) as deaths,
    -- Population-weighted average of % of patients visiting primary care providers with ILI.
    sum(rate(ili_total, total_patients) * population) / sum(population) as ili_per_patient,
    -- Population-weighted average of % of specimens that test positive for flu.
    sum(rate(total_positive, total_specimens) * population) / sum(population) as positive_per_specimen,
    -- Categorical variable for seasonal trend.
    format('Month %d', extract(month from date)) as seasonal_trend,
from covid.patients
join covid.tests using (date, region)
join covid.census_population_by_region using (date, region)
left join covid.new_cases_by_region using (date, region)
group by date
having num_providers > 0
order by date;

create or replace model covid.national_model
options (model_type = 'linear_reg', input_label_cols = ['ili_per_patient']) as
select ili_per_patient, positive_per_specimen, seasonal_trend
from covid.features
where extract(year from date) <> 2020;