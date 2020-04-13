drop table if exists model;

create table model (
    region text,
    slope real,
    intercept real,
    primary key (region)
);

-- Used Tableau to fit per-region linear model 
--   ili_total / num_providers ~ a + b * clinical_positive
-- for 2015-2019
insert into model values
    ('Region 10', 0.0109605, 1.98493),
    ('Region 9', 0.0124357, 5.7387),
    ('Region 8', 0.0139038, 3.61114),
    ('Region 7', 0.0174275, 2.27597),
    ('Region 6', 0.0106659, 7.44221),
    ('Region 5', 0.0029964, 3.94442),
    ('Region 4', 0.0123566, 4.65887),
    ('Region 3', 0.0227055, 6.35093),
    ('Region 2', 0.0206975, 11.9451),
    ('Region 1', 0.0116584, 3.10331);

.mode csv
.headers on
.once "out.csv"
select surveillance.*, population, cases, deaths, model.intercept + model.slope * surveillance.clinical_positive as model_ili_per_provider
from surveillance
join model on surveillance.region = model.region
join region_population on surveillance.region = region_population.region
left join weekly_cases on surveillance.d = weekly_cases.d and surveillance.region = weekly_cases.region;