.mode csv
.headers on
.once "out.csv"
select surveillance.*, population, cases, deaths
from surveillance
join region_population on surveillance.region = region_population.region
left join weekly_cases on surveillance.d = weekly_cases.d and surveillance.region = weekly_cases.region;