/*
Our goal is to fit the model:

ili_total(state) / num_providers(state) ~ 
*/

create or replace model covid.linear_model
options (model_type = 'linear_reg') as
select 
    if(total_patients > 0, ili_total / total_patients, 0) as label,
    total_positive
from covid.combined
where extract(year from date) < 2020
and state = 'New York';

select * 
from ml.predict(model covid.linear_model, (
    select 
        if(total_patients > 0, ili_total / total_patients, 0) as ili_per_patient,
        total_positive
    from covid.combined
    where state = 'New York'
))

create or replace model covid.linear_model
options (model_type='linear_reg') as
select
    if(total_patients > 0, ili_total / total_patients, 0) as label,
    if(state = 'Alabama', total_positive, 0) as flu_alabama,
    if(state = 'Alaska', total_positive, 0) as flu_alaska,
    if(state = 'Arizona', total_positive, 0) as flu_arizona,
    if(state = 'Arkansas', total_positive, 0) as flu_arkansas,
    if(state = 'California', total_positive, 0) as flu_california,
    if(state = 'Colorado', total_positive, 0) as flu_colorado,
    if(state = 'Connecticut', total_positive, 0) as flu_connecticut,
    if(state = 'Delaware', total_positive, 0) as flu_delaware,
    if(state = 'District of Columbia', total_positive, 0) as flu_district_of_columbia,
    if(state = 'Florida', total_positive, 0) as flu_florida,
    if(state = 'Georgia', total_positive, 0) as flu_georgia,
    if(state = 'Hawaii', total_positive, 0) as flu_hawaii,
    if(state = 'Idaho', total_positive, 0) as flu_idaho,
    if(state = 'Illinois', total_positive, 0) as flu_illinois,
    if(state = 'Indiana', total_positive, 0) as flu_indiana,
    if(state = 'Iowa', total_positive, 0) as flu_iowa,
    if(state = 'Kansas', total_positive, 0) as flu_kansas,
    if(state = 'Kentucky', total_positive, 0) as flu_kentucky,
    if(state = 'Louisiana', total_positive, 0) as flu_louisiana,
    if(state = 'Maine', total_positive, 0) as flu_maine,
    if(state = 'Maryland', total_positive, 0) as flu_maryland,
    if(state = 'Massachusetts', total_positive, 0) as flu_massachusetts,
    if(state = 'Michigan', total_positive, 0) as flu_michigan,
    if(state = 'Minnesota', total_positive, 0) as flu_minnesota,
    if(state = 'Mississippi', total_positive, 0) as flu_mississippi,
    if(state = 'Missouri', total_positive, 0) as flu_missouri,
    if(state = 'Montana', total_positive, 0) as flu_montana,
    if(state = 'Nebraska', total_positive, 0) as flu_nebraska,
    if(state = 'Nevada', total_positive, 0) as flu_nevada,
    if(state = 'New Hampshire', total_positive, 0) as flu_new_hampshire,
    if(state = 'New Jersey', total_positive, 0) as flu_new_jersey,
    if(state = 'New Mexico', total_positive, 0) as flu_new_mexico,
    if(state = 'New York', total_positive, 0) as flu_new_york,
    if(state = 'North Carolina', total_positive, 0) as flu_north_carolina,
    if(state = 'North Dakota', total_positive, 0) as flu_north_dakota,
    if(state = 'Ohio', total_positive, 0) as flu_ohio,
    if(state = 'Oklahoma', total_positive, 0) as flu_oklahoma,
    if(state = 'Oregon', total_positive, 0) as flu_oregon,
    if(state = 'Pennsylvania', total_positive, 0) as flu_pennsylvania,
    if(state = 'Puerto Rico', total_positive, 0) as flu_puerto_rico,
    if(state = 'Rhode Island', total_positive, 0) as flu_rhode_island,
    if(state = 'South Carolina', total_positive, 0) as flu_south_carolina,
    if(state = 'South Dakota', total_positive, 0) as flu_south_dakota,
    if(state = 'Tennessee', total_positive, 0) as flu_tennessee,
    if(state = 'Texas', total_positive, 0) as flu_texas,
    if(state = 'Utah', total_positive, 0) as flu_utah,
    if(state = 'Vermont', total_positive, 0) as flu_vermont,
    if(state = 'Virginia', total_positive, 0) as flu_virginia,
    if(state = 'Washington', total_positive, 0) as flu_washington,
    if(state = 'West Virginia', total_positive, 0) as flu_west_virginia,
    if(state = 'Wisconsin', total_positive, 0) as flu_wisconsin,
    if(state = 'Wyoming', total_positive, 0) as flu_wyoming
from covid.combined
where extract(year from date) < 2020;