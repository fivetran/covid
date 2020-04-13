
drop table if exists ilinet_visits;
drop table if exists clinical_labs;
drop table if exists combined_labs;
drop table if exists nyt_cases;
drop table if exists census_population;

create table ilinet_visits (
    -- REGION TYPE
    region_type text,
    -- REGION
    -- Note that "New York City" is a separate region.
    region text,
    -- YEAR
    year integer,
    -- WEEK
    week integer,
    -- % WEIGHTED ILI
    weighted_ili text,
    -- %UNWEIGHTED ILI
    unweighted_ili text,
    -- AGE 0-4
    age_0_4 text,
    -- AGE 25-49
    age_25_29 text,
    -- AGE 25-64
    age_25_64 text,
    -- AGE 5-24
    age_5_24 text,
    -- AGE 50-64
    age_50_64 text,
    -- AGE 65
    age_65 text,
    -- ILITOTAL
    ili_total integer,
    -- NUM. OF PROVIDERS
    num_providers integer,
    -- TOTAL PATIENTS
    total_patients integer,
    primary key (region, year, week)
);

-- After 2015, flu testing data is reported separately for clinical labs and public health labs.
-- Clinical lab data is reported by state, while public health lab data is only reported by region.
-- Clinical labs report ~5x more samples than public health labs.
create table clinical_labs (
    -- REGION TYPE
    region_type text,
    -- REGION
    -- Note that "New York City" is a separate region, but it always reports null.
    region text,
    -- YEAR
    year integer,
    -- WEEK
    week integer,
    -- TOTAL SPECIMENS
    total_specimens integer,
    -- TOTAL A
    total_a integer,
    -- TOTAL B
    total_b integer,
    -- PERCENT POSITIVE
    percent_positive real,
    -- PERCENT A
    percent_a real,
    -- PERCENT B
    percent_b real,
    primary key (region, year, week)
);

create table combined_labs (
    -- REGION TYPE
    region_type text,
    -- REGION
    -- Note that "New York City" is a separate region.
    region text,
    -- YEAR
    year integer,
    -- WEEK
    week integer,
    -- TOTAL SPECIMENS
    total_specimens integer,
    -- PERCENT POSITIVE
    percent_positive real,
    -- A (2009 H1N1)
    a_h1n1 integer,
    -- A (H1)
    a_h1 integer,
    -- A (H3)
    a_h3 integer,
    -- A (Subtyping not Performed)
    a_no_subtype integer,
    -- A (Unable to Subtype)
    a_unable_to_subtype integer,
    -- B
    b integer,
    -- H3N2v
    h3n2v integer,
    primary key (region, year, week)
);

create table nyt_cases (
    date date,
    state text,
    fips integer,
    cases integer,
    deaths integer,
    primary key (date, state)
);

create table census_population (
    state text,
    year integer,
    population integer,
    primary key (state, year)
);

copy ilinet_visits from './data/ILINet.csv' with header null 'X';
copy clinical_labs from './data/WHO_NREVSS_Clinical_Labs.csv' with header null 'X';
copy combined_labs from './data/WHO_NREVSS_Combined_prior_to_2015_16.csv' with header null 'X';
copy nyt_cases from './data/NYT_Cases.csv' with header;
copy census_population from './data/Census_Population.csv' with header;