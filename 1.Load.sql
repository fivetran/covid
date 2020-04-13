
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

copy ilinet_visits from './data/ILINet.csv' with header null 'X';
copy clinical_labs from './data/WHO_NREVSS_Clinical_Labs.csv' with header null 'X';
copy combined_labs from './data/WHO_NREVSS_Combined_prior_to_2015_16.csv' with header null 'X';
copy nyt_cases from './data/NYT_Cases.csv' with header;