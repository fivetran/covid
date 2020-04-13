-- Download the data for all seasons by HHS region from FluView interactive.
-- Delete the header lines from the CSVs.
-- 
-- Download case data from https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-states.csv
-- Remove header, save as us_states.csv
-- 
-- Make hhs_regions.csv out of HHS region definitions from https://www.marchofdimes.org/peristats/popup.aspx?width=50%&s=faq&reg=&top=&id=20
--
-- Download state populations from https://www2.census.gov/programs-surveys/popest/tables/2010-2019/state/totals/nst-est2019-01.xlsx
-- Select 2019 estimate and save as census_population.csv

drop table if exists ilinet_visits;
drop table if exists clinical_labs;
drop table if exists public_health_labs;
drop table if exists combined_labs;
drop table if exists hhs_regions;
drop table if exists census_population;
drop table if exists nyt_cases;

create table ilinet_visits (
    -- REGION TYPE
    region_type text,
    -- REGION
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

create table public_health_labs (
    -- REGION TYPE
    region_type text,
    -- REGION
    region text,
    -- YEAR
    year integer,
    -- WEEK
    week integer,
    -- TOTAL SPECIMENS
    total_specimens integer,
    -- A (2009 H1N1)
    a_h1n1 integer,
    -- A (H3)
    a_h3 integer,
    -- A (Subtyping not Performed)
    a_no_subtype integer,
    -- B
    b integer,
    -- BVic
    b_vic integer,
    -- BYam
    b_yam integer,
    -- H3N2v
    h3n2v integer,
    primary key (region, year, week)
);

create table combined_labs (
    -- REGION TYPE
    region_type text,
    -- REGION
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

create table hhs_regions (
    region text,
    state text,
    primary key (region, state)
);

create table census_population (
    state text, 
    population integer,
    primary key (state)
);

create table nyt_cases (
    d date,
    state text,
    fips integer,
    cases integer,
    deaths integer,
    primary key (d, state)
);

.mode csv
.import ILINet.csv ilinet_visits
.import WHO_NREVSS_Clinical_Labs.csv clinical_labs
.import WHO_NREVSS_Public_Health_Labs.csv public_health_labs
.import WHO_NREVSS_Combined_prior_to_2015_16.csv combined_labs
.import hhs_regions.csv hhs_regions
.import census_population.csv census_population
.import us_states.csv nyt_cases