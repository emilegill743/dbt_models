/*preparing global time evolution dataset
with cases, deaths, new_cases, new_deaths
for each geographical province*/

with jhu_global_cases as (
    select * from {{ source('covid19', 'jhu_global_cases') }}
),

jhu_global_deaths as (
    select * from {{ source('covid19', 'jhu_global_deaths') }}
),

jhu_us_cases as (
	select * from {{ source('covid19', 'jhu_us_cases') }}
),

jhu_us_deaths as (
	select * from {{ source('covid19', 'jhu_us_deaths') }}
),

us_states_coords as (
	select * from {{ ref('kaggle_states') }}
),

-- combining global cases with us cases by state
cases as (
	select * from jhu_global_cases
	where region <> 'US'
	union
	select
		jhu_us_cases.region,
		jhu_us_cases.province,
		us_states_coords."latitude" as lat,
		us_states_coords."longitude" as long,
		jhu_us_cases.date,
		jhu_us_cases.cases
	from jhu_us_cases
	-- mapping to state latitude/longitude
	-- in us_states_coords table
	inner join us_states_coords
	on jhu_us_cases.province = us_states_coords."city"
	),
	
-- combining global deaths with us deaths by state
deaths as (
	select * from jhu_global_deaths
	where region <> 'US'
	union
	select
		jhu_us_deaths.region,
		jhu_us_deaths.province,
		us_states_coords."latitude" as lat,
		us_states_coords."longitude" as long,
		jhu_us_deaths.date,
		jhu_us_deaths.deaths
	from jhu_us_deaths
	-- mapping to state latitude/longitude
	-- in us_states_coords table
	inner join us_states_coords
	on jhu_us_deaths.province = us_states_coords."city"
	),
	
-- joining cases and deaths datasets
cases_deaths as (
	select
		cases.region,
		cases.province,
		cases.lat,
		cases.long,
        cases.date,
        cases.cases as cases,
        deaths.deaths as deaths
    from cases
    inner join deaths
    on
        cases.date = deaths.date
        and
        cases.region = deaths.region
        and
        cases.province = deaths.province)

-- calculating daily new cases/deaths
select
    date, region, province, lat, long, cases, deaths,
    cases - lag(cases)
        over( partition by region, province
			  order by region, province, date
			) as new_cases,
    deaths - lag(deaths)
        over( partition by region, province
			  order by region, province, date
			) as new_deaths
from cases_deaths
-- filtering out non-geographical locations
where lat <> 0  and long <> 0