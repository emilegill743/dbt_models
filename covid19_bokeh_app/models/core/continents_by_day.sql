/*Preparing continents_by_day dataset
with cases, deaths, new_cases, new_deaths
by continents by day*/

with jhu_global_cases as (
    select * from {{ source('covid19', 'jhu_global_cases') }}
),

jhu_global_deaths as (
    select * from {{ source('covid19', 'jhu_global_deaths') }}
),

jhu_lookup as (
    select * from {{ source('covid19', 'jhu_lookup') }}
),

continents as (
    select distinct
        country_region,
        continent
    from
        jhu_lookup
    where province_state is null),

cases as (
    select
        jhu_global_cases.region,
        jhu_global_cases.date,
        sum(jhu_global_cases.cases) as cases
    from jhu_global_cases
    group by
        jhu_global_cases.date,
        jhu_global_cases.region),
    
deaths as (
    select
        jhu_global_deaths.date,
        jhu_global_deaths.region,
        sum(jhu_global_deaths.deaths) as deaths
    from jhu_global_deaths
    group by
        jhu_global_deaths.date,
        jhu_global_deaths.region),

cases_deaths as (
    select
        cases.date,
        cases.region,
        cases.cases,
        deaths.deaths
    from cases 
    inner join deaths
        on cases.date = deaths.date
        and cases.region = deaths.region
    order by cases.date, cases.region)

select
    date,
    case continent
		when 'AF' then 'Africa'
		when 'AS' then 'Asia'
		when 'EU' then 'Europe'
		when 'NA' then 'North America'
		when 'OC' then 'Oceania'
		when 'SA' then 'South America'
		else continent
	end as continent,
	sum(cases) as cases,
	sum(deaths) as deaths,
    sum(cases) - lag(sum(cases))
        over(partition by continent order by date) as new_cases,
    sum(deaths) - lag(sum(deaths))
        over(partition by continent order by date) as new_deaths
from cases_deaths
inner join continents
	on cases_deaths.region = continents.country_region
where continent <> 'N/A'
group by date, continent
