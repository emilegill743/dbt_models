/*preparing global_by_day dataset
with cases, deaths, new_cases, new_deaths
and vaccinations stats by day*/

with jhu_global_cases as (
    select * from {{ source('covid19', 'jhu_global_cases') }}
),

jhu_global_deaths as (
    select * from {{ source('covid19', 'jhu_global_deaths') }}
),

owid_global_vaccinations as (
	select * from {{ source('covid19', 'owid_global_vaccinations') }}
),

cases as ( 
	select
		jhu_global_cases.date,
		sum(jhu_global_cases.cases) as cases
	from jhu_global_cases
	group by jhu_global_cases.date),
	
deaths as (
	select jhu_global_deaths.date,
		sum(jhu_global_deaths.deaths) as deaths
	from jhu_global_deaths
	group by jhu_global_deaths.date),
	
vaccinations as (
	select
		date,
		total_vaccinations,
		people_vaccinated,
		people_fully_vaccinated,
		daily_vaccinations,
		total_vaccinations_per_hundred,
		people_vaccinated_per_hundred,
		people_fully_vaccinated_per_hundred,
		daily_vaccinations_per_million
	from owid_global_vaccinations
	where location = 'World'
	order by date)

select
	cases.date,
	cases.cases,
	cases.cases - lag(cases.cases)
		over(order by cases.date) as new_cases,
	deaths.deaths,
	deaths - lag(deaths)
		over(order by cases.date) as new_deaths,
	vaccinations.total_vaccinations,
	vaccinations.people_vaccinated,
	vaccinations.people_fully_vaccinated,
	vaccinations.daily_vaccinations,
	vaccinations.total_vaccinations_per_hundred,
	vaccinations.people_vaccinated_per_hundred,
	vaccinations.people_fully_vaccinated_per_hundred,
	vaccinations.daily_vaccinations_per_million
from cases
inner join deaths
	on cases.date = deaths.date
left join vaccinations
	on cases.date = vaccinations.date
order by cases.date
