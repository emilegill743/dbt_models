with owid_global_vaccinations as (
	select * from {{ source('covid19', 'owid_global_vaccinations') }}
),

jhu_lookup as (
	select * from {{ source('covid19', 'jhu_lookup') }}
),

continents as (
	select distinct
		iso3,
		continent
	from
		jhu_lookup
    where province_state is null)

select
	location,
	case
		when location = 'Jersey' then 'Europe'
		when location = 'Guernsey' then 'Europe'
		when continent = 'AF' then 'Africa'
		when continent = 'AS' then 'Asia'
		when continent = 'EU' then 'Europe'
		when continent = 'NA' then 'North America'
		when continent = 'OC' then 'Oceania'
		when continent = 'SA' then 'South America'
		else continent
	end as continent,
	date,
	max(total_vaccinations)
		over (partition by location
			  order by date rows between
			  unbounded preceding and current row
			 ) as total_vaccinations,
	people_vaccinated,
	people_fully_vaccinated,
	daily_vaccinations,
	total_vaccinations_per_hundred,
	people_vaccinated_per_hundred,
	people_fully_vaccinated_per_hundred,
	daily_vaccinations_per_million
from
	owid_global_vaccinations
	left join
	continents
	on owid_global_vaccinations.iso_code = continents.iso3
where
	owid_global_vaccinations.iso_code is not null
	and
	owid_global_vaccinations.iso_code not like 'OWID_%%'
order by location, date
