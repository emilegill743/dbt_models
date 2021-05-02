with owid_global_vaccinations as (
	select * from {{ source('covid19', 'owid_global_vaccinations') }}
)

select
	owid.location as continent,
	owid.date,
	max(total_vaccinations)
		over (partition by owid.location
			  order by owid.date rows between
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
	owid_global_vaccinations as owid
where
	owid.location in ('Asia', 'Africa', 'Europe', 'North America', 'South America', 'Oceania')
order by
	owid.location,
	owid.date
