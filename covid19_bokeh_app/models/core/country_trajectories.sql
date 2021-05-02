with jhu_global_cases as (
    select * from {{ source('covid19', 'jhu_global_cases') }}
)

select
	region,
	rank()
		over (partition by region
			  order by date)
		as days_since_arrival,
	cases
from
	(select region, date, sum(cases) as cases
	 from jhu_global_cases
	 group by region, date) as subquery
where cases > 100
