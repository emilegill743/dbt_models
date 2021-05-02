with local_uk as (
	select * from {{ source('covid19', 'local_uk') }}
)

select
	date,
	area_name,
	area_code,
	new_cases,
	cum_cases,
	sum(new_cases)
		over(partition by area_code
			 order by date
			 rows between 6 preceding
			 and current row) as weekly_cases,
	avg(new_cases)
		over(partition by area_code
			 order by date
			 rows between 6 preceding
			 and current row) as weekly_average
from local_uk

