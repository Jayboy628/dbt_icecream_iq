with

cities as (

    select * from {{ source('retail', 'cities') }}

),

final as (

	SELECT [CityID] AS cityid
	      ,[CityName] AS city_name
	      ,[ProvinceID] AS provinceid
	      ,[Population] AS population
	      ,[ModifiedDate] AS modified_date
	  FROM cities
)

select * from final