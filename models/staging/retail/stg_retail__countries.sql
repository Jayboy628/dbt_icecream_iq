with

countries as (

    select * from {{ source('retail', 'countries') }}

),

final as (

	SELECT [CountryID] AS countryid
	      ,[CountryName] AS Country_name
	      ,[FormalName]AS formal_name
	      ,[CountryCode] AS country_code
	      ,[Population] AS population
	      ,[Continent] AS contient
	      ,[Region] AS region
	      ,[Subregion] AS subregion
	      ,[ModifiedDate] AS modified_date
	  FROM countries
)

select * from final