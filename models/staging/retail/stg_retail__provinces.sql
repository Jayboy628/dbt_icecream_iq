

with

provinces as (

    select * from {{ source('retail', 'provinces') }}

),

final as (

	SELECT [ProvinceID]		AS provinceid
	      ,[ProvinceCode]	AS province_code
	      ,[ProvinceName]	AS province_name
	      ,[CountryID]		AS countryid
	      ,[Population]		AS population
	      ,[ModifiedDate]	AS modified_date
	 FROM provinces
)

select * from final