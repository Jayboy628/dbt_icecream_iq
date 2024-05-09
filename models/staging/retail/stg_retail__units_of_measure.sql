

with

units_of_measure as (

    select * from {{ source('retail', 'unitsofmeasure') }}

),

final as (

	SELECT UnitOfMeasureID 	AS units_of_measureid
	      ,UnitMeasureCode 	AS units_measure_code	
	      ,Name			 	AS name
	      ,ModifiedDate	 	AS modified_date
	FROM units_of_measure
)

select * from final