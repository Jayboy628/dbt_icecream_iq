
with

package_types as (

    select * from {{ source('retail', 'packagetypes') }}

),

final as (

	SELECT [PackageTypeID] 		AS package_typeid
	      ,[PackageTypeName]	AS package_type_name
	      ,[ModifiedDate]		AS modified_date
	  FROM package_types
)

select * from final