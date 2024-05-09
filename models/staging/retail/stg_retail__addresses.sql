with

addresses as (

    select * from {{ source('retail', 'addresses') }}

),

final as (

	SELECT AddressID AS addressid
	      ,AddressLine1 AS address_line1
	      ,AddressLine2 AS address_line2
	      ,CityID AS cityid
	      ,PostalCode AS postal_code
	      ,ModifiedDate AS modified_date
	  FROM addresses

)

select * from final