
with

payment_types as (

    select * from {{ source('retail', 'paymenttypes') }}

),

final as (

	SELECT [PaymentTypeID] 		AS payment_typeid
	      ,[PaymentTypeName]	AS payment_type_name
	      ,[ModifiedDate]		AS modified_date
	FROM payment_types
)

select * from final