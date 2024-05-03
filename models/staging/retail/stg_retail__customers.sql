with

customers as (

    select * from {{ source('retail', 'customers') }}

),

final as (

	SELECT [CustomerID] 			AS customerid
	      ,[FirstName] 				AS first_name
	      ,[LastName] 				AS last_name
	      ,[FullName]				AS full_name
	      ,[Title]					AS title
	      ,[DeliveryAddressID]		AS delivery_addressid
	      ,[BillingAddressID]		AS billing_addressid
	      ,[PhoneNumber]			AS phone_number
	      ,[Email]					AS email	
	      ,[ModifiedDate]			AS modified_date
	FROM  customers
)

select * from final