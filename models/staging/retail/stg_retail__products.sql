
with

products as (

    select * from {{ source('retail', 'products') }}

),

final as (

	SELECT [ProductID]				AS productid
	      ,[ProductName]			AS product_name
	      ,[ProductCode]			AS product_code
	      ,[ProductDescription]		AS product_description
	      ,[SubcategoryID]			AS subcategoryid
	      ,[UnitOfMeasureID]		AS unit_of_measureID
	      ,[UnitPrice]				AS unit_price
	      ,[Discontinued]			AS discontinued
	      ,[ModifiedDate]			AS modified_date
	FROM products
)

select * from final