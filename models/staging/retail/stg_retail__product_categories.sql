
with

product_categories as (

    select * from {{ source('retail', 'productcategories') }}

),

final as (

	SELECT [CategoryID] 			AS categoryid
	      ,[CategoryName]			AS category_name
	      ,[CategoryDescription]	AS category_description
	      ,[DepartmentID]			AS departmentid
	      ,[ModifiedDate]			AS modified_date
	  FROM product_categories
)

select * from final