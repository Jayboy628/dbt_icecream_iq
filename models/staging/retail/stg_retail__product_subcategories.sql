
with

product_subcategories as (

    select * from {{ source('retail', 'productsubcategories') }}

),

final as (

	SELECT ProductSubcategoryID 		AS product_subcategoryid
	      ,ProductCategoryID			AS product_categoryid
	      ,SubcategoryName				AS subcategory_name
	      ,SubcategoryDescription		AS subcategory_description
	      ,ModifiedDate					AS modified_date
	FROM product_subcategories
)

select * from final