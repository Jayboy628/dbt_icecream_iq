
with

product_departments as (

    select * from {{ source('retail', 'productdepartments') }}

),

final as (

	SELECT DepartmentID	AS departmentid
	      ,Name			AS name
	      ,Description	AS description
	      ,ModifiedDate	AS modified_date
	FROM product_departments
)

select * from final