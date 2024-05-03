
with

employees as (

    select * from {{ source('retail', 'employees') }}

),

final as (

	SELECT [EmployeeID] 			AS employeeid
	      ,[FirstName] 				AS first_name
	      ,[LastName] 				AS last_name
	      ,[Title]					AS title
		  ,[BirthDate]				AS birth_date
		  ,[Gender]					AS gender
		  ,[HireDate]				AS hire_date
		  ,[JobTitle]				AS job_title
		  ,[AddressID]				AS addressid
		  ,[ManagerID]				AS manager_key
		  ,[ModifiedDate]			AS modified_date
   FROM employees
)

select * from final