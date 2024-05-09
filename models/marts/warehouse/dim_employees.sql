{{
config(
  materialized='incremental',
  unique_key='_sourcekey',
  tags=['incremental_model'],
  incremental_strategy='merge'
)
}}

with

employees AS (
    SELECT * FROM {{ ref('stg_retail__employees') }}
),

addresses AS (
    SELECT * FROM {{ ref('stg_retail__addresses') }}
),

cities AS (
    SELECT * FROM {{ ref('stg_retail__cities') }}
),

provinces AS (
    SELECT * FROM {{ ref('stg_retail__provinces') }}
),

countries AS (
    SELECT * FROM {{ ref('stg_retail__countries') }}
),

raw_data AS (
  SELECT 
    emp.employeeid,
	cou.countryid,
	prv.provinceid,
	cit.cityid, 
	adr.addressid,
	emp.last_name, 		
	emp.first_name, 		
	emp.title, 			
	emp.birth_date, 		
	emp.gender, 			
	emp.hire_date,		
	emp.job_title,		
	adr.address_line1, 
	cit.city_name, 		
	cou.country_name,
	emp.manager_key, 		
	emp.modified_date 	AS employee_modified_date,
	adr.modified_date 	AS address_modified_date
FROM employees emp
	LEFT JOIN addresses adr ON emp.addressid = adr.addressid
	LEFT JOIN cities cit ON adr.cityid = cit.cityid
  	LEFT JOIN provinces prv ON cit.provinceid = prv.provinceid
  	LEFT JOIN countries cou ON prv.countryid = cou.countryid
),
max_dates AS (
    SELECT MAX(t) AS max_modified_date
    FROM (
        SELECT employee_modified_date AS t FROM raw_data
        UNION ALL
        SELECT address_modified_date AS t FROM raw_data
    )
)
SELECT 
    'HSD|' || 
    COALESCE(CAST(employeeid AS VARCHAR(15)), 'n/a')    AS _sourcekey,
	LEFT(CONCAT_WS('HSD|',
    COALESCE(CAST(countryid 	AS VARCHAR), '0'),
    COALESCE(CAST(provinceid 	AS VARCHAR), '0'),
    COALESCE(CAST(cityid 		AS VARCHAR), '0'),
    COALESCE(CAST(addressid 	AS VARCHAR), '0')
	),30) AS location_key,
	COALESCE(CAST(last_name 	AS VARCHAR(100)), 'n/a') AS last_name,
	COALESCE(CAST(first_name 	AS VARCHAR(100)), 'n/a') AS first_name,
	COALESCE(CAST(title 		AS VARCHAR(30)),  'n/a') AS title,
	COALESCE(CAST(birth_date    AS DATE), 'not given')   AS birth_date,
	COALESCE(CAST(gender 		AS VARCHAR(10)),  'n/a') AS gender,
	COALESCE(CAST(hire_date     AS DATE), 'not given')   AS hire_date,
	COALESCE(CAST(job_title     AS VARCHAR(100)), 'n/a') AS job_title,
	COALESCE(CAST(address_line1 AS VARCHAR(100)), 'n/a') AS address_line,
	COALESCE(CAST(city_name 	AS VARCHAR(100)), 'n/a') AS city,
	COALESCE(CAST(country_name 	AS VARCHAR(100)), 'n/a') AS country,
	'HSD|' || 
	COALESCE(CAST(manager_key   AS VARCHAR(15)),  '0')   AS manager_key,
	COALESCE(employee_modified_date, '1753-01-01') 		 AS employee_modified_date,
	COALESCE(address_modified_date, '1753-01-01')        AS address_modified_date,
    md.max_modified_date AS valid_from,
    TO_DATE('9999-12-31') AS valid_to
FROM raw_data
CROSS JOIN max_dates md
{% if is_incremental() %}
LEFT JOIN max_dates md_incremental 
  ON md_incremental.max_modified_date = COALESCE(raw_data.employee_modified_date, '1900-01-01')
  OR md_incremental.max_modified_date = COALESCE(raw_data.address_modified_date, '1900-01-01')
{% endif %}
