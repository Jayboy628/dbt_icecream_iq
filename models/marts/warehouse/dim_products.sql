{{ config(
  materialized='incremental',
  unique_key='_sourcekey',
  tags='incremental_model',
  incremental_strategy='merge'
) }}

with

products AS (
    SELECT * FROM {{ ref('stg_retail__products') }}
),

product_subcategories AS (
    SELECT * FROM {{ ref('stg_retail__product_subcategories') }}
),

product_categories AS (
    SELECT * FROM {{ ref('stg_retail__product_categories') }}
    
),

units_of_measure AS (
    SELECT * FROM {{ ref('stg_retail__units_of_measure') }}
),

product_departments AS (
    SELECT * FROM {{ ref('stg_retail__product_departments') }}
),
raw_data AS (
    SELECT 
        prod.productid              AS productid,
        prod.product_name           AS product_name,
        prod.product_Code           AS product_code,
        prod.product_description    AS product_description,
        subcat.subcategory_name     AS subcategory,
        cat.category_name           AS category,
        dep.name                    AS department,
        um.units_measure_code       AS units_measure_code,
        um.name                     AS units_measure_name,
        prod.unit_price             AS unit_price,
        prod.discontinued           AS discontinued,
        prod.modified_date          AS product_modified_date,
        subcat.modified_date        AS subcategory_modified_date,
        cat.modified_date           AS category_modified_date,
        dep.modified_date           AS department_modified_date,
        um.modified_date            AS modified_date
    FROM products prod
    LEFT JOIN product_subcategories subcat ON prod.subcategoryid = subcat.product_subcategoryid
    LEFT JOIN product_categories cat       ON subcat.product_categoryid = cat.categoryid
    LEFT JOIN product_departments dep      ON cat.departmentid = dep.departmentid
    LEFT JOIN units_of_measure um          ON prod.unit_of_measureid = um.units_of_measureid
   
)
,
max_dates AS (
    SELECT MAX(t) AS max_modified_date
    FROM (
        SELECT product_modified_date 		AS t FROM raw_data
        UNION ALL
        SELECT subcategory_modified_date 	AS t FROM raw_data
        UNION ALL
        SELECT category_modified_date 		AS t FROM raw_data
        UNION ALL
        SELECT department_modified_date 	AS t FROM raw_data
        UNION ALL
        SELECT modified_date 				AS t FROM raw_data
    )
)
    SELECT 
        'HSD|' || COALESCE(CAST(productid             AS VARCHAR(15)), 'n/a')   AS _sourceKey,
        COALESCE(CAST(product_name 			AS VARCHAR(200)), 'n/a')  AS product_name,
        COALESCE(CAST(product_code 			AS VARCHAR(50)),  'n/a')  AS product_code,
        COALESCE(CAST(product_description 	AS VARCHAR(200)), 'n/a')  AS product_description,
        COALESCE(CAST(subcategory 		    AS VARCHAR(200)), 'n/a')  AS subcategory,
        COALESCE(CAST(category 		        AS VARCHAR(200)), 'n/a')  AS category,
        COALESCE(CAST(department			AS VARCHAR(200)), 'n/a')  AS department,
        COALESCE(CAST(units_measure_code    AS VARCHAR(10)),  'n/a')  AS units_measure_code,
        COALESCE(CAST(units_measure_name 	AS VARCHAR(50)),  'n/a')  AS units_measure_name,
        COALESCE(CAST(unit_price  			AS DECIMAL(18,2)), 0)     AS unit_price, 
        COALESCE(CAST(CASE discontinued WHEN 1 THEN 'Yes' ELSE 'No' END AS VARCHAR), 'n/a') AS discontinued,
        COALESCE(product_modified_date, 	'1753-01-01')       AS product_modified_date,
        COALESCE(subcategory_modified_date,'1753-01-01')        AS subcategory_modified_date,
        COALESCE(category_modified_date, 	'1753-01-01')       AS category_modified_date,
        COALESCE(department_modified_date, '1753-01-01')        AS department_modified_date,
        COALESCE(modified_date, 			'1753-01-01')       AS modified_date,
		md.max_modified_date                                   AS valid_from,
		TO_DATE('9999-12-31')                                  AS valid_to
	FROM raw_data
	CROSS JOIN max_dates md
		{% if is_incremental() %}
	LEFT JOIN max_dates md_incremental 
		  ON md_incremental.max_modified_date = COALESCE(raw_data.product_modified_date, 		  '1900-01-01')
		  OR md_incremental.max_modified_date = COALESCE(raw_data.subcategory_modified_date, 	  '1900-01-01')
		  OR md_incremental.max_modified_date = COALESCE(raw_data.category_modified_date,  		  '1900-01-01')
		  OR md_incremental.max_modified_date = COALESCE(raw_data.department_modified_date, 	  '1900-01-01')
		  OR md_incremental.max_modified_date = COALESCE(raw_data.modified_date,  				  '1900-01-01')
		{% endif %}
		
		