{{
config(
  materialized='incremental',
  unique_key='_sourceKey',
  tags = "incremental_model",
  incremental_strategy='merge'
)
}}

with

payment_types AS (

    SELECT * FROM {{ ref('stg_retail__payment_types') }}
),

raw_data AS (
	
	SELECT 
		 payment_typeid,
		 payment_type_name,
		 modified_date AS payment_modified_date
	
	FROM	
		payment_types
),
max_dates AS (
    SELECT MAX(t) AS max_modified_date
    FROM (
        SELECT  payment_modified_date AS t FROM raw_data
	)
)
SELECT 
	 'HSD|' || 
	 COALESCE(CAST(payment_typeid    AS VARCHAR(15)), 'n/a') AS _sourcekey,
	 COALESCE(CAST(payment_type_name AS VARCHAR(70)), 'n/a') AS payment_type_name,
	 COALESCE(payment_modified_date, '1753-01-01')			 AS payment_modified_date,
    	md.max_modified_date 								 AS valid_from,
   	 TO_DATE('9999-12-31')                                   AS valid_to
FROM raw_data
CROSS JOIN max_dates md
{% if is_incremental() %}
LEFT JOIN max_dates md_incremental 
  ON md_incremental.max_modified_date = COALESCE(raw_data.payment_modified_date, '1900-01-01')
{% endif %}

