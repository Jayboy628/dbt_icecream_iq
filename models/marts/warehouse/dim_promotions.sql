
{{ config(
  materialized='incremental',
  unique_key='_sourcekey',
  tags='incremental_model',
  incremental_strategy='merge'
) }}

with

promotions AS 
(
    SELECT * FROM {{ ref('stg_retail__promotions') }}
),

raw_data AS (
    SELECT 
        pro.promotionid                 AS promotionid,
        pro.deal_description            AS deal_description,
        pro.start_date                  AS start_date,
        pro.end_date                    AS end_date,
        pro.discount_Percentage         AS discount_Percentage, 
        pro.modified_date               AS promotion_modified_date,
        pro.modified_date               AS modified_date
    FROM    
        promotions pro
),

transformed_data AS (
	SELECT 
	    'HSD|' || COALESCE(CAST(promotionid AS VARCHAR(15)), 'n/a') AS _sourceKey,
	    COALESCE(CAST(deal_description      AS VARCHAR(100)), 'n/a') AS deal_description,
	    COALESCE(CAST(start_date AS DATE), '1753-01-01') AS start_date,
	    COALESCE(CAST(end_date AS DATE), '1753-01-01') AS end_date,
	    COALESCE(CAST(discount_Percentage AS DECIMAL(18,2)), 0) AS discount_Percentage,
	    COALESCE(CAST(promotion_modified_date AS DATETIME), '1753-01-01') AS promotion_modified_date,
	    COALESCE(CAST(modified_date AS DATETIME), '1753-01-01') AS valid_from,
	    '9999-12-31' AS valid_to
	FROM raw_data
    
)

SELECT * FROM transformed_data

{% if is_incremental() %}
WHERE 
    promotion_modified_date > (SELECT COALESCE(MAX(promotion_modified_date), '1900-01-01') FROM {{ this }})
{% endif %}
