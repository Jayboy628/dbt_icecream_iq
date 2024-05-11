{{
config(
  materialized='incremental',
  unique_key='order_date',
  tags = "incremental_model",
  incremental_strategy='merge'
)
}}

with

orders AS (

    SELECT * FROM {{ ref('stg_retail__orders') }}

),
order_lines AS (

    SELECT * FROM {{ ref('stg_retail__order_lines') }}

),


package_types AS (

    SELECT * FROM {{ ref('stg_retail__package_types') }}

),


dim_data AS (

SELECT 
       CAST(COALESCE(ord.order_date, '1900-01-01') AS date)     AS order_date,
       CAST(COALESCE(ord.delivery_date, '1900-01-01') AS date)  AS delivery_date,
       ord.orderid												AS orderid,
	   COALESCE(orl.order_lineid, 0)							AS order_lineid,
	   ord.customerid                                  AS customerid,
       ord.employeeid                                  AS employeeid,
       ord.payment_typeid                              AS payment_typeid,
       ord.delivery_addressid                          AS delivery_addressid,
       orl.package_typeid                              AS package_typeid,
       orl.productid                                   AS productid,
       orl.promotionid                                 AS promotionid,
	   COALESCE(orl.description, 'N/A')							AS description,
       COALESCE(pck.package_type_name, 'N/A')					AS package,
       orl.quantity												AS quantity,
       orl.unit_price											AS unit_price,
       COALESCE(orl.vat_rate, 0.20)								AS vat_rate,
       orl.quantity * orl.unit_price							AS total_excluding_vat,
	   orl.quantity * orl.unit_price * COALESCE(orl.vat_rate, 0.20) AS vat_amount,
	   orl.quantity * orl.unit_price * (1 + COALESCE(orl.vat_rate, 0.20)) AS total_including_vat,
       CASE 
		WHEN orl.modified_date > ord.modified_date 
			THEN orl.modified_date 
		ELSE ord.modified_date END								AS modified_date
FROM 
	orders ord
	LEFT JOIN order_lines orl   ON ord.orderid = orl.orderid
	LEFT JOIN package_types pck ON orl.package_typeid = pck.package_typeid 

)

SELECT * FROM dim_data
