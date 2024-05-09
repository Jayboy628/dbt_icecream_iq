{{
config(
  materialized='incremental',
  unique_key='_source_order_datekey',
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


customers AS (

    SELECT * FROM {{ ref('dim_customers') }}

),


locations AS (

    SELECT * FROM {{ ref('dim_locations') }}

),

employees AS (

    SELECT * FROM {{ ref('dim_employees') }}

),
payment_types AS (

    SELECT * FROM {{ ref('dim_payment_types') }}

),
products AS (

    SELECT * FROM {{ ref('dim_products') }}

),
package_types AS (

    SELECT * FROM {{ ref('stg_retail__package_types') }}

),
promotions AS (

    SELECT * FROM {{ ref('dim_employees') }}

),


dim_data AS (

SELECT 
       CAST(COALESCE(ord.order_date, '1900-01-01') AS date)     AS _source_order_datekey,
       CAST(COALESCE(ord.delivery_date, '1900-01-01') AS date)  AS _source_delivery_datekey,
       ord.orderid												AS _source_order,
	   COALESCE(orl.order_lineid, 0)							AS _source_order_line,
	   cus._sourcekey											AS _source_customerkey,
	   emp._sourcekey											AS _source_employeekey,
	   prd._sourcekey											AS _source_productkey,
	   pmt._sourcekey									        AS _source_payment_typekey,
	   loc._source_countrykey									AS _source_delivery_countrykey,
	   loc._source_provincekey									AS _source_delivery_provincekey,
	   loc._source_citykey										AS _source_delivery_citykey,
	   loc._source_addresskey									AS _source_delivery_addresskey,
	   loc._sourcekey											AS _source_delivery_Locationkey,
	   pro._sourcekey											AS _source_promotionkey,
	   COALESCE(orl.description, 'N/A')							AS description,
       COALESCE(pck.package_type_name, 'N/A')					AS package,
       orl.quantity												AS quantity,
       orl.unit_price											AS unit_price,
       COALESCE(orl.vat_rate, 0.20)								AS vat_rate,
       orl.quantity * orl.unit_price							AS total_excluding_vat,
	   orl.quantity * orl.unit_price * COALESCE(orl.vat_rate, 0.20) AS vat_amount,
	   orl.quantity*orl.unit_price*(1+ COALESCE(orl.vat_rate, 0.20)) AS total_including_vat,
       CASE 
		WHEN orl.modified_date > ord.modified_date 
			THEN orl.modified_date 
		ELSE ord.modified_date END								AS modified_date
FROM 
	orders ord
	LEFT JOIN order_lines orl 		ON ord.orderid = orl.orderid
    LEFT JOIN dim_customers cus 	ON ord.customerid = cus._sourcekey
	LEFT JOIN dim_locations loc 	ON ord.delivery_addressid = loc._source_addresskey
    LEFT JOIN dim_employees emp 	ON ord.employeeid = emp._sourcekey
    LEFT JOIN dim_payment_types pmt ON ord.payment_typeid = pmt._sourcekey
	LEFT JOIN dim_products prd 		ON orl.productid = prd._sourcekey
	LEFT JOIN package_types pck 	ON orl.package_typeid = pck.package_typeid 
	LEFT JOIN dim_promotions pro 	ON orl.promotionid = pro._sourcekey


)

SELECT * FROM dim_data
