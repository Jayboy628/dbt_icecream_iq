{{
config(
  materialized='incremental',
  unique_key='_source_order_datekey',
  tags = "incremental_model",
  incremental_strategy='merge'
)
}}

with

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

    SELECT * FROM {{ ref('dim_promotions') }}

),

orders AS (

    SELECT * FROM {{ ref('dim_orders') }}

),

dim_data AS (

SELECT 
       ord.order_date                                           AS _source_order_datekey,
       ord.delivery_date                                        AS _source_delivery_datekey,
       ord.orderid												AS _source_order,
	   ord.order_lineid			                				AS _source_order_line,
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
	   ord.description                   						AS description,
       pck.package_type_name                       				AS package,
       ord.quantity												AS quantity,
       ord.unit_price											AS unit_price,
       vat_rate     								            AS vat_rate,
       ord. total_excluding_vat							        AS total_excluding_vat,
	   ord.vat_amount                                           AS vat_amount,
	   ord.total_including_vat                                  AS total_including_vat,
       ord.modified_date							            AS modified_date
FROM 
	orders ord
    LEFT JOIN dim_customers cus 	ON ord.customerid = cus._sourcekey
	LEFT JOIN dim_locations loc 	ON ord.delivery_addressid = loc._source_addresskey
    LEFT JOIN dim_employees emp 	ON ord.employeeid = emp._sourcekey
    LEFT JOIN dim_payment_types pmt ON ord.payment_typeid = pmt._sourcekey
	LEFT JOIN dim_products prd 		ON ord.productid = prd._sourcekey
	LEFT JOIN package_types pck 	ON ord.package_typeid = pck.package_typeid 
	LEFT JOIN dim_promotions pro 	ON ord.promotionid = pro._sourcekey


)

SELECT * FROM dim_data
