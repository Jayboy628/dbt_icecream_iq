{{ config(
  materialized='incremental',
  unique_key='_sourcekey',
  tags=['marts', 'warehouse'],
  incremental_strategy='merge'
) }}

with

customers as (
    select * from {{ ref('stg_retail__customers') }}
),

addresses as (
    select * from {{ ref('stg_retail__addresses') }}
),

cities as (
    select * from {{ ref('stg_retail__cities') }}
),

provinces as (
    select * from {{ ref('stg_retail__provinces') }}
),

countries as (
    select * from {{ ref('stg_retail__countries') }}
),

raw_data as (
  select 
    cus.customerid,
    cus.first_name,
    cus.last_name,
    cus.full_name,
    cus.title,
    cus.phone_number,
    cus.email,
    cus.modified_date  as customer_modified_date,
    badr.modified_date as billing_address_modified_date,
    dadr.modified_date as delivery_address_modified_date,
    cus.billing_addressid,
    cus.delivery_addressid,
    badr.cityid     as billing_cityid,
    bprv.provinceid as billing_provinceid,
    bcou.countryid  as billing_countryid,
    dadr.cityid     as delivery_cityid,
    dprv.provinceid as delivery_provinceid,
    dcou.countryid  as delivery_countryid
  from customers cus
  left join addresses badr on cus.billing_addressid = badr.addressid
  left join cities bcit    on badr.cityid = bcit.cityid
  left join provinces bprv on bcit.provinceid = bprv.provinceid
  left join countries bcou on bprv.countryid = bcou.countryid

  left join addresses dadr on cus.delivery_addressid = dadr.addressid
  left join cities dcit    on dadr.cityid = dcit.cityid
  left join provinces dprv on dcit.provinceid = dprv.provinceid
  left join countries dcou on dprv.countryid = dcou.countryid
),
max_dates AS (
    SELECT MAX(t) AS max_modified_date
    FROM (
        SELECT customer_modified_date AS t FROM raw_data
        UNION ALL
        SELECT delivery_address_modified_date AS t FROM raw_data
        UNION ALL
        SELECT billing_address_modified_date AS t FROM raw_data
    )
)

select 

    'HSD|' ||CAST(customerid  AS VARCHAR(10))                        AS _sourcekey,
    COALESCE(CAST(first_name  AS VARCHAR(100)), 'n/a')               AS first_name,
    COALESCE(CAST(last_name   AS VARCHAR(100)), 'n/a')               AS last_name,
    COALESCE(CAST(full_name   AS VARCHAR(200)), 'n/a')               AS full_name,
    COALESCE(CAST(title       AS VARCHAR(100)), 'n/a')               AS title,
	
    'HSD|' || LEFT(CONCAT_WS('|', 
        COALESCE(CAST(delivery_countryid    AS VARCHAR), '0'),
        COALESCE(CAST(delivery_provinceid   AS VARCHAR), '0'),
        COALESCE(CAST(delivery_cityid       AS VARCHAR), '0'),
        COALESCE(CAST(delivery_addressid    AS VARCHAR), '0')), 15) AS delivery_location_key,

    'HSD|' || LEFT(CONCAT_WS('|', 
        COALESCE(CAST(billing_countryid     AS VARCHAR), '0'),
        COALESCE(CAST(billing_provinceid    AS VARCHAR), '0'),
        COALESCE(CAST(billing_cityid        AS VARCHAR), '0'),
        COALESCE(CAST(billing_addressid     AS VARCHAR), '0')), 15) AS billing_location_key,


    COALESCE(CAST(phone_number AS VARCHAR(24)), 'n/a')              AS phone_number,
    COALESCE(customer_modified_date,         '1753-01-01') 			AS customer_modified_date,
	COALESCE(delivery_address_modified_date, '1753-01-01') 		    AS delivery_address_modified_date,
	COALESCE(billing_address_modified_date,  '1753-01-01') 			AS billing_address_modified_date,
    md.max_modified_date                                            AS valid_from,
    TO_DATE('9999-12-31')                                           AS valid_to
FROM raw_data
CROSS JOIN max_dates md
{% if is_incremental() %}
LEFT JOIN max_dates md_incremental 
  ON md_incremental.max_modified_date = COALESCE(raw_data.customer_modified_date,         '1900-01-01')
  OR md_incremental.max_modified_date = COALESCE(raw_data.delivery_address_modified_date, '1900-01-01')
  OR md_incremental.max_modified_date = COALESCE(raw_data.billing_address_modified_date,  '1900-01-01')
{% endif %}



