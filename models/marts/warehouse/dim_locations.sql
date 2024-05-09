

{{ config(
  materialized='incremental',
  unique_key='_sourcekey',
  tags=['incremental_model']
) }}

WITH
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
        adr.addressid,
        cit.cityid, 
        prv.provinceid,
        cou.countryid,
        cou.continent,
        cou.region,
        cou.subregion,
        cou.country_code,
        cou.country_name,
        cou.Formal_name AS country_formal_name,
        cou.population AS country_population,
        prv.province_code,
        prv.province_name AS province,
        prv.population AS province_population,
        cit.city_name AS city,
        cit.population AS city_population,
        adr.postal_code,
        adr.address_line1,
        adr.address_line2,
        adr.modified_date AS address_modified_date,
        cit.modified_date AS city_modified_date,
        prv.modified_date AS province_modified_date,
        cou.modified_date AS country_modified_date
    FROM
        addresses adr
        FULL JOIN cities cit ON adr.cityid = cit.cityid
        FULL JOIN provinces prv ON cit.provinceid = prv.provinceid
        FULL JOIN countries cou ON prv.countryid = cou.countryid
),
max_dates AS (
    SELECT MAX(t) AS max_modified_date
    FROM (
        SELECT address_modified_date AS t FROM raw_data
        UNION ALL
        SELECT city_modified_date AS t FROM raw_data
        UNION ALL
        SELECT province_modified_date AS t FROM raw_data
        UNION ALL
        SELECT country_modified_date AS t FROM raw_data
    )
)


    SELECT 
		'HSD|' || LEFT(CONCAT_WS('|',
			    COALESCE(CAST(countryid 	AS VARCHAR), '0'),
			    COALESCE(CAST(provinceid 	AS VARCHAR), '0'),
			    COALESCE(CAST(cityid 		AS VARCHAR), '0'),
			    COALESCE(CAST(addressid 	AS VARCHAR), '0')),(30)) AS _sourcekey,

                COALESCE(countryid,  0)         AS _source_countrykey,
			    COALESCE(provinceid, 0)         AS _source_provincekey,
			    COALESCE(cityid, 0)             AS _source_citykey,
			    'HSD'||COALESCE(CAST(addressid  AS VARCHAR(10)), '0')  AS _source_addresskey,


        COALESCE(CAST(continent             AS VARCHAR(200)), 'n/a') AS continent,
        COALESCE(CAST(region   			    AS VARCHAR(200)), 'n/a') AS region,
        COALESCE(CAST(subregion             AS VARCHAR(200)), 'n/a') AS subregion,
        COALESCE(CAST(country_code          AS VARCHAR(200)), 'n/a') AS country_code, 
        COALESCE(CAST(country_name          AS VARCHAR(200)), 'n/a') AS country, 
        COALESCE(CAST(country_formal_name   AS VARCHAR(200)), 'n/a') AS country_formal_name,
        COALESCE(country_population, -1)    AS country_population,
        COALESCE(CAST(province_code         AS VARCHAR(200)), 'n/a') AS province_code,
		COALESCE(CAST(province              AS VARCHAR(200)), 'n/a') AS province,
        COALESCE(province_population, -1)   AS province_population,
        COALESCE(CAST(city                  AS VARCHAR(200)), 'n/a') AS city,
        COALESCE(city_population, -1)       AS city_population,
        COALESCE(CAST(postal_code           AS VARCHAR(200)), 'n/a') AS postal_code,
        COALESCE(CAST(address_line1         AS VARCHAR(200)), 'n/a') AS address_line1,
        COALESCE(CAST(address_line2         AS VARCHAR(200)), 'n/a') AS address_line2,
        COALESCE(address_modified_date,    '1753-01-01')             AS address_modified_date,
        COALESCE(city_modified_date,       '1753-01-01')             AS city_modified_date,
        COALESCE(province_modified_date,   '1753-01-01')             AS province_modified_date,
        COALESCE(country_modified_date,    '1753-01-01')             AS country_modified_date,
        md.max_modified_date AS valid_from,
        TO_DATE('9999-12-31') AS valid_to
    FROM raw_data
    CROSS JOIN max_dates md
{% if is_incremental() %}
LEFT JOIN max_dates md_incremental 
  ON md_incremental.max_modified_date = COALESCE(raw_data.address_modified_date, '1900-01-01')
  OR md_incremental.max_modified_date = COALESCE(raw_data.city_modified_date,     '1900-01-01')
  OR md_incremental.max_modified_date = COALESCE(raw_data.province_modified_date, '1900-01-01')
  OR md_incremental.max_modified_date = COALESCE(raw_data.country_modified_date,  '1900-01-01')
{% endif %}
