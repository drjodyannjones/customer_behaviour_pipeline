{{ config(materialized='table') }}

WITH product AS (
    SELECT
        product_id,
        category_id,
        CASE
          WHEN brand IS NULL THEN 'Unknown' ELSE brand END AS brand,
        price,
        -- Extract product_name
        CASE
            WHEN LENGTH(category_code) - LENGTH(REPLACE(category_code, '.', '')) = 2 THEN
                SUBSTRING(
                    category_code,
                    STRPOS(category_code, '.') +
                        STRPOS(SUBSTRING(category_code, STRPOS(category_code, '.') + 1), '.') + 1,
                    LENGTH(category_code)
                )
            WHEN LENGTH(category_code) - LENGTH(REPLACE(category_code, '.', '')) = 1 THEN
                SUBSTRING(category_code, STRPOS(category_code, '.') + 1, LENGTH(category_code) - STRPOS(category_code, '.'))
            ELSE 'Unknown'
        END AS product_name

    FROM
        {{ source('gcs_cbp_bronze_layer', 'raw_events_oct_2019') }}
)

SELECT
  distinct product_id, category_id, product_name, brand, price
FROM
  product
