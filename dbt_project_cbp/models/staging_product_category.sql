{{ config(materialized='table') }}

WITH product_category AS (
    SELECT
        category_id,
        -- Extract product_category
        CASE
            WHEN LENGTH(category_code) - LENGTH(REPLACE(category_code, '.', '')) = 2 THEN
                SUBSTRING(category_code, 1, STRPOS(category_code, '.') - 1)
            WHEN LENGTH(category_code) - LENGTH(REPLACE(category_code, '.', '')) = 1 THEN
                SUBSTRING(category_code, 1, STRPOS(category_code, '.') - 1)
            ELSE 'Unknown'
        END AS product_category,

        -- Extract product_sub_category (only if there are two periods)
        CASE
            WHEN LENGTH(category_code) - LENGTH(REPLACE(category_code, '.', '')) = 2 THEN
                SUBSTRING(
                    category_code,
                    STRPOS(category_code, '.') + 1,
                    STRPOS(SUBSTRING(category_code, STRPOS(category_code, '.') + 1), '.') - 1
                )
            ELSE 'Unknown'
        END AS product_sub_category

    FROM
        {{ source('gcs_cbp_bronze_layer', 'raw_events_oct_2019') }}
)

SELECT
  DISTINCT pc.category_id,
  pc.product_category,
  pc.product_sub_category
FROM
  product_category pc
