{{ config(materialized='table') }}

WITH gold_most_popular_brand AS (
    SELECT
        p.brand,
        COUNTIF(e.event_type = 'cart') AS total_carted,
        COUNTIF(e.event_type = 'view') AS total_views,
        COUNTIF(e.event_type = 'purchase') AS total_purchases,
        SUM(p.price) AS total_revenue
    FROM
        {{ ref('silver_events') }} e
    JOIN
        {{ ref('silver_product') }} p
    ON
        e.product_id = p.product_id
    GROUP BY
        p.brand
)

SELECT
    brand,
    total_carted,
    total_views,
    total_purchases,
    total_revenue
FROM
    gold_most_popular_brand
ORDER BY
    total_purchases DESC
LIMIT 1
