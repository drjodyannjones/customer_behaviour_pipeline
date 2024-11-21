{{ config(materialized='table') }}

WITH gold_most_popular_product AS (
    SELECT
        p.product_name,
        COUNTIF(e.event_type = 'purchase') AS total_purchases,
        SUM(p.price) AS total_revenue
    FROM
        {{ ref('silver_events') }} e
    JOIN
        {{ ref('silver_product') }} p
    ON
        e.product_id = p.product_id
    GROUP BY
        p.product_name
)

SELECT
    product_name,
    total_purchases,
    total_revenue
FROM
    gold_most_popular_product
ORDER BY
    total_purchases DESC, total_revenue DESC
LIMIT 1
