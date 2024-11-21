{{ config(materialized='table') }}

WITH gold_most_popular_category AS (
    SELECT
        pc.product_category,
        COUNTIF(e.event_type = 'purchase') AS total_purchases,
        SUM(p.price) AS total_revenue
    FROM
        {{ ref('silver_events') }} e
    JOIN
        {{ ref('silver_product') }} p
    ON
        e.product_id = p.product_id
    JOIN
        {{ ref('silver_product_category') }} pc
    ON
        p.category_id = pc.category_id
    GROUP BY
        pc.product_category
)

SELECT
    product_category,
    total_purchases,
    total_revenue
FROM
    gold_most_popular_category
ORDER BY
    total_purchases DESC, total_revenue DESC
LIMIT 1
