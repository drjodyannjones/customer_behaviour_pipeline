{{ config(materialized='table') }}

WITH gold_high_traffic_days AS (
    SELECT
        DATE(e.event_time) AS event_date,
        COUNT(e.event_id) AS total_events
    FROM
        {{ ref('silver_events') }} e
    GROUP BY
        DATE(e.event_time)
)

SELECT
    event_date,
    total_events
FROM
    gold_high_traffic_days
ORDER BY
    total_events DESC
LIMIT 10
