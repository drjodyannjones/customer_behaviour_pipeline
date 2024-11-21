{{ config(materialized='table') }}

WITH gold_most_recent_users AS (
    SELECT
        s.user_id,
        MAX(e.event_time) AS latest_event_time,
        ARRAY_AGG(e.event_type ORDER BY e.event_time DESC) AS recent_event_types
    FROM
        {{ ref('silver_events') }} e
    JOIN
        {{ ref('silver_user_session') }} s
    ON
        e.user_session = s.user_session
    GROUP BY
        e.user_session, s.user_id
)

SELECT
    user_id,
    latest_event_time,
    recent_event_types
FROM
    gold_most_recent_users
ORDER BY
    latest_event_time DESC
LIMIT 10
