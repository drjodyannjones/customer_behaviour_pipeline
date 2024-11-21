{{ config(materialized='table') }}

WITH gold_sessions_per_user AS (
    SELECT
        s.user_id,
        COUNT(DISTINCT s.user_session) AS total_sessions,
        AVG(COUNT(DISTINCT s.user_session)) OVER () AS avg_sessions_per_user
    FROM
        {{ ref('silver_user_session') }} s
    GROUP BY
        s.user_id
)

SELECT
    user_id,
    total_sessions,
    avg_sessions_per_user
FROM
    gold_sessions_per_user
