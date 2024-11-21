{{ config(materialized='table') }}

WITH user_session AS (
    SELECT
        user_session,
        user_id
    FROM
        {{ source('gcs_cbp_bronze_layer', 'raw_events_oct_2019') }}
    WHERE
        IFNULL(user_session,'') <> ''
)
SELECT DISTINCT
  s.user_session,
  s.user_id
FROM
  user_session s
