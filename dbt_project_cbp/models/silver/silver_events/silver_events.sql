{{ config(materialized='view') }}

WITH silver_events AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY event_time) AS row_id,
        user_session,
        event_time,
        event_type,
        product_id
    FROM
        {{ source('gcs_cbp_bronze_layer', 'raw_events_oct_2019') }}
    WHERE
        IFNULL(user_session,'') <> ''
)

SELECT
  md5(CAST(event_time as string) || row_id || user_session) as event_id,
  user_session,
  event_time,
  event_type,
  product_id
FROM
  silver_events
