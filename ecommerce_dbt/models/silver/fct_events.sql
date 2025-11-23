{{ config(
    materialized='incremental',
    unique_key='event_id',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
) }}

WITH src AS (
    SELECT
        event_id::string        AS event_id,
        event_type::string      AS event_type,
        event_ts::timestamp_ntz AS event_ts,
        user_id::string         AS user_id,
        session_id::string      AS session_id,
        payload:campaign_id::string AS campaign_id,
        payload:currency::string    AS currency,
        inserted_at::timestamp_ntz  AS inserted_at
    FROM {{ source('bronze', 'events') }}

    {% if is_incremental() %}
      -- reprocessa só os últimos 10 dias
      WHERE event_ts >= DATEADD(day, -10, CURRENT_DATE())
    {% endif %}
),

final AS (
    SELECT
        event_id,
        event_type,
        event_ts,
        DATE(event_ts) AS event_date,
        user_id,
        session_id,
        campaign_id,
        currency,
        inserted_at,
        CASE WHEN event_type = 'view_item'   THEN 1 ELSE 0 END AS is_view_item,
        CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END AS is_add_to_cart,
        CASE WHEN event_type = 'purchase'    THEN 1 ELSE 0 END AS is_purchase
    FROM src
)

SELECT * FROM final
