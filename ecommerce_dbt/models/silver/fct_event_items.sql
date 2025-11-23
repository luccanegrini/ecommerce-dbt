{{ config(
    materialized='incremental',
    unique_key=['event_id', 'product_id'],
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
        inserted_at::timestamp_ntz  AS inserted_at,
        payload:items              AS items_array
    FROM {{ source('bronze', 'events') }}
    {% if is_incremental() %}
      WHERE event_ts >= DATEADD(day, -10, CURRENT_DATE())
    {% endif %}
),

flattened AS (
    SELECT
        e.event_id,
        e.event_type,
        e.event_ts,
        e.user_id,
        e.session_id,
        e.campaign_id,
        e.currency,
        e.inserted_at,
        i.value:product_id::string AS product_id,
        i.value:qty::number        AS qty,
        i.value:price::number      AS price
    FROM src e,
         LATERAL FLATTEN(input => e.items_array) i
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
        product_id,
        qty,
        price,
        qty * price AS gross_revenue,
        inserted_at
    FROM flattened
)

SELECT * FROM final
