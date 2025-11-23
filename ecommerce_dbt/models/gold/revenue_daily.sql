{{ config(
    materialized='incremental',
    unique_key='event_date',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
) }}

WITH base AS (
    SELECT
        DATE(event_ts) AS event_date,
        SUM(gross_revenue) AS total_revenue,
        SUM(qty)           AS total_qty
    FROM {{ ref('fct_event_items') }}
    {% if is_incremental() %}
      WHERE event_ts >= DATEADD(day, -10, CURRENT_DATE())
    {% endif %}
    GROUP BY DATE(event_ts)
)

SELECT
    event_date,
    total_revenue,
    total_qty,
    CASE WHEN total_qty > 0 THEN total_revenue::float / total_qty ELSE NULL END AS avg_price
FROM base
ORDER BY event_date
