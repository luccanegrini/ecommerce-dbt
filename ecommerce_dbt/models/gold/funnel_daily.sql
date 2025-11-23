{{ config(
    materialized='incremental',
    unique_key='event_date',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
) }}

WITH base AS (
    SELECT
        event_date,
        COUNT(*)              AS total_events,
        SUM(is_view_item)     AS views,
        SUM(is_add_to_cart)   AS add_to_cart,
        SUM(is_purchase)      AS purchases
    FROM {{ ref('fct_events') }}
    {% if is_incremental() %}
      WHERE event_date >= DATEADD(day, -10, CURRENT_DATE())
    {% endif %}
    GROUP BY event_date
)

SELECT
    event_date,
    total_events,
    views,
    add_to_cart,
    purchases,
    CASE WHEN views > 0 THEN add_to_cart::float / views ELSE NULL END      AS view_to_cart_rate,
    CASE WHEN add_to_cart > 0 THEN purchases::float / add_to_cart ELSE NULL END AS cart_to_purchase_rate,
    CASE WHEN views > 0 THEN purchases::float / views ELSE NULL END        AS view_to_purchase_rate
FROM base
ORDER BY event_date
