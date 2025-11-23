{{ config(
    materialized='incremental',
    unique_key='product_id',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
) }}

WITH recent_products AS (
    {% if is_incremental() %}
        SELECT product_id
        FROM {{ ref('dim_products') }}
        WHERE last_event_ts >= DATEADD(day, -10, CURRENT_DATE())
    {% else %}
        SELECT product_id
        FROM {{ ref('dim_products') }}
    {% endif %}
),

base AS (
    SELECT dp.*
    FROM {{ ref('dim_products') }} dp
    JOIN recent_products rp
      ON dp.product_id = rp.product_id
)

SELECT
    product_id,
    first_event_ts,
    last_event_ts,
    total_views,
    total_purchases,
    total_qty_sold,
    total_revenue,
    CASE WHEN total_views > 0 THEN total_purchases::float / total_views ELSE NULL END AS view_to_purchase_rate,
    CASE WHEN total_purchases > 0 THEN total_revenue::float / total_purchases ELSE NULL END AS avg_ticket
FROM base
ORDER BY total_revenue DESC
