{{ config(
    materialized='incremental',
    unique_key='campaign_id',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
) }}

WITH recent_campaigns AS (
    {% if is_incremental() %}
        SELECT campaign_id
        FROM {{ ref('dim_campaigns') }}
        WHERE last_event_ts >= DATEADD(day, -10, CURRENT_DATE())
    {% else %}
        SELECT campaign_id
        FROM {{ ref('dim_campaigns') }}
    {% endif %}
),

base AS (
    SELECT dc.*
    FROM {{ ref('dim_campaigns') }} dc
    JOIN recent_campaigns rc
      ON dc.campaign_id = rc.campaign_id
)

SELECT
    campaign_id,
    first_event_ts,
    last_event_ts,
    total_events,
    total_views,
    total_add_to_cart,
    total_purchases,
    unique_users,
    total_revenue,
    CASE WHEN total_views > 0 THEN total_add_to_cart::float / total_views ELSE NULL END   AS view_to_cart_rate,
    CASE WHEN total_add_to_cart > 0 THEN total_purchases::float / total_add_to_cart ELSE NULL END AS cart_to_purchase_rate,
    CASE WHEN total_views > 0 THEN total_purchases::float / total_views ELSE NULL END     AS view_to_purchase_rate,
    CASE WHEN total_purchases > 0 THEN total_revenue::float / total_purchases ELSE NULL END AS avg_ticket
FROM base
ORDER BY total_revenue DESC
