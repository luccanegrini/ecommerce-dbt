{{ config(
    materialized='incremental',
    unique_key='product_id',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
) }}

WITH recent_products AS (
    {% if is_incremental() %}
        SELECT DISTINCT product_id
        FROM {{ ref('fct_event_items') }}
        WHERE event_ts >= DATEADD(day, -10, CURRENT_DATE())
    {% else %}
        SELECT DISTINCT product_id
        FROM {{ ref('fct_event_items') }}
    {% endif %}
),

product_events AS (
    SELECT fi.*
    FROM {{ ref('fct_event_items') }} fi
    JOIN recent_products rp
      ON fi.product_id = rp.product_id
),

agg AS (
    SELECT
        product_id,
        MIN(event_ts) AS first_event_ts,
        MAX(event_ts) AS last_event_ts,
        SUM(qty) AS total_qty_sold,
        SUM(gross_revenue) AS total_revenue,
        COUNT(DISTINCT CASE WHEN event_type = 'view_item' THEN event_id END) AS total_views,
        COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN event_id END) AS total_purchases
    FROM product_events
    GROUP BY product_id
)

SELECT * FROM agg
