{{ config(
    materialized='incremental',
    unique_key='campaign_id',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
) }}

WITH recent_campaigns AS (
    {% if is_incremental() %}
        SELECT DISTINCT campaign_id
        FROM {{ ref('fct_events') }}
        WHERE event_ts >= DATEADD(day, -10, CURRENT_DATE())
          AND campaign_id IS NOT NULL
    {% else %}
        SELECT DISTINCT campaign_id
        FROM {{ ref('fct_events') }}
        WHERE campaign_id IS NOT NULL
    {% endif %}
),

campaign_events AS (
    SELECT fe.*
    FROM {{ ref('fct_events') }} fe
    JOIN recent_campaigns rc
      ON fe.campaign_id = rc.campaign_id
),

campaign_items AS (
    SELECT fi.*
    FROM {{ ref('fct_event_items') }} fi
    JOIN recent_campaigns rc
      ON fi.campaign_id = rc.campaign_id
),

agg AS (
    SELECT
        ce.campaign_id,
        MIN(ce.event_ts) AS first_event_ts,
        MAX(ce.event_ts) AS last_event_ts,
        COUNT(*) AS total_events,
        SUM(ce.is_view_item) AS total_views,
        SUM(ce.is_add_to_cart) AS total_add_to_cart,
        SUM(ce.is_purchase) AS total_purchases,
        COUNT(DISTINCT ce.user_id) AS unique_users,
        SUM(ci.gross_revenue) AS total_revenue
    FROM campaign_events ce
    LEFT JOIN campaign_items ci ON ce.event_id = ci.event_id
    GROUP BY ce.campaign_id
)

SELECT * FROM agg
