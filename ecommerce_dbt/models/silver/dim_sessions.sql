{{ config(
    materialized='incremental',
    unique_key='session_id',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
) }}

WITH recent_sessions AS (
    {% if is_incremental() %}
        SELECT DISTINCT session_id
        FROM {{ ref('fct_events') }}
        WHERE event_ts >= DATEADD(day, -10, CURRENT_DATE())
    {% else %}
        SELECT DISTINCT session_id
        FROM {{ ref('fct_events') }}
    {% endif %}
),

session_events AS (
    SELECT fe.*
    FROM {{ ref('fct_events') }} fe
    JOIN recent_sessions rs
      ON fe.session_id = rs.session_id
),

agg AS (
    SELECT
        session_id,
        MIN(event_ts) AS session_start_ts,
        MAX(event_ts) AS session_end_ts,
        DATEDIFF('second',
                 MIN(event_ts),
                 MAX(event_ts)) AS session_duration_seconds,
        COUNT(*) AS events_count,
        SUM(is_view_item) AS views_count,
        SUM(is_add_to_cart) AS add_to_cart_count,
        SUM(is_purchase) AS purchase_count,
        MAX(is_purchase) AS has_purchase,
        ANY_VALUE(user_id) AS user_id,
        ANY_VALUE(campaign_id) AS campaign_id
    FROM session_events
    GROUP BY session_id
)

SELECT * FROM agg
