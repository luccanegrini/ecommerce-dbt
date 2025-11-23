{{ config(
    materialized='incremental',
    unique_key='user_id',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
) }}

WITH recent_users AS (
    {% if is_incremental() %}
        SELECT DISTINCT user_id
        FROM {{ ref('fct_events') }}
        WHERE event_ts >= DATEADD(day, -10, CURRENT_DATE())
    {% else %}
        SELECT DISTINCT user_id
        FROM {{ ref('fct_events') }}
    {% endif %}
),

user_events AS (
    SELECT fe.*
    FROM {{ ref('fct_events') }} fe
    JOIN recent_users ru
      ON fe.user_id = ru.user_id
),

last_campaign AS (
    SELECT
        user_id,
        campaign_id,
        ROW_NUMBER() OVER (
            PARTITION BY user_id
            ORDER BY event_ts DESC
        ) AS rn
    FROM user_events
),

agg AS (
    SELECT
        ue.user_id,
        MIN(ue.event_ts) AS first_event_ts,
        MAX(ue.event_ts) AS last_event_ts,
        COUNT(*) AS total_events,
        SUM(ue.is_purchase) AS total_purchases,
        MAX(CASE WHEN ue.is_purchase = 1 THEN ue.event_ts END) AS last_purchase_ts,
        lc.campaign_id AS last_campaign_id
    FROM user_events ue
    LEFT JOIN last_campaign lc
      ON ue.user_id = lc.user_id AND lc.rn = 1
    GROUP BY ue.user_id, lc.campaign_id
)

SELECT * FROM agg
