-- daily_engagement_matrix.sql
-- Daily event counts per user, bucketed into engagement tiers.
-- Adapted from Heap T1 (all users) and T2 (active audience only).
-- Returns day-level distribution of users across event-count buckets.
--
-- Parameters:
--   {{START_DATE}} - e.g., '2025-01-01'
--   {{DOMAIN}}     - e.g., 'acme.atlan.com'
--
-- Output columns: event_date, bucket, user_count, user_count_active_audience
-- Buckets: '0 events', '1-4 events', '5-9 events', '10-19 events', '20+ events'

WITH user_domains AS (
    SELECT user_id, MAX(domain) AS domain
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES
    WHERE domain IS NOT NULL
    GROUP BY user_id
),

-- All activity events (tracks + pages)
activity AS (
    SELECT
        t.user_id,
        DATE(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', t.TIMESTAMP)) AS event_date
    FROM {{DATABASE}}.{{SCHEMA}}.TRACKS t
    INNER JOIN user_domains ud ON ud.user_id = t.user_id
    WHERE t.TIMESTAMP >= {{START_DATE}}
      AND ud.domain = {{DOMAIN}}
      AND t.event_text NOT IN (
          'atlan_analaytics_aggregateinfo_fetch',
          'api_error_emit', 'api_evaluator_cancelled', 'api_evaluator_succeeded',
          'Experiment Started', '$experiment_started',
          'web_vital_metric_inp_track', 'web_vital_metric_ttfb_track',
          'performance_metric_user_timing_discovery_search',
          'performance_metric_user_timing_app_bootstrap',
          'web_vital_metric_fcp_track', 'web_vital_metric_lcp_track'
      )
      AND t.event_text NOT LIKE 'workflow_%'

    UNION ALL

    SELECT
        p.user_id,
        DATE(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', p.TIMESTAMP)) AS event_date
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES p
    INNER JOIN user_domains ud ON ud.user_id = p.user_id
    WHERE p.TIMESTAMP >= {{START_DATE}}
      AND ud.domain = {{DOMAIN}}
),

-- Derive sessions using time-gap logic for active audience detection
raw_events AS (
    SELECT
        user_id,
        TIMESTAMP,
        DATE(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', TIMESTAMP)) AS event_date,
        LAG(TIMESTAMP) OVER (PARTITION BY user_id ORDER BY TIMESTAMP) AS prev_ts
    FROM (
        SELECT p.user_id, p.TIMESTAMP
        FROM {{DATABASE}}.{{SCHEMA}}.PAGES p
        INNER JOIN user_domains ud ON ud.user_id = p.user_id
        WHERE p.TIMESTAMP >= {{START_DATE}}
          AND ud.domain = {{DOMAIN}}

        UNION ALL

        SELECT t.user_id, t.TIMESTAMP
        FROM {{DATABASE}}.{{SCHEMA}}.TRACKS t
        INNER JOIN user_domains ud ON ud.user_id = t.user_id
        WHERE t.TIMESTAMP >= {{START_DATE}}
          AND ud.domain = {{DOMAIN}}
          AND t.event_text NOT IN (
              'atlan_analaytics_aggregateinfo_fetch',
              'api_error_emit', 'api_evaluator_cancelled', 'api_evaluator_succeeded',
              'Experiment Started', '$experiment_started',
              'web_vital_metric_inp_track', 'web_vital_metric_ttfb_track',
              'performance_metric_user_timing_discovery_search',
              'performance_metric_user_timing_app_bootstrap',
              'web_vital_metric_fcp_track', 'web_vital_metric_lcp_track'
          )
          AND t.event_text NOT LIKE 'workflow_%'
    ) AS combined
),

session_boundaries AS (
    SELECT
        user_id,
        TIMESTAMP,
        event_date,
        CASE
            WHEN prev_ts IS NULL THEN 1
            WHEN DATEDIFF('second', prev_ts, TIMESTAMP) > 1800 THEN 1
            ELSE 0
        END AS is_new_session
    FROM raw_events
),

session_numbered AS (
    SELECT
        user_id,
        event_date,
        SUM(is_new_session) OVER (PARTITION BY user_id ORDER BY TIMESTAMP) AS session_id
    FROM session_boundaries
),

-- Users with a derived session on each day (active audience)
session_users AS (
    SELECT DISTINCT user_id, event_date AS session_date
    FROM session_numbered
),

-- Event counts per user per day
user_day_counts AS (
    SELECT
        a.user_id,
        a.event_date,
        COUNT(*) AS event_count,
        CASE WHEN su.user_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_session
    FROM activity a
    LEFT JOIN session_users su ON su.user_id = a.user_id AND su.session_date = a.event_date
    GROUP BY a.user_id, a.event_date, (su.user_id IS NOT NULL)
),

-- Assign buckets
bucketed AS (
    SELECT
        event_date,
        user_id,
        event_count,
        has_session,
        CASE
            WHEN event_count = 0 THEN '0 events'
            WHEN event_count BETWEEN 1 AND 4 THEN '1-4 events'
            WHEN event_count BETWEEN 5 AND 9 THEN '5-9 events'
            WHEN event_count BETWEEN 10 AND 19 THEN '10-19 events'
            ELSE '20+ events'
        END AS bucket
    FROM user_day_counts
)

SELECT
    event_date,
    bucket,
    COUNT(DISTINCT user_id) AS user_count,
    COUNT(DISTINCT CASE WHEN has_session THEN user_id END) AS user_count_active_audience
FROM bucketed
GROUP BY event_date, bucket
ORDER BY event_date DESC, bucket;
