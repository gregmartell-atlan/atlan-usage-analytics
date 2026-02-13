-- actions_per_session.sql
-- Average events/pages per session per domain per month.
-- Indicates depth of each visit. Uses time-gap derived sessions (30-min inactivity threshold).
--
-- Parameters:
--   {{START_DATE}} - e.g., '2025-01-01'
--   {{DOMAIN}}     - e.g., 'acme.atlan.com'

WITH user_domains AS (
    SELECT user_id, MAX(domain) AS domain
    FROM {{DATABASE}}.{{SCHEMA}}.PAGES
    WHERE domain IS NOT NULL
    GROUP BY user_id
),

-- Raw events with timestamps per user
raw_events AS (
    SELECT
        user_id,
        TIMESTAMP,
        CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', TIMESTAMP) AS event_ts,
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

-- Mark session boundaries (new session when gap > 30 min or first event)
session_boundaries AS (
    SELECT
        user_id,
        TIMESTAMP,
        event_ts,
        CASE
            WHEN prev_ts IS NULL THEN 1
            WHEN DATEDIFF('second', prev_ts, TIMESTAMP) > 1800 THEN 1
            ELSE 0
        END AS is_new_session
    FROM raw_events
),

-- Assign session IDs using cumulative sum of boundaries
session_numbered AS (
    SELECT
        user_id,
        TIMESTAMP,
        event_ts,
        SUM(is_new_session) OVER (PARTITION BY user_id ORDER BY TIMESTAMP) AS session_id
    FROM session_boundaries
),

-- Aggregate to session level with month
session_stats AS (
    SELECT
        sn.user_id,
        DATE_TRUNC('MONTH', MIN(sn.event_ts)) AS event_month,
        sn.session_id,
        COUNT(*) AS actions_in_session
    FROM session_numbered sn
    GROUP BY sn.user_id, sn.session_id
)

SELECT
    {{DOMAIN}} AS domain,
    event_month,
    COUNT(*) AS total_sessions,
    ROUND(AVG(actions_in_session), 1) AS avg_actions_per_session,
    ROUND(MEDIAN(actions_in_session), 1) AS median_actions_per_session,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY actions_in_session) AS p90_actions
FROM session_stats
GROUP BY event_month
ORDER BY event_month DESC;
