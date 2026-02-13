-- session_duration_daily.sql
-- Daily session duration analysis (avg/median in seconds).
-- Adapted from Heap T7. Uses time-gap derived sessions (30-min inactivity threshold).
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
        SELECT t.user_id, t.TIMESTAMP
        FROM {{DATABASE}}.{{SCHEMA}}.TRACKS t
        INNER JOIN user_domains ud ON ud.user_id = t.user_id
        WHERE ud.domain = {{DOMAIN}}
          AND t.event_text NOT IN (
              'workflows_run_ended', 'atlan_analaytics_aggregateinfo_fetch',
              'workflow_run_finished', 'workflow_step_finished', 'api_error_emit',
              'api_evaluator_cancelled', 'api_evaluator_succeeded', 'Experiment Started',
              '$experiment_started',
              'web_vital_metric_inp_track', 'web_vital_metric_ttfb_track',
              'performance_metric_user_timing_discovery_search',
              'performance_metric_user_timing_app_bootstrap',
              'web_vital_metric_fcp_track', 'web_vital_metric_lcp_track'
          )
          AND t.TIMESTAMP >= {{START_DATE}}

        UNION ALL

        SELECT p.user_id, p.TIMESTAMP
        FROM {{DATABASE}}.{{SCHEMA}}.PAGES p
        INNER JOIN user_domains ud ON ud.user_id = p.user_id
        WHERE ud.domain = {{DOMAIN}}
          AND p.TIMESTAMP >= {{START_DATE}}
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

-- Aggregate to session level
derived_sessions AS (
    SELECT
        user_id,
        session_id,
        DATE(MIN(event_ts)) AS session_date,
        COUNT(*) AS event_count,
        DATEDIFF('second', MIN(TIMESTAMP), MAX(TIMESTAMP)) AS duration_seconds
    FROM session_numbered
    GROUP BY user_id, session_id
    HAVING COUNT(*) > 1  -- Exclude single-event sessions
)

SELECT
    ds.session_date,
    COUNT(*) AS total_sessions,
    COUNT(DISTINCT ds.user_id) AS unique_users,
    ROUND(AVG(ds.duration_seconds), 1) AS avg_duration_seconds,
    ROUND(MEDIAN(ds.duration_seconds), 1) AS median_duration_seconds,
    ROUND(AVG(ds.event_count), 1) AS avg_events_per_session,
    MAX(ds.duration_seconds) AS max_duration_seconds
FROM derived_sessions ds
WHERE ds.duration_seconds > 0
  AND ds.duration_seconds < 28800  -- Exclude >8hr outliers
GROUP BY ds.session_date
ORDER BY ds.session_date DESC;
